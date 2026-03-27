"""Shared fetch primitives for forarbete pipelines."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path
import time
from typing import Any

import requests
import yaml

logger = logging.getLogger("paragrafenai.noop")

MIN_DELAY_BETWEEN_REQUESTS_S = 0.2


class FetchError(Exception):
    """Raised when an HTTP request fails after retries."""


@dataclass
class FetchResult:
    fetched: int = 0
    skipped: int = 0
    errors: int = 0
    dry_run: bool = False


@dataclass
class RawDocument:
    dok_id: str
    filename: str
    metadata: dict[str, Any]
    status_json: dict[str, Any]
    html_content: str
    html_available: bool
    content_hash: str
    fetched_at: str


class ForarbeteFetcher(ABC):
    """Bas-fetcher för riksdagens API. Subklassas av adapters."""

    def __init__(
        self,
        config_path: str | Path = "config/sources.yaml",
        *,
        checkpoint_dir: str | Path | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.config_path = self._resolve_path(config_path)
        self.config = self._load_config(self.config_path)

        api_cfg = self.config.get("riksdagen_api", {})
        if not isinstance(api_cfg, dict):
            raise ValueError("Config saknar giltig riksdagen_api-sektion.")

        self.doktyp = str(self.get_doktyp()).strip().lower()
        source_cfg = api_cfg.get(self.doktyp, {})
        if not isinstance(source_cfg, dict):
            raise ValueError(f"Config saknar riksdagen_api.{self.doktyp}.")

        rate_cfg = self.config.get("rate_limiting", {})
        http_cfg = self.config.get("http", {})
        progress_cfg = self.config.get("progress", {})

        self.base_url = str(api_cfg.get("base_url", "")).strip()
        self.list_url = self._join_url(
            self.base_url,
            str(source_cfg.get("list_endpoint", "/dokumentlista/")),
        )
        self.status_url_template = self._join_url(
            self.base_url,
            "/dokumentstatus/{dok_id}.json",
        )
        self.html_endpoint_template = str(
            source_cfg.get("document_html_endpoint", "/dokument/{dok_id}")
        )

        self.output_dir = self._resolve_path(self.get_output_dir())

        default_checkpoint_dir = self.repo_root / "data" / "state" / "checkpoints"
        self.checkpoint_dir = self._resolve_path(checkpoint_dir or default_checkpoint_dir)
        self.errors_path = self._resolve_path(
            source_cfg.get("errors_file", self.output_dir / f"{self.doktyp}_errors.jsonl")
        )
        self.skip_list_path = self.output_dir / "_skip_list.jsonl"

        self.pagesize = int(source_cfg.get("pagesize", 200))
        self.utformat = str(source_cfg.get("utformat", "json"))
        self.delay_between = max(
            float(rate_cfg.get("delay_between_requests_s", 1.0)),
            MIN_DELAY_BETWEEN_REQUESTS_S,
        )
        self.max_retries = int(rate_cfg.get("max_retries", 3))
        self.retry_backoff_base_s = float(rate_cfg.get("retry_backoff_base_s", 1.0))
        self.timeout = float(rate_cfg.get("request_timeout_s", 30))
        self.log_every = max(int(progress_cfg.get("log_every_n_documents", 100)), 1)
        self.headers = {
            "User-Agent": str(http_cfg.get("user_agent", "paragrafenai-fetcher/0.1")),
            "Accept-Encoding": str(http_cfg.get("accept_encoding", "gzip, deflate")),
        }
        self.session = session or requests.Session()
        self._last_request_monotonic = 0.0

    @abstractmethod
    def get_doktyp(self) -> str:
        """Return the source-specific doktyp."""

    @abstractmethod
    def get_output_dir(self) -> Path:
        """Return the raw output directory for this adapter."""

    @abstractmethod
    def build_filename(self, document: dict[str, Any]) -> str | None:
        """Build a normalized filename stem."""

    @abstractmethod
    def should_skip(self, document: dict[str, Any]) -> tuple[bool, str]:
        """Return whether a document should be skipped and why."""

    def fetch_all(
        self,
        *,
        dry_run: bool = False,
        riksmote: str | None = None,
        incremental: bool = False,
        max_docs: int | None = None,
    ) -> FetchResult:
        """
        Paginera genom dokumentlistan och hämta rådata.

        - dry_run=True: räkna utan att spara
        - riksmote: filtrera på riksmöte (t.ex. "2024/25")
        - incremental=True: hämta bara dokument med datum > checkpoint.last_observed_date
        - max_docs: begränsa antalet dokument som faktiskt hämtas/räknas
        """
        result = FetchResult(dry_run=dry_run)
        checkpoint = self.read_checkpoint()
        checkpoint_date = str(checkpoint.get("last_observed_date") or "").strip()
        latest_observed = checkpoint_date
        page = 1
        selected_documents = 0

        if max_docs is not None and max_docs <= 0:
            return result

        if not dry_run:
            self.output_dir.mkdir(parents=True, exist_ok=True)

        while True:
            params: dict[str, Any] = {
                "doktyp": self.doktyp,
                "utformat": self.utformat,
                "pagesize": self.pagesize,
                "p": page,
            }
            if riksmote:
                params["rm"] = riksmote

            try:
                page_payload = self._request_json_with_retry(
                    url=self.list_url,
                    params=params,
                )
            except FetchError as exc:
                logger.error("Kunde inte hämta dokumentlista för %s sida %s: %s", self.doktyp, page, exc)
                if not dry_run:
                    self._append_jsonl(
                        self.errors_path,
                        {
                            "source": f"{self.doktyp}_list",
                            "page": page,
                            "error": str(exc),
                            "fetched_at": self._now_iso(),
                        },
                    )
                result.errors += 1
                break

            document_list = page_payload.get("dokumentlista", {})
            if not isinstance(document_list, dict):
                document_list = {}

            documents = self._extract_documents(page_payload)
            stop_after_page = False
            for document in documents:
                if max_docs is not None and selected_documents >= max_docs:
                    stop_after_page = True
                    break

                if riksmote and not self._matches_riksmote(document, riksmote):
                    result.skipped += 1
                    continue

                observed_date = self._extract_observed_date(document)
                if observed_date and self._is_newer(observed_date, latest_observed):
                    latest_observed = observed_date

                if incremental and checkpoint_date and not self._is_newer(observed_date, checkpoint_date):
                    result.skipped += 1
                    continue

                skip, reason = self.should_skip(document)
                if skip:
                    result.skipped += 1
                    if not dry_run:
                        self._append_jsonl(
                            self.skip_list_path,
                            {
                                "dok_id": self._first_non_empty(document, "dok_id", "id"),
                                "reason": reason,
                                "skipped_at": self._now_iso(),
                            },
                        )
                    continue

                filename = self.build_filename(document)
                if not filename:
                    logger.warning("Kunde inte skapa filnamn för %s-dokument.", self.doktyp)
                    result.errors += 1
                    continue

                out_path = self.output_dir / f"{filename}.json"
                if out_path.exists() and not incremental:
                    result.skipped += 1
                    continue

                selected_documents += 1
                if dry_run:
                    result.fetched += 1
                    continue

                try:
                    raw_document = self._fetch_document_from_metadata(document, filename=filename)
                except FetchError as exc:
                    logger.error("Kunde inte hämta %s/%s: %s", self.doktyp, filename, exc)
                    self._append_jsonl(
                        self.errors_path,
                        {
                            "source": f"{self.doktyp}_document",
                            "dok_id": self._first_non_empty(document, "dok_id", "id"),
                            "filename": filename,
                            "error": str(exc),
                            "fetched_at": self._now_iso(),
                        },
                    )
                    result.errors += 1
                    continue

                if raw_document is None:
                    result.errors += 1
                    continue

                if self._write_json(out_path, asdict(raw_document)):
                    result.fetched += 1
                    if result.fetched % self.log_every == 0:
                        logger.info("Hämtade %s %s-dokument.", result.fetched, self.doktyp)
                else:
                    result.errors += 1

            if stop_after_page:
                break

            current_page = self._extract_page_number(document_list, "@sida", "@page")
            total_pages = self._extract_page_number(document_list, "@sidor", "@pages")
            if current_page is not None and total_pages is not None and current_page >= total_pages:
                break

            remaining = self._extract_remaining(page_payload)
            if remaining == 0:
                break
            if remaining is None and not documents:
                break
            page += 1

        if not dry_run:
            checkpoint["document_type"] = self.doktyp
            if incremental:
                checkpoint["last_incremental_run"] = self._now_iso()
            else:
                checkpoint["last_full_run"] = self._now_iso()
            checkpoint["last_observed_date"] = latest_observed or checkpoint.get("last_observed_date")
            checkpoint["total_documents_fetched"] = int(
                checkpoint.get("total_documents_fetched", 0)
            ) + result.fetched
            self.write_checkpoint(checkpoint)

        return result

    def fetch_document(self, dok_id: str) -> RawDocument | None:
        """Hämta ett enskilt dokument (JSON + HTML)."""
        if not dok_id.strip():
            return None

        try:
            status_json = self._request_json_with_retry(
                url=self.status_url_template.format(dok_id=dok_id),
                params=None,
            )
        except FetchError as exc:
            logger.error("Kunde inte hämta dokumentstatus för %s: %s", dok_id, exc)
            return None

        document = self._extract_status_document(status_json)
        if not document:
            logger.warning("Dokumentstatus saknar dokumentblock för %s.", dok_id)
            return None

        filename = self.build_filename(document)
        if not filename:
            logger.warning("Kunde inte skapa filnamn för dok_id=%s.", dok_id)
            return None

        try:
            return self._fetch_document_from_metadata(document, filename=filename, status_json=status_json)
        except FetchError as exc:
            logger.error("Kunde inte hämta dokumentinnehåll för %s: %s", dok_id, exc)
            return None

    def read_checkpoint(self) -> dict[str, Any]:
        """Läs data/state/checkpoints/checkpoint_{doktyp}.yaml."""
        checkpoint_path = self._checkpoint_path()
        if not checkpoint_path.exists():
            return self._default_checkpoint()
        try:
            with checkpoint_path.open("r", encoding="utf-8") as fh:
                payload = yaml.safe_load(fh) or {}
        except (OSError, yaml.YAMLError) as exc:
            logger.warning("Kunde inte läsa checkpoint %s: %s", checkpoint_path, exc)
            return self._default_checkpoint()

        if not isinstance(payload, dict):
            return self._default_checkpoint()
        return {**self._default_checkpoint(), **payload}

    def write_checkpoint(self, data: dict[str, Any]) -> None:
        """Skriv checkpoint efter körning."""
        checkpoint_path = self._checkpoint_path()
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {**self._default_checkpoint(), **data}
        try:
            with checkpoint_path.open("w", encoding="utf-8") as fh:
                yaml.safe_dump(payload, fh, allow_unicode=True, sort_keys=False)
        except (OSError, yaml.YAMLError) as exc:
            logger.error("Kunde inte skriva checkpoint %s: %s", checkpoint_path, exc)

    def _fetch_document_from_metadata(
        self,
        document: dict[str, Any],
        *,
        filename: str,
        status_json: dict[str, Any] | None = None,
    ) -> RawDocument | None:
        dok_id = self._first_non_empty(document, "dok_id", "id")
        if not dok_id:
            logger.warning("Dokument saknar dok_id.")
            return None

        status_payload = status_json
        if status_payload is None:
            status_payload = self._request_json_with_retry(
                url=self.status_url_template.format(dok_id=dok_id),
                params=None,
            )

        html_content = ""
        html_available = False
        html_url = self._resolve_html_url(document, dok_id=dok_id)
        text_url = self._normalize_document_url(
            self._first_non_empty(document, "dokument_url_text", "fil_url", "filUrl")
        )

        if html_url:
            try:
                response = self._request_with_retry(url=html_url, params=None)
                candidate = response.text.strip()
                if candidate:
                    html_content = candidate
                    html_available = True
            except FetchError as exc:
                logger.warning("HTML-hämtning misslyckades för %s: %s", dok_id, exc)

        if not html_available and text_url:
            try:
                response = self._request_with_retry(url=text_url, params=None)
                candidate = response.text.strip()
                if candidate:
                    html_content = candidate
                    html_available = True
            except FetchError as exc:
                logger.warning("Fallback-hämtning misslyckades för %s: %s", dok_id, exc)

        fetched_at = self._now_iso()
        return RawDocument(
            dok_id=dok_id,
            filename=filename,
            metadata=document,
            status_json=status_payload,
            html_content=html_content,
            html_available=html_available,
            content_hash=f"sha256:{hashlib.sha256(html_content.encode('utf-8')).hexdigest()}",
            fetched_at=fetched_at,
        )

    def _load_config(self, config_path: Path) -> dict[str, Any]:
        try:
            with config_path.open("r", encoding="utf-8") as fh:
                payload = yaml.safe_load(fh) or {}
        except (OSError, yaml.YAMLError) as exc:
            raise ValueError(f"Kunde inte läsa config {config_path}: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError("Config-roten måste vara en mapping.")
        return payload

    def _checkpoint_path(self) -> Path:
        return self.checkpoint_dir / f"checkpoint_{self.doktyp}.yaml"

    def _default_checkpoint(self) -> dict[str, Any]:
        return {
            "document_type": self.doktyp,
            "last_full_run": None,
            "last_incremental_run": None,
            "last_observed_date": None,
            "total_documents_fetched": 0,
            "total_documents_normalized": 0,
            "total_chunks_indexed": 0,
            "notes": "",
        }

    def _write_json(self, path: Path, payload: dict[str, Any]) -> bool:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2)
        except OSError as exc:
            logger.error("Kunde inte skriva råfil %s: %s", path, exc)
            return False
        return True

    def _append_jsonl(self, path: Path, payload: dict[str, Any]) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except OSError as exc:
            logger.error("Kunde inte skriva jsonl %s: %s", path, exc)

    def _extract_documents(self, list_payload: dict[str, Any]) -> list[dict[str, Any]]:
        document_list = list_payload.get("dokumentlista", {})
        if not isinstance(document_list, dict):
            return []
        documents = document_list.get("dokument", [])
        if isinstance(documents, list):
            return [document for document in documents if isinstance(document, dict)]
        if isinstance(documents, dict):
            return [documents]
        return []

    def _extract_remaining(self, list_payload: dict[str, Any]) -> int | None:
        document_list = list_payload.get("dokumentlista", {})
        if not isinstance(document_list, dict):
            return None
        for key in ("@återstående", "@aterstaende", "@återstaende", "@remaining"):
            raw_value = document_list.get(key)
            if raw_value is None:
                continue
            try:
                return int(raw_value)
            except (TypeError, ValueError):
                return None
        return None

    def _extract_page_number(self, document_list: dict[str, Any], *keys: str) -> int | None:
        for key in keys:
            raw_value = document_list.get(key)
            if raw_value is None:
                continue
            try:
                return int(raw_value)
            except (TypeError, ValueError):
                return None
        return None

    def _first_non_empty(self, document: dict[str, Any], *keys: str) -> str:
        for key in keys:
            value = document.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    def _matches_riksmote(self, document: dict[str, Any], riksmote: str) -> bool:
        candidate = self._first_non_empty(document, "rm", "riksmote")
        if not candidate:
            return False
        return candidate.replace("-", "/") == riksmote.replace("-", "/")

    def _extract_observed_date(self, document: dict[str, Any]) -> str:
        return self._first_non_empty(
            document,
            "datum",
            "publicerad",
            "systemdatum",
            "uppdaterad",
        )

    def _extract_status_document(self, status_json: dict[str, Any]) -> dict[str, Any]:
        status_block = status_json.get("dokumentstatus", {})
        if not isinstance(status_block, dict):
            return {}
        document = status_block.get("dokument", {})
        if isinstance(document, dict):
            return document
        return {}

    def _resolve_html_url(self, document: dict[str, Any], *, dok_id: str) -> str:
        html_url = self._normalize_document_url(
            self._first_non_empty(document, "dokument_url_html", "url")
        )
        if html_url:
            return html_url
        if not dok_id:
            return ""
        return self._join_url(
            self.base_url,
            self.html_endpoint_template.format(dok_id=dok_id),
        )

    def _join_url(self, base_url: str, endpoint: str) -> str:
        return f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"

    def _normalize_document_url(self, url: str) -> str:
        value = (url or "").strip()
        if not value:
            return ""
        if value.startswith("//"):
            return f"https:{value}"
        if value.startswith("/"):
            return self._join_url(self.base_url, value)
        return value

    def _resolve_path(self, value: str | Path) -> Path:
        path = Path(value)
        if path.is_absolute():
            return path
        return self.repo_root / path

    def _respect_rate_limit(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_monotonic
        if elapsed < self.delay_between:
            time.sleep(self.delay_between - elapsed)

    def _request_with_retry(
        self,
        *,
        url: str,
        params: dict[str, Any] | None,
    ) -> requests.Response:
        last_exc: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                self._respect_rate_limit()
                response = self.session.get(
                    url,
                    headers=self.headers,
                    params=params,
                    timeout=self.timeout,
                )
                self._last_request_monotonic = time.monotonic()
                response.raise_for_status()
                return response
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
                last_exc = exc
                if attempt >= self.max_retries:
                    break
                time.sleep(self.retry_backoff_base_s * (2 ** (attempt - 1)))
        raise FetchError(str(last_exc) if last_exc else "Unknown request failure")

    def _request_json_with_retry(
        self,
        *,
        url: str,
        params: dict[str, Any] | None,
    ) -> dict[str, Any]:
        response = self._request_with_retry(url=url, params=params)
        try:
            payload = response.json()
        except ValueError as exc:
            raise FetchError(str(exc)) from exc
        if not isinstance(payload, dict):
            raise FetchError("Response JSON must be a mapping.")
        return payload

    def _is_newer(self, value: str, baseline: str) -> bool:
        if not value:
            return False
        if not baseline:
            return True
        value_dt = self._coerce_datetime(value)
        baseline_dt = self._coerce_datetime(baseline)
        if value_dt and baseline_dt:
            return value_dt > baseline_dt
        return value > baseline

    def _coerce_datetime(self, value: str) -> datetime | None:
        candidate = (value or "").strip()
        if not candidate:
            return None

        normalized = candidate.replace("Z", "+00:00")
        for parser in (
            lambda: datetime.fromisoformat(normalized),
            lambda: datetime.strptime(candidate[:10], "%Y-%m-%d"),
            lambda: datetime.strptime(candidate[:19], "%Y-%m-%d %H:%M:%S"),
        ):
            try:
                return parser()
            except ValueError:
                continue
        return None

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()


__all__ = [
    "FetchError",
    "FetchResult",
    "ForarbeteFetcher",
    "RawDocument",
]
