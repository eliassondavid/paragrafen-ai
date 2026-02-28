"""Fetch consolidated SFS documents from Riksdagen API and store raw JSON."""

from __future__ import annotations

from datetime import date, datetime, timezone
import json
import logging
from pathlib import Path
import re
import time
from typing import Any

import requests
import yaml

logger = logging.getLogger("paragrafenai.noop")

IKRAFT_KEY = "ikrafttr\u00e4dandedatum"


class FetchError(Exception):
    """Raised when an HTTP request fails after retries."""


class InvalidJsonResponseError(Exception):
    """Raised when an API response cannot be decoded as expected JSON."""


def load_sources_config(config_path: str | Path = "config/sources.yaml") -> dict[str, Any]:
    """Load ingest source config from YAML."""
    path = Path(config_path)
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError("Config root must be a mapping.")
    return data


def normalize_sfs_number(beteckning: str) -> str | None:
    """Normalize SFS number, e.g. 'SFS 1962:700' -> '1962:700'."""
    value = (beteckning or "").strip()
    if not value:
        return None

    value = re.sub(r"(?i)^sfs\.?\s*", "", value).strip()
    value = re.sub(r"\s*:\s*", ":", value)

    match = re.fullmatch(r"(\d{4}):(\d+)", value)
    if match:
        year = match.group(1)
        number = str(int(match.group(2)))
        return f"{year}:{number}"

    return value


def _join_url(base_url: str, endpoint: str) -> str:
    return f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"


def _extract_documents(list_payload: dict[str, Any]) -> list[dict[str, Any]]:
    document_list = list_payload.get("dokumentlista", {})
    if not isinstance(document_list, dict):
        return []

    documents = document_list.get("dokument", [])
    if isinstance(documents, list):
        return [doc for doc in documents if isinstance(doc, dict)]
    if isinstance(documents, dict):
        return [documents]
    return []


def _extract_remaining(list_payload: dict[str, Any]) -> int | None:
    document_list = list_payload.get("dokumentlista", {})
    if not isinstance(document_list, dict):
        return None

    keys = (
        "@\u00e5terst\u00e5ende",
        "@aterstaende",
        "@\u00e5terstaende",
        "@remaining",
    )
    for key in keys:
        raw_value = document_list.get(key)
        if raw_value is None:
            continue
        try:
            return int(raw_value)
        except (TypeError, ValueError):
            return None
    return None


def _first_non_empty(document: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = document.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _extract_ikraft_value(document: dict[str, Any]) -> str | None:
    for key in (IKRAFT_KEY, "ikrafttradandedatum"):
        value = document.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _normalize_iso_date(raw_value: str | None) -> str | None:
    if raw_value is None:
        return None

    value = raw_value.strip()
    if not value or value == "0000-00-00":
        return None

    match = re.match(r"^(\d{4}-\d{2}-\d{2})", value)
    if match:
        try:
            return date.fromisoformat(match.group(1)).isoformat()
        except ValueError:
            return None

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.date().isoformat()
    except ValueError:
        return None


def _sanitize_filename_stem(stem: str) -> str:
    cleaned = stem.replace(":", "-")
    cleaned = re.sub(r"[\\/:*?\"<>|]", "_", cleaned)
    return cleaned or "unknown"


def _is_document_inactive(document: dict[str, Any]) -> bool:
    boolean_keys = (
        "upphavd",
        "upph\u00e4vd",
        "gallrad",
        "inaktiv",
        "upphort",
        "upph\u00f6rt",
    )
    for key in boolean_keys:
        value = document.get(key)
        if isinstance(value, bool):
            if value:
                return True
            continue
        if isinstance(value, (int, float)) and int(value) == 1:
            return True
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"1", "true", "ja", "j", "upphavd", "upph\u00e4vd", "inaktiv", "upphort", "upph\u00f6rt"}:
                return True

    for key in ("status", "forfattningsstatus", "rattstatus"):
        value = document.get(key)
        if isinstance(value, str):
            lowered = value.strip().lower()
            if "upph" in lowered or "inaktiv" in lowered:
                return True

    return False


class SfsFetcher:
    """Fetcher for consolidated SFS documents from Riksdagen API."""

    def __init__(
        self,
        config_path: str | Path = "config/sources.yaml",
        *,
        session: requests.Session | None = None,
    ) -> None:
        self.config = load_sources_config(config_path)

        api_cfg = self.config["riksdagen_api"]
        sfs_cfg = api_cfg["sfs"]
        rate_cfg = self.config.get("rate_limiting", {})
        http_cfg = self.config.get("http", {})
        progress_cfg = self.config.get("progress", {})

        self.base_url = str(api_cfg["base_url"])
        self.list_url = _join_url(self.base_url, str(sfs_cfg["list_endpoint"]))
        self.document_html_template = str(sfs_cfg["document_html_endpoint"])

        self.doktyp = str(sfs_cfg["doktyp"])
        self.utformat = str(sfs_cfg["utformat"])
        self.pagesize = int(sfs_cfg["pagesize"])

        self.output_dir = Path(str(sfs_cfg["output_dir"]))
        self.errors_path = Path(str(sfs_cfg["errors_file"]))
        self.consolidation_source = str(sfs_cfg.get("consolidation_source", "rk"))
        self.only_active = bool(sfs_cfg.get("only_active", False))

        self.delay_between = float(rate_cfg.get("delay_between_requests_s", 1.0))
        self.max_retries = int(rate_cfg.get("max_retries", 3))
        self.retry_backoff_base_s = float(rate_cfg.get("retry_backoff_base_s", 1.0))
        self.timeout = float(rate_cfg.get("request_timeout_s", 30))
        self.log_every = int(progress_cfg.get("log_every_n_documents", 100))

        self.headers = {
            "User-Agent": str(http_cfg.get("user_agent", "paragrafenai-fetcher/0.1")),
            "Accept-Encoding": str(http_cfg.get("accept_encoding", "gzip, deflate")),
        }

        self.session = session if session is not None else requests.Session()

    def _append_error(self, payload: dict[str, Any]) -> None:
        try:
            self.errors_path.parent.mkdir(parents=True, exist_ok=True)
            with self.errors_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except OSError as exc:
            logger.critical("Failed writing errors file %s: %s", self.errors_path, exc)
            raise SystemExit(1) from exc

    def _write_json_file(self, path: Path, payload: dict[str, Any]) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2)
        except OSError as exc:
            logger.critical("Failed writing output file %s: %s", path, exc)
            raise SystemExit(1) from exc

    def _request_with_retry(self, *, url: str, params: dict[str, Any] | None) -> requests.Response:
        last_exc: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(
                    url,
                    headers=self.headers,
                    params=params,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                return response
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
                last_exc = exc
                if attempt >= self.max_retries:
                    break
                time.sleep(self.retry_backoff_base_s * (2 ** (attempt - 1)))

        raise FetchError(str(last_exc) if last_exc else "Unknown request failure")

    def _request_json_with_retry(self, *, url: str, params: dict[str, Any] | None) -> dict[str, Any]:
        last_exc: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(
                    url,
                    headers=self.headers,
                    params=params,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("Response JSON must be a mapping.")
                return payload
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
                last_exc = exc
            except ValueError as exc:
                last_exc = exc

            if attempt < self.max_retries:
                time.sleep(self.retry_backoff_base_s * (2 ** (attempt - 1)))

        if isinstance(last_exc, ValueError):
            raise InvalidJsonResponseError(str(last_exc)) from last_exc
        raise FetchError(str(last_exc) if last_exc else "Unknown JSON request failure")

    def fetch_all(self) -> dict[str, Any]:
        """Fetch all SFS documents from paginated list API and save raw JSON files."""
        started_at = datetime.now(timezone.utc)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.errors_path.parent.mkdir(parents=True, exist_ok=True)

        summary: dict[str, Any] = {
            "saved": 0,
            "skipped_existing": 0,
            "skipped_inactive": 0,
            "errors": 0,
            "processed_documents": 0,
            "pages_fetched": 0,
            "started_at": started_at.isoformat(),
        }

        page = 1
        consecutive_page_failures = 0

        while True:
            params: dict[str, Any] = {
                "doktyp": self.doktyp,
                "utformat": self.utformat,
                "pagesize": self.pagesize,
                "p": page,
            }

            try:
                page_payload = self._request_json_with_retry(url=self.list_url, params=params)
            except (FetchError, InvalidJsonResponseError) as exc:
                summary["errors"] += 1
                self._append_error(
                    {
                        "source": "sfs_list",
                        "page": page,
                        "error": str(exc),
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                consecutive_page_failures += 1
                if consecutive_page_failures >= self.max_retries:
                    break
                page += 1
                if self.delay_between > 0:
                    time.sleep(self.delay_between)
                continue

            summary["pages_fetched"] += 1
            consecutive_page_failures = 0
            documents = _extract_documents(page_payload)

            for document in documents:
                summary["processed_documents"] += 1

                if self.only_active and _is_document_inactive(document):
                    summary["skipped_inactive"] += 1
                    continue

                dok_id = _first_non_empty(document, "dok_id", "id")
                if not dok_id:
                    summary["errors"] += 1
                    self._append_error(
                        {
                            "source": "sfs_document",
                            "page": page,
                            "error": "Missing dok_id.",
                            "fetched_at": datetime.now(timezone.utc).isoformat(),
                        }
                    )
                    continue

                beteckning = _first_non_empty(document, "beteckning")
                titel = _first_non_empty(document, "titel")
                datum_raw = _first_non_empty(document, "datum")
                datum = _normalize_iso_date(datum_raw) or datum_raw

                normalized_sfs_nr = normalize_sfs_number(beteckning)
                if not normalized_sfs_nr:
                    logger.warning("Missing SFS beteckning for dok_id=%s; using dok_id as filename.", dok_id)
                    normalized_sfs_nr = dok_id

                out_stem = _sanitize_filename_stem(normalized_sfs_nr)
                out_path = self.output_dir / f"{out_stem}.json"
                if out_path.exists():
                    summary["skipped_existing"] += 1
                    continue

                html_url = _join_url(self.base_url, self.document_html_template.format(dok_id=dok_id))
                try:
                    html_response = self._request_with_retry(url=html_url, params=None)
                except FetchError as exc:
                    summary["errors"] += 1
                    self._append_error(
                        {
                            "source": "sfs_document_html",
                            "dok_id": dok_id,
                            "sfs_nr": normalized_sfs_nr,
                            "error": str(exc),
                            "fetched_at": datetime.now(timezone.utc).isoformat(),
                        }
                    )
                    continue

                html_content = html_response.text.strip()
                fetched_at = datetime.now(timezone.utc)

                ikraft_raw = _extract_ikraft_value(document)
                ikrafttradedatum = _normalize_iso_date(ikraft_raw)
                if ikrafttradedatum:
                    ikraft_date = date.fromisoformat(ikrafttradedatum)
                    if ikraft_date > fetched_at.date():
                        logger.warning(
                            "Future ikrafttr\u00e4dandedatum for dok_id=%s: %s > %s",
                            dok_id,
                            ikrafttradedatum,
                            fetched_at.date().isoformat(),
                        )

                raw_payload: dict[str, Any] = {
                    "sfs_nr": normalized_sfs_nr,
                    "dok_id": dok_id,
                    "titel": titel,
                    "datum": datum,
                    IKRAFT_KEY: ikrafttradedatum,
                    "consolidation_source": self.consolidation_source,
                    "source_url": html_url,
                    "html_content": html_content,
                    "html_available": bool(html_content),
                    "fetched_at": fetched_at.isoformat(),
                }

                self._write_json_file(out_path, raw_payload)
                summary["saved"] += 1

                if self.log_every > 0 and summary["processed_documents"] % self.log_every == 0:
                    logger.info(
                        "Processed SFS documents=%s saved=%s errors=%s",
                        summary["processed_documents"],
                        summary["saved"],
                        summary["errors"],
                    )

                if self.delay_between > 0:
                    time.sleep(self.delay_between)

            remaining = _extract_remaining(page_payload)
            if remaining == 0:
                break
            if remaining is None and not documents:
                break

            page += 1
            if self.delay_between > 0:
                time.sleep(self.delay_between)

        summary["finished_at"] = datetime.now(timezone.utc).isoformat()
        return summary
