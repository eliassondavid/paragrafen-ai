"""Fetch rskr documents from Riksdagen API and store flat raw JSON per document."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import re
import time
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests
import yaml

logger = logging.getLogger("paragrafenai.noop")

SCHEMA_VERSION = "v0.15"
LICENSE = "public_domain"
DEFAULT_TIMEOUT_S = 30.0
MAX_RETRIES = 3
RETRY_DELAYS_S = (2.0, 4.0, 8.0)


class FetchError(Exception):
    """Raised when an HTTP request fails after retries."""


def load_sources_config(config_path: str | Path = "config/sources.yaml") -> dict[str, Any]:
    """Load ingest source config from YAML."""
    path = Path(config_path)
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError("Config root must be a mapping.")
    return data


def build_existing_dok_ids(raw_dir: Path) -> set[str]:
    """Build a set of existing dok_id values from both dataset and fetched files."""
    existing: set[str] = set()
    for raw_path in raw_dir.glob("*.json"):
        if raw_path.name.startswith("_"):
            continue
        try:
            data = json.loads(raw_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            continue
        if not isinstance(data, dict):
            continue

        dok_id = extract_dok_id(data)
        if dok_id:
            existing.add(dok_id.upper())
    return existing


def build_filename(beteckning: str, dok_id: str) -> str:
    """Build normalized output filename from beteckning or fall back to dok_id."""
    match = re.search(r"(\d{4}[/-]\d{2,4})[:\s]+(\d+)", beteckning or "")
    if match:
        rm_norm = match.group(1).replace("/", "-")
        nummer = match.group(2)
        return f"rskr_{rm_norm}_{nummer}.json"
    return f"rskr_{dok_id.lower()}.json"


def extract_dok_id(raw: dict[str, Any]) -> str:
    """Extract dok_id from flat or nested raw structures."""
    value = raw.get("dok_id")
    if isinstance(value, str) and value.strip():
        return value.strip()

    dokument = (
        raw.get("dokumentstatus", {}).get("dokument", {})
        if isinstance(raw.get("dokumentstatus"), dict)
        else {}
    )
    value = dokument.get("dok_id")
    if isinstance(value, str) and value.strip():
        return value.strip()

    dokument = (
        raw.get("status_json", {}).get("dokumentstatus", {}).get("dokument", {})
        if isinstance(raw.get("status_json"), dict)
        else {}
    )
    value = dokument.get("dok_id")
    if isinstance(value, str) and value.strip():
        return value.strip()

    return ""


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


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


def _extract_document_list(list_payload: dict[str, Any]) -> dict[str, Any]:
    dokumentlista = list_payload.get("dokumentlista", {})
    return dokumentlista if isinstance(dokumentlista, dict) else {}


def _extract_total_hits(list_payload: dict[str, Any]) -> int:
    raw_total = _extract_document_list(list_payload).get("@traffar")
    try:
        return int(raw_total)
    except (TypeError, ValueError):
        return 0


def _extract_remaining(list_payload: dict[str, Any]) -> int | None:
    remaining_keys = (
        "@återstående",
        "@aterstaende",
        "@återstaende",
        "@remaining",
    )
    dokumentlista = _extract_document_list(list_payload)
    for key in remaining_keys:
        raw_value = dokumentlista.get(key)
        if raw_value is None:
            continue
        try:
            return int(raw_value)
        except (TypeError, ValueError):
            return None
    return None


def _extract_next_page(list_payload: dict[str, Any]) -> int | None:
    dokumentlista = _extract_document_list(list_payload)
    next_page = dokumentlista.get("@nasta_sida") or dokumentlista.get("@next_page")
    if not isinstance(next_page, str) or not next_page.strip():
        return None

    parsed = urlparse(next_page)
    raw_page = parse_qs(parsed.query).get("p", [None])[0]
    try:
        return int(raw_page) if raw_page is not None else None
    except (TypeError, ValueError):
        return None


def _join_url(base_url: str, endpoint: str) -> str:
    return f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"


def _normalize_url(base_url: str, url: str) -> str:
    value = (url or "").strip()
    if not value:
        return ""
    if value.startswith("//"):
        return f"https:{value}"
    if value.startswith("/"):
        return _join_url(base_url, value)
    return value


def _request_with_retry(
    session: requests.Session,
    *,
    url: str,
    headers: dict[str, str],
    params: dict[str, Any] | None,
    timeout: float,
) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, headers=headers, params=params, timeout=timeout)
            response.raise_for_status()
            return response
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
            last_exc = exc
            if attempt >= MAX_RETRIES:
                break
            time.sleep(RETRY_DELAYS_S[attempt - 1])
    raise FetchError(str(last_exc) if last_exc else "Unknown request failure")


def _request_json_with_retry(
    session: requests.Session,
    *,
    url: str,
    headers: dict[str, str],
    params: dict[str, Any] | None,
    timeout: float,
) -> dict[str, Any]:
    response = _request_with_retry(
        session,
        url=url,
        headers=headers,
        params=params,
        timeout=timeout,
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise FetchError(str(exc)) from exc
    if not isinstance(payload, dict):
        raise FetchError("Response JSON must be a mapping.")
    return payload


def _coerce_int(value: Any) -> int:
    try:
        return int(str(value).strip())
    except (AttributeError, TypeError, ValueError):
        return 0


def _normalize_beteckning(beteckning: str, rm: str) -> str:
    value = (beteckning or "").strip()
    if re.search(r"(?i)\brskr\b", value):
        return value
    if rm and value:
        return f"rskr {rm}:{value}"
    return value


def _is_html_available(html_content: str) -> bool:
    text = (html_content or "").strip()
    if not text:
        return False
    if text == "HTML saknas":
        return False
    return len(text) >= 100


class RskrFetcher:
    """Fetch rskr list pages and HTML documents into flat raw JSON files."""

    def __init__(
        self,
        config_path: str | Path = "config/sources.yaml",
        *,
        session: requests.Session | None = None,
    ) -> None:
        self.config_path = Path(config_path)
        self.config = load_sources_config(self.config_path)
        api_cfg = self.config.get("riksdagen_api", {})
        rskr_cfg = api_cfg.get("rskr", {})
        http_cfg = self.config.get("http", {})
        rate_cfg = self.config.get("rate_limiting", {})

        self.base_url = str(api_cfg.get("base_url", "https://data.riksdagen.se"))
        self.list_url = _join_url(self.base_url, str(rskr_cfg.get("list_endpoint", "/dokumentlista/")))
        self.output_dir = Path(str(rskr_cfg.get("output_dir", "data/raw/rskr")))
        self.errors_path = self.output_dir / "_errors.jsonl"
        self.timeout = float(rate_cfg.get("request_timeout_s", DEFAULT_TIMEOUT_S))
        self.delay_between = self._resolve_rate_limit(self.config)
        self.headers = {
            "User-Agent": str(http_cfg.get("user_agent", "paragrafenai-fetcher/0.1")),
            "Accept-Encoding": str(http_cfg.get("accept_encoding", "gzip, deflate")),
        }
        self.session = session or requests.Session()

    def _resolve_rate_limit(self, config: dict[str, Any]) -> float:
        riksdagen_cfg = config.get("riksdagen", {})
        if isinstance(riksdagen_cfg, dict):
            raw_value = riksdagen_cfg.get("rate_limit_seconds")
            try:
                return float(raw_value)
            except (TypeError, ValueError):
                pass

        rate_cfg = config.get("rate_limiting", {})
        if isinstance(rate_cfg, dict):
            raw_value = rate_cfg.get("delay_between_requests_s")
            try:
                return float(raw_value)
            except (TypeError, ValueError):
                pass

        return 1.0

    def _sleep_between_requests(self) -> None:
        if self.delay_between > 0:
            time.sleep(self.delay_between)

    def run(self, *, max_docs: int | None = None) -> dict[str, int]:
        """Fetch list pages and document HTML until exhausted or max_docs is reached."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        existing_ids = build_existing_dok_ids(self.output_dir)

        page = 1
        try:
            page_payload = self._fetch_list_page(page)
        except FetchError as exc:
            _append_jsonl(
                self.errors_path,
                {
                    "url": self.list_url,
                    "error": str(exc),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )
            raise

        total_i_api = _extract_total_hits(page_payload)
        logger.info("Riksdagens API: %s rskr (@traffar)", total_i_api)
        logger.info("Befintliga dok_id på disk: %s", len(existing_ids))

        stats = {
            "total_i_api": total_i_api,
            "saved": 0,
            "skipped_existing": 0,
            "failed": 0,
            "processed": 0,
        }

        while True:
            documents = _extract_documents(page_payload)
            if not documents:
                break

            for document in documents:
                if max_docs is not None and stats["processed"] >= max_docs:
                    self._log_final_coverage(total_i_api)
                    return stats

                dok_id = str(document.get("dok_id") or "").strip()
                if not dok_id:
                    stats["failed"] += 1
                    stats["processed"] += 1
                    _append_jsonl(
                        self.errors_path,
                        {
                            "dok_id": "",
                            "url": self.list_url,
                            "error": "Missing dok_id in dokumentlista response.",
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                    self._log_progress(stats, total_i_api)
                    continue

                if dok_id.upper() in existing_ids:
                    stats["skipped_existing"] += 1
                    stats["processed"] += 1
                    self._log_progress(stats, total_i_api)
                    continue

                try:
                    payload = self._build_raw_payload(document)
                    filename = build_filename(payload["beteckning"], payload["dok_id"])
                    _write_json_file(self.output_dir / filename, payload)
                    existing_ids.add(payload["dok_id"].upper())
                    stats["saved"] += 1
                except (FetchError, OSError, TypeError, ValueError) as exc:
                    stats["failed"] += 1
                    _append_jsonl(
                        self.errors_path,
                        {
                            "dok_id": dok_id,
                            "url": self._document_url(dok_id),
                            "error": str(exc),
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                finally:
                    stats["processed"] += 1
                    self._log_progress(stats, total_i_api)

            remaining = _extract_remaining(page_payload)
            if remaining == 0:
                break

            next_page = _extract_next_page(page_payload)
            if next_page is None or next_page <= page:
                break

            page = next_page
            try:
                page_payload = self._fetch_list_page(page)
            except FetchError as exc:
                stats["failed"] += 1
                _append_jsonl(
                    self.errors_path,
                    {
                        "url": self.list_url,
                        "error": f"Failed to fetch list page {page}: {exc}",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    },
                )
                break

        self._log_final_coverage(total_i_api)
        return stats

    def _fetch_list_page(self, page: int) -> dict[str, Any]:
        params: dict[str, Any] = {
            "doktyp": "rskr",
            "utformat": "json",
            "sz": 200,
            "p": page,
        }
        payload = _request_json_with_retry(
            self.session,
            url=self.list_url,
            headers=self.headers,
            params=params,
            timeout=self.timeout,
        )
        self._sleep_between_requests()
        return payload

    def _document_url(self, dok_id: str) -> str:
        return f"{self.base_url.rstrip('/')}/dokument/{dok_id}"

    def _build_raw_payload(self, document: dict[str, Any]) -> dict[str, Any]:
        dok_id = str(document.get("dok_id") or "").strip()
        rm = str(document.get("rm") or "").strip()
        beteckning_raw = str(document.get("beteckning") or "").strip()
        beteckning = _normalize_beteckning(beteckning_raw, rm)
        source_url = self._document_url(dok_id)
        html_url = _normalize_url(
            self.base_url,
            str(document.get("dokument_url_html") or f"//data.riksdagen.se/dokument/{dok_id}.html"),
        )
        html_content = self._fetch_html(html_url)
        html_available = _is_html_available(html_content)

        if not html_available and html_content.strip() == "HTML saknas":
            logger.warning("HTML saknas for rskr dok_id=%s", dok_id)

        nummer = _coerce_int(beteckning_raw)
        if nummer <= 0:
            match = re.search(r":\s*(\d+)\s*$", beteckning)
            if match:
                nummer = _coerce_int(match.group(1))

        return {
            "beteckning": beteckning,
            "dok_id": dok_id,
            "rm": rm,
            "nummer": nummer,
            "titel": str(document.get("titel") or "").strip(),
            "datum": str(document.get("datum") or "").strip()[:10],
            "organ": str(document.get("organ") or "").strip(),
            "source_url": source_url,
            "dokument_url_html": source_url,
            "html_content": html_content,
            "html_available": html_available,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": SCHEMA_VERSION,
            "license": LICENSE,
        }

    def _fetch_html(self, html_url: str) -> str:
        response = _request_with_retry(
            self.session,
            url=html_url,
            headers=self.headers,
            params=None,
            timeout=self.timeout,
        )
        self._sleep_between_requests()
        return response.text.strip()

    def _log_progress(self, stats: dict[str, int], total_i_api: int) -> None:
        processed = stats["processed"]
        if processed == 0 or processed % 500 != 0:
            return
        pct = (processed / total_i_api * 100.0) if total_i_api else 0.0
        logger.info(
            "Fetchat %s/%s (%.1f%%) — %s skippade",
            processed,
            total_i_api,
            pct,
            stats["skipped_existing"],
        )

    def _log_final_coverage(self, total_i_api: int) -> None:
        filer_pa_disk = len(list(self.output_dir.glob("*.json")))
        logger.info("Filer på disk efter fetch: %s", filer_pa_disk)
        logger.info("Diff mot API: %s", total_i_api - filer_pa_disk)


def fetch_rskr_documents(
    config_path: str | Path = "config/sources.yaml",
    *,
    max_docs: int | None = None,
    session: requests.Session | None = None,
) -> dict[str, int]:
    """Convenience wrapper used by tests and CLI."""
    fetcher = RskrFetcher(config_path=config_path, session=session)
    return fetcher.run(max_docs=max_docs)


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Fetch riksdagsskrivelser from Riksdagen API")
    parser.add_argument("--max-docs", type=int, default=None)
    parser.add_argument("--config", default="config/sources.yaml")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s:%(name)s:%(message)s",
    )

    try:
        stats = fetch_rskr_documents(args.config, max_docs=args.max_docs)
    except (FetchError, OSError, ValueError) as exc:
        logger.error("Rskr fetch failed: %s", exc)
        return 1

    logger.info(
        "Rskr fetch clear: saved=%s skipped=%s failed=%s",
        stats["saved"],
        stats["skipped_existing"],
        stats["failed"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
