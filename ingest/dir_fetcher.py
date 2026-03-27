"""Fetch kommittedirektiv from Riksdagen API and store one flat JSON file per document."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import re
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "data/raw/dir"
ERRORS_PATH = OUTPUT_DIR / "_errors.jsonl"

LIST_URL = "https://data.riksdagen.se/dokumentlista/"
HTML_URL_TEMPLATE = "https://data.riksdagen.se/dokument/{dok_id}.html"
STATUS_URL_TEMPLATE = "https://data.riksdagen.se/dokumentstatus/{dok_id}.json"

LIST_PAGE_SIZE = 200
RATE_LIMIT_SECONDS = 2.0
REQUEST_TIMEOUT_SECONDS = 30.0
RETRY_DELAYS_SECONDS = (5.0, 15.0, 45.0)
LOG_EVERY_N_DOCUMENTS = 100

SCHEMA_VERSION = "v0.15"
LICENSE = "public_domain"

DIR_BETECKNING_RE = re.compile(r"(?i)^\s*dir\.?\s*(\d{4})\s*:\s*(\d+)\s*$")


class FetchError(Exception):
    """Raised when an HTTP request fails after retries."""


class NotFoundError(FetchError):
    """Raised when an endpoint responds with HTTP 404."""


class RateLimitedClient:
    """Requests wrapper that enforces a minimum delay between outbound calls."""

    def __init__(
        self,
        session: requests.Session | None = None,
        *,
        min_interval_seconds: float = RATE_LIMIT_SECONDS,
    ) -> None:
        self.session = session or requests.Session()
        self.min_interval_seconds = min_interval_seconds
        self._last_request_started_at: float | None = None

    def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        timeout: float = REQUEST_TIMEOUT_SECONDS,
        accept: str = "application/json",
    ) -> requests.Response:
        if self._last_request_started_at is not None:
            elapsed = time.monotonic() - self._last_request_started_at
            if elapsed < self.min_interval_seconds:
                time.sleep(self.min_interval_seconds - elapsed)
        self._last_request_started_at = time.monotonic()
        response = self.session.get(
            url,
            params=params,
            timeout=timeout,
            headers={
                "User-Agent": "paragrafenai-dir-fetcher/0.15",
                "Accept": accept,
                "Accept-Encoding": "gzip, deflate",
            },
        )
        response.raise_for_status()
        return response


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_logging() -> None:
    if logging.getLogger().handlers:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _append_error(errors_path: Path, payload: dict[str, Any]) -> None:
    errors_path.parent.mkdir(parents=True, exist_ok=True)
    with errors_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def _read_json_file(path: Path) -> dict[str, Any] | None:
    try:
        with path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Kunde inte läsa %s vid skip-indexering: %s", path, exc)
        return None
    if not isinstance(raw, dict):
        logger.warning("JSON-roten i %s är inte ett objekt", path)
        return None
    return raw


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


def _parse_int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _extract_remaining(list_payload: dict[str, Any]) -> int | None:
    document_list = list_payload.get("dokumentlista", {})
    if not isinstance(document_list, dict):
        return None

    for key in ("@återstående", "@aterstaende", "@återstaende", "@remaining"):
        remaining = _parse_int(document_list.get(key))
        if remaining is not None:
            return remaining

    total_hits = _parse_int(document_list.get("@traffar"))
    current_end = _parse_int(document_list.get("@traff_till"))
    if total_hits is not None and current_end is not None:
        return max(total_hits - current_end, 0)
    return None


def _extract_total_hits(list_payload: dict[str, Any]) -> int | None:
    document_list = list_payload.get("dokumentlista", {})
    if not isinstance(document_list, dict):
        return None
    return _parse_int(document_list.get("@traffar"))


def _extract_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _first_non_empty(document: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = _extract_text(document.get(key))
        if value:
            return value
    return ""


def normalize_rm(rm: str) -> str:
    return rm.replace("/", "_").strip()


def parse_dir_identifier(
    *,
    beteckning: str,
    rm: str,
    nummer: str,
    dok_id: str,
) -> tuple[str, int] | None:
    match = DIR_BETECKNING_RE.match(beteckning)
    if match:
        return normalize_rm(match.group(1)), int(match.group(2))

    if beteckning.isdigit() and rm:
        return normalize_rm(rm), int(beteckning)

    if nummer.isdigit() and rm:
        return normalize_rm(rm), int(nummer)

    return None


def normalize_beteckning(
    *,
    beteckning: str,
    rm: str,
    nummer: str,
) -> str:
    match = DIR_BETECKNING_RE.match(beteckning)
    if match:
        return f"Dir. {match.group(1)}:{int(match.group(2))}"

    if rm and (beteckning.isdigit() or nummer.isdigit()):
        number = int(beteckning) if beteckning.isdigit() else int(nummer)
        return f"Dir. {rm}:{number}"

    return beteckning


def build_output_filename(document: dict[str, Any]) -> str:
    beteckning = _first_non_empty(document, "beteckning")
    rm = _first_non_empty(document, "rm")
    nummer = _first_non_empty(document, "nummer")
    dok_id = _first_non_empty(document, "dok_id", "id").lower()

    parsed = parse_dir_identifier(
        beteckning=beteckning,
        rm=rm,
        nummer=nummer,
        dok_id=dok_id,
    )
    if parsed:
        rm_norm, number = parsed
        return f"dir_{rm_norm}_{number}.json"
    if dok_id:
        return f"dir_{dok_id}.json"
    raise ValueError("Could not derive filename from dir document.")


def extract_existing_dok_id(raw: dict[str, Any]) -> str:
    return _extract_text(
        raw.get("dok_id")
        or raw.get("dokumentstatus", {}).get("dokumentstatus", {}).get("dokument", {}).get("dok_id", "")
    )


def build_existing_ids(output_dir: Path) -> set[str]:
    existing_ids: set[str] = set()
    if not output_dir.exists():
        return existing_ids

    for path in sorted(output_dir.glob("*.json")):
        raw = _read_json_file(path)
        if raw is None:
            continue
        dok_id = extract_existing_dok_id(raw)
        if dok_id:
            existing_ids.add(dok_id)
    return existing_ids


def count_raw_document_files(output_dir: Path) -> int:
    return sum(1 for path in output_dir.glob("*.json") if path.is_file())


def _is_404_error(exc: requests.HTTPError) -> bool:
    response = exc.response
    return response is not None and response.status_code == 404


def request_json_with_retry(
    client: RateLimitedClient,
    *,
    url: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt, delay in enumerate(RETRY_DELAYS_SECONDS, start=1):
        try:
            payload = client.get(url, params=params).json()
            if not isinstance(payload, dict):
                raise ValueError("Response JSON root must be an object.")
            return payload
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError, ValueError) as exc:
            if isinstance(exc, requests.HTTPError) and _is_404_error(exc):
                raise NotFoundError("404 Not Found") from exc
            last_error = exc
            if attempt == len(RETRY_DELAYS_SECONDS):
                break
            logger.warning("Retry %s/%s för %s efter fel: %s", attempt, len(RETRY_DELAYS_SECONDS), url, exc)
            time.sleep(delay)
    raise FetchError(str(last_error) if last_error else f"Unknown JSON fetch failure for {url}")


def request_text_with_retry(
    client: RateLimitedClient,
    *,
    url: str,
) -> str:
    last_error: Exception | None = None
    for attempt, delay in enumerate(RETRY_DELAYS_SECONDS, start=1):
        try:
            return client.get(url, accept="text/html, application/xhtml+xml;q=0.9, */*;q=0.8").text
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
            if isinstance(exc, requests.HTTPError) and _is_404_error(exc):
                raise NotFoundError("404 Not Found") from exc
            last_error = exc
            if attempt == len(RETRY_DELAYS_SECONDS):
                break
            logger.warning("Retry %s/%s för %s efter fel: %s", attempt, len(RETRY_DELAYS_SECONDS), url, exc)
            time.sleep(delay)
    raise FetchError(str(last_error) if last_error else f"Unknown text fetch failure for {url}")


def fetch_document_payload(
    client: RateLimitedClient,
    *,
    document: dict[str, Any],
) -> dict[str, Any] | None:
    dok_id = _first_non_empty(document, "dok_id", "id")
    beteckning = _first_non_empty(document, "beteckning")
    rm = _first_non_empty(document, "rm")
    nummer = _first_non_empty(document, "nummer")
    titel = _first_non_empty(document, "titel")
    datum = _first_non_empty(document, "datum")
    organ = _first_non_empty(document, "organ")

    if not dok_id:
        raise ValueError("dir document is missing dok_id")

    status_url = STATUS_URL_TEMPLATE.format(dok_id=dok_id)
    html_url = HTML_URL_TEMPLATE.format(dok_id=dok_id)

    status_payload = request_json_with_retry(client, url=status_url)
    status_document = status_payload.get("dokumentstatus", {}).get("dokument", {})
    if not isinstance(status_document, dict):
        status_document = {}

    html_content = ""
    html_available = False
    try:
        html_response_text = request_text_with_retry(client, url=html_url)
        html_content = html_response_text.strip()
        html_available = bool(html_content)
    except NotFoundError:
        logger.warning("HTML saknas för %s: 404 på HTML-endpoint", dok_id)
        html_content = ""
        html_available = False
    except FetchError as exc:
        logger.warning("HTML saknas eller kunde inte hämtas för %s: %s", dok_id, exc)
        embedded_html = _extract_text(status_document.get("html"))
        html_content = embedded_html
        html_available = bool(embedded_html)

    final_rm = _extract_text(status_document.get("rm")) or rm
    final_nummer = _extract_text(status_document.get("nummer")) or nummer
    final_beteckning_raw = _extract_text(status_document.get("beteckning")) or beteckning
    final_titel = _extract_text(status_document.get("titel")) or titel
    final_datum = (_extract_text(status_document.get("datum")) or datum)[:10]
    final_organ = _extract_text(status_document.get("organ")) or organ

    return {
        "beteckning": normalize_beteckning(
            beteckning=final_beteckning_raw,
            rm=final_rm,
            nummer=final_nummer,
        ),
        "dok_id": dok_id,
        "rm": final_rm,
        "nummer": int(final_nummer) if final_nummer.isdigit() else final_nummer,
        "titel": final_titel,
        "datum": final_datum,
        "organ": final_organ,
        "source_url": status_url,
        "dokument_url_html": html_url,
        "html_content": html_content,
        "html_available": html_available,
        "fetched_at": utc_now_iso(),
        "schema_version": SCHEMA_VERSION,
        "license": LICENSE,
    }


def fetch_dir_documents(
    *,
    max_docs: int | None = None,
    output_dir: Path = OUTPUT_DIR,
    errors_path: Path = ERRORS_PATH,
    session: requests.Session | None = None,
) -> dict[str, int | None]:
    output_dir.mkdir(parents=True, exist_ok=True)
    errors_path.parent.mkdir(parents=True, exist_ok=True)

    existing_ids = build_existing_ids(output_dir)
    logger.info("Befintliga: %s dok_id inlästa från disk", len(existing_ids))

    client = RateLimitedClient(session=session)
    new_count = 0
    skipped_count = 0
    processed_count = 0
    page = 1
    expected_documents: int | None = None

    while True:
        try:
            page_payload = request_json_with_retry(
                client,
                url=LIST_URL,
                params={
                    "doktyp": "dir",
                    "utformat": "json",
                    "sz": LIST_PAGE_SIZE,
                    "p": page,
                },
            )
        except FetchError as exc:
            logger.error("Misslyckades att hämta list-sida %s: %s", page, exc)
            _append_error(
                errors_path,
                {
                    "dok_id": "",
                    "error": f"list page {page}: {exc}",
                    "timestamp": utc_now_iso(),
                },
            )
            break

        if expected_documents is None:
            expected_documents = _extract_total_hits(page_payload)
            if expected_documents is not None:
                logger.info("Förväntade dokument (API @traffar): %s", expected_documents)

        documents = _extract_documents(page_payload)
        if not documents:
            break

        for document in documents:
            dok_id = _first_non_empty(document, "dok_id", "id")
            if dok_id and dok_id in existing_ids:
                skipped_count += 1
                processed_count += 1
                if processed_count % LOG_EVERY_N_DOCUMENTS == 0:
                    logger.info("Processerade %s dokument. Nya: %s, Skippade: %s", processed_count, new_count, skipped_count)
                continue

            try:
                output_filename = build_output_filename(document)
                out_path = output_dir / output_filename
                if out_path.exists():
                    skipped_count += 1
                    processed_count += 1
                    if dok_id:
                        existing_ids.add(dok_id)
                    continue

                payload = fetch_document_payload(client, document=document)
                if payload is None:
                    continue
                _write_json_file(out_path, payload)
                if dok_id:
                    existing_ids.add(dok_id)
                new_count += 1
                processed_count += 1
                if processed_count % LOG_EVERY_N_DOCUMENTS == 0:
                    logger.info("Processerade %s dokument. Nya: %s, Skippade: %s", processed_count, new_count, skipped_count)
                if max_docs is not None and new_count >= max_docs:
                    break
            except NotFoundError:
                logger.warning("Hoppar över %s: 404 på status-endpoint", dok_id or "<saknas>")
                _append_error(
                    errors_path,
                    {
                        "dok_id": dok_id,
                        "error": "404 Not Found",
                        "timestamp": utc_now_iso(),
                    },
                )
                processed_count += 1
                continue
            except (FetchError, OSError, ValueError, TypeError) as exc:
                logger.error("Misslyckades med dok_id=%s: %s", dok_id or "<saknas>", exc)
                _append_error(
                    errors_path,
                    {
                        "dok_id": dok_id,
                        "error": str(exc),
                        "timestamp": utc_now_iso(),
                    },
                )
                continue

        if max_docs is not None and new_count >= max_docs:
            break

        remaining = _extract_remaining(page_payload)
        if remaining == 0:
            break

        page += 1

    disk_count = count_raw_document_files(output_dir)
    if expected_documents is not None:
        logger.info("Filer på disk efter fetch: %s", disk_count)
        logger.info("Diff: %s", expected_documents - disk_count)
    logger.info("Nya: %s, Skippade: %s", new_count, skipped_count)
    return {
        "new_count": new_count,
        "skipped_count": skipped_count,
        "expected_documents": expected_documents,
        "disk_count": disk_count,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch kommittedirektiv från Riksdagen API.")
    parser.add_argument(
        "--max-docs",
        type=int,
        default=None,
        help="Max antal nya dokument att hämta.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_logging()
    args = parse_args(argv)
    fetch_dir_documents(max_docs=args.max_docs)
    return 0


__all__ = [
    "ERRORS_PATH",
    "FetchError",
    "HTML_URL_TEMPLATE",
    "LIST_PAGE_SIZE",
    "LIST_URL",
    "NotFoundError",
    "OUTPUT_DIR",
    "PROJECT_ROOT",
    "STATUS_URL_TEMPLATE",
    "build_existing_ids",
    "build_output_filename",
    "count_raw_document_files",
    "fetch_dir_documents",
    "fetch_document_payload",
    "main",
    "normalize_beteckning",
    "normalize_rm",
    "parse_dir_identifier",
    "parse_args",
]


if __name__ == "__main__":
    raise SystemExit(main())
