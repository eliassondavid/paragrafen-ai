"""Fetch proposition documents from Riksdagen API and store raw JSON per document."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import re
import time
from typing import Any

import requests
import yaml

logger = logging.getLogger("paragrafenai.noop")

MIN_DELAY_BETWEEN_REQUESTS_S = 0.2


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


def normalize_riksmote(rm: str) -> str:
    """Normalize `2016/17` -> `2016-17`; keep single-year values unchanged."""
    value = (rm or "").strip()
    match = re.fullmatch(r"(\d{4})\s*/\s*(\d{2})", value)
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    return value


def normalize_prop_beteckning(beteckning: str) -> str | None:
    """Normalize e.g. 'prop. 2016/17:180' to 'prop_2016-17_180'."""
    match = re.search(
        r"(?i)\bprop\.?\s*(\d{4})(?:\s*/\s*(\d{2}))?\s*:\s*(\d+)\b",
        beteckning or "",
    )
    if not match:
        return None

    start_year = match.group(1)
    end_year = match.group(2)
    number = int(match.group(3))
    rm_norm = f"{start_year}-{end_year}" if end_year else start_year
    return f"prop_{rm_norm}_{number}"


def _append_error(errors_path: Path, payload: dict[str, Any]) -> None:
    try:
        errors_path.parent.mkdir(parents=True, exist_ok=True)
        with errors_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except OSError as exc:
        logger.critical("Failed writing errors file %s: %s", errors_path, exc)
        raise SystemExit(1) from exc


def _write_json_file(path: Path, payload: dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
    except OSError as exc:
        logger.critical("Failed writing output file %s: %s", path, exc)
        raise SystemExit(1) from exc


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

    remaining_keys = (
        "@återstående",
        "@aterstaende",
        "@återstaende",
        "@remaining",
    )
    for key in remaining_keys:
        raw_value = document_list.get(key)
        if raw_value is None:
            continue
        try:
            return int(raw_value)
        except (TypeError, ValueError):
            return None
    return None


def _extract_page_number(document_list: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        raw_value = document_list.get(key)
        if raw_value is None:
            continue
        try:
            return int(raw_value)
        except (TypeError, ValueError):
            return None
    return None


def _request_with_retry(
    session: requests.Session,
    *,
    url: str,
    headers: dict[str, str],
    params: dict[str, Any] | None,
    timeout: float,
    max_retries: int,
    retry_backoff_base_s: float,
) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, headers=headers, params=params, timeout=timeout)
            response.raise_for_status()
            return response
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
            last_exc = exc
            if attempt >= max_retries:
                break
            time.sleep(retry_backoff_base_s * (2 ** (attempt - 1)))
    raise FetchError(str(last_exc) if last_exc else "Unknown request failure")


def _request_json_with_retry(
    session: requests.Session,
    *,
    url: str,
    headers: dict[str, str],
    params: dict[str, Any] | None,
    timeout: float,
    max_retries: int,
    retry_backoff_base_s: float,
) -> dict[str, Any]:
    response = _request_with_retry(
        session,
        url=url,
        headers=headers,
        params=params,
        timeout=timeout,
        max_retries=max_retries,
        retry_backoff_base_s=retry_backoff_base_s,
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise FetchError(str(exc)) from exc
    if not isinstance(payload, dict):
        raise FetchError("Response JSON must be a mapping.")
    return payload


def _first_non_empty(document: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = document.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _join_url(base_url: str, endpoint: str) -> str:
    return f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"


def _normalize_document_url(base_url: str, url: str) -> str:
    value = (url or "").strip()
    if not value:
        return ""
    if value.startswith("//"):
        return f"https:{value}"
    if value.startswith("/"):
        return _join_url(base_url, value)
    return value


def _extract_prop_parts(document: dict[str, Any], beteckning: str) -> tuple[str, int | None]:
    riksmote = _first_non_empty(document, "rm", "riksmote")
    nummer_raw = _first_non_empty(document, "nummer")
    nummer: int | None = None
    if nummer_raw:
        try:
            nummer = int(nummer_raw)
        except ValueError:
            nummer = None

    if riksmote and nummer is not None:
        return riksmote, nummer

    match = re.search(r"(?i)\bprop\.?\s*(\d{4}(?:/\d{2})?)\s*:\s*(\d+)\b", beteckning or "")
    if match:
        riksmote = match.group(1)
        nummer = int(match.group(2))

    return riksmote, nummer


def _build_filename(riksmote: str, nummer: int | None, beteckning: str, dok_id: str) -> str | None:
    rm_norm = normalize_riksmote(riksmote)
    if rm_norm and nummer is not None:
        return f"prop_{rm_norm}_{nummer}"

    normalized_name = normalize_prop_beteckning(beteckning)
    if normalized_name:
        return normalized_name

    return dok_id or None


def _extract_pdf_url(base_url: str, document: dict[str, Any]) -> str:
    attachment = document.get("filbilaga")
    candidates: list[str] = []

    if isinstance(attachment, dict):
        nested = attachment.get("fil")
        if isinstance(nested, list):
            for item in nested:
                if isinstance(item, dict):
                    candidates.append(str(item.get("url", "") or ""))
        elif isinstance(nested, dict):
            candidates.append(str(nested.get("url", "") or ""))
        candidates.append(str(attachment.get("url", "") or ""))
    elif isinstance(attachment, list):
        for item in attachment:
            if isinstance(item, dict):
                nested = item.get("fil")
                if isinstance(nested, dict):
                    candidates.append(str(nested.get("url", "") or ""))
                candidates.append(str(item.get("url", "") or ""))

    for candidate in candidates:
        normalized = _normalize_document_url(base_url, candidate)
        if normalized:
            return normalized
    return ""


def fetch_prop_documents(
    config_path: str | Path = "config/sources.yaml",
    *,
    session: requests.Session | None = None,
) -> int:
    """Fetch all proposition documents and store one raw JSON file per document."""
    config = load_sources_config(config_path)
    api_cfg = config["riksdagen_api"]
    prop_cfg = api_cfg["prop"]
    rate_cfg = config.get("rate_limiting", {})
    http_cfg = config.get("http", {})
    progress_cfg = config.get("progress", {})

    base_url = str(api_cfg["base_url"])
    list_url = _join_url(base_url, str(prop_cfg["list_endpoint"]))
    html_template = str(prop_cfg["document_html_endpoint"])

    output_dir = Path(str(prop_cfg["output_dir"]))
    errors_path = Path(str(prop_cfg["errors_file"]))

    delay_between = max(float(rate_cfg.get("delay_between_requests_s", 1.0)), MIN_DELAY_BETWEEN_REQUESTS_S)
    max_retries = int(rate_cfg.get("max_retries", 3))
    retry_backoff_base_s = float(rate_cfg.get("retry_backoff_base_s", 1.0))
    timeout = float(rate_cfg.get("request_timeout_s", 30))
    log_every = int(progress_cfg.get("log_every_n_documents", 100))

    headers = {
        "User-Agent": str(http_cfg.get("user_agent", "paragrafenai-fetcher/0.1")),
        "Accept-Encoding": str(http_cfg.get("accept_encoding", "gzip, deflate")),
    }

    if session is None:
        session = requests.Session()

    output_dir.mkdir(parents=True, exist_ok=True)
    errors_path.parent.mkdir(parents=True, exist_ok=True)

    saved_count = 0
    page = 1
    while True:
        params: dict[str, Any] = {
            "doktyp": prop_cfg["doktyp"],
            "utformat": prop_cfg["utformat"],
            "pagesize": prop_cfg["pagesize"],
            "p": page,
        }

        try:
            page_payload = _request_json_with_retry(
                session,
                url=list_url,
                headers=headers,
                params=params,
                timeout=timeout,
                max_retries=max_retries,
                retry_backoff_base_s=retry_backoff_base_s,
            )
        except FetchError as exc:
            logger.error("Failed to fetch proposition list page %s: %s", page, exc)
            _append_error(
                errors_path,
                {
                    "source": "prop_list",
                    "page": page,
                    "error": str(exc),
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            break

        document_list = page_payload.get("dokumentlista", {})
        if not isinstance(document_list, dict):
            document_list = {}

        documents = _extract_documents(page_payload)
        for document in documents:
            beteckning = _first_non_empty(document, "beteckning")
            dok_id = _first_non_empty(document, "dok_id", "id")
            titel = _first_non_empty(document, "titel")
            datum = _first_non_empty(document, "datum")
            organ = _first_non_empty(document, "organ")

            riksmote, nummer = _extract_prop_parts(document, beteckning)
            normalized_name = _build_filename(riksmote, nummer, beteckning, dok_id)
            if not normalized_name:
                logger.warning(
                    "Missing both normalizable proposition beteckning and dok_id; skipping document."
                )
                _append_error(
                    errors_path,
                    {
                        "source": "prop_document",
                        "beteckning": beteckning,
                        "error": "Could not derive filename from beteckning or dok_id.",
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
                continue

            out_path = output_dir / f"{normalized_name}.json"
            if out_path.exists():
                continue

            source_url = _join_url(base_url, f"/dokument/{dok_id}") if dok_id else ""
            html_url = _normalize_document_url(base_url, _first_non_empty(document, "dokument_url_html"))
            if not html_url and dok_id:
                html_url = _join_url(base_url, html_template.format(dok_id=dok_id))

            text_url = _normalize_document_url(
                base_url,
                _first_non_empty(document, "dokument_url_text", "fil_url", "filUrl"),
            )
            pdf_url = _extract_pdf_url(base_url, document)

            html_content = ""
            html_available = False
            fetch_failed = False

            if html_url:
                try:
                    response = _request_with_retry(
                        session,
                        url=html_url,
                        headers=headers,
                        params=None,
                        timeout=timeout,
                        max_retries=max_retries,
                        retry_backoff_base_s=retry_backoff_base_s,
                    )
                    html_candidate = response.text.strip()
                    if html_candidate:
                        html_content = html_candidate
                        html_available = True
                except FetchError as exc:
                    fetch_failed = True
                    logger.warning("Failed HTML fetch for proposition dok_id=%s: %s", dok_id, exc)
                    _append_error(
                        errors_path,
                        {
                            "source": "prop_document_html",
                            "dok_id": dok_id,
                            "beteckning": beteckning,
                            "error": str(exc),
                            "fetched_at": datetime.now(timezone.utc).isoformat(),
                        },
                    )

            if not html_available and text_url:
                try:
                    response = _request_with_retry(
                        session,
                        url=text_url,
                        headers=headers,
                        params=None,
                        timeout=timeout,
                        max_retries=max_retries,
                        retry_backoff_base_s=retry_backoff_base_s,
                    )
                    content_type = response.headers.get("Content-Type", "").lower()
                    html_candidate = response.text.strip()
                    if html_candidate and any(token in content_type for token in ("text", "html", "xml")):
                        html_content = html_candidate
                        html_available = True
                except FetchError as exc:
                    fetch_failed = True
                    logger.warning("Fallback fetch failed for proposition dok_id=%s: %s", dok_id, exc)
                    _append_error(
                        errors_path,
                        {
                            "source": "prop_document_fallback",
                            "dok_id": dok_id,
                            "beteckning": beteckning,
                            "error": str(exc),
                            "fetched_at": datetime.now(timezone.utc).isoformat(),
                        },
                    )

            if fetch_failed and not html_available and (html_url or text_url):
                logger.error("Could not fetch proposition content after retries for dok_id=%s", dok_id)
                _append_error(
                    errors_path,
                    {
                        "source": "prop_document",
                        "dok_id": dok_id,
                        "beteckning": beteckning,
                        "error": "Could not fetch content after retries.",
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
                continue

            raw_payload: dict[str, Any] = {
                "beteckning": beteckning,
                "dok_id": dok_id,
                "rm": riksmote,
                "nummer": nummer,
                "titel": titel,
                "datum": datum,
                "organ": organ,
                "source_url": source_url,
                "dokument_url_html": html_url,
                "pdf_url": pdf_url,
                "html_content": html_content,
                "html_available": html_available,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }

            _write_json_file(out_path, raw_payload)
            saved_count += 1
            if saved_count % log_every == 0:
                logger.info("Fetched proposition documents: %s", saved_count)

            time.sleep(delay_between)

        current_page = _extract_page_number(document_list, "@sida", "@page")
        total_pages = _extract_page_number(document_list, "@sidor", "@pages")
        if current_page is not None and total_pages is not None and current_page >= total_pages:
            break

        remaining = _extract_remaining(page_payload)
        if remaining == 0:
            break
        if remaining is None and not documents:
            break
        page += 1

    return saved_count


def main() -> int:
    return fetch_prop_documents()


if __name__ == "__main__":
    raise SystemExit(main())
