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


def normalize_prop_beteckning(beteckning: str) -> str | None:
    """Normalize e.g. 'prop. 2016/17:180' to 'prop_2016-17_180'."""
    match = re.search(r"(?i)\bprop\.?\s*(\d{4})\s*/\s*(\d{2})\s*:\s*(\d+)\b", beteckning or "")
    if not match:
        return None
    start_year = match.group(1)
    end_year = match.group(2)
    number = int(match.group(3))
    return f"prop_{start_year}-{end_year}_{number}"


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
        "@\u00e5terst\u00e5ende",
        "@aterstaende",
        "@\u00e5terstaende",
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
    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, headers=headers, params=params, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("Response JSON must be a mapping.")
            return payload
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError, ValueError) as exc:
            last_exc = exc
            if attempt >= max_retries:
                break
            time.sleep(retry_backoff_base_s * (2 ** (attempt - 1)))
    raise FetchError(str(last_exc) if last_exc else "Unknown JSON request failure")


def _first_non_empty(document: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = document.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _join_url(base_url: str, endpoint: str) -> str:
    return f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"


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

    match = re.search(r"(?i)\bprop\.?\s*(\d{4}/\d{2})\s*:\s*(\d+)\b", beteckning or "")
    if match:
        riksmote = match.group(1)
        nummer = int(match.group(2))

    return riksmote, nummer


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

    delay_between = float(rate_cfg.get("delay_between_requests_s", 1.0))
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

        documents = _extract_documents(page_payload)
        for document in documents:
            beteckning = _first_non_empty(document, "beteckning")
            dok_id = _first_non_empty(document, "dok_id", "id")
            titel = _first_non_empty(document, "titel")
            datum = _first_non_empty(document, "datum")
            organ = _first_non_empty(document, "organ")
            fil_url = _first_non_empty(document, "filUrl", "fil_url")

            riksmote, nummer = _extract_prop_parts(document, beteckning)
            normalized_name = normalize_prop_beteckning(beteckning)
            if not normalized_name:
                if dok_id:
                    logger.warning("Could not normalize proposition beteckning '%s'; using dok_id.", beteckning)
                    normalized_name = dok_id
                else:
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

            source_url = fil_url
            html_content = ""
            html_available = False
            any_fetch_success = False
            html_url = ""

            if dok_id:
                html_url = _join_url(base_url, html_template.format(dok_id=dok_id))
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
                    any_fetch_success = True
                    source_url = html_url
                    maybe_text = response.text.strip()
                    if maybe_text:
                        html_content = maybe_text
                        html_available = True
                except FetchError as exc:
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

            if not html_available and fil_url:
                try:
                    response = _request_with_retry(
                        session,
                        url=fil_url,
                        headers=headers,
                        params=None,
                        timeout=timeout,
                        max_retries=max_retries,
                        retry_backoff_base_s=retry_backoff_base_s,
                    )
                    any_fetch_success = True
                    source_url = fil_url
                    content_type = response.headers.get("Content-Type", "").lower()
                    if any(token in content_type for token in ("text", "html", "xml")):
                        maybe_text = response.text.strip()
                        if maybe_text:
                            html_content = maybe_text
                            html_available = True
                except FetchError as exc:
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

            if not any_fetch_success and (dok_id or fil_url):
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

            if not html_available and not fil_url and not dok_id:
                logger.error("Proposition document missing filUrl and dok_id: %s", beteckning)
                _append_error(
                    errors_path,
                    {
                        "source": "prop_document",
                        "beteckning": beteckning,
                        "error": "Missing filUrl and dok_id.",
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
                continue

            raw_payload: dict[str, Any] = {
                "beteckning": beteckning,
                "dok_id": dok_id,
                "titel": titel,
                "riksmote": riksmote,
                "nummer": nummer,
                "datum": datum,
                "organ": organ,
                "source_url": source_url,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
            if html_available:
                raw_payload["html_content"] = html_content
            else:
                raw_payload["html_available"] = False

            _write_json_file(out_path, raw_payload)
            saved_count += 1
            if saved_count % log_every == 0:
                logger.info("Fetched proposition documents: %s", saved_count)

            time.sleep(delay_between)

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
