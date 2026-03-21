"""Fetch Försäkringskassans föreskrifter och allmänna råd till rålagret."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import html
import json
import logging
from pathlib import Path
import re
import sys
import time
from typing import Any
from urllib.parse import urlencode

import httpx

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logger = logging.getLogger("paragrafenai.noop")

SCHEMA_VERSION = "v0.15"
LICENSE = "public_domain"
BASE_URL = "https://lagrummet.forsakringskassan.se"
CATALOG_OUTPUT = "data/raw/foreskrift/fk/catalog.json"
PDF_DIR = "data/raw/foreskrift/fk/pdf"
METADATA_DIR = "data/raw/foreskrift/fk/metadata"
DEFAULT_TIMEOUT_S = 30.0
MAX_RETRIES = 3
RETRY_DELAYS_S = (2.0, 4.0, 8.0)
SLEEP_BETWEEN_REQUESTS = 1.0
PRIORITY_ORDER = ["FKFS", "FKAR", "RFFS", "RAR"]
DETAIL_PAGE_PATH = "/foreskrifter/dokument"
SOURCES = [
    {
        "url": f"{BASE_URL}/allmanna-rad",
        "samlingar": ["FKAR", "RAR"],
    },
    {
        "url": f"{BASE_URL}/foreskrifter",
        "samlingar": ["FKFS", "RFFS"],
    },
]

CATALOG_FIELDS = (
    "nummer",
    "titel",
    "uppslagsord",
    "node_id",
    "base_document_id",
    "isRevoked",
    "upphavdDatum",
    "isChangeDocument",
    "forfattningssamling",
    "arsutgava",
    "lopnummer",
    "samling",
    "catalog_url",
)
DETAIL_FIELD_MAP = {
    "diarienummer": "diarienr",
    "beslutsdatum": "beslutsdatum",
    "ikraftträdande": "ikrafttradande",
    "upphävd_datum": "upphavd_datum",
    "forfattning": "forfattning",
    "beslutad_av": "beslutad_av",
    "aktuell_titel": "aktuell_titel",
    "bemyndigande": "bemyndigande",
    "forman": "forman",
    "andring": "andring",
    "celex": "celex",
    "tryckdatum": "tryckdatum",
}


class FetchError(Exception):
    """Raised when a remote request fails after retries."""


@dataclass(frozen=True)
class SourceConfig:
    """One catalog source on lagrummet.forsakringskassan.se."""

    url: str
    samlingar: tuple[str, ...]


def utc_now_iso() -> str:
    """Return UTC timestamp without fractional seconds."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize_samling(document: dict[str, Any]) -> str:
    """Resolve samling consistently across catalog sources."""
    raw = document.get("samling") or document.get("forfattningssamling") or ""
    return str(raw).strip().upper()


def coerce_bool(value: Any) -> bool:
    """Best-effort boolean coercion."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "ja", "yes"}
    return bool(value)


def clean_scalar(value: Any) -> str:
    """Normalize one metadata scalar to string."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def ensure_list(value: Any) -> list[str]:
    """Normalize list-like metadata to a flat string list."""
    if value is None:
        return []
    if isinstance(value, list):
        return [clean_scalar(item) for item in value if clean_scalar(item)]
    cleaned = clean_scalar(value)
    return [cleaned] if cleaned else []


def build_filename(samling: str, arsutgava: str, lopnummer: str) -> str:
    """Build the mandated filename stem."""
    samling_norm = clean_scalar(samling).upper()
    ars_norm = clean_scalar(arsutgava)
    lop_norm = clean_scalar(lopnummer)
    if not samling_norm or not ars_norm or not lop_norm:
        raise ValueError("samling, arsutgava och lopnummer kravs for filnamn")
    return f"{samling_norm}_{ars_norm}_{lop_norm}"


def merge_catalog_documents(
    current: dict[str, Any],
    incoming: dict[str, Any],
) -> dict[str, Any]:
    """Merge duplicate catalog entries, preferring populated values."""
    merged = dict(current)
    empty_values = (None, "", [])
    for key in CATALOG_FIELDS:
        if key not in incoming:
            continue
        incoming_value = incoming.get(key)
        current_value = merged.get(key)
        if key == "uppslagsord":
            merged[key] = ensure_list(current_value) or ensure_list(incoming_value)
            if ensure_list(incoming_value):
                merged[key] = ensure_list(incoming_value)
            continue
        if current_value in empty_values and incoming_value not in empty_values:
            merged[key] = incoming_value
            continue
        if key in {"isRevoked", "isChangeDocument"}:
            merged[key] = coerce_bool(current_value) or coerce_bool(incoming_value)
    return merged


def extract_data_props_payloads(page_html: str) -> list[dict[str, Any]]:
    """Extract all HTML-escaped JSON payloads embedded as data-props."""
    payloads: list[dict[str, Any]] = []
    for raw_payload in re.findall(r'data-props="([^"]+)"', page_html):
        try:
            parsed = json.loads(html.unescape(raw_payload))
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            payloads.append(parsed)
    return payloads


def extract_initial_states(page_html: str) -> list[dict[str, Any]]:
    """Extract JSON payloads from AppRegistry.registerInitialState calls."""
    marker = "AppRegistry.registerInitialState("
    states: list[dict[str, Any]] = []
    cursor = 0
    while True:
        marker_index = page_html.find(marker, cursor)
        if marker_index < 0:
            break
        json_start = page_html.find("{", marker_index)
        if json_start < 0:
            break
        depth = 0
        in_string = False
        escaped = False
        for index in range(json_start, len(page_html)):
            char = page_html[index]
            if in_string:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    in_string = False
                continue
            if char == '"':
                in_string = True
                continue
            if char == "{":
                depth += 1
                continue
            if char == "}":
                depth -= 1
                if depth == 0:
                    candidate = page_html[json_start : index + 1]
                    try:
                        parsed = json.loads(candidate)
                    except json.JSONDecodeError:
                        cursor = index + 1
                        break
                    if isinstance(parsed, dict):
                        states.append(parsed)
                    cursor = index + 1
                    break
        else:
            break
    return states


def flatten_detail_metadata(raw_metadata: dict[str, Any]) -> dict[str, Any]:
    """Flatten FK's metadata objects to plain Python values."""
    flattened: dict[str, Any] = {}
    for key, raw_value in raw_metadata.items():
        if not isinstance(raw_value, dict):
            flattened[key] = raw_value
            continue
        value = raw_value.get("value")
        if isinstance(value, list):
            flattened[key] = ensure_list(value)
        elif value is None:
            flattened[key] = ""
        else:
            flattened[key] = clean_scalar(value)
    return flattened


def build_detail_url(node_id: str) -> str:
    """Build the canonical document page URL."""
    return f"{BASE_URL}{DETAIL_PAGE_PATH}?{urlencode({'id': node_id})}"


class FkForeskriftFetcher:
    """Fetch catalog, document metadata and PDFs for FK regulatory sources."""

    def __init__(
        self,
        *,
        catalog_output: str | Path = CATALOG_OUTPUT,
        pdf_dir: str | Path = PDF_DIR,
        metadata_dir: str | Path = METADATA_DIR,
        timeout: float = DEFAULT_TIMEOUT_S,
        sleep_between_requests: float = SLEEP_BETWEEN_REQUESTS,
        client: httpx.Client | None = None,
    ) -> None:
        self.repo_root = Path(__file__).resolve().parent.parent
        self.catalog_output = self._resolve_path(catalog_output)
        self.pdf_dir = self._resolve_path(pdf_dir)
        self.metadata_dir = self._resolve_path(metadata_dir)
        self.timeout = timeout
        self.sleep_between_requests = max(0.0, float(sleep_between_requests))
        self.client = client or httpx.Client(
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (compatible; ParagrafenBot/0.15; "
                    "+https://github.com/openai/codex)"
                )
            },
            timeout=self.timeout,
        )
        self._last_request_started_at = 0.0

        self.catalog_output.parent.mkdir(parents=True, exist_ok=True)
        self.pdf_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.sources = [SourceConfig(url=entry["url"], samlingar=tuple(entry["samlingar"])) for entry in SOURCES]

    def run(
        self,
        *,
        samling: str | None = None,
        catalog_only: bool = False,
        limit: int | None = None,
        verbose: bool = False,
    ) -> dict[str, Any]:
        """Execute the sequential fetch pipeline."""
        selected_samling = clean_scalar(samling).upper()
        all_catalog_docs = self.fetch_catalog_documents()
        if selected_samling:
            all_catalog_docs = [
                document for document in all_catalog_docs if document["samling"] == selected_samling
            ]
        all_catalog_docs = self._sort_documents(all_catalog_docs)
        if limit is not None:
            all_catalog_docs = all_catalog_docs[: max(0, int(limit))]

        catalog_payload = {
            "schema_version": SCHEMA_VERSION,
            "license": LICENSE,
            "myndighet": "Försäkringskassan",
            "fetched_at": utc_now_iso(),
            "document_count": len(all_catalog_docs),
            "documents": all_catalog_docs,
        }
        self._write_json(self.catalog_output, catalog_payload)

        stats: dict[str, Any] = {
            "catalog_documents": len(all_catalog_docs),
            "metadata_written": 0,
            "pdf_downloaded": 0,
            "pdf_skipped_existing": 0,
            "failed": 0,
        }

        for index, catalog_document in enumerate(all_catalog_docs, start=1):
            node_id = catalog_document["node_id"]
            try:
                detail_document = self.fetch_detail_document(node_id=node_id)
                normalized = self.normalize_document(catalog_document, detail_document)
                stem = build_filename(
                    normalized["samling"],
                    normalized["arsutgava"],
                    normalized["lopnummer"],
                )
                metadata_path = self.metadata_dir / f"{stem}.json"
                self._write_json(metadata_path, normalized)
                stats["metadata_written"] += 1

                if not catalog_only:
                    pdf_path = self.pdf_dir / f"{stem}.pdf"
                    if pdf_path.exists() and pdf_path.stat().st_size > 0:
                        stats["pdf_skipped_existing"] += 1
                    else:
                        self.download_pdf(normalized["pdf_url"], pdf_path)
                        stats["pdf_downloaded"] += 1

                if verbose:
                    print(
                        f"[OK]   {index}/{len(all_catalog_docs)} "
                        f"{stem} ({normalized['samling']}, node_id={node_id})"
                    )
            except Exception as exc:
                stats["failed"] += 1
                logger.error("FAIL: %s — %s", node_id, exc)
                if verbose:
                    print(f"[FAIL] {node_id} -> {exc}")

        return stats

    def fetch_catalog_documents(self) -> list[dict[str, Any]]:
        """Fetch and normalize all active catalog entries across configured sources."""
        deduped: dict[str, dict[str, Any]] = {}
        for source in self.sources:
            page_html = self._get_text(source.url)
            payloads = extract_data_props_payloads(page_html)
            for payload in payloads:
                documents = payload.get("documents")
                if not isinstance(documents, list):
                    continue
                for raw_document in documents:
                    if not isinstance(raw_document, dict):
                        continue
                    normalized = self.normalize_catalog_document(raw_document, catalog_url=source.url)
                    if normalized["samling"] not in source.samlingar:
                        continue
                    if normalized["isRevoked"]:
                        continue
                    node_id = normalized["node_id"]
                    existing = deduped.get(node_id)
                    deduped[node_id] = (
                        normalized if existing is None else merge_catalog_documents(existing, normalized)
                    )
        return list(deduped.values())

    def normalize_catalog_document(
        self,
        raw_document: dict[str, Any],
        *,
        catalog_url: str,
    ) -> dict[str, Any]:
        """Normalize catalog metadata to the pipeline schema."""
        samling = normalize_samling(raw_document)
        uppslagsord = ensure_list(raw_document.get("uppslagsord"))
        return {
            "nummer": clean_scalar(raw_document.get("nummer")),
            "titel": clean_scalar(raw_document.get("titel")),
            "uppslagsord": uppslagsord,
            "node_id": clean_scalar(raw_document.get("id")),
            "base_document_id": clean_scalar(raw_document.get("baseDocumentId")),
            "isRevoked": coerce_bool(raw_document.get("isRevoked")),
            "upphavdDatum": clean_scalar(raw_document.get("upphavdDatum")),
            "isChangeDocument": coerce_bool(raw_document.get("isChangeDocument")),
            "forfattningssamling": clean_scalar(raw_document.get("forfattningssamling")),
            "arsutgava": clean_scalar(raw_document.get("arsutgava")),
            "lopnummer": clean_scalar(raw_document.get("lopnummer")),
            "samling": samling,
            "catalog_url": catalog_url,
        }

    def fetch_detail_document(self, *, node_id: str) -> dict[str, Any]:
        """Fetch one document page and extract the base document payload."""
        detail_url = build_detail_url(node_id)
        page_html = self._get_text(detail_url)
        for state in extract_initial_states(page_html):
            if state.get("name") != "fklag-dokumentvisning":
                continue
            props = state.get("props")
            if not isinstance(props, dict):
                continue
            base_document = props.get("baseDocument")
            if isinstance(base_document, dict):
                return {
                    "detail_url": detail_url,
                    "type_of_document": clean_scalar(props.get("typeOfDocument")),
                    "document": base_document,
                }
        raise FetchError(f"kunde inte extrahera detaljmetadata for node_id={node_id}")

    def normalize_document(
        self,
        catalog_document: dict[str, Any],
        detail_document: dict[str, Any],
    ) -> dict[str, Any]:
        """Merge catalog and detail metadata into one normalized JSON document."""
        base_document = detail_document["document"]
        raw_metadata = base_document.get("metadata")
        if not isinstance(raw_metadata, dict):
            raise FetchError("detail page saknar metadata")

        flattened = flatten_detail_metadata(raw_metadata)
        samling = clean_scalar(
            catalog_document.get("samling")
            or flattened.get("samling")
            or flattened.get("forfattningssamling")
        ).upper()
        arsutgava = clean_scalar(catalog_document.get("arsutgava") or flattened.get("arsutgava"))
        lopnummer = clean_scalar(catalog_document.get("lopnummer") or flattened.get("lopnummer"))
        pdf_url = clean_scalar(base_document.get("url"))
        if pdf_url.startswith("/"):
            pdf_url = f"{BASE_URL}{pdf_url}"
        if not pdf_url:
            raise FetchError("detail page saknar pdf-url")

        normalized = {
            "schema_version": SCHEMA_VERSION,
            "license": LICENSE,
            "myndighet": "Försäkringskassan",
            "source_domain": BASE_URL,
            "source_type": (
                "foreskrift" if samling in {"FKFS", "RFFS"} else "allmannarad"
            ),
            "fetched_at": utc_now_iso(),
            "samling": samling,
            "nummer": clean_scalar(catalog_document.get("nummer") or flattened.get("dokument_nr")),
            "titel": clean_scalar(catalog_document.get("titel") or flattened.get("titel")),
            "uppslagsord": ensure_list(catalog_document.get("uppslagsord") or flattened.get("uppslagsord")),
            "node_id": clean_scalar(catalog_document.get("node_id") or base_document.get("id")),
            "base_document_id": clean_scalar(catalog_document.get("base_document_id")),
            "arsutgava": arsutgava,
            "lopnummer": lopnummer,
            "is_revoked": coerce_bool(catalog_document.get("isRevoked")),
            "upphavd_datum_catalog": clean_scalar(catalog_document.get("upphavdDatum")),
            "is_change_document": coerce_bool(catalog_document.get("isChangeDocument")),
            "forfattningssamling": clean_scalar(
                catalog_document.get("forfattningssamling") or flattened.get("forfattningssamling")
            ),
            "pdf_url": pdf_url,
            "detail_url": clean_scalar(detail_document.get("detail_url")),
            "display_name": clean_scalar(base_document.get("displayName")),
            "headline": clean_scalar(base_document.get("headline")),
            "current_headline": clean_scalar(base_document.get("currentHeadline")),
            "type_of_document": clean_scalar(detail_document.get("type_of_document")),
            "raw_metadata": flattened,
        }
        for output_key, source_key in DETAIL_FIELD_MAP.items():
            value = flattened.get(source_key)
            normalized[output_key] = ensure_list(value) if output_key == "uppslagsord" else value or ""
        normalized["uppslagsord"] = ensure_list(normalized.get("uppslagsord"))
        return normalized

    def download_pdf(self, pdf_url: str, target_path: Path) -> None:
        """Download one PDF to disk."""
        response = self._request("GET", pdf_url)
        response.raise_for_status()
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(response.content)

    def _request(self, method: str, url: str) -> httpx.Response:
        """Issue one rate-limited HTTP request with retry."""
        last_error: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            self._respect_rate_limit()
            try:
                response = self.client.request(method, url)
                response.raise_for_status()
                return response
            except (httpx.HTTPError, httpx.TimeoutException) as exc:
                last_error = exc
                if attempt >= MAX_RETRIES:
                    break
                time.sleep(RETRY_DELAYS_S[attempt - 1])
        raise FetchError(str(last_error) if last_error else f"okant fel for {url}")

    def _get_text(self, url: str) -> str:
        response = self._request("GET", url)
        return response.text

    def _respect_rate_limit(self) -> None:
        if self.sleep_between_requests <= 0:
            self._last_request_started_at = time.monotonic()
            return
        elapsed = time.monotonic() - self._last_request_started_at
        remaining = self.sleep_between_requests - elapsed
        if remaining > 0:
            time.sleep(remaining)
        self._last_request_started_at = time.monotonic()

    def _sort_documents(self, documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
        priority = {samling: index for index, samling in enumerate(PRIORITY_ORDER)}

        def sort_key(document: dict[str, Any]) -> tuple[int, str, str, str]:
            return (
                priority.get(document.get("samling", ""), len(priority)),
                clean_scalar(document.get("arsutgava")),
                clean_scalar(document.get("lopnummer")).zfill(8),
                clean_scalar(document.get("node_id")),
            )

        return sorted(documents, key=sort_key)

    def _write_json(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)

    def _resolve_path(self, path_value: str | Path) -> Path:
        candidate = Path(path_value)
        if candidate.is_absolute():
            return candidate
        return self.repo_root / candidate


def print_summary(stats: dict[str, Any], *, catalog_only: bool) -> None:
    """Print end-of-run summary."""
    print("FK föreskrifts-fetch klar.")
    print(f"  Katalogdokument:      {stats['catalog_documents']}")
    print(f"  Metadata skrivna:     {stats['metadata_written']}")
    if catalog_only:
        print("  PDF-nedladdning:      AV")
    else:
        print(f"  PDF:er nedladdade:    {stats['pdf_downloaded']}")
        print(f"  PDF:er redan fanns:   {stats['pdf_skipped_existing']}")
    print(f"  Misslyckade:          {stats['failed']}")
    print(f"  Katalogfil:           {CATALOG_OUTPUT}")


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="Hamta FK-föreskrifter och allmänna råd till rålagret"
    )
    parser.add_argument("--samling", default=None, help="Filtrera till en samling, t.ex. FKFS")
    parser.add_argument("--catalog-only", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s:%(name)s:%(message)s",
    )

    fetcher = FkForeskriftFetcher()
    stats = fetcher.run(
        samling=args.samling,
        catalog_only=args.catalog_only,
        limit=args.limit,
        verbose=args.verbose,
    )
    print_summary(stats, catalog_only=args.catalog_only)
    return 1 if stats["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
