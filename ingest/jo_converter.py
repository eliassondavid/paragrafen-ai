"""Convert JO PDFs into the paragrafen.ai raw JSON format."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import re
import subprocess
from typing import Any


PDFTOTEXT_BIN = "/opt/homebrew/bin/pdftotext"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data/raw/jo"
PDF_DIR = RAW_DIR / "pdf"
META_DIR = RAW_DIR / "metadata"
JSON_DIR = RAW_DIR / "json"
SCHEMA_VERSION = "v0.15"
EMPTY_TEXT_ESCALATION_THRESHOLD = 0.20
BEDOMNING_WARNING_THRESHOLD = 0.10

DEFAULT_SOURCE_URL = "https://www.jo.se/jo-beslut/sokresultat/"
DEFAULT_SOURCE_TYPE = "myndighetsbeslut"
DEFAULT_DOCUMENT_SUBTYPE = "jo"
DEFAULT_AUTHORITY_LEVEL = "guiding"
DEFAULT_LICENSE = "public_domain"

logger = logging.getLogger("jo_converter")

SECTION_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("bakgrund", re.compile(r"(?im)^(?:anmälan|bakgrund|initiativ)")),
    ("utredning", re.compile(r"(?im)^(?:utredning|remiss|yttrande)")),
    (
        "bedomning",
        re.compile(r"(?im)^(?:(?:JO|Justitieombudsmannen)s?\s+bedömning|bedömning)"),
    ),
    ("atgard", re.compile(r"(?im)^(?:åtgärd|beslut|avslutning)")),
]

SECTION_TITLES = {
    "bakgrund": "Bakgrund",
    "utredning": "Utredning",
    "bedomning": "JO:s bedömning",
    "atgard": "Åtgärd/Beslut",
    "other": "Övrigt",
}


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def pdf_to_text(pdf_path: Path) -> tuple[str, str]:
    """Extract text with pdftotext -layout and return (text, extraction_method)."""
    try:
        result = subprocess.run(
            [PDFTOTEXT_BIN, "-layout", str(pdf_path), "-"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=60,
        )
    except subprocess.TimeoutExpired:
        logger.error("Timeout vid konvertering av %s", pdf_path.name)
        return "", "timeout"
    except FileNotFoundError:
        logger.error("pdftotext hittades inte på %s", PDFTOTEXT_BIN)
        raise

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        logger.warning(
            "pdftotext returncode=%d för %s%s",
            result.returncode,
            pdf_path.name,
            f" ({stderr})" if stderr else "",
        )
        return "", "failed"

    text = result.stdout.strip()
    if not text:
        logger.warning("Tom text från %s", pdf_path.name)
        return "", "empty"
    return text, "pdftotext"


def _find_section_spans(text: str) -> list[tuple[str, int]]:
    hits: list[tuple[str, int]] = []
    for name, pattern in SECTION_PATTERNS:
        for match in pattern.finditer(text):
            line_start = text.rfind("\n", 0, match.start()) + 1
            hits.append((name, line_start))

    unique_hits: list[tuple[str, int]] = []
    seen_names: set[str] = set()
    seen_positions: set[tuple[str, int]] = set()
    for name, position in sorted(hits, key=lambda item: item[1]):
        key = (name, position)
        if name in seen_names or key in seen_positions:
            continue
        seen_names.add(name)
        seen_positions.add(key)
        unique_hits.append((name, position))

    return unique_hits


def split_into_sections(text: str) -> list[dict[str, str]]:
    """Split text into JO sections or return one `other` section on failure."""
    stripped = text.strip()
    if not stripped:
        return [{"section": "other", "section_title": SECTION_TITLES["other"], "text": ""}]

    spans = _find_section_spans(text)
    if not spans:
        return [{"section": "other", "section_title": SECTION_TITLES["other"], "text": stripped}]

    sections: list[dict[str, str]] = []
    first_start = spans[0][1]
    preamble = text[:first_start].strip()
    if preamble:
        sections.append(
            {
                "section": "other",
                "section_title": SECTION_TITLES["other"],
                "text": preamble,
            }
        )

    for index, (name, start) in enumerate(spans):
        end = spans[index + 1][1] if index + 1 < len(spans) else len(text)
        section_text = text[start:end].strip()
        if not section_text:
            continue
        sections.append(
            {
                "section": name,
                "section_title": SECTION_TITLES[name],
                "text": section_text,
            }
        )

    return sections or [{"section": "other", "section_title": SECTION_TITLES["other"], "text": stripped}]


def normalize_dnr(value: str) -> str:
    return value.strip().replace("/", "-")


def dnr_to_dok_id(dnr: str) -> str:
    return "jo_" + normalize_dnr(dnr).replace("-", "_")


def dnr_to_output_filename(dnr: str) -> str:
    return f"{dnr_to_dok_id(dnr)}.json"


def extract_dnr_from_pdf_path(pdf_path: Path) -> str | None:
    stem = pdf_path.stem
    if not stem.startswith("jo_"):
        return None
    dnr = normalize_dnr(stem[3:])
    if not dnr:
        return None
    return dnr


def load_metadata(dnr: str) -> dict[str, Any]:
    meta_path = META_DIR / f"jo_{normalize_dnr(dnr)}.json"
    if not meta_path.exists():
        return {}

    try:
        raw = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Kunde inte läsa metadata för %s: %s", dnr, exc)
        return {}

    return raw if isinstance(raw, dict) else {}


def build_document_payload(pdf_path: Path, *, text: str, sections: list[dict[str, str]]) -> dict[str, Any]:
    dnr = extract_dnr_from_pdf_path(pdf_path)
    if not dnr:
        raise ValueError(f"Kunde inte extrahera dnr från {pdf_path.name}")

    meta = load_metadata(dnr)
    return {
        "dok_id": dnr_to_dok_id(dnr),
        "dnr": dnr,
        "source_type": DEFAULT_SOURCE_TYPE,
        "document_subtype": DEFAULT_DOCUMENT_SUBTYPE,
        "authority_level": DEFAULT_AUTHORITY_LEVEL,
        "title": str(meta.get("titel") or meta.get("title") or "").strip(),
        "beslutsdatum": str(meta.get("beslutsdatum") or "").strip(),
        "pdf_url": str(meta.get("pdf_url") or "").strip(),
        "source_url": str(meta.get("source_url") or DEFAULT_SOURCE_URL).strip(),
        "text_content": text,
        "sections": sections,
        "extraction_method": "pdftotext",
        "schema_version": SCHEMA_VERSION,
        "license": DEFAULT_LICENSE,
    }


def convert_pdf(pdf_path: Path) -> tuple[dict[str, Any] | None, str]:
    """Convert one PDF and return (document_or_none, status)."""
    dnr = extract_dnr_from_pdf_path(pdf_path)
    if not dnr:
        logger.warning("Oväntat filnamn: %s", pdf_path.name)
        return None, "invalid_name"

    text, extraction_method = pdf_to_text(pdf_path)
    if extraction_method != "pdftotext":
        logger.warning("Skip %s (extraction_method=%s)", dnr, extraction_method)
        return None, extraction_method

    sections = split_into_sections(text)
    if not any(section["section"] == "bedomning" for section in sections):
        logger.debug("Ingen 'bedomning'-sektion i %s", dnr)

    return build_document_payload(pdf_path, text=text, sections=sections), "ok"


def count_bedomning_documents(results: list[dict[str, Any]]) -> int:
    return sum(
        1
        for result in results
        if any(section.get("section") == "bedomning" for section in result.get("sections", []))
    )


def log_bedomning_rate(results: list[dict[str, Any]]) -> None:
    total = len(results)
    if total == 0:
        return

    found = count_bedomning_documents(results)
    rate = found / total
    if rate < BEDOMNING_WARNING_THRESHOLD:
        logger.warning(
            "VARNING: 'bedomning'-sektion hittades i bara %.0f%% av besluten (%d/%d).",
            rate * 100,
            found,
            total,
        )
        return

    logger.info(
        "Sektionsigenkänning OK: 'bedomning' i %.0f%% av besluten (%d/%d).",
        rate * 100,
        found,
        total,
    )


def log_empty_text_escalation(*, empty_count: int, processed_count: int) -> None:
    if processed_count == 0:
        return
    empty_ratio = empty_count / processed_count
    if empty_ratio > EMPTY_TEXT_ESCALATION_THRESHOLD:
        logger.error(
            "ESKALERA: >20%% av PDF:er gav tom text (%d/%d, %.0f%%).",
            empty_count,
            processed_count,
            empty_ratio * 100,
        )


def iter_pdf_paths(max_docs: int | None) -> list[Path]:
    paths = sorted(PDF_DIR.glob("jo_*.pdf"))
    if max_docs is not None:
        return paths[:max_docs]
    return paths


def write_document_json(document: dict[str, Any]) -> Path:
    JSON_DIR.mkdir(parents=True, exist_ok=True)
    destination = JSON_DIR / dnr_to_output_filename(str(document["dnr"]))
    destination.write_text(
        json.dumps(document, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return destination


def run(max_docs: int | None, verbose: bool) -> int:
    setup_logging(verbose)
    JSON_DIR.mkdir(parents=True, exist_ok=True)

    pdf_paths = iter_pdf_paths(max_docs)
    logger.info("Konverterar %d PDF:er.", len(pdf_paths))

    ok_count = 0
    failed_count = 0
    empty_count = 0
    processed_count = 0
    results: list[dict[str, Any]] = []

    for index, pdf_path in enumerate(pdf_paths, start=1):
        if index == 1 or index % 100 == 0:
            logger.info(
                "Progress %d/%d | ok=%d | fel=%d | tomma=%d",
                index,
                len(pdf_paths),
                ok_count,
                failed_count,
                empty_count,
            )

        document, status = convert_pdf(pdf_path)
        processed_count += 1

        if document is None:
            failed_count += 1
            if status == "empty":
                empty_count += 1
            continue

        write_document_json(document)
        results.append(document)
        ok_count += 1

    log_bedomning_rate(results)
    log_empty_text_escalation(empty_count=empty_count, processed_count=processed_count)

    logger.info(
        "Klar. ok=%d | fel=%d | tomma=%d | totalt=%d",
        ok_count,
        failed_count,
        empty_count,
        processed_count,
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Konvertera JO-PDF:er till paragrafen.ai JSON-format.",
    )
    parser.add_argument("--max-docs", type=int, default=None, help="Begränsa antal dokument.")
    parser.add_argument("--verbose", action="store_true", help="Visa debug-loggning.")
    args = parser.parse_args()
    raise SystemExit(run(args.max_docs, args.verbose))


if __name__ == "__main__":
    main()
