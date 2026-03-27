#!/usr/bin/env python3
"""socialstyrelsen_converter.py — PDF till strukturerade JSON-filer."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
import subprocess
import sys
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

RAW_DIR = Path("data/raw/socialstyrelsen")
CATALOG_PATH = RAW_DIR / "catalog.json"
JSON_DIR = RAW_DIR / "json"
ERRORS_PATH = RAW_DIR / "convert_errors.jsonl"
MANDATORY_MAP = {
    "113": "halso_sjukvard",
    "114": "socialtjanst",
    "115": "tandvard",
}
SECTION_PATTERNS = [
    ("allmant_rad", re.compile(r"(?m)^[ \t]*Allm(?:änna|änt)\s+råd\b", re.IGNORECASE)),
    ("foreskrift_kap", re.compile(r"(?m)^\d+\s+kap\.")),
    ("foreskrift_par", re.compile(r"(?m)^\s*\d+\s*§\s")),
    ("bemyndigande", re.compile(r"(?m)^Bemyndigande\b", re.IGNORECASE)),
    ("ikraftträdande", re.compile(r"(?m)^(?:Ikraftträdande|Denna .{0,30}träder i kraft)", re.IGNORECASE)),
    ("bilaga", re.compile(r"(?m)^Bilaga\b", re.IGNORECASE)),
]
SCHEMA_VERSION = "v0.15"
LICENSE = "public_domain"


def log(message: str) -> None:
    print(message, flush=True)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def load_catalog(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"catalog.json saknas: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("catalog.json måste innehålla en lista.")
    return [item for item in payload if isinstance(item, dict)]


def pdf_to_text(pdf_path: Path) -> str:
    """Extrahera text från PDF med pdftotext."""
    result = subprocess.run(
        ["pdftotext", "-layout", str(pdf_path), "-"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(f"pdftotext misslyckades: {result.stderr.strip()}")
    return result.stdout


def extract_foreskrift_number(name: str) -> dict[str, Any]:
    """
    Extrahera föreskriftsnummer och år.
    Returnerar {'samling': 'SOSFS', 'year': 2014, 'number': 5, 'nummer': 'SOSFS 2014:5'}.
    """
    match = re.match(r"(SOSFS|HSLF-FS)\s+(\d{4}):(\d+)", name)
    if match:
        samling = match.group(1)
        year = int(match.group(2))
        number = int(match.group(3))
        return {
            "samling": samling,
            "year": year,
            "number": number,
            "nummer": f"{samling} {year}:{number}",
        }
    return {}


def derive_title(name: str) -> str:
    title = re.sub(r"^(SOSFS|HSLF-FS)\s+\d{4}:\d+\s*", "", name).strip(" -–")
    return title or name.strip()


def get_authority_level(section: str) -> str:
    if section == "allmant_rad":
        return "guiding"
    return "binding"


def split_into_sections(text: str) -> list[dict[str, Any]]:
    """
    Dela upp text i sektioner baserat på SECTION_PATTERNS.
    Allmänna råd sträcker sig från rubrik till nästa sektion-rubrik.
    """
    boundaries: list[tuple[int, str, str]] = []
    for section_type, pattern in SECTION_PATTERNS:
        for match in pattern.finditer(text):
            boundaries.append((match.start(), section_type, match.group()))

    boundaries.sort(key=lambda item: item[0])

    if not boundaries:
        stripped = text.strip()
        return [{"section": "other", "section_heading": "Dokumenttext", "text": stripped, "start_pos": 0}] if stripped else []

    sections: list[dict[str, Any]] = []

    if boundaries[0][0] > 0:
        preamble = text[: boundaries[0][0]].strip()
        if preamble:
            sections.append(
                {
                    "section": "foreskrift",
                    "section_heading": "Inledning",
                    "text": preamble,
                    "start_pos": 0,
                }
            )

    for index, (position, section_type, heading) in enumerate(boundaries):
        end_pos = boundaries[index + 1][0] if index + 1 < len(boundaries) else len(text)
        section_text = text[position:end_pos].strip()
        if section_text:
            sections.append(
                {
                    "section": section_type,
                    "section_heading": heading.strip(),
                    "text": section_text,
                    "start_pos": position,
                }
            )

    return sections


def make_dok_id(doc_subtype: str, year: int, number: int) -> str:
    prefix = "sosfs" if doc_subtype == "sosfs" else "hslf_fs"
    return f"{prefix}_{year}_{number}"


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def convert_entry(entry: dict[str, Any]) -> dict[str, Any] | None:
    doc_type = str(entry.get("doc_type") or "").strip()
    if doc_type not in {"sosfs", "hslf_fs"}:
        return None

    number_info = extract_foreskrift_number(str(entry.get("name") or "").strip())
    if not number_info:
        return None

    pdf_path_value = str(entry.get("pdf_path") or "").strip()
    pdf_path = Path(pdf_path_value)
    if not pdf_path.is_absolute():
        pdf_path = Path(__file__).resolve().parent.parent / pdf_path
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF saknas: {pdf_path}")

    text_content = pdf_to_text(pdf_path).strip()
    if len(text_content) < 100:
        raise ValueError("TOM TEXT")

    sections = split_into_sections(text_content)
    sections_with_authority = []
    for section in sections:
        sections_with_authority.append(
            {
                "section": section["section"],
                "section_heading": section["section_heading"],
                "text": section["text"],
                "authority_level": get_authority_level(section["section"]),
            }
        )

    mandatory_area = MANDATORY_MAP.get(str(entry.get("mandatoryId") or "").strip())
    mandatory_areas = [mandatory_area] if mandatory_area else []
    dok_id = make_dok_id(doc_type, number_info["year"], number_info["number"])

    converted = {
        "dok_id": dok_id,
        "nummer": number_info["nummer"],
        "titel": derive_title(str(entry.get("name") or "").strip()),
        "source_type": "foreskrift",
        "document_subtype": doc_type,
        "samling": number_info["samling"],
        "myndighet": "Socialstyrelsen",
        "artikelnummer": str(entry.get("articleNumber") or "").strip(),
        "year": number_info["year"],
        "mandatory_areas": mandatory_areas,
        "sections": sections_with_authority,
        "total_sections": len(sections_with_authority),
        "allmant_rad_sections": sum(1 for section in sections_with_authority if section["section"] == "allmant_rad"),
        "text_content": text_content,
        "char_count": len(text_content),
        "extraction_method": "pdftotext",
        "schema_version": SCHEMA_VERSION,
        "license": LICENSE,
    }
    return converted


def run(*, max_docs: int | None, verbose: bool) -> dict[str, int]:
    catalog = load_catalog(CATALOG_PATH)
    ERRORS_PATH.write_text("", encoding="utf-8")
    JSON_DIR.mkdir(parents=True, exist_ok=True)

    eligible = [entry for entry in catalog if str(entry.get("doc_type") or "") in {"sosfs", "hslf_fs"}]
    if max_docs is not None:
        eligible = eligible[:max_docs]

    attempted = 0
    converted_total = 0
    error_total = 0

    for index, entry in enumerate(eligible, start=1):
        attempted += 1
        try:
            converted = convert_entry(entry)
            if converted is None:
                continue
            output_path = JSON_DIR / f"{converted['dok_id']}.json"
            save_json(output_path, converted)
            converted_total += 1
            if verbose:
                log(f"[{index}/{len(eligible)}] {converted['nummer']} — OK ({converted['total_sections']} sektioner)")
        except Exception as exc:
            error_total += 1
            append_jsonl(
                ERRORS_PATH,
                {
                    "articleNumber": entry.get("articleNumber"),
                    "name": entry.get("name"),
                    "pdf_path": entry.get("pdf_path"),
                    "error": str(exc),
                },
            )
            if verbose:
                log(f"[{index}/{len(eligible)}] {entry.get('name')} — FEL ({exc})")

    if attempted > 0 and (error_total / attempted) > 0.30:
        raise RuntimeError("ESKALERA — för många tomma PDFer")

    return {
        "eligible": len(eligible),
        "attempted": attempted,
        "converted": converted_total,
        "errors": error_total,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Konvertera Socialstyrelsens PDF:er till JSON.")
    parser.add_argument("--max-docs", type=int, default=None, help="Begränsa antal dokument.")
    parser.add_argument("--verbose", action="store_true", help="Visa logg per dokument.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    stats = run(max_docs=args.max_docs, verbose=args.verbose)
    log(
        "Klart: "
        f"eligible={stats['eligible']} "
        f"attempted={stats['attempted']} "
        f"converted={stats['converted']} "
        f"errors={stats['errors']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
