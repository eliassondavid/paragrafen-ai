"""Normalize juridikbok-harvester raw documents into doktrin norm documents."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import logging
from functools import lru_cache
from pathlib import Path
import re
from typing import Any

import yaml

logger = logging.getLogger("paragrafenai.noop")

MAX_CHUNK_TOKENS = 600
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")
AUTHOR_SPLIT_RE = re.compile(r"\s+och\s+|,\s*")
TRANSLATION_TABLE = str.maketrans(
    {
        "å": "a",
        "ä": "a",
        "ö": "o",
        "é": "e",
        "Å": "a",
        "Ä": "a",
        "Ö": "o",
        "É": "e",
    }
)
LICENSE_URL = "https://creativecommons.org/licenses/by-nc/4.0/"
LICENSE_NAME = "CC BY-NC 4.0"
EXCLUDED_AREAS = {
    "straffrätt",
    "straffrätt_exkl",
    "skatterätt",
    "skatterätt_exkl",
    "migrationsrätt",
    "migrationsrätt_exkl",
    "utlänningsrätt",
}
SUBJECT_MAP = {
    "Avtalsrätt": ["civilrätt", "avtalsrätt"],
    "Familjerätt": ["civilrätt", "familjerätt"],
    "Fastighetsrätt": ["civilrätt", "fastighetsrätt"],
    "Arbetsrätt": ["arbetsrätt"],
    "Förvaltningsrätt": ["förvaltningsrätt"],
    "Processrätt": ["processrätt"],
    "Europarätt": ["eu_rätt"],
    "Marknadsrätt": ["civilrätt", "marknadsrätt"],
    "Straffrätt": ["straffrätt_exkl"],
    "Skatterätt": ["skatterätt_exkl"],
    "Migrationsrätt": ["migrationsrätt_exkl"],
    "Utlänningsrätt": ["migrationsrätt_exkl"],
    "Förmögenhetsrätt": ["civilrätt", "avtalsrätt"],
    "Konkurrensrätt": ["konkurrensrätt"],
    "Immaterialrätt": ["immaterialrätt"],
    "Europarätt och EU-rätt": ["eu_rätt"],
    "EU-rätt": ["eu_rätt"],
    "Associationsrätt": ["associationsrätt"],
    "Bolagsrätt": ["bolagsrätt"],
    "Skadeståndsrätt": ["civilrätt", "skadeståndsrätt"],
    "Köprätt": ["civilrätt", "avtalsrätt", "köprätt"],
    "Socialrätt": ["socialrätt"],
    "Miljörätt": ["miljörätt"],
    "Folkrätt": ["folkrätt"],
    "Transporträtt": ["transporträtt"],
    "Offentlig rätt": ["offentlig rätt"],
    "Statsrätt": ["statsrätt"],
    "Processrätt och exekutionsrätt": ["processrätt"],
    "Fastighetsrätt och miljörätt": ["civilrätt", "fastighetsrätt", "miljörätt"],
    "Allmän civilrätt": ["civilrätt"],
    "Allmän rättslära": ["rättslära"],
    "Allmänna verk": ["rättslära"],
    "Exekutionsrätt": ["processrätt", "exekutionsrätt"],
    "Festskrifter": ["övrigt"],
    "Försäkringsrätt": ["civilrätt", "försäkringsrätt"],
    "Internationell privat- och processrätt": ["internationell_rätt", "processrätt"],
    "Internationell rätt": ["internationell_rätt"],
    "Rättshistoria": ["rättshistoria"],
    "Sakrätt": ["civilrätt", "sakrätt"],
    "Sjö- och transporträtt": ["civilrätt", "transporträtt"],
    "Skiljeförfarande": ["processrätt", "skiljeförfarande"],
}


def normalize_one(
    metadata: dict[str, Any],
    extracted: dict[str, Any],
    *,
    legal_areas_config_path: str | Path = "config/legal_areas.yaml",
    output_basename_value: str | None = None,
    urn_suffix: str = "",
) -> dict[str, Any] | None:
    filename = str(metadata.get("filename") or extracted.get("filename") or "").strip()
    if not filename:
        logger.warning("Hoppar över doktrinpost utan filename: %s", metadata.get("title", "okänd"))
        return None

    author = select_author(metadata, extracted)
    title = str(metadata.get("title") or extracted.get("title") or "").strip()
    year = _coerce_int(metadata.get("year") or extracted.get("year"))
    if not author or not title or not year:
        logger.warning("Hoppar över ofullständig doktrinpost: %s", filename)
        return None

    authors, is_edited_volume = parse_authors(author)
    author_last_norm = normalize_author_last(metadata.get("author_last"), author)
    legal_areas = classify_legal_areas(
        metadata.get("subjects") or extracted.get("subjects") or [],
        title=title,
        work_type=str(metadata.get("work_type") or extracted.get("work_type") or ""),
        config_path=legal_areas_config_path,
    )
    excluded_at_retrieval = should_exclude_at_retrieval(legal_areas, title=title)

    pages = extract_pages(extracted)
    if not pages:
        logger.warning("Inga användbara sidor för %s", filename)
        return None

    source_subtype = determine_source_subtype(pages, extracted)
    citation_hd_value = build_citation_hd(metadata, author, title, year)
    citation_academic_value = build_citation_academic(metadata, authors, title, year)

    chunk_units = build_chunk_units(pages)
    if not chunk_units:
        logger.warning("Kunde inte skapa chunk-enheter för %s", filename)
        return None

    chunks = assemble_chunks(
        chunk_units,
        metadata=metadata,
        author=author,
        authors=authors,
        author_last_norm=author_last_norm,
        is_edited_volume=is_edited_volume,
        title=title,
        year=year,
        legal_areas=legal_areas,
        excluded_at_retrieval=excluded_at_retrieval,
        source_subtype=source_subtype,
        citation_hd_value=citation_hd_value,
        citation_academic_value=citation_academic_value,
        urn_suffix=urn_suffix,
    )
    if not chunks:
        logger.warning("Inga norm-chunks skapades för %s", filename)
        return None

    result: dict[str, Any] = {
        "filename": filename,
        "output_basename": output_basename_value or output_basename(filename),
        "source_type": "doktrin",
        "source_subtype": source_subtype,
        "title": title,
        "author": author,
        "author_last": author_last_norm,
        "authors": authors,
        "is_edited_volume": is_edited_volume,
        "year": year,
        "edition": max(_coerce_int(metadata.get("edition") or extracted.get("edition") or 1), 1),
        "authority_level": "persuasive",
        "legal_area": legal_areas,
        "excluded_at_retrieval": excluded_at_retrieval,
        "citation_hd": citation_hd_value,
        "citation_academic": citation_academic_value,
        "work_type": str(metadata.get("work_type") or extracted.get("work_type") or ""),
        "publisher": str(metadata.get("publisher") or "").strip(),
        "series": str(metadata.get("series") or "").strip(),
        "source_url": str(metadata.get("source_url") or "").strip(),
        "pdf_url": str(metadata.get("pdf_url") or "").strip(),
        "urn": str(metadata.get("urn") or "").strip(),
        "license": LICENSE_NAME,
        "license_url": LICENSE_URL,
        "chunk_count": len(chunks),
        "pages_indexed": len(pages),
        "total_pages": _coerce_int(extracted.get("total_pages")),
        "chunks": chunks,
    }

    isbn = str(metadata.get("isbn") or extracted.get("isbn") or "").strip()
    if isbn:
        result["isbn"] = isbn

    return result


def normalize_all(
    raw_dir: str | Path = "data/raw/doktrin",
    norm_dir: str | Path = "data/norm/doktrin",
    *,
    metadata_path: str | Path | None = None,
    extracted_dir: str | Path | None = None,
    force: bool = False,
    max_docs: int | None = None,
    legal_areas_config_path: str | Path = "config/legal_areas.yaml",
    skip_log_path: str | Path | None = None,
) -> dict[str, int]:
    raw_dir = Path(raw_dir)
    norm_dir = Path(norm_dir)
    norm_dir.mkdir(parents=True, exist_ok=True)

    metadata_path = Path(metadata_path) if metadata_path is not None else raw_dir / "metadata.json"
    extracted_dir = Path(extracted_dir) if extracted_dir is not None else raw_dir / "extracted_text"
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadatafil saknas: {metadata_path}")
    if not extracted_dir.exists():
        raise FileNotFoundError(f"Extracted-text-katalog saknas: {extracted_dir}")

    with metadata_path.open("r", encoding="utf-8") as fh:
        entries = json.load(fh)
    if not isinstance(entries, list):
        raise ValueError("metadata.json måste innehålla en lista.")

    counts = {"ok": 0, "skipped": 0, "failed": 0}
    docs = entries[:max_docs] if max_docs is not None else entries
    collision_counts = build_collision_counts(entries)
    skip_rows: list[dict[str, Any]] = []

    for metadata in docs:
        if not isinstance(metadata, dict):
            counts["failed"] += 1
            skip_rows.append(
                _skip_row(
                    reason="invalid_metadata_row",
                    message="Metadata-raden är inte ett objekt.",
                )
            )
            continue

        filename = str(metadata.get("filename") or "").strip()
        if not filename:
            logger.warning("Hoppar över metadata utan filename.")
            counts["skipped"] += 1
            skip_rows.append(
                _skip_row(
                    filename="",
                    reason="missing_filename",
                    message="Metadata saknar filename.",
                )
            )
            continue

        extracted_path = extracted_dir / f"{output_basename(filename)}.json"
        if not extracted_path.exists():
            logger.warning("Extracted text saknas för %s", filename)
            counts["skipped"] += 1
            skip_rows.append(
                _skip_row(
                    filename=filename,
                    reason="missing_extracted_text",
                    message=f"Extracted text saknas: {extracted_path.name}",
                )
            )
            continue

        resolved_basename = resolve_output_basename(metadata, collision_counts)
        urn_sfx = urn_suffix(str(metadata.get("urn") or ""))
        out_path = norm_dir / f"{resolved_basename}.json"
        if out_path.exists() and not force:
            counts["skipped"] += 1
            skip_rows.append(
                _skip_row(
                    filename=filename,
                    reason="already_exists",
                    message=f"Normfil finns redan: {out_path.name}",
                )
            )
            continue

        try:
            with extracted_path.open("r", encoding="utf-8") as fh:
                extracted = json.load(fh)
        except Exception as exc:
            logger.error("Kunde inte läsa %s: %s", extracted_path.name, exc)
            counts["failed"] += 1
            skip_rows.append(
                _skip_row(
                    filename=filename,
                    reason="read_error",
                    message=f"Kunde inte läsa extracted text: {exc}",
                )
            )
            continue

        result = normalize_one(
            metadata,
            extracted,
            legal_areas_config_path=legal_areas_config_path,
            output_basename_value=resolved_basename,
            urn_suffix=urn_sfx,
        )
        if result is None:
            counts["failed"] += 1
            skip_rows.append(
                _skip_row(
                    filename=filename,
                    reason="normalize_one_returned_none",
                    message="normalize_one returnerade None.",
                )
            )
            continue

        try:
            with out_path.open("w", encoding="utf-8") as fh:
                json.dump(result, fh, ensure_ascii=False, indent=2)
            counts["ok"] += 1
        except Exception as exc:
            logger.error("Kunde inte skriva %s: %s", out_path.name, exc)
            counts["failed"] += 1
            skip_rows.append(
                _skip_row(
                    filename=filename,
                    reason="write_error",
                    message=f"Kunde inte skriva normfil: {exc}",
                )
            )

    if skip_log_path is not None:
        write_skip_log(skip_log_path, skip_rows)
    return counts


def output_basename(filename: str) -> str:
    return Path(filename).stem


def build_collision_counts(entries: list[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        filename = str(entry.get("filename") or "").strip()
        if not filename:
            continue
        counts[filename] = counts.get(filename, 0) + 1
    return counts


def resolve_output_basename(metadata: dict[str, Any], collision_counts: dict[str, int]) -> str:
    filename = str(metadata.get("filename") or "").strip()
    base = output_basename(filename)
    if collision_counts.get(filename, 0) <= 1:
        return base
    suffix = urn_suffix(str(metadata.get("urn") or "").strip())
    return f"{base}_{suffix}" if suffix else base


def urn_suffix(urn: str) -> str:
    match = re.search(r"juridikbokse-([^:]+)$", urn or "")
    if match:
        return match.group(1)
    return ""


def select_author(metadata: dict[str, Any], extracted: dict[str, Any]) -> str:
    for candidate in (
        str(metadata.get("author") or "").strip(),
        str(extracted.get("author") or "").strip(),
        str(metadata.get("publisher") or "").strip(),
        "okand",
    ):
        if candidate:
            return candidate
    return "okand"


def parse_authors(author_raw: str) -> tuple[list[dict[str, str]], bool]:
    text = str(author_raw or "").strip()
    if not text:
        return ([], False)

    is_edited_volume = bool(re.search(r"\((?:red\.?|eds\.?)\)", text, re.IGNORECASE))
    cleaned = re.sub(r"\((?:red\.?|eds\.?)\)", "", text, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)

    if " m.fl." in cleaned:
        cleaned = cleaned.split(" m.fl.", 1)[0].strip()
        parts = [cleaned] if cleaned else []
    else:
        parts = [part.strip() for part in AUTHOR_SPLIT_RE.split(cleaned) if part.strip()]

    role = "editor" if is_edited_volume else "author"
    authors: list[dict[str, str]] = []
    for part in parts:
        first, last = split_person_name(part)
        if not first and not last:
            continue
        authors.append({"first": first, "last": last, "role": role})
    return (authors, is_edited_volume)


def split_person_name(name: str) -> tuple[str, str]:
    text = str(name or "").strip()
    if not text:
        return ("", "")
    if "," in text:
        last, first = [item.strip() for item in text.split(",", 1)]
        return (first, last)
    parts = text.split()
    if len(parts) == 1:
        return ("", parts[0])
    return (" ".join(parts[:-1]), parts[-1])


def normalize_author_last(author_last: Any, author: str) -> str:
    raw = str(author_last or "").strip()
    if not raw:
        _, raw = split_person_name(author)
    normalized = raw.translate(TRANSLATION_TABLE).lower()
    normalized = re.sub(r"[^a-z0-9\s_-]", "", normalized)
    normalized = re.sub(r"\s+", "_", normalized).strip("_")
    return normalized


def classify_legal_areas(
    subjects: list[Any],
    *,
    title: str,
    work_type: str,
    config_path: str | Path = "config/legal_areas.yaml",
) -> list[str]:
    raw_areas: list[str] = []
    for subject in subjects or []:
        mapped = SUBJECT_MAP.get(str(subject).strip())
        if mapped:
            raw_areas.extend(mapped)
        elif str(subject).strip():
            logger.warning("Okänt doktrin-subject, lämnas utan mappning: %s", subject)

    if not raw_areas:
        inferred = infer_legal_areas_from_text(title=title, work_type=work_type)
        raw_areas.extend(inferred)

    return normalize_legal_areas(raw_areas, config_path=config_path)


def infer_legal_areas_from_text(*, title: str, work_type: str) -> list[str]:
    haystack = f"{title} {work_type}".lower()
    inferred: list[str] = []
    keyword_map = {
        "avtal": ["civilrätt", "avtalsrätt"],
        "köp": ["civilrätt", "köprätt"],
        "hyra": ["civilrätt", "fastighetsrätt"],
        "skilje": ["processrätt"],
        "miljö": ["miljörätt"],
        "marknad": ["civilrätt", "marknadsrätt"],
        "konkurrens": ["konkurrensrätt"],
        "arbets": ["arbetsrätt"],
        "familj": ["civilrätt", "familjerätt"],
        "förvalt": ["förvaltningsrätt"],
        "skatt": ["skatterätt_exkl"],
        "straff": ["straffrätt_exkl"],
        "migration": ["migrationsrätt_exkl"],
        "utlänning": ["migrationsrätt_exkl"],
    }
    history_keywords = ["historia", "rättshistoria", "historisk", "trolldom", "rättshistorisk"]
    process_keywords = [
        "rättegång",
        "tvistemål",
        "brottmål",
        "rättskraft",
        "forum",
        "bevisrätt",
        "kvarstad",
        "överklagande",
        "rättegångskostnad",
        "processrätt",
    ]
    for needle, areas in keyword_map.items():
        if needle in haystack:
            inferred.extend(areas)
    if any(keyword in haystack for keyword in history_keywords):
        inferred.append("rättshistoria")
    if work_type == "Kommentar" or any(keyword in haystack for keyword in process_keywords):
        inferred.append("processrätt")
    return inferred


def should_exclude_at_retrieval(legal_areas: list[str], *, title: str) -> bool:
    legal_area_set = set(legal_areas)
    if "processrätt" in legal_area_set and legal_area_set.isdisjoint(EXCLUDED_AREAS):
        return False
    if legal_area_set.intersection(EXCLUDED_AREAS):
        if "process" in title.lower() and "processrätt" in legal_area_set:
            return False
        return True
    return False


def extract_pages(extracted: dict[str, Any]) -> list[dict[str, Any]]:
    usable: list[dict[str, Any]] = []
    for page in extracted.get("pages", []) or []:
        if should_skip_page(page):
            continue
        text = str(page.get("text") or "").strip()
        if not text:
            continue
        usable.append(
            {
                "page_num": _coerce_int(page.get("page_num")),
                "text": text,
                "quality_score": _coerce_float(page.get("quality_score"), default=0.0),
                "method": normalize_page_method(page.get("method")),
                "char_count": _coerce_int(page.get("char_count")),
            }
        )
    return usable


def should_skip_page(page: dict[str, Any]) -> bool:
    text = str(page.get("text") or "")
    page_num = _coerce_int(page.get("page_num"))
    char_count = _coerce_int(page.get("char_count"))
    quality_score = _coerce_float(page.get("quality_score"), default=0.0)

    if char_count < 50:
        return True
    if quality_score < 0.3:
        logger.warning("Hoppar över lågkvalitetssida %s", page_num)
        return True
    if page_num <= 4 and is_license_page(text):
        return True
    return False


def is_license_page(text: str) -> bool:
    haystack = (text or "").lower()
    return "creative commons" in haystack or "juridikbok.se" in haystack


def normalize_page_method(method: Any) -> str:
    value = str(method or "").strip().lower()
    if value in {"native", "pdftotext"}:
        return "native"
    if value == "ocr":
        return "ocr"
    return value or "native"


def determine_source_subtype(pages: list[dict[str, Any]], extracted: dict[str, Any]) -> str:
    methods = {page["method"] for page in pages}
    stats = extracted.get("extraction_stats") or {}
    if _coerce_int(stats.get("ocr_pages")) > 0 or "ocr" in methods:
        if methods and methods.issubset({"ocr"}):
            return "monografi_ocr"
        if "native" not in methods:
            return "monografi_ocr"
    return "monografi_digital"


def build_chunk_units(pages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    units: list[dict[str, Any]] = []
    for page in pages:
        parts = split_long_text(page["text"])
        for part in parts:
            units.append(
                {
                    "page_start": page["page_num"],
                    "page_end": page["page_num"],
                    "text": part,
                    "token_count": count_tokens(part),
                    "quality_score": page["quality_score"],
                    "method": page["method"],
                }
            )
    return units


def assemble_chunks(
    units: list[dict[str, Any]],
    *,
    metadata: dict[str, Any],
    author: str,
    authors: list[dict[str, str]],
    author_last_norm: str,
    is_edited_volume: bool,
    title: str,
    year: int,
    legal_areas: list[str],
    excluded_at_retrieval: bool,
    source_subtype: str,
    citation_hd_value: str,
    citation_academic_value: str,
    urn_suffix: str = "",
) -> list[dict[str, Any]]:
    chunks: list[dict[str, Any]] = []
    cursor = 0
    while cursor < len(units):
        token_total = 0
        group: list[dict[str, Any]] = []
        while cursor < len(units):
            unit = units[cursor]
            if group and token_total + unit["token_count"] > MAX_CHUNK_TOKENS:
                break
            group.append(unit)
            token_total += unit["token_count"]
            cursor += 1
        if not group:
            cursor += 1
            continue

        page_start = group[0]["page_start"]
        page_end = group[-1]["page_end"]
        chunk_index = len(chunks)
        chunk: dict[str, Any] = {
            "id": build_namespace(author_last_norm, year, page_start, chunk_index, urn_suffix),
            "source_type": "doktrin",
            "source_subtype": source_subtype,
            "text": "\n\n".join(item["text"] for item in group).strip(),
            "title": title,
            "author": author,
            "author_last": author_last_norm,
            "authors": json.dumps(authors, ensure_ascii=False),
            "is_edited_volume": is_edited_volume,
            "year": year,
            "edition": max(_coerce_int(metadata.get("edition") or 1), 1),
            "authority_level": "persuasive",
            "legal_area": json.dumps(legal_areas, ensure_ascii=False),
            "excluded_at_retrieval": excluded_at_retrieval,
            "citation_hd": citation_hd_value,
            "citation_academic": citation_academic_value,
            "chunk_index": chunk_index,
            "page_start": page_start,
            "page_end": page_end,
            "pinpoint": build_pinpoint(page_start, page_end),
            "references_to": "[]",
            "avg_quality": round(sum(item["quality_score"] for item in group) / len(group), 4),
            "extraction_method": determine_chunk_extraction_method(group),
            "license": LICENSE_NAME,
            "license_url": LICENSE_URL,
            "filename": str(metadata.get("filename") or "").strip(),
            "source_url": str(metadata.get("source_url") or "").strip(),
            "urn": str(metadata.get("urn") or "").strip(),
        }

        if chunk["avg_quality"] < 0.5:
            chunk["low_quality"] = True

        for optional_key in ("publisher", "series", "work_type"):
            optional_value = str(metadata.get(optional_key) or "").strip()
            if optional_value:
                chunk[optional_key] = optional_value

        isbn = str(metadata.get("isbn") or "").strip()
        if isbn:
            chunk["isbn"] = isbn

        chunks.append(chunk)

    total = len(chunks)
    for chunk in chunks:
        chunk["chunk_total"] = total
    return chunks


def determine_chunk_extraction_method(group: list[dict[str, Any]]) -> str:
    methods = {item["method"] for item in group}
    if methods == {"ocr"}:
        return "ocr"
    if "ocr" in methods and "native" in methods:
        return "mixed"
    return "native"


def build_namespace(
    author_last_norm: str,
    year: int,
    page_start: int,
    chunk_index: int,
    urn_suffix: str = "",
) -> str:
    if urn_suffix:
        return f"doktrin::{author_last_norm}_{year}_{urn_suffix}_s{page_start:03d}_chunk_{chunk_index:03d}"
    return f"doktrin::{author_last_norm}_{year}_s{page_start:03d}_chunk_{chunk_index:03d}"


def build_pinpoint(page_start: int, page_end: int) -> str:
    if page_start <= 0:
        return ""
    if page_start == page_end:
        return f"s. {page_start}"
    return f"s. {page_start}–{page_end}"


def build_citation_hd(metadata: dict[str, Any], author: str, title: str, year: int) -> str:
    hd_citation = str(metadata.get("hd_citation") or "").strip()
    if hd_citation and not hd_citation.startswith(","):
        return hd_citation
    edition = max(_coerce_int(metadata.get("edition") or 1), 1)
    if edition <= 1:
        return f"{author}, {title}, {year}"
    return f"{author}, {title}, {edition} uppl. {year}"


def build_citation_academic(
    metadata: dict[str, Any],
    authors: list[dict[str, str]],
    title: str,
    year: int,
) -> str:
    author_part = format_academic_author_list(authors) or str(metadata.get("author") or "").strip()
    edition = max(_coerce_int(metadata.get("edition") or 1), 1)
    publisher = str(metadata.get("publisher") or "").strip()
    pieces = [author_part, title]
    if edition > 1:
        pieces.append(f"{edition} uppl.")
    if publisher:
        pieces.append(publisher)
    pieces.append(str(year))
    return ", ".join(piece for piece in pieces if piece)


def format_academic_author_list(authors: list[dict[str, str]]) -> str:
    formatted: list[str] = []
    for author in authors:
        first = str(author.get("first") or "").strip()
        last = str(author.get("last") or "").strip()
        if last and first:
            formatted.append(f"{last}, {first}")
        elif last:
            formatted.append(last)
        elif first:
            formatted.append(first)

    if not formatted:
        return ""
    if len(formatted) == 1:
        return formatted[0]
    if len(formatted) == 2:
        return " och ".join(formatted)
    return ", ".join(formatted[:-1]) + " och " + formatted[-1]


def split_long_text(text: str) -> list[str]:
    if count_tokens(text) <= MAX_CHUNK_TOKENS:
        return [text.strip()]

    paragraphs = split_paragraphs(text)
    units: list[str] = []
    for paragraph in paragraphs:
        if count_tokens(paragraph) <= MAX_CHUNK_TOKENS:
            units.append(paragraph)
            continue
        units.extend(split_long_paragraph(paragraph))

    chunks: list[str] = []
    current: list[str] = []
    token_total = 0
    for unit in units:
        unit_tokens = count_tokens(unit)
        if current and token_total + unit_tokens > MAX_CHUNK_TOKENS:
            chunks.append("\n\n".join(current).strip())
            current = [unit]
            token_total = unit_tokens
            continue
        current.append(unit)
        token_total += unit_tokens

    if current:
        chunks.append("\n\n".join(current).strip())
    return chunks


def split_paragraphs(text: str) -> list[str]:
    paragraphs = [paragraph.strip() for paragraph in re.split(r"\n\s*\n+", text or "") if paragraph.strip()]
    if paragraphs:
        return paragraphs
    return [line.strip() for line in (text or "").splitlines() if line.strip()]


def split_long_paragraph(paragraph: str) -> list[str]:
    sentences = [sentence.strip() for sentence in SENTENCE_SPLIT_RE.split(paragraph) if sentence.strip()]
    if not sentences:
        return split_sentence_by_words(paragraph)

    chunks: list[str] = []
    current: list[str] = []
    token_total = 0
    for sentence in sentences:
        sentence_tokens = count_tokens(sentence)
        if sentence_tokens > MAX_CHUNK_TOKENS:
            if current:
                chunks.append(" ".join(current).strip())
                current = []
                token_total = 0
            chunks.extend(split_sentence_by_words(sentence))
            continue
        if current and token_total + sentence_tokens > MAX_CHUNK_TOKENS:
            chunks.append(" ".join(current).strip())
            current = [sentence]
            token_total = sentence_tokens
            continue
        current.append(sentence)
        token_total += sentence_tokens

    if current:
        chunks.append(" ".join(current).strip())
    return chunks


def split_sentence_by_words(text: str) -> list[str]:
    words = text.split()
    return [
        " ".join(words[index : index + MAX_CHUNK_TOKENS]).strip()
        for index in range(0, len(words), MAX_CHUNK_TOKENS)
        if words[index : index + MAX_CHUNK_TOKENS]
    ]


def count_tokens(text: str) -> int:
    return len((text or "").split())


def _skip_row(
    *,
    reason: str,
    message: str,
    filename: str | None = None,
) -> dict[str, str]:
    row = {
        "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
        "reason": reason,
        "message": message,
    }
    if filename is not None:
        row["filename"] = filename
    return row


def write_skip_log(path_value: str | Path, rows: list[dict[str, Any]]) -> None:
    path = Path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


@lru_cache(maxsize=4)
def load_legal_area_ids(config_path: str | Path) -> set[str]:
    path = Path(config_path)
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[1] / path
    if not path.exists():
        logger.warning("legal_areas-config saknas: %s", path)
        return set()

    with path.open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh) or {}

    items = payload.get("areas")
    if not items:
        items = payload.get("legal_areas")
    ids: set[str] = set()
    for item in items or []:
        area_id = str((item or {}).get("id") or "").strip()
        if area_id:
            ids.add(area_id)
    return ids


def normalize_legal_areas(raw_areas: list[str], *, config_path: str | Path) -> list[str]:
    valid_ids = load_legal_area_ids(config_path)
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in raw_areas:
        value = str(raw or "").strip()
        if not value or value in seen:
            continue
        if valid_ids and value not in valid_ids:
            logger.warning("legal_area utanför config/legal_areas.yaml behålls: %s", value)
        seen.add(value)
        normalized.append(value)
    return normalized


def _coerce_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _coerce_float(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Normalize raw doktrin documents.")
    parser.add_argument("--raw-dir", default="data/raw/doktrin")
    parser.add_argument("--norm-dir", default="data/norm/doktrin")
    parser.add_argument("--metadata", default=None)
    parser.add_argument("--extracted", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--skip-log", default=None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--max-docs", type=int, default=None)
    args = parser.parse_args(argv)

    counts = normalize_all(
        raw_dir=args.raw_dir,
        norm_dir=args.output or args.norm_dir,
        metadata_path=args.metadata,
        extracted_dir=args.extracted,
        force=args.force,
        max_docs=args.max_docs,
        skip_log_path=args.skip_log,
    )
    print(counts)
    return 0 if counts["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
