"""
§AI — svensk-forarbete — F-3 (uppdaterad F-4)
Modul: metadata_builder.py

Konstruerar fullständigt chunk-metadata-schema per chunk.
Namespace-format: forarbete::sou_{year}_{number:03d}_chunk_{chunk_index:03d}

Uppdateringar mot ursprunglig F-3-version:
  - Lägger till citation_format ("SOU 2015:31 s. 42")
  - Lägger till forarbete_type ("sou") — alias för doc_type
  - Lägger till beteckning som alias för sou_designation (spec-kompatibilitet)
  - SOU-nummer nollpaddas INTE i namespace (Ö5)

related_sfs extraheras via SFS_PATTERN och valideras mot år/nummerspan.
legal_area normaliseras mot config/legal_areas.yaml.
"""
from __future__ import annotations

import re
import uuid
from pathlib import Path
from typing import Any

import yaml

try:
    from pai_logging import warning as log_warning
except ImportError:
    import logging as _stdlib
    _logger = _stdlib.getLogger("paragrafenai.noop")
    def log_warning(msg, **kwargs):  # type: ignore[assignment]
        _logger.warning("%s %s", msg, kwargs if kwargs else "")


_CONFIG_PATH = Path(__file__).parent / "config" / "sou_api_config.yaml"


def _load_config() -> dict[str, Any]:
    with open(_CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _load_legal_areas(cfg: dict[str, Any]) -> set[str]:
    areas_file = Path(__file__).parent / cfg.get(
        "legal_areas_file", "config/legal_areas.yaml"
    )
    with open(areas_file, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return {a.lower() for a in data.get("areas", [])}


# ---------------------------------------------------------------------------
# Namespace-konstruktion
# ---------------------------------------------------------------------------
def build_namespace(year: int, number: int, chunk_index: int) -> str:
    """
    forarbete::sou_{year}_{number}_chunk_{chunk_index:03d}
    Exempel: forarbete::sou_1969_46_chunk_003

    OBS: SOU-nummer nollpaddas INTE (beslut Ö5 — inga kollisioner).
    Sidnummer ingår inte i namespace-formatet utan i page_start/page_end.
    """
    return f"forarbete::sou_{year}_{number}_chunk_{chunk_index:03d}"


# ---------------------------------------------------------------------------
# citation_format
# ---------------------------------------------------------------------------
def build_citation_format(year: int, number: int, page_start: int) -> str:
    """
    Returnerar formell citationsformat, t.ex. "SOU 2015:31 s. 42".
    Om page_start är -1 (okänd) utelämnas sidangivelsen.
    """
    base = f"SOU {year}:{number}"
    if page_start >= 0:
        return f"{base} s. {page_start}"
    return base


# ---------------------------------------------------------------------------
# SFS-extraktion
# ---------------------------------------------------------------------------
def extract_related_sfs(text: str, cfg: dict[str, Any]) -> list[str]:
    """
    Extraherar SFS-nummer ur löptext.
    Returnerar deduplicerad lista, t.ex. ["1949:381", "1998:204"].
    Kantfall 4: falskt positiva accepteras — filtreras i retrieval.
    """
    sfs_cfg = cfg.get("sfs_extraction", {})
    pattern = sfs_cfg.get("pattern", r'\b(\d{4})\s*:\s*(\d+)\b')
    year_min = int(sfs_cfg.get("year_min", 1700))
    year_max = int(sfs_cfg.get("year_max", 2030))
    num_min = int(sfs_cfg.get("number_min", 1))
    num_max = int(sfs_cfg.get("number_max", 9999))

    matches = re.findall(pattern, text)
    seen: set[str] = set()
    result: list[str] = []
    for year_str, num_str in matches:
        year = int(year_str)
        num = int(num_str)
        if year_min <= year <= year_max and num_min <= num <= num_max:
            sfs = f"{year}:{num}"
            if sfs not in seen:
                seen.add(sfs)
                result.append(sfs)
    return result


# ---------------------------------------------------------------------------
# legal_area-normalisering (placeholder — Haiku 4.5 runtime tar över)
# ---------------------------------------------------------------------------
def normalize_legal_area(
    raw_areas: list[str],
    valid_areas: set[str],
) -> list[str]:
    """
    Normaliserar legal_area mot legal_areas.yaml.
    I Fas 1: enkel lowercase-match; Haiku 4.5 tar över vid runtime ingest.
    """
    normalized: list[str] = []
    for area in raw_areas:
        a = area.lower().strip()
        if a in valid_areas:
            normalized.append(a)
    return normalized


# ---------------------------------------------------------------------------
# SOU-beteckning (t.ex. "SOU 2015:31") → (year, number)
# ---------------------------------------------------------------------------
def parse_sou_designation(designation: str) -> tuple[int, int]:
    """
    Parsar beteckning av typen "SOU 2015:31" eller "1969:46".
    Returnerar (year, number) som int-par.
    """
    m = re.search(r'(\d{4})\s*:\s*(\d+)', designation)
    if m:
        return int(m.group(1)), int(m.group(2))
    return 0, 0


# ---------------------------------------------------------------------------
# Huvud-funktion: build_chunk_metadata
# ---------------------------------------------------------------------------
def build_chunk_metadata(
    chunk: dict[str, Any],
    sou_api_doc: dict[str, Any],
    source_id: str | None = None,
) -> dict[str, Any]:
    """
    Konstruerar fullständigt metadata-schema för ett chunk.

    Args:
        chunk:       Output från chunker.chunk_sou (ett element).
        sou_api_doc: Rådata från sou_fetcher (id, namn, ar, nummer, titel, url, sha256).
        source_id:   UUID för dokumentet; genereras om None.

    Returns:
        Komplett metadata-dict enligt F-3/F-4-schemat.
    """
    cfg = _load_config()
    valid_areas = _load_legal_areas(cfg)

    # Dokumentnivå-metadata
    sou_id = str(sou_api_doc.get("id", ""))
    raw_designation = str(sou_api_doc.get("namn", ""))
    year_raw = sou_api_doc.get("ar", 0)
    nummer_raw = sou_api_doc.get("nummer", 0)
    titel = str(sou_api_doc.get("titel", ""))
    url = str(sou_api_doc.get("url", ""))
    sha256 = str(sou_api_doc.get("sha256", ""))

    # Parsning av år/nummer — API-fält föredras, beteckning som fallback
    try:
        year = int(year_raw)
    except (ValueError, TypeError):
        year, _ = parse_sou_designation(raw_designation)

    try:
        number = int(nummer_raw)
    except (ValueError, TypeError):
        _, number = parse_sou_designation(raw_designation)

    if year == 0 or number == 0:
        log_warning(
            "Kunde inte parsa år/nummer",
            sou_id=sou_id,
            designation=raw_designation,
        )

    # Beteckning i formell form
    beteckning = f"SOU {year}:{number}" if year and number else raw_designation

    # source_id — UUID
    if source_id is None:
        source_id = str(uuid.uuid4())

    # Chunk-nivå
    chunk_index: int = chunk.get("chunk_index", 0)
    chunk_total: int = chunk.get("chunk_total", 1)
    text: str = chunk.get("text", "")

    section_type: str = chunk.get("section_type", "unknown")
    section_title: str = chunk.get("section_title", "")
    toc_found: bool = bool(chunk.get("toc_found", False))
    toc_version: str = chunk.get("toc_version", "")
    page_start: int = int(chunk.get("page_start", -1))
    page_end: int = int(chunk.get("page_end", -1))

    # authority_level
    if section_type == "forfattningsforslag":
        authority_level = cfg.get("authority_level_non_binding", "preparatory")
    else:
        authority_level = cfg.get("authority_level_default", "preparatory")

    # related_sfs
    related_sfs = extract_related_sfs(text, cfg)

    # legal_area (placeholder — normalisering mot YAML)
    raw_legal_areas: list[str] = sou_api_doc.get("legal_area", [])
    legal_area = normalize_legal_area(raw_legal_areas, valid_areas)

    # Namespace
    namespace = build_namespace(year, number, chunk_index)

    # citation_format
    citation_format = build_citation_format(year, number, page_start)

    return {
        # Primär-ID
        "namespace": namespace,
        "text": text,
        "source_id": source_id,

        # source_type / doc_type (dubbla nycklar för schema-kompatibilitet)
        "source_type": cfg.get("source_type", "forarbete"),
        "doc_type": cfg.get("doc_type_sou", "sou"),
        "forarbete_type": "sou",            # alias — spec kräver detta fält

        # Dokumentidentifiering
        "beteckning": beteckning,           # formell: "SOU 2015:31"
        "sou_designation": beteckning,      # bakåtkompatibelt alias
        "titel": titel,
        "title": titel,                     # eng. alias — indexer använder detta
        "year": year,
        "url": url,
        "sha256": sha256,

        # Chunk-position
        "section_type": section_type,
        "section": section_title,           # spec-fält
        "section_title": section_title,     # fullständigt alias
        "toc_found": toc_found,
        "toc_version": toc_version,
        "page_start": page_start,
        "page_end": page_end,

        # Juridisk metadata
        "authority_level": authority_level,
        "legal_area": legal_area,
        "related_sfs": related_sfs,
        "citation_format": citation_format,

        # Embedding (populeras av indexer i F-4)
        "embedding_model": "",

        # Chunk-räknare
        "chunk_index": chunk_index,
        "chunk_total": chunk_total,
    }


# ---------------------------------------------------------------------------
# Batchhjälp: bygg metadata för hela dokumentets chunks på en gång
# ---------------------------------------------------------------------------
def build_all_metadata(
    chunks: list[dict[str, Any]],
    sou_api_doc: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    Kör build_chunk_metadata för varje chunk i listan.
    source_id delas (samma UUID) för alla chunks från samma dokument.
    """
    if not chunks:
        return []
    shared_source_id = str(uuid.uuid4())
    return [
        build_chunk_metadata(chunk, sou_api_doc, source_id=shared_source_id)
        for chunk in chunks
    ]
