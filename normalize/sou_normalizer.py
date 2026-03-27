"""Normalize SOU raw documents into SOU norm documents."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import re
from typing import Any

import yaml

from normalize.sou_parser import parse_sou_html

logger = logging.getLogger("paragrafenai.noop")

MAX_CHUNK_TOKENS = 600
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def _parse_beteckning(beteckning: str) -> tuple[str, int] | None:
    """Parse 'SOU 2015:14' → ('2015', 14). Returns None if no match."""
    match = re.search(r"(?i)\bSOU\s+(\d{4})\s*:\s*(\d+)\b", beteckning or "")
    if match:
        return match.group(1), int(match.group(2))
    return None


def build_sou_namespace(år: str, nr: int, chunk_index: int) -> str:
    return f"forarbete::sou_{år}_{nr}_chunk_{chunk_index:03d}"


def build_citation(år: str, nr: int, pinpoint: str = "") -> str:
    base = f"SOU {år}:{nr}"
    return f"{base} {pinpoint}" if pinpoint else base


def build_pinpoint(page_start: int, page_end: int) -> str:
    if page_start == 0 and page_end == 0:
        return ""
    if page_start == page_end or page_end == 0:
        return f"s. {page_start}"
    return f"s. {page_start}–{page_end}"


def classify_legal_area(organ: str, titel: str) -> list[str]:
    """Simple keyword-based legal-area classification for SOU."""
    text = f"{organ or ''} {titel or ''}".lower()
    if any(w in text for w in ["arbets", "anställ", "lön", "fackl"]):
        return ["arbetsrätt"]
    if any(w in text for w in ["familj", "barn", "förälder", "vårdnad"]):
        return ["civilrätt", "familjerätt"]
    if any(w in text for w in ["fastighet", "mark", "plan", "bygg"]):
        return ["civilrätt", "fastighetsrätt"]
    if any(w in text for w in ["avtal", "köp", "konsument", "handel"]):
        return ["civilrätt", "avtalsrätt"]
    if any(w in text for w in ["förvaltning", "myndighet", "offentlig"]):
        return ["förvaltningsrätt"]
    if any(w in text for w in ["process", "rättegång", "domstol"]):
        return ["processrätt"]
    if any(w in text for w in ["miljö", "natur", "vatten"]):
        return ["miljörätt"]
    return []


def extract_sfs_references(text: str) -> list[str]:
    """Extract SFS references from text."""
    pattern = r"\b(\d{4}:\d+)\b"
    refs = [
        f"sfs::{match}"
        for match in re.findall(pattern, text or "")
        if 1700 <= int(match.split(":")[0]) <= 2030
    ]
    return list(dict.fromkeys(refs))


def load_sou_rank(config_path: str | Path = "config/forarbete_rank.yaml") -> int:
    path = Path(config_path)
    with path.open("r", encoding="utf-8") as fh:
        rank_cfg = yaml.safe_load(fh) or {}
    rank = rank_cfg.get("forarbete_types", {}).get("sou", {}).get("rank")
    if not isinstance(rank, int):
        raise ValueError("forarbete_rank för sou saknas eller är inte ett heltal.")
    return rank


def normalize_sou(
    raw_data: dict[str, Any],
    *,
    rank_config_path: str | Path = "config/forarbete_rank.yaml",
) -> dict[str, Any] | None:
    """Normalize one raw SOU document into norm format."""
    html_content = str(raw_data.get("html_content") or "").strip()
    if not html_content:
        logger.warning("Hoppar över SOU utan HTML: %s", raw_data.get("beteckning", "okänt"))
        return None

    beteckning = str(raw_data.get("beteckning") or "").strip()
    parsed = _parse_beteckning(beteckning)
    if parsed is None:
        # Fallback: försök från titel
        parsed = _parse_beteckning(str(raw_data.get("titel") or ""))
    if parsed is None:
        logger.warning("Kunde inte parsa beteckning: %s", beteckning)
        return None

    år, nr = parsed
    dok_id = str(raw_data.get("dok_id") or "").strip()
    titel = str(raw_data.get("titel") or "").strip()
    organ = str(raw_data.get("organ") or "").strip()
    datum = str(raw_data.get("datum") or "").strip()
    riksmote = str(raw_data.get("rm") or "").strip()

    sections = parse_sou_html(html_content, dok_id)
    if not sections:
        logger.warning("Parsern returnerade inga sektioner: %s", beteckning)
        return None

    rank = load_sou_rank(rank_config_path)
    legal_area = classify_legal_area(organ, titel)
    all_text = "\n\n".join(section.get("text", "") for section in sections)
    references_to = extract_sfs_references(all_text)

    chunks: list[dict[str, Any]] = []
    for section in sections:
        for chunk_text in _chunk_section_text(str(section.get("text") or "")):
            if not chunk_text.strip():
                continue
            page_start = _coerce_int(section.get("page_start"))
            page_end = _coerce_int(section.get("page_end"))
            pinpoint = build_pinpoint(page_start, page_end)
            chunk_index = len(chunks)
            namespace = build_sou_namespace(år, nr, chunk_index)
            chunks.append({
                "chunk_index": chunk_index,
                "namespace": namespace,
                "section": str(section.get("section") or "other"),
                "section_title": str(section.get("section_title") or "other"),
                "text": chunk_text,
                "page_start": page_start,
                "page_end": page_end,
                "pinpoint": pinpoint,
                "citation": build_citation(år, nr, pinpoint),
            })

    if not chunks:
        logger.warning("Inga norm-chunks skapades för %s", beteckning)
        return None

    return {
        "beteckning": beteckning,
        "dok_id": dok_id,
        "år": år,
        "nr": nr,
        "titel": titel,
        "datum": datum,
        "organ": organ,
        "riksmote": riksmote,
        "source_url": str(raw_data.get("source_url") or ""),
        "dokument_url_html": str(raw_data.get("dokument_url_html") or ""),
        "source_type": "forarbete",
        "forarbete_type": "sou",
        "document_subtype": "sou",
        "authority_level": "preparatory",
        "forarbete_rank": rank,
        "legal_area": legal_area,
        "references_to": references_to,
        "fetched_at": str(raw_data.get("fetched_at") or ""),
        "chunks": chunks,
    }


def normalize_all(
    raw_dir: str | Path = "data/raw/sou",
    norm_dir: str | Path = "data/norm/sou",
    *,
    force: bool = False,
    max_docs: int | None = None,
    rank_config_path: str | Path = "config/forarbete_rank.yaml",
) -> dict[str, int]:
    """Normalize all raw SOU documents."""
    raw_dir = Path(raw_dir)
    norm_dir = Path(norm_dir)
    norm_dir.mkdir(parents=True, exist_ok=True)

    counts = {"ok": 0, "skipped": 0, "failed": 0}
    raw_files = sorted(raw_dir.glob("*.json"))

    # Exkludera filer som börjar med _ (felfiler)
    raw_files = [f for f in raw_files if not f.name.startswith("_")]

    if max_docs is not None:
        raw_files = raw_files[:max_docs]

    for raw_path in raw_files:
        out_path = norm_dir / raw_path.name
        if out_path.exists() and not force:
            counts["skipped"] += 1
            continue

        try:
            with raw_path.open("r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except Exception as exc:
            logger.error("Kunde inte läsa %s: %s", raw_path.name, exc)
            counts["failed"] += 1
            continue

        result = normalize_sou(raw, rank_config_path=rank_config_path)
        if result is None:
            counts["failed"] += 1
            continue

        try:
            with out_path.open("w", encoding="utf-8") as fh:
                json.dump(result, fh, ensure_ascii=False, indent=2)
            counts["ok"] += 1
        except Exception as exc:
            logger.error("Kunde inte skriva %s: %s", out_path.name, exc)
            counts["failed"] += 1

    return counts


def _coerce_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _chunk_section_text(text: str) -> list[str]:
    paragraphs = _split_paragraphs(text)
    if not paragraphs:
        return []

    units: list[str] = []
    for paragraph in paragraphs:
        if _count_tokens(paragraph) <= MAX_CHUNK_TOKENS:
            units.append(paragraph)
            continue
        units.extend(_split_long_paragraph(paragraph))

    chunks: list[str] = []
    start = 0
    while start < len(units):
        token_total = 0
        end = start
        chunk_parts: list[str] = []
        while end < len(units):
            candidate = units[end]
            candidate_tokens = _count_tokens(candidate)
            if chunk_parts and token_total + candidate_tokens > MAX_CHUNK_TOKENS:
                break
            chunk_parts.append(candidate)
            token_total += candidate_tokens
            end += 1

        chunks.append("\n\n".join(chunk_parts).strip())
        if end >= len(units):
            break
        start = end - 1 if len(chunk_parts) > 1 else end

    return chunks


def _split_paragraphs(text: str) -> list[str]:
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n+", text or "") if p.strip()]
    if paragraphs:
        return paragraphs
    return [line.strip() for line in (text or "").splitlines() if line.strip()]


def _split_long_paragraph(paragraph: str) -> list[str]:
    sentences = [s.strip() for s in SENTENCE_SPLIT_RE.split(paragraph) if s.strip()]
    if not sentences:
        return [paragraph.strip()]

    chunks: list[str] = []
    current: list[str] = []
    token_total = 0
    for sentence in sentences:
        sentence_tokens = _count_tokens(sentence)
        if current and token_total + sentence_tokens > MAX_CHUNK_TOKENS:
            chunks.append(" ".join(current).strip())
            current = [sentence]
            token_total = sentence_tokens
            continue
        if sentence_tokens > MAX_CHUNK_TOKENS:
            words = sentence.split()
            for index in range(0, len(words), MAX_CHUNK_TOKENS):
                chunks.append(" ".join(words[index: index + MAX_CHUNK_TOKENS]).strip())
            current = []
            token_total = 0
            continue
        current.append(sentence)
        token_total += sentence_tokens

    if current:
        chunks.append(" ".join(current).strip())
    return chunks


def _count_tokens(text: str) -> int:
    return len((text or "").split())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Normalize raw SOU documents.")
    parser.add_argument("--raw-dir", default="data/raw/sou")
    parser.add_argument("--norm-dir", default="data/norm/sou")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--max-docs", type=int, default=None)
    args = parser.parse_args(argv)

    counts = normalize_all(
        raw_dir=args.raw_dir,
        norm_dir=args.norm_dir,
        force=args.force,
        max_docs=args.max_docs,
    )
    print(counts)
    return 0 if counts["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
