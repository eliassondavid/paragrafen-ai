"""Normalize proposition raw documents into proposition norm documents."""

from __future__ import annotations

import argparse
from html import escape
import json
import logging
from pathlib import Path
import re
from typing import Any

import yaml

from normalize.prop_parser import parse_prop_html

logger = logging.getLogger("paragrafenai.noop")

MAX_CHUNK_TOKENS = 600
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def classify_legal_area(organ: str) -> list[str]:
    """Simple department-based legal-area classification."""
    mapping = {
        "Justitiedepartementet": ["civilrätt"],
        "Finansdepartementet": ["skatterätt", "finansrätt"],
        "Socialdepartementet": ["socialrätt"],
        "Utbildningsdepartementet": ["utbildningsrätt"],
    }
    for key, areas in mapping.items():
        if key in (organ or ""):
            return areas
    return []


def extract_sfs_references(text: str) -> list[str]:
    """Extract SFS references and prefix with `sfs::`."""
    pattern = r"\b(\d{4}:\d+)\b"
    refs = [
        f"sfs::{match}"
        for match in re.findall(pattern, text or "")
        if 1700 <= int(match.split(":")[0]) <= 2030
    ]
    return list(dict.fromkeys(refs))


def build_pinpoint(page_start: int, page_end: int) -> str:
    if page_start == 0 and page_end == 0:
        return ""
    if page_start == page_end or page_end == 0:
        return f"s. {page_start}"
    return f"s. {page_start}–{page_end}"


def build_citation(rm: str, nummer: int, pinpoint: str) -> str:
    base = f"Prop. {rm}:{nummer}"
    return f"{base} {pinpoint}" if pinpoint else base


def load_prop_rank(config_path: str | Path = "config/forarbete_rank.yaml") -> int:
    path = Path(config_path)
    with path.open("r", encoding="utf-8") as fh:
        rank_cfg = yaml.safe_load(fh) or {}
    rank = rank_cfg.get("forarbete_types", {}).get("proposition", {}).get("rank")
    if not isinstance(rank, int):
        raise ValueError("forarbete_rank för proposition saknas eller är inte ett heltal.")
    return rank


def normalize_one(
    raw: dict[str, Any],
    *,
    rank_config_path: str | Path = "config/forarbete_rank.yaml",
) -> dict[str, Any] | None:
    """Normalize one raw proposition document into norm format."""
    html_content = _resolve_html_content(raw)
    is_curated = _is_curated_prop(raw)
    if not html_content:
        message = f"Hoppar över proposition utan HTML: {raw.get('beteckning', 'okänt')}"
        if is_curated:
            logger.error("Curerad prop-fil producerade 0 chunks: %s", message)
        else:
            logger.warning(message)
        return None

    sections = parse_prop_html(html_content, str(raw.get("dok_id") or ""))
    if not sections:
        if is_curated:
            logger.error(
                "Curerad prop-fil producerade 0 chunks: parsern returnerade inga sektioner för %s",
                raw.get("beteckning", "okänt"),
            )
        else:
            logger.warning("Parsern returnerade inga sektioner: %s", raw.get("beteckning", "okänt"))
        return None

    rank = load_prop_rank(rank_config_path)
    rm = str(raw.get("rm") or raw.get("riksmote_norm") or raw.get("riksmote") or "").strip()
    nummer = _coerce_int(raw.get("nummer"))
    organ = str(raw.get("organ") or "").strip()
    legal_area = classify_legal_area(organ)
    references_to = extract_sfs_references("\n\n".join(section.get("text", "") for section in sections))

    chunks: list[dict[str, Any]] = []
    for section in sections:
        for chunk_text in _chunk_section_text(str(section.get("text") or "")):
            if not chunk_text.strip():
                continue
            page_start = _coerce_int(section.get("page_start"))
            page_end = _coerce_int(section.get("page_end"))
            pinpoint = build_pinpoint(page_start, page_end)
            chunks.append(
                {
                    "chunk_index": len(chunks),
                    "section": str(section.get("section") or "other"),
                    "section_title": str(section.get("section_title") or "other"),
                    "text": chunk_text,
                    "page_start": page_start,
                    "page_end": page_end,
                    "pinpoint": pinpoint,
                    "citation": build_citation(rm, nummer, pinpoint) if rm and nummer else "",
                }
            )

    if not chunks:
        if is_curated:
            logger.error(
                "Curerad prop-fil producerade 0 chunks: inga norm-chunks skapades för %s",
                raw.get("beteckning", "okänt"),
            )
        else:
            logger.warning("Inga norm-chunks skapades för %s", raw.get("beteckning", "okänt"))
        return None

    return {
        "beteckning": str(raw.get("beteckning") or ""),
        "dok_id": str(raw.get("dok_id") or ""),
        "rm": rm,
        "nummer": nummer,
        "titel": str(raw.get("titel") or ""),
        "datum": str(raw.get("datum") or ""),
        "organ": organ,
        "source_url": str(raw.get("source_url") or ""),
        "pdf_url": str(raw.get("pdf_url") or ""),
        "source_type": "forarbete",
        "forarbete_type": "proposition",
        "authority_level": "preparatory",
        "forarbete_rank": rank,
        "legal_area": legal_area,
        "references_to": references_to,
        "fetched_at": str(raw.get("fetched_at") or ""),
        "chunks": chunks,
    }


def normalize_all(
    raw_dir: str | Path = "data/raw/forarbete/prop",
    norm_dir: str | Path = "data/norm/prop",
    *,
    force: bool = False,
    max_docs: int | None = None,
    rank_config_path: str | Path = "config/forarbete_rank.yaml",
) -> dict[str, int]:
    """Normalize all raw proposition documents."""
    raw_dir = Path(raw_dir)
    norm_dir = Path(norm_dir)
    norm_dir.mkdir(parents=True, exist_ok=True)

    counts = {"ok": 0, "skipped": 0, "failed": 0}
    raw_files = sorted(raw_dir.glob("*.json"))
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

        result = normalize_one(raw, rank_config_path=rank_config_path)
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


def _is_curated_prop(raw: dict[str, Any]) -> bool:
    return bool(raw.get("pages")) or bool(raw.get("curated_by")) or bool(raw.get("curated_note"))


def _resolve_html_content(raw: dict[str, Any]) -> str:
    html_content = str(raw.get("html_content") or "").strip()
    if html_content:
        return html_content
    return _build_html_from_pages(raw.get("pages"))


def _build_html_from_pages(pages: Any) -> str:
    if not isinstance(pages, list):
        return ""

    html_pages: list[str] = []
    for page in pages:
        if not isinstance(page, dict):
            continue
        page_num = _coerce_int(page.get("page_num"))
        page_text = str(page.get("text") or "").strip()
        if page_num <= 0 or not page_text:
            continue
        paragraphs = [
            f"<p>{escape(line.strip())}</p>"
            for line in page_text.splitlines()
            if line.strip()
        ]
        if not paragraphs:
            continue
        html_pages.append(f'<div id="page_{page_num}">{"".join(paragraphs)}</div>')

    if not html_pages:
        return ""
    return f"<html><body>{''.join(html_pages)}</body></html>"


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
    paragraphs = [paragraph.strip() for paragraph in re.split(r"\n\s*\n+", text or "") if paragraph.strip()]
    if paragraphs:
        return paragraphs
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    return lines


def _split_long_paragraph(paragraph: str) -> list[str]:
    sentences = [sentence.strip() for sentence in SENTENCE_SPLIT_RE.split(paragraph) if sentence.strip()]
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
                chunks.append(" ".join(words[index : index + MAX_CHUNK_TOKENS]).strip())
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
    parser = argparse.ArgumentParser(description="Normalize raw proposition documents.")
    parser.add_argument("--raw-dir", default="data/raw/forarbete/prop")
    parser.add_argument("--norm-dir", default="data/norm/prop")
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
