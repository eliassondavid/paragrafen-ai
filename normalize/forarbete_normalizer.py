"""Normalize raw SOU fetcher output → data/norm/forarbete/sou/*.json (F-5c)."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

from normalize.forarbete_parser import ForarbeteParser
from normalize.metadata_builder import build_all_metadata

logger = logging.getLogger("paragrafenai.noop")


def _fetcher_doc_to_api_doc(raw: dict[str, Any]) -> dict[str, Any]:
    """Bridge fetcher format → metadata_builder's sou_api_doc format."""
    beteckning = raw.get("beteckning", "")
    m = re.search(r"(\d{4})\s*:\s*(\d+)", beteckning)
    year = int(m.group(1)) if m else 0
    number = int(m.group(2)) if m else 0
    return {
        "id": raw.get("dok_id", ""),
        "namn": beteckning,
        "ar": year,
        "nummer": number,
        "titel": raw.get("titel", ""),
        "url": raw.get("source_url", ""),
        "sha256": raw.get("sha256", ""),
        "legal_area": [],
    }


def normalize_one(raw: dict[str, Any], parser: ForarbeteParser) -> dict[str, Any] | None:
    """Parse + build metadata for one raw document. Returns None on failure."""
    # Fetcher may omit html_available flag even when html_content is present
    if raw.get("html_content") and not raw.get("html_available"):
        raw = {**raw, "html_available": True}
    parsed = parser.parse(raw)
    if parsed is None:
        logger.warning("Parser returnerade None för %s", raw.get("beteckning", "?"))
        return None

    api_doc = _fetcher_doc_to_api_doc(raw)
    chunks_with_meta = build_all_metadata(parsed["chunks"], api_doc)

    return {
        "beteckning": parsed["beteckning"],
        "title": parsed["title"],
        "year": parsed["year"],
        "department": parsed["department"],
        "source_url": parsed["source_url"],
        "doc_type": "sou",
        "source_type": "forarbete",
        "source_origin": "riksdagen",
        "source_origin": "riksdagen",
        "chunk_count": len(chunks_with_meta),
        "chunks": chunks_with_meta,
    }


def normalize_all(
    raw_dir: str | Path = "data/raw/forarbete/sou",
    norm_dir: str | Path = "data/norm/forarbete/sou",
    force: bool = False,
) -> dict[str, int]:
    """Normalize all raw SOU documents. Returns counts: ok, skipped, failed."""
    raw_dir = Path(raw_dir)
    norm_dir = Path(norm_dir)
    norm_dir.mkdir(parents=True, exist_ok=True)

    parser = ForarbeteParser()
    counts = {"ok": 0, "skipped": 0, "failed": 0}

    raw_files = sorted(raw_dir.glob("*.json"))
    logger.info("Normaliserar %d råfiler från %s", len(raw_files), raw_dir)

    for raw_path in raw_files:
        out_path = norm_dir / raw_path.name
        if out_path.exists() and not force:
            counts["skipped"] += 1
            continue

        try:
            with raw_path.open(encoding="utf-8") as f:
                raw = json.load(f)
        except Exception as exc:
            logger.error("Kunde inte läsa %s: %s", raw_path.name, exc)
            counts["failed"] += 1
            continue

        result = normalize_one(raw, parser)
        if result is None:
            counts["failed"] += 1
            continue

        try:
            with out_path.open("w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            counts["ok"] += 1
        except Exception as exc:
            logger.error("Kunde inte skriva %s: %s", out_path.name, exc)
            counts["failed"] += 1

        if (counts["ok"] + counts["failed"]) % 500 == 0:
            logger.info("Progress: %s", counts)

    logger.info("Normalisering klar: %s", counts)
    return counts


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Normalize raw SOU documents")
    parser.add_argument("--force", action="store_true", help="Skriv över befintliga norm-filer")
    parser.add_argument("--raw-dir", default="data/raw/forarbete/sou")
    parser.add_argument("--norm-dir", default="data/norm/forarbete/sou")
    args = parser.parse_args()
    counts = normalize_all(raw_dir=args.raw_dir, norm_dir=args.norm_dir, force=args.force)
    print(counts)
    return 0 if counts["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
