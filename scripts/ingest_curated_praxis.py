"""
Stub: Ingest-pipeline för curaterad underrättspraxis.

Fas 2 — implementeras EFTER att den automatiserade pipeline är verifierad.
Denna stub definierar CLI-gränssnittet och data-flödet.

Flöde:
1. David tillhandahåller PDF (bildskannad eller textbaserad)
2. Gemini Vision → textextraktion → data/curated/praxis/{dok_id}.json
3. Manuell metadata-komplettering: domstol, mål-nr, datum, rättsområde
4. Denna script → normalisering → Chroma-ingest
5. norm_boost.py: authority_level "indicative" → vikt 0.35

Namespace: praxis_curated::{domstol}_{år}_{dok_id}_chunk_{index:03d}
Authority level: indicative
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

# import structlog
# logger = structlog.get_logger()


@dataclass
class CuratedDocument:
    """
    Schema för curaterad underrättspraxis.

    Metadata fylls i manuellt av David + stöd av Gemini Vision.
    """
    # Obligatoriska fält
    domstol: str              # t.ex. "stockholms_tr" (lowercase, _ för mellanslag)
    domstol_display: str      # t.ex. "Stockholms tingsrätt"
    malnummer: str            # t.ex. "T 12345-18"
    avgorandedatum: str       # ISO-format YYYY-MM-DD
    text: str                 # Extraherad fulltext

    # Metadata
    legal_area: list[str]     # t.ex. ["socialrätt", "LSS"]
    extraction_method: str    # "gemini_ocr" | "pdftotext" | "native"
    curated_by: str           # "david_eliasson"
    curated_date: str         # ISO-format
    curator_note: str         # Varför detta avgörande curaterats

    # Valfria
    citation: str = ""        # t.ex. "Stockholms tingsrätt, T 12345-18, 2019-03-15"
    references_to: list[str] | None = None  # SFS-referenser

    def generate_citation(self) -> str:
        if self.citation:
            return self.citation
        return f"{self.domstol_display}, {self.malnummer}, {self.avgorandedatum}"


def ingest_curated(
    input_dir: Path,
    *,
    dry_run: bool = True,
    write: bool = False,
    chroma_path: str | None = None,
) -> None:
    """
    Huvudfunktion: läser curaterade JSON-filer och indexerar till Chroma.

    STUB — ej implementerad. Returnerar utan effekt.
    """
    # TODO: Fas 2 implementation
    #
    # Steg 1: Iterera JSON-filer i input_dir
    # Steg 2: Validera schema (CuratedDocument)
    # Steg 3: Chunka text med samma strategi som praxis_normalizer
    # Steg 4: Generera namespace:
    #          praxis_curated::{domstol}_{år}_{dok_id}_chunk_{index:03d}
    # Steg 5: Bygga metadata:
    #          - source_type: "praxis_curated"
    #          - authority_level: "indicative"
    #          - extraction_method: från JSON
    #          - curated_by, curated_date, curator_note: från JSON
    # Steg 6: Upserta till Chroma (om --write, inte --dry-run)
    #
    # Gemini Vision-anrop:
    # TODO: Integrera med Gemini API för OCR av skannade PDF:er
    # Endpoint: generativelanguage.googleapis.com/v1beta/models/gemini-pro-vision
    # Input: Base64-kodad PDF-sida
    # Output: Extraherad text med bevarad styckestruktur
    #
    # Eskaleringsvillkor:
    # - Om OCR-kvalitet < 90% (mätt som andel igenkända ord) → manuell granskning
    # - Om rättsområde ej kan klassificeras → flagga för David

    if dry_run:
        print(f"[DRY RUN] Skulle läsa filer från: {input_dir}")
        json_files = list(input_dir.glob("*.json")) if input_dir.exists() else []
        print(f"[DRY RUN] Hittade {len(json_files)} filer")
        return

    print("STUB: ingest_curated_praxis.py är ej implementerad (Fas 2)")
    print("Kör med --dry-run för att verifiera setup.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest curaterad underrättspraxis till ChromaDB (STUB)"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/curated/praxis"),
        help="Katalog med curaterade JSON-filer",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Dry-run: validera utan att skriva till Chroma (default)",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Skriv till Chroma (kräver --no-dry-run)",
    )
    parser.add_argument(
        "--chroma-path",
        type=str,
        default=None,
        help="Sökväg till Chroma persistent storage",
    )

    args = parser.parse_args()

    dry_run = not args.write
    ingest_curated(
        input_dir=args.input_dir,
        dry_run=dry_run,
        write=args.write,
        chroma_path=args.chroma_path,
    )


if __name__ == "__main__":
    main()
