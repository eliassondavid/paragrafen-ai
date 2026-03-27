"""Run proposition normalization for curated raw files."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from normalize.prop_normalizer import normalize_one

logger = logging.getLogger("paragrafenai.noop")


def run_curated_props(
    *,
    input_dir: str | Path = "data/raw/prop/curated",
    output_dir: str | Path = "data/norm/prop",
    dry_run: bool = False,
) -> tuple[int, int, int]:
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    files = sorted(input_path.glob("*.json"))

    parsed_files = 0
    zero_chunk_files = 0
    error_files = 0

    if not dry_run:
        output_path.mkdir(parents=True, exist_ok=True)

    for raw_path in files:
        parsed_files += 1
        try:
            with raw_path.open("r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except Exception as exc:
            logger.error("%s\tread_error=%s", raw_path.name, exc)
            error_files += 1
            continue

        try:
            result = normalize_one(raw)
        except Exception as exc:
            logger.error("%s\tnormalize_error=%s", raw_path.name, exc)
            error_files += 1
            continue

        chunk_count = len((result or {}).get("chunks") or [])
        if chunk_count == 0:
            zero_chunk_files += 1
            logger.error("%s\tchunks=0", raw_path.name)
            continue

        if dry_run:
            logger.info("%s\tchunks=%s\tdry_run=1", raw_path.name, chunk_count)
            continue

        out_path = output_path / raw_path.name
        try:
            with out_path.open("w", encoding="utf-8") as fh:
                json.dump(result, fh, ensure_ascii=False, indent=2)
            logger.info("%s\tchunks=%s\toutput=%s", raw_path.name, chunk_count, out_path)
        except Exception as exc:
            logger.error("%s\twrite_error=%s", raw_path.name, exc)
            error_files += 1

    logger.info(
        "summary\tfiles=%s\tzero_chunks=%s\terrors=%s\tdry_run=%s",
        parsed_files,
        zero_chunk_files,
        error_files,
        int(dry_run),
    )
    return parsed_files, zero_chunk_files, error_files


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Normalize curated raw proposition files.")
    parser.add_argument("--input-dir", default="data/raw/prop/curated")
    parser.add_argument("--output-dir", default="data/norm/prop")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING, format="%(message)s")

    _, zero_chunk_files, error_files = run_curated_props(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        dry_run=args.dry_run,
    )
    return 1 if zero_chunk_files > 1 or error_files > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
