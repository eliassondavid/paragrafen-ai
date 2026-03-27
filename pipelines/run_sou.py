#!/usr/bin/env python3
"""Kör SOU-pipeline: fetch + normalize + index."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.adapters.sou_adapter import SouAdapter


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("--riksmote", default=None)
    parser.add_argument("--incremental", action="store_true")
    parser.add_argument("--max-docs", type=int, default=None)
    args = parser.parse_args()

    adapter = SouAdapter()
    result = adapter.run(
        dry_run=args.dry_run,
        fetch=args.fetch,
        riksmote=args.riksmote,
        incremental=args.incremental,
        max_docs=args.max_docs,
    )
    print(result)


if __name__ == "__main__":
    main()
