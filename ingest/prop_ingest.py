"""Orchestrate proposition fetch, normalize and index steps."""

from __future__ import annotations

import argparse
import logging
from typing import Any

from index.prop_indexer import PropIndexer
from ingest.prop_fetcher import fetch_prop_documents
from normalize.prop_normalizer import normalize_all

logger = logging.getLogger("paragrafenai.noop")


def run_pipeline(
    from_date: str | None = None,
    to_date: str | None = None,
    rm: str | None = None,
    max_docs: int | None = None,
    dry_run: bool = False,
    skip_fetch: bool = False,
    skip_normalize: bool = False,
) -> dict[str, Any]:
    """
    Run the proposition pipeline.

    Fetch-date filters are accepted for CLI compatibility but are not applied in the
    fetcher yet because the current repository fetcher contract is document-type based.
    """
    if from_date or to_date or rm:
        logger.info(
            "Filterparametrar mottagna men används inte i fetch-steget ännu: from_date=%s to_date=%s rm=%s",
            from_date,
            to_date,
            rm,
        )

    fetched = 0
    normalized = {"ok": 0, "skipped": 0, "failed": 0}
    if not skip_fetch:
        fetched = fetch_prop_documents()

    if not skip_normalize:
        normalized = normalize_all(max_docs=max_docs)

    indexer = PropIndexer()
    indexed = indexer.index_all(dry_run=dry_run, max_docs=max_docs)

    return {
        "fetched": fetched,
        "normalized": normalized,
        "indexed": indexed.as_dict(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the proposition ingest pipeline.")
    parser.add_argument("--from-date", default=None)
    parser.add_argument("--to-date", default=None)
    parser.add_argument("--rm", default=None)
    parser.add_argument("--max-docs", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-fetch", action="store_true")
    parser.add_argument("--skip-normalize", action="store_true")
    args = parser.parse_args(argv)

    summary = run_pipeline(
        from_date=args.from_date,
        to_date=args.to_date,
        rm=args.rm,
        max_docs=args.max_docs,
        dry_run=args.dry_run,
        skip_fetch=args.skip_fetch,
        skip_normalize=args.skip_normalize,
    )
    print(summary)
    return 0 if summary["indexed"]["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
