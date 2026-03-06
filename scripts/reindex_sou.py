#!/usr/bin/env python3
"""Repair incorrect SOU chunks in the Chroma forarbete collection."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


CHROMA_PATH = os.environ.get("CHROMA_PATH", "data/index/chroma")
COLLECTION_NAME = "paragrafen_forarbete_v1"
SOU_ID_PATTERN = re.compile(
    r"^forarbete::sou_(?P<year>\d{4})_(?P<number>\d+)(?:_d(?P<part>\d+))?_chunk_(?P<index>\d+)$"
)
CITATION_PATTERN = re.compile(r"SOU\s+(?P<year>\d{4}):(?P<number>\d+)")
LIST_METADATA_FIELDS = ("legal_area", "references_to")


@dataclass
class ChunkRecord:
    chunk_id: str
    metadata: dict[str, Any]
    document: str | None
    embedding: list[float] | None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log operations without writing to Chroma.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit how many candidate chunks to process.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    return parser.parse_args(argv)


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(message)s", force=True)


def load_forarbete_rank(
    config_path: str | os.PathLike[str] = "config/forarbete_rank.yaml",
) -> dict[str, int]:
    with Path(config_path).open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    return {
        forarbete_type: values["rank"]
        for forarbete_type, values in config["forarbete_types"].items()
    }


def connect_collection(chroma_path: str = CHROMA_PATH, name: str = COLLECTION_NAME) -> Any:
    import chromadb

    client = chromadb.PersistentClient(path=chroma_path)
    return client.get_collection(name)


def fetch_candidate_chunks(collection: Any, limit: int | None = None) -> list[ChunkRecord]:
    include = ["metadatas", "documents", "embeddings"]
    results_a = collection.get(
        where={"forarbete_type": {"$eq": "sou"}},
        include=include,
    )
    results_b = collection.get(
        where={"authority_level": {"$eq": "persuasive"}},
        include=include,
    )

    merged: dict[str, ChunkRecord] = {}
    for result in (results_a, results_b):
        for record in iter_result_records(result):
            if not is_sou_candidate(record):
                continue
            existing = merged.get(record.chunk_id)
            if existing is None:
                merged[record.chunk_id] = record
                continue

            if existing.document is None and record.document is not None:
                existing.document = record.document
            if existing.embedding is None and record.embedding is not None:
                existing.embedding = record.embedding
            if not existing.metadata and record.metadata:
                existing.metadata = record.metadata

    records = list(merged.values())
    if limit is not None:
        return records[:limit]
    return records


def iter_result_records(result: dict[str, Any]) -> list[ChunkRecord]:
    ids = result.get("ids") or []
    metadatas = result.get("metadatas") or []
    documents = result.get("documents") or []
    embeddings = result.get("embeddings") or []

    records: list[ChunkRecord] = []
    for index, chunk_id in enumerate(ids):
        records.append(
            ChunkRecord(
                chunk_id=chunk_id,
                metadata=(metadatas[index] or {}) if index < len(metadatas) else {},
                document=documents[index] if index < len(documents) else None,
                embedding=embeddings[index] if index < len(embeddings) else None,
            )
        )
    return records


def is_sou_candidate(record: ChunkRecord) -> bool:
    source_type = record.metadata.get("source_type")
    forarbete_type = record.metadata.get("forarbete_type")
    return (
        source_type == "forarbete"
        and (forarbete_type == "sou" or record.chunk_id.startswith("forarbete::sou_"))
    )


def build_corrected_metadata(
    original_metadata: dict[str, Any],
    sou_rank: int,
) -> dict[str, Any]:
    corrected = dict(original_metadata)
    corrected["authority_level"] = "preparatory"
    corrected["forarbete_rank"] = sou_rank
    corrected["forarbete_type"] = "sou"
    return corrected


def needs_metadata_repair(metadata: dict[str, Any], sou_rank: int) -> bool:
    return (
        metadata.get("authority_level") != "preparatory"
        or metadata.get("forarbete_rank") != sou_rank
        or metadata.get("forarbete_type") != "sou"
    )


def build_expected_chunk_id(chunk_id: str, metadata: dict[str, Any]) -> str:
    year, number = extract_sou_reference(chunk_id, metadata)
    chunk_index = metadata.get("chunk_index")
    if not isinstance(chunk_index, int):
        raise ValueError(f"Invalid chunk_index for {chunk_id!r}: {chunk_index!r}")

    part = metadata.get("del")
    if part in (None, "", 0):
        return f"forarbete::sou_{year}_{number}_chunk_{chunk_index:03d}"
    if not isinstance(part, int):
        raise ValueError(f"Invalid del for {chunk_id!r}: {part!r}")
    return f"forarbete::sou_{year}_{number}_d{part}_chunk_{chunk_index:03d}"


def extract_sou_reference(chunk_id: str, metadata: dict[str, Any]) -> tuple[int, int]:
    citation = metadata.get("citation")
    if isinstance(citation, str):
        citation_match = CITATION_PATTERN.search(citation)
        if citation_match:
            return int(citation_match.group("year")), int(citation_match.group("number"))

    id_match = SOU_ID_PATTERN.match(chunk_id)
    if id_match:
        return int(id_match.group("year")), int(id_match.group("number"))

    year = metadata.get("year")
    if isinstance(year, int):
        raise ValueError(f"Could not determine SOU number from citation or id for {chunk_id!r}")
    raise ValueError(f"Could not determine SOU reference for {chunk_id!r}")


def namespace_requires_repair(chunk_id: str, metadata: dict[str, Any]) -> bool:
    return build_expected_chunk_id(chunk_id, metadata) != chunk_id


def validate_namespace_repair(record: ChunkRecord, metadata: dict[str, Any]) -> None:
    if not isinstance(record.document, str) or not record.document.strip():
        raise ValueError("documents field is empty")
    if record.embedding is None:
        raise ValueError("embedding is missing")
    for field in LIST_METADATA_FIELDS:
        value = metadata.get(field)
        if value is not None and not isinstance(value, str):
            raise ValueError(f"{field} must be a JSON string, got {type(value).__name__}")
        if isinstance(value, str):
            json.loads(value)


def delete_old_ids(collection: Any, ids: list[str], dry_run: bool) -> None:
    logger = logging.getLogger(__name__)
    if dry_run:
        logger.debug("[DRY-RUN] delete ids=%s", ids)
        return
    collection.delete(ids=ids)


def upsert_repaired_chunk(
    collection: Any,
    old_id: str,
    new_id: str,
    metadata: dict[str, Any],
    document: str,
    embedding: list[float],
    dry_run: bool,
) -> None:
    logger = logging.getLogger(__name__)
    delete_old_ids(collection, [old_id], dry_run=dry_run)
    if dry_run:
        logger.debug("[DRY-RUN] upsert old_id=%s new_id=%s", old_id, new_id)
        return
    collection.upsert(
        ids=[new_id],
        metadatas=[metadata],
        documents=[document],
        embeddings=[embedding],
    )


def update_chunk_metadata(
    collection: Any,
    chunk_id: str,
    metadata: dict[str, Any],
    dry_run: bool,
) -> None:
    logger = logging.getLogger(__name__)
    if dry_run:
        logger.debug("[DRY-RUN] update id=%s", chunk_id)
        return
    collection.update(ids=[chunk_id], metadatas=[metadata])


def process_chunks(
    collection: Any,
    records: list[ChunkRecord],
    sou_rank: int,
    dry_run: bool,
) -> tuple[int, int, int]:
    logger = logging.getLogger(__name__)
    metadata_updates = 0
    namespace_repairs = 0
    failures = 0

    for record in records:
        try:
            corrected_metadata = build_corrected_metadata(record.metadata, sou_rank=sou_rank)
            needs_metadata = needs_metadata_repair(record.metadata, sou_rank=sou_rank)
            needs_namespace = namespace_requires_repair(record.chunk_id, corrected_metadata)

            if not needs_metadata and not needs_namespace:
                continue

            if needs_namespace:
                validate_namespace_repair(record, corrected_metadata)
                new_id = build_expected_chunk_id(record.chunk_id, corrected_metadata)
                upsert_repaired_chunk(
                    collection=collection,
                    old_id=record.chunk_id,
                    new_id=new_id,
                    metadata=corrected_metadata,
                    document=record.document,
                    embedding=record.embedding,
                    dry_run=dry_run,
                )
                namespace_repairs += 1
                logger.debug("repaired namespace %s -> %s", record.chunk_id, new_id)
                continue

            update_chunk_metadata(
                collection=collection,
                chunk_id=record.chunk_id,
                metadata=corrected_metadata,
                dry_run=dry_run,
            )
            metadata_updates += 1
            logger.debug("repaired metadata %s", record.chunk_id)
        except Exception as exc:
            failures += 1
            logger.error("failed chunk %s: %s", record.chunk_id, exc)

    return metadata_updates, namespace_repairs, failures


def check_failure_threshold(total_records: int, failures: int) -> int:
    if total_records == 0:
        return 0
    if failures / total_records > 0.01:
        logging.error(
            "failure threshold exceeded: %s/%s chunks failed (%.2f%%)",
            failures,
            total_records,
            (failures / total_records) * 100,
        )
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)

    ranks = load_forarbete_rank()
    sou_rank = ranks["sou"]
    collection = connect_collection()
    records = fetch_candidate_chunks(collection, limit=args.limit)

    logging.info(
        "processing %s candidate SOU chunks from %s%s",
        len(records),
        COLLECTION_NAME,
        " [DRY-RUN]" if args.dry_run else "",
    )

    metadata_updates, namespace_repairs, failures = process_chunks(
        collection=collection,
        records=records,
        sou_rank=sou_rank,
        dry_run=args.dry_run,
    )

    logging.info(
        "summary metadata_updates=%s namespace_repairs=%s failures=%s",
        metadata_updates,
        namespace_repairs,
        failures,
    )
    return check_failure_threshold(len(records), failures)


if __name__ == "__main__":
    raise SystemExit(main())
