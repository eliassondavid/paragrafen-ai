"""Verify SOU namespaces in Chroma without modifying the index."""

from __future__ import annotations

import argparse
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import chromadb

logger = logging.getLogger("paragrafenai.noop")

COLLECTION_NAME = "paragrafen_forarbete_v1"
DEFAULT_CHROMA_PATH = "data/index/chroma/"
DEFAULT_BATCH_SIZE = 10_000
NAMESPACE_RE = re.compile(r"^forarbete::sou_\d{4}_\d+_chunk_\d{3}$")


@dataclass
class VerificationResult:
    total_chunks: int
    sou_chunks: int
    valid_sou_chunks: int
    invalid_namespaces: list[str]


class CollectionNotFoundError(Exception):
    """Raised when the target Chroma collection does not exist."""


def _resolve_chroma_path(path_value: str | Path) -> Path:
    candidate = Path(path_value)
    if candidate.is_absolute():
        return candidate
    repo_root = Path(__file__).resolve().parent.parent
    return repo_root / candidate


def _verify_namespaces(chroma_path: Path, batch_size: int) -> VerificationResult:
    client = chromadb.PersistentClient(path=str(chroma_path))

    try:
        collection = client.get_collection(name=COLLECTION_NAME)
    except Exception as exc:
        raise CollectionNotFoundError(str(exc)) from exc

    total_chunks = collection.count()
    offset = 0
    sou_chunks = 0
    valid_sou_chunks = 0
    invalid_namespaces: list[str] = []

    while True:
        batch = collection.get(
            where={"forarbete_type": "sou"},
            include=["metadatas"],
            limit=batch_size,
            offset=offset,
        )
        metadatas = batch.get("metadatas") or []
        if not metadatas:
            break

        sou_chunks += len(metadatas)

        for metadata in metadatas:
            namespace = ""
            if isinstance(metadata, dict):
                raw_namespace = metadata.get("namespace", "")
                if raw_namespace is not None:
                    namespace = str(raw_namespace).strip()

            if NAMESPACE_RE.fullmatch(namespace):
                valid_sou_chunks += 1
            else:
                invalid_namespaces.append(namespace if namespace else "<saknas namespace>")

        offset += len(metadatas)
        if len(metadatas) < batch_size:
            break

    return VerificationResult(
        total_chunks=total_chunks,
        sou_chunks=sou_chunks,
        valid_sou_chunks=valid_sou_chunks,
        invalid_namespaces=invalid_namespaces,
    )


def _print_report(result: VerificationResult) -> None:
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    invalid_count = len(result.invalid_namespaces)

    print("=== Namespace-verifiering SOU-chunks ===")
    print(f"Collection: {COLLECTION_NAME}")
    print(f"Datum: {now_iso}")
    print()
    print(f"Totalt antal chunks i collection: {result.total_chunks}")
    print(f"Antal SOU-chunks identifierade: {result.sou_chunks}")
    print(f"Antal SOU-chunks med korrekt namespace: {result.valid_sou_chunks}")
    print(f"Antal SOU-chunks med AVVIKANDE namespace: {invalid_count}")
    print()
    print("AVVIKANDE CHUNKS (max 100 visas):")
    for namespace in result.invalid_namespaces[:100]:
        print(namespace)
    print()

    if invalid_count == 0:
        print("RESULTAT: [OK — 0 avvikelser]")
    else:
        print(f"RESULTAT: [VARNING — {invalid_count} avvikelser funna]")


def _run_diagnostics(chroma_path: Path) -> None:
    print("=== Diagnostikläge ===")
    print(f"Chroma-sökväg: {chroma_path} (exists: {chroma_path.exists()})")

    if not chroma_path.exists():
        print("Diagnostik: Chroma-sökvägen existerar inte.")
        return

    try:
        client = chromadb.PersistentClient(path=str(chroma_path))
    except Exception as exc:
        print(f"Diagnostik: Kunde inte initiera Chroma-klient: {exc}")
        return

    try:
        collections = [collection.name for collection in client.list_collections()]
    except Exception as exc:
        print(f"Diagnostik: Kunde inte lista collections: {exc}")
        return

    print(f"Collections i instansen: {collections}")

    if COLLECTION_NAME not in collections:
        print(f"Diagnostik: Collection '{COLLECTION_NAME}' finns inte i instansen.")
        return

    try:
        count = client.get_collection(name=COLLECTION_NAME).count()
    except Exception as exc:
        print(f"Diagnostik: Kunde inte läsa collection '{COLLECTION_NAME}': {exc}")
        return

    if count == 0:
        print(f"Diagnostik: Collection '{COLLECTION_NAME}' finns men är tom.")
    else:
        print(f"Diagnostik: Collection '{COLLECTION_NAME}' finns och innehåller {count} chunks.")


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verifiera namespace-format för SOU-chunks i Chroma.")
    parser.add_argument(
        "--chroma-path",
        default=DEFAULT_CHROMA_PATH,
        help=f"Sökväg till Chroma persistent path (default: {DEFAULT_CHROMA_PATH}).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Batch-storlek för collection.get (default: {DEFAULT_BATCH_SIZE}).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    if args.batch_size <= 0:
        print("Fel: --batch-size måste vara > 0")
        return 1

    chroma_path = _resolve_chroma_path(args.chroma_path)

    if not chroma_path.exists():
        _run_diagnostics(chroma_path)
        return 1

    try:
        result = _verify_namespaces(chroma_path=chroma_path, batch_size=args.batch_size)
    except CollectionNotFoundError as exc:
        message = (
            f"Kunde inte hitta collection '{COLLECTION_NAME}' i Chroma på '{chroma_path}': {exc}"
        )
        logger.error(message)
        print(message)
        _run_diagnostics(chroma_path)
        return 1
    except Exception as exc:
        logger.error("Namespace-verifiering misslyckades: %s", exc)
        print(f"Namespace-verifiering misslyckades: {exc}")
        _run_diagnostics(chroma_path)
        return 1

    _print_report(result)

    if result.total_chunks == 0:
        _run_diagnostics(chroma_path)

    return 0 if len(result.invalid_namespaces) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
