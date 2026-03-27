from __future__ import annotations

import argparse
import importlib.util
import random
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import chromadb

COLLECTION_NAME = "paragrafen_forarbete_v1"
MIN_SOU_COUNT = 802_161
REQUIRED_FIELDS = {
    "namespace": str,
    "source_type": str,
    "authority_level": str,
    "volym_år": int,
    "lag": str,
    "citation_source": str,
    "citation_precision": str,
    "legal_area": list,
    "embedding_model": str,
    "chunk_index": int,
    "chunk_total": int,
    "source_url": str,
    "fetched_at": str,
}


@dataclass
class ChunkRow:
    document: str
    metadata: dict[str, Any]


def load_ingest_module() -> Any:
    module_name = "scripts_ingest_nja_ii"
    if module_name in sys.modules:
        return sys.modules[module_name]
    script_path = Path(__file__).resolve().parent / "ingest_nja_ii.py"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Kunde inte ladda {script_path}")
    module = importlib.util.module_from_spec(spec)
    # Registrera i sys.modules innan exec_module så att @dataclass kan
    # slå upp annotationsnamnet "int | None" via sys.modules.get(cls.__module__).
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def resolve_chroma_path(config_path: str | Path) -> Path:
    ingest = load_ingest_module()
    config = ingest.load_config(config_path)
    return Path(str(config.get("chroma_path", "")))


def fetch_nja_rows(collection: Any, batch_size: int = 1_000) -> list[ChunkRow]:
    """Hämtar endast nja_ii-chunks via where-filter — undviker full collection-scan."""
    rows: list[ChunkRow] = []
    offset = 0

    while True:
        batch = collection.get(
            where={"source_type": {"$eq": "nja_ii"}},
            include=["documents", "metadatas"],
            limit=batch_size,
            offset=offset,
        )
        documents = batch.get("documents") or []
        metadatas = batch.get("metadatas") or []
        if not metadatas:
            break

        for document, metadata in zip(documents, metadatas, strict=False):
            if isinstance(metadata, dict):
                rows.append(ChunkRow(document=str(document or ""), metadata=metadata))

        offset += len(metadatas)
        if len(metadatas) < batch_size:
            break

    return rows


def fetch_sou_count(collection: Any) -> int:
    """Räknar SOU-chunks via where-filter utan att hämta dokumenten.
    SOU-chunks har source_type='forarbete' och forarbete_type='sou'."""
    result = collection.get(
        where={"forarbete_type": {"$eq": "sou"}},
        include=[],
    )
    return len(result.get("ids") or [])


def validate_row(row: ChunkRow) -> list[str]:
    errors: list[str] = []

    for key, expected_type in REQUIRED_FIELDS.items():
        value = row.metadata.get(key)
        if value is None:
            errors.append(f"saknar {key}")
            continue
        if not isinstance(value, expected_type):
            errors.append(
                f"{key} har typ {type(value).__name__}, väntade {expected_type.__name__}"
            )

    if row.metadata.get("forarbete_rank") is not None:
        errors.append("forarbete_rank finns i metadata")
    if row.metadata.get("source_type") != "nja_ii":
        errors.append("source_type != nja_ii")

    return errors


def check_brb(nja_rows: list[ChunkRow]) -> str:
    # Verifierar att BrB-chunks har legal_area innehållande "straffrätt".
    # area_blocker.py-integrationstestet tillhör qa/ — inte detta skript.
    brb_row = next((row for row in nja_rows if row.metadata.get("lag") == "brb"), None)
    if brb_row is None:
        return "FAIL: inget brb-chunk hittades"
    legal_area = brb_row.metadata.get("legal_area", [])
    if isinstance(legal_area, list) and "straffrätt" in legal_area:
        return "PASS"
    return "FAIL: brb legal_area saknar straffrätt"


def print_table(rows: list[tuple[str, str]]) -> None:
    width = max(len(name) for name, _ in rows)
    print("=== NJA II Verification ===")
    for name, result in rows:
        print(f"{name.ljust(width)} : {result}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verifiera NJA II-ingest i Chroma.")
    parser.add_argument(
        "--config",
        default="config/nja_ii_config.yaml",
        help="Sökväg till config-fil.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=10,
        help="Antal slumpmässiga chunks att validera.",
    )
    args = parser.parse_args(argv)

    chroma_path = resolve_chroma_path(args.config)
    if not chroma_path.exists():
        print(f"Chroma-sökväg saknas: {chroma_path}")
        return 1

    client = chromadb.PersistentClient(path=str(chroma_path))
    collection = client.get_collection(name=COLLECTION_NAME)

    print("Hämtar NJA II-chunks...")
    nja_rows = fetch_nja_rows(collection)

    print("Räknar SOU-chunks...")
    sou_count = fetch_sou_count(collection)

    sample_size = min(max(args.sample_size, 1), len(nja_rows))
    sample = random.sample(nja_rows, sample_size) if sample_size else []
    sample_errors = [error for row in sample for error in validate_row(row)]

    brb_result = check_brb(nja_rows)

    report_rows = [
        ("Totalt NJA II-chunks", str(len(nja_rows))),
        ("Totalt SOU-chunks", str(sou_count)),
        ("Sample-validering", "PASS" if not sample_errors else f"FAIL ({len(sample_errors)} fel)"),
        (
            "forarbete_rank",
            "PASS" if all("forarbete_rank" not in row.metadata for row in sample) else "FAIL",
        ),
        ("BrB-kontroll", brb_result),
        ("SOU-count", "PASS" if sou_count >= MIN_SOU_COUNT else f"FAIL (< {MIN_SOU_COUNT})"),
    ]
    print_table(report_rows)

    if sample_errors:
        print("\nExempel på fel:")
        for message in sample_errors[:10]:
            print(f"- {message}")

    return 0 if not sample_errors and brb_result == "PASS" and sou_count >= MIN_SOU_COUNT else 1


if __name__ == "__main__":
    raise SystemExit(main())
