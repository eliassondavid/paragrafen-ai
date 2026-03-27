#!/usr/bin/env python3
"""Bulk-index Socialstyrelsens föreskrifter till ChromaDB."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any

import chromadb
from transformers import AutoTokenizer

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from index.embedder import Embedder

logging.getLogger("chromadb").setLevel(logging.ERROR)
logging.getLogger("sentence_transformers").setLevel(logging.ERROR)

COLLECTION_NAME = "paragrafen_foreskrift_v1"
CHROMA_PATH = "data/index/chroma/foreskrift"
EMBEDDING_MODEL = "KBLab/sentence-bert-swedish-cased"
SCHEMA_VERSION = "v0.15"
CHUNK_MIN_TOKENS = 150
CHUNK_MAX_TOKENS = 350
CHUNK_OVERLAP = 35
RAW_JSON_DIR = "data/raw/socialstyrelsen/json"
LICENSE = "public_domain"
BATCH_SIZE = 100

os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

_TOKENIZER: Any | None = None


def log(message: str) -> None:
    print(message, flush=True)


def get_tokenizer() -> Any:
    global _TOKENIZER
    if _TOKENIZER is None:
        _TOKENIZER = AutoTokenizer.from_pretrained(EMBEDDING_MODEL)
    return _TOKENIZER


def make_namespace(doc_subtype: str, year: int, number: int, chunk_idx: int) -> str:
    if doc_subtype == "sosfs":
        return f"sosfs::{year}_{number}_chunk_{chunk_idx:03d}"
    if doc_subtype == "hslf_fs":
        return f"hslf_fs::{year}_{number}_chunk_{chunk_idx:03d}"
    return f"sos_other::{year}_{number}_chunk_{chunk_idx:03d}"


def get_authority_level(section: str) -> str:
    if section == "allmant_rad":
        return "guiding"
    if section in ("foreskrift", "foreskrift_kap", "foreskrift_par", "bemyndigande"):
        return "binding"
    return "binding"


def content_hash(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def chunk_section(text: str, min_tokens: int, max_tokens: int, overlap: int) -> list[str]:
    """Token-baserad chunkning av en sektion."""
    tokenizer = get_tokenizer()
    tokens = tokenizer.encode(text, add_special_tokens=False)
    if not tokens:
        return []

    chunks: list[str] = []
    start = 0
    while start < len(tokens):
        end = min(start + max_tokens, len(tokens))
        chunk_tokens = tokens[start:end]
        if len(chunk_tokens) >= min_tokens or not chunks:
            chunk_text = tokenizer.decode(
                chunk_tokens,
                skip_special_tokens=True,
                clean_up_tokenization_spaces=True,
            ).strip()
            if chunk_text:
                chunks.append(chunk_text)
        if end >= len(tokens):
            break
        next_start = max(start + 1, end - overlap)
        start = next_start
    return chunks


def resolve_repo_path(path_value: str | Path) -> Path:
    candidate = Path(path_value)
    if candidate.is_absolute():
        return candidate
    return Path(__file__).resolve().parent.parent / candidate


def load_document(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON-filen innehåller inte ett objekt: {path}")
    return payload


def parse_year_number(document: dict[str, Any]) -> tuple[int, int]:
    year = int(document["year"])
    nummer = str(document["nummer"])
    match = nummer.split(" ", 1)[-1]
    _, _, number_part = match.partition(":")
    return year, int(number_part)


def prepare_chunks(document: dict[str, Any]) -> list[dict[str, Any]]:
    doc_subtype = str(document.get("document_subtype") or "other").strip()
    year, number = parse_year_number(document)
    sections = document.get("sections")
    if not isinstance(sections, list):
        raise ValueError("Dokument saknar sections-lista.")

    prepared: list[dict[str, Any]] = []
    chunk_index = 0
    for section in sections:
        if not isinstance(section, dict):
            continue
        section_type = str(section.get("section") or "other").strip()
        section_text = str(section.get("text") or "").strip()
        if not section_text:
            continue

        chunks = chunk_section(
            section_text,
            min_tokens=CHUNK_MIN_TOKENS,
            max_tokens=CHUNK_MAX_TOKENS,
            overlap=CHUNK_OVERLAP,
        )
        for chunk_text in chunks:
            namespace = make_namespace(doc_subtype, year, number, chunk_index)
            metadata = {
                "chunk_id": namespace,
                "namespace": namespace,
                "source_type": str(document.get("source_type") or "foreskrift"),
                "schema_version": SCHEMA_VERSION,
                "document_subtype": doc_subtype,
                "samling": str(document.get("samling") or ""),
                "nummer": f"{year}:{number}",
                "titel": str(document.get("titel") or ""),
                "myndighet": str(document.get("myndighet") or "Socialstyrelsen"),
                "artikelnummer": str(document.get("artikelnummer") or ""),
                "year": year,
                "authority_level": get_authority_level(section_type),
                "section": section_type,
                "mandatory_areas": json.dumps(document.get("mandatory_areas") or [], ensure_ascii=False),
                "chunk_index": chunk_index,
                "content_hash": content_hash(chunk_text),
                "extraction_method": str(document.get("extraction_method") or "pdftotext"),
                "license": str(document.get("license") or LICENSE),
            }
            prepared.append(
                {
                    "id": namespace,
                    "text": chunk_text,
                    "metadata": metadata,
                }
            )
            chunk_index += 1
    return prepared


def iter_json_files(raw_json_dir: Path, max_docs: int | None) -> list[Path]:
    files = sorted(path for path in raw_json_dir.glob("*.json") if path.is_file())
    if max_docs is not None:
        files = files[:max_docs]
    return files


def upsert_batch(collection: Any, embedder: Embedder, batch: list[dict[str, Any]]) -> None:
    texts = [item["text"] for item in batch]
    embeddings = embedder.embed(texts)
    if len(embeddings) != len(batch):
        raise RuntimeError("Embedding returnerade fel antal vektorer.")
    collection.upsert(
        ids=[item["id"] for item in batch],
        documents=texts,
        embeddings=embeddings,
        metadatas=[item["metadata"] for item in batch],
    )


def filter_existing_chunks(collection: Any, batch: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Returnera bara chunks som inte redan finns i collection."""
    if not batch:
        return [], 0

    ids = [item["id"] for item in batch]
    existing = collection.get(ids=ids, include=[])
    existing_ids = set(existing.get("ids") or [])
    filtered = [item for item in batch if item["id"] not in existing_ids]
    return filtered, len(existing_ids)


def run(*, max_docs: int | None, dry_run: bool, verbose: bool) -> dict[str, int]:
    raw_json_dir = resolve_repo_path(RAW_JSON_DIR)
    chroma_path = resolve_repo_path(CHROMA_PATH)
    if not raw_json_dir.exists():
        raise FileNotFoundError(f"JSON-katalog saknas: {raw_json_dir}")

    chroma_path.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(chroma_path))
    collection = client.get_or_create_collection(name=COLLECTION_NAME)

    existing = collection.count()
    log(f"Befintliga chunks i collection: {existing}")
    assert existing >= 2000, f"FK-chunks saknas! ({existing} < 2000) — ESKALERA"

    files = iter_json_files(raw_json_dir, max_docs)
    if not files:
        raise RuntimeError("Inga JSON-filer att indexera — kontrollera JSON-filer")

    stats = {
        "documents_seen": 0,
        "documents_indexed": 0,
        "chunks_prepared": 0,
        "chunks_skipped_existing": 0,
        "chunks_upserted": 0,
    }
    batch: list[dict[str, Any]] = []
    embedder: Embedder | None = None

    for file_index, file_path in enumerate(files, start=1):
        document = load_document(file_path)
        chunks = prepare_chunks(document)
        stats["documents_seen"] += 1
        stats["chunks_prepared"] += len(chunks)
        stats["documents_indexed"] += 1 if chunks else 0

        for chunk in chunks:
            if dry_run:
                log(f"[DRY-RUN] Skulle indexera chunk {chunk['id']}")
                if verbose:
                    log(json.dumps(chunk["metadata"], ensure_ascii=False))
                continue

            batch.append(chunk)
            if len(batch) >= BATCH_SIZE:
                filtered_batch, skipped_existing = filter_existing_chunks(collection, batch)
                stats["chunks_skipped_existing"] += skipped_existing
                if filtered_batch:
                    if embedder is None:
                        embedder = Embedder()
                    upsert_batch(collection, embedder, filtered_batch)
                    stats["chunks_upserted"] += len(filtered_batch)
                batch = []

        if verbose:
            log(f"[{file_index}/{len(files)}] {file_path.name} — {len(chunks)} chunks")

    if not dry_run and batch:
        filtered_batch, skipped_existing = filter_existing_chunks(collection, batch)
        stats["chunks_skipped_existing"] += skipped_existing
        if filtered_batch:
            if embedder is None:
                embedder = Embedder()
            upsert_batch(collection, embedder, filtered_batch)
            stats["chunks_upserted"] += len(filtered_batch)

    final = collection.count()
    added = final - existing
    log(f"Adderade chunks: {added}")
    log(f"Totalt i collection: {final}")

    if dry_run:
        return stats

    if stats["chunks_prepared"] == 0:
        raise AssertionError("Inga chunks förbereddes — kontrollera JSON-filer")
    if added == 0:
        log("Inga nya chunks adderades; upsert var idempotent mot befintliga chunk-id:n.")
    return stats


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bulk-index Socialstyrelsen till Chroma.")
    parser.add_argument("--dry-run", action="store_true", help="Generera chunks utan upsert.")
    parser.add_argument("--max-docs", type=int, default=None, help="Begränsa antal dokument.")
    parser.add_argument("--verbose", action="store_true", help="Visa extra loggning.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    stats = run(max_docs=args.max_docs, dry_run=args.dry_run, verbose=args.verbose)
    log(
        "Klart: "
        f"documents_seen={stats['documents_seen']} "
        f"documents_indexed={stats['documents_indexed']} "
        f"chunks_prepared={stats['chunks_prepared']} "
        f"chunks_upserted={stats['chunks_upserted']} "
        f"chunks_skipped_existing={stats['chunks_skipped_existing']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
