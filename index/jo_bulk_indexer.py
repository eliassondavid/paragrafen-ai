"""Bulk-index JO JSON documents into ChromaDB."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import sys
from typing import Any, Protocol

import chromadb
from sentence_transformers import SentenceTransformer

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from transformers import AutoTokenizer

os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")

logger = logging.getLogger("jo_bulk_indexer")

COLLECTION_NAME = "paragrafen_jo_v1"
CHROMA_PATH = "data/index/chroma/jo"
INPUT_DIR = "data/raw/jo/json"
DOCUMENT_SUBTYPE = "jo"
SOURCE_TYPE = "myndighetsbeslut"
AUTHORITY_LEVEL = "guiding"
SCHEMA_VERSION = "v0.15"
LICENSE = "public_domain"
COVERAGE_NOTE = "publicerat urval från jo.se, ej komplett beslutsmassa"

EMBEDDING_MODEL = "KBLab/sentence-bert-swedish-cased"
CHUNK_MIN_TOKENS = 150
CHUNK_MAX_TOKENS = 350
CHUNK_OVERLAP_TOKENS = 35
BATCH_SIZE = 64
FAIL_RATE_WARNING_THRESHOLD = 0.01


class TokenizerLike(Protocol):
    def encode(self, text: str, add_special_tokens: bool = False) -> list[int]:
        ...

    def decode(self, token_ids: list[int], skip_special_tokens: bool = True) -> str:
        ...


@dataclass
class PreparedChunk:
    chunk_id: str
    text: str
    metadata: dict[str, Any]


class SkipDocument(Exception):
    """Raised when a JO document should be skipped."""


class JOEmbedder:
    """Embedder bound to the required SentenceTransformer model."""

    def __init__(self, model_name: str = EMBEDDING_MODEL) -> None:
        self.model_name = model_name
        self.model = SentenceTransformer(model_name)

    def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        vectors = self.model.encode(
            texts,
            convert_to_numpy=True,
            show_progress_bar=False,
        )
        return [vector.tolist() for vector in vectors]


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def make_namespace(dnr: str, chunk_index: int) -> str:
    return f"jo::{dnr}_chunk_{chunk_index:03d}"


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def collect_json_files(input_dir: Path) -> list[Path]:
    return sorted(path for path in input_dir.glob("jo_*.json") if path.is_file())


def load_json_document(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("JSON-roten är inte ett objekt.")
    return payload


def split_text_units(text: str) -> list[str]:
    units: list[str] = []
    for paragraph in re.split(r"\n\s*\n", text):
        paragraph = paragraph.strip()
        if not paragraph:
            continue
        parts = re.split(r"(?<=[.!?])\s+(?=[A-ZÅÄÖ0-9])", paragraph)
        paragraph_units = [part.strip() for part in parts if part.strip()]
        units.extend(paragraph_units or [paragraph])
    return units


def tail_overlap_units(
    units: list[str],
    *,
    tokenizer: TokenizerLike,
    overlap_tokens: int,
) -> list[str]:
    overlap: list[str] = []
    token_total = 0
    for unit in reversed(units):
        unit_tokens = len(tokenizer.encode(unit, add_special_tokens=False))
        if token_total + unit_tokens > overlap_tokens:
            break
        overlap.insert(0, unit)
        token_total += unit_tokens
    return overlap


def chunk_long_unit(
    unit: str,
    *,
    tokenizer: TokenizerLike,
    max_tokens: int,
    overlap_tokens: int,
) -> list[str]:
    token_ids = tokenizer.encode(unit, add_special_tokens=False)
    if not token_ids:
        return []

    chunks: list[str] = []
    start = 0
    step = max(max_tokens - overlap_tokens, 1)
    while start < len(token_ids):
        end = min(start + max_tokens, len(token_ids))
        chunk_text = tokenizer.decode(token_ids[start:end], skip_special_tokens=True).strip()
        if chunk_text:
            chunks.append(chunk_text)
        if end >= len(token_ids):
            break
        start += step
    return chunks


def chunk_section_text(
    text: str,
    *,
    tokenizer: TokenizerLike,
    min_tokens: int = CHUNK_MIN_TOKENS,
    max_tokens: int = CHUNK_MAX_TOKENS,
    overlap_tokens: int = CHUNK_OVERLAP_TOKENS,
) -> list[str]:
    stripped = text.strip()
    if not stripped:
        return []

    units = split_text_units(stripped)
    if not units:
        return []

    chunks: list[str] = []
    current_units: list[str] = []
    current_tokens = 0

    for unit in units:
        unit_tokens = len(tokenizer.encode(unit, add_special_tokens=False))
        if unit_tokens == 0:
            continue

        if unit_tokens > max_tokens:
            if current_units:
                chunks.append("\n\n".join(current_units).strip())
                current_units = []
                current_tokens = 0
            chunks.extend(
                chunk_long_unit(
                    unit,
                    tokenizer=tokenizer,
                    max_tokens=max_tokens,
                    overlap_tokens=overlap_tokens,
                )
            )
            continue

        would_overflow = current_tokens + unit_tokens > max_tokens
        if would_overflow and current_units:
            chunks.append("\n\n".join(current_units).strip())
            overlap = tail_overlap_units(
                current_units,
                tokenizer=tokenizer,
                overlap_tokens=overlap_tokens,
            )
            current_units = overlap.copy()
            current_tokens = sum(
                len(tokenizer.encode(overlap_unit, add_special_tokens=False))
                for overlap_unit in current_units
            )

        current_units.append(unit)
        current_tokens += unit_tokens

    if current_units:
        chunks.append("\n\n".join(current_units).strip())

    merged_chunks: list[str] = []
    for chunk in chunks:
        chunk = chunk.strip()
        if not chunk:
            continue
        token_count = len(tokenizer.encode(chunk, add_special_tokens=False))
        if merged_chunks and token_count < min_tokens:
            merged_chunks[-1] = f"{merged_chunks[-1]}\n\n{chunk}".strip()
            continue
        merged_chunks.append(chunk)

    return [chunk for chunk in merged_chunks if chunk.strip()]


class JOBulkIndexer:
    """Read converted JO JSON, chunk by section, embed, and upsert to Chroma."""

    def __init__(
        self,
        *,
        input_dir: str | Path = INPUT_DIR,
        chroma_path: str | Path = CHROMA_PATH,
        collection_name: str = COLLECTION_NAME,
        batch_size: int = BATCH_SIZE,
        tokenizer: TokenizerLike | None = None,
        embedder: JOEmbedder | None = None,
        client_factory: Any | None = None,
    ) -> None:
        self.repo_root = Path(__file__).resolve().parent.parent
        self.input_dir = self._resolve_path(input_dir)
        self.chroma_path = self._resolve_path(chroma_path)
        self.collection_name = collection_name
        self.batch_size = max(1, int(batch_size))
        self._tokenizer = tokenizer
        self._embedder = embedder
        self.client_factory = client_factory or chromadb.PersistentClient
        self._collection: Any | None = None

    def run(
        self,
        *,
        max_docs: int | None = None,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> dict[str, int]:
        json_files = collect_json_files(self.input_dir)
        if max_docs is not None:
            json_files = json_files[:max_docs]

        stats = {
            "json_files_read": 0,
            "documents_indexed": 0,
            "failed": 0,
            "total_chunks": 0,
            "count_before": 0,
            "count_after": 0,
        }

        if dry_run:
            logger.info("DRY-RUN aktiverat — ingen Chroma-skrivning.")
        else:
            stats["count_before"] = int(self.collection.count())

        pending_chunks: list[PreparedChunk] = []

        for index, json_path in enumerate(json_files, start=1):
            stats["json_files_read"] += 1

            try:
                document = load_json_document(json_path)
                prepared_chunks = self.prepare_document(document)
            except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError, SkipDocument) as exc:
                logger.warning("FAIL: %s — %s", json_path.name, exc)
                stats["failed"] += 1
                continue

            pending_chunks.extend(prepared_chunks)
            stats["documents_indexed"] += 1
            stats["total_chunks"] += len(prepared_chunks)

            if verbose:
                namespace = prepared_chunks[0].chunk_id.rsplit("_chunk_", 1)[0]
                logger.info("[%d/%d] %s — %d chunks", index, len(json_files), namespace, len(prepared_chunks))
            elif index == 1 or index % 100 == 0:
                logger.info(
                    "Progress %d/%d | dokument=%d | chunks=%d | fel=%d",
                    index,
                    len(json_files),
                    stats["documents_indexed"],
                    stats["total_chunks"],
                    stats["failed"],
                )

            while len(pending_chunks) >= self.batch_size:
                batch = pending_chunks[: self.batch_size]
                self.process_batch(batch, dry_run=dry_run)
                pending_chunks = pending_chunks[self.batch_size :]

        if pending_chunks:
            self.process_batch(pending_chunks, dry_run=dry_run)

        if not dry_run:
            stats["count_after"] = int(self.collection.count())

        return stats

    def prepare_document(self, document: dict[str, Any]) -> list[PreparedChunk]:
        dnr = str(document.get("dnr") or "").strip()
        if not dnr:
            raise SkipDocument("saknar dnr")

        title = str(document.get("title") or "").strip()
        beslutsdatum = str(document.get("beslutsdatum") or "").strip()
        source_url = str(document.get("source_url") or "").strip()
        sections_raw = document.get("sections")

        if not isinstance(sections_raw, list) or not sections_raw:
            text_content = str(document.get("text_content") or "").strip()
            if not text_content:
                raise SkipDocument("saknar text_content och sections")
            sections_raw = [
                {
                    "section": "other",
                    "section_title": "Övrigt",
                    "text": text_content,
                }
            ]

        prepared_chunks: list[PreparedChunk] = []
        chunk_index = 0

        for section in sections_raw:
            if not isinstance(section, dict):
                continue
            section_name = str(section.get("section") or "other").strip() or "other"
            section_title = str(section.get("section_title") or "").strip()
            section_text = str(section.get("text") or "").strip()
            if not section_text:
                continue

            chunk_texts = chunk_section_text(
                section_text,
                tokenizer=self.tokenizer,
                min_tokens=CHUNK_MIN_TOKENS,
                max_tokens=CHUNK_MAX_TOKENS,
                overlap_tokens=CHUNK_OVERLAP_TOKENS,
            )
            for chunk_text in chunk_texts:
                chunk_id = make_namespace(dnr, chunk_index)
                prepared_chunks.append(
                    PreparedChunk(
                        chunk_id=chunk_id,
                        text=chunk_text,
                        metadata={
                            "chunk_id": chunk_id,
                            "namespace": chunk_id,
                            "source_type": SOURCE_TYPE,
                            "document_subtype": DOCUMENT_SUBTYPE,
                            "authority_level": AUTHORITY_LEVEL,
                            "dnr": dnr,
                            "title": title,
                            "beslutsdatum": beslutsdatum,
                            "section": section_name,
                            "section_title": section_title,
                            "content_hash": content_hash(chunk_text),
                            "source_url": source_url,
                            "schema_version": SCHEMA_VERSION,
                            "license": LICENSE,
                            "is_active": True,
                            "coverage_note": COVERAGE_NOTE,
                        },
                    )
                )
                chunk_index += 1

        if not prepared_chunks:
            raise SkipDocument("inga chunks skapades")
        return prepared_chunks

    def process_batch(self, prepared_chunks: list[PreparedChunk], *, dry_run: bool) -> None:
        texts = [chunk.text for chunk in prepared_chunks]
        embeddings = self.embedder.embed(texts)
        if len(embeddings) != len(texts):
            raise RuntimeError(f"fel antal embeddings ({len(embeddings)} av {len(texts)})")
        if dry_run:
            return

        self.collection.upsert(
            ids=[chunk.chunk_id for chunk in prepared_chunks],
            embeddings=embeddings,
            documents=texts,
            metadatas=[chunk.metadata for chunk in prepared_chunks],
        )

    @property
    def tokenizer(self) -> TokenizerLike:
        if self._tokenizer is None:
            self._tokenizer = AutoTokenizer.from_pretrained(EMBEDDING_MODEL)
        return self._tokenizer

    @property
    def embedder(self) -> JOEmbedder:
        if self._embedder is None:
            self._embedder = JOEmbedder()
        return self._embedder

    @property
    def collection(self) -> Any:
        if self._collection is None:
            self.chroma_path.mkdir(parents=True, exist_ok=True)
            client = self.client_factory(path=str(self.chroma_path.resolve()))
            self._collection = client.get_or_create_collection(
                name=self.collection_name,
                metadata={"hnsw:space": "cosine"},
            )
        return self._collection

    def _resolve_path(self, path_value: str | Path) -> Path:
        path = Path(path_value)
        if path.is_absolute():
            return path
        return self.repo_root / path


def print_summary(stats: dict[str, int], *, dry_run: bool) -> None:
    total_files = stats["json_files_read"]
    failed = stats["failed"]
    fail_rate = (failed / total_files * 100.0) if total_files else 0.0

    print("JO-indexering klar.")
    print(f"  JSON-filer lästa:    {stats['json_files_read']}")
    print(f"  Dokument indexerade: {stats['documents_indexed']}")
    print(f"  Failed:              {stats['failed']}")
    print(f"  Chunks totalt:       {stats['total_chunks']}")
    print(f"  Felkvot:             {fail_rate:.2f}%")
    print(f"  Körläge:             {'DRY-RUN' if dry_run else 'LIVE'}")
    if not dry_run:
        print(f"  Collection före:     {stats['count_before']}")
        print(f"  Collection efter:    {stats['count_after']}")


def run(max_docs: int | None, dry_run: bool, verbose: bool) -> int:
    setup_logging(verbose)
    indexer = JOBulkIndexer()
    stats = indexer.run(max_docs=max_docs, dry_run=dry_run, verbose=verbose)

    total_files = stats["json_files_read"]
    failed = stats["failed"]
    fail_rate = (failed / total_files) if total_files else 0.0
    if fail_rate > FAIL_RATE_WARNING_THRESHOLD:
        logger.warning("Felkvot %.2f%% överstiger 1%%-tröskeln.", fail_rate * 100)

    if dry_run:
        logger.info(
            "Coverage: dry-run | chunks_förberedda=%d | dokument_indexerade=%d",
            stats["total_chunks"],
            stats["documents_indexed"],
        )
    else:
        logger.info(
            "Coverage: collection.count()=%d | chunks_indexerade_i_körningen=%d | dokument=%d",
            stats["count_after"],
            stats["total_chunks"],
            stats["documents_indexed"],
        )

    print_summary(stats, dry_run=dry_run)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Indexera JO-JSON till ChromaDB.")
    parser.add_argument("--max-docs", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)
    return run(args.max_docs, args.dry_run, args.verbose)


if __name__ == "__main__":
    raise SystemExit(main())
