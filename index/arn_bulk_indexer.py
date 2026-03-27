"""Bulk-index converted ARN JSON documents into ChromaDB."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any

import chromadb
from sentence_transformers import SentenceTransformer

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# normalize.chunker saknas i detta repo; använd projektets befintliga common chunker.
from pipelines.common.chunk_base import ChunkConfig, ForarbeteChunker
from pipelines.common.parse_base import Section

os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")

logger = logging.getLogger("paragrafenai.noop")

COLLECTION_NAME = "paragrafen_namnder_v1"
CHROMA_PATH = "data/index/chroma/namnder"
INPUT_DIR = "data/raw/arn/json"
CHUNK_CONFIG = ChunkConfig(min_tokens=150, max_tokens=350, overlap_tokens=35)
AUTHORITY_LEVEL = "persuasive"
DOCUMENT_SUBTYPE = "arn"
SCHEMA_VERSION = "v0.15"
EMBEDDING_MODEL = "KBLab/sentence-bert-swedish-cased"
BATCH_SIZE = 100


@dataclass
class PreparedChunk:
    """One chunk ready for embedding."""

    chunk_id: str
    text: str
    metadata: dict[str, Any]


class SkipDocument(Exception):
    """Raised when one JSON file should be skipped."""


class ExistingCollectionError(Exception):
    """Raised when the target collection is already populated."""

    def __init__(self, count: int) -> None:
        super().__init__(str(count))
        self.count = count


class ArnEmbedder:
    """Explicit ARN embedder bound to the required model."""

    def __init__(self, model_name: str = EMBEDDING_MODEL) -> None:
        self.model_name = model_name
        self.model = SentenceTransformer(model_name)

    def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        vectors = self.model.encode(
            texts,
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        )
        return [vector.tolist() for vector in vectors]


def dnr_to_namespace_prefix(dnr: str) -> str:
    """Convert dnr to ARN namespace prefix."""
    year, running_number = dnr.split("-", 1)
    return f"arn::{year}_{running_number}"


def build_chunk_id(dnr: str, chunk_index: int) -> str:
    """Build the required chunk id."""
    return f"{dnr_to_namespace_prefix(dnr)}_chunk_{chunk_index:03d}"


def collect_json_files(input_dir: Path) -> list[Path]:
    """Collect converted ARN JSON files, excluding log files."""
    return sorted(
        path
        for path in input_dir.glob("*.json")
        if not path.name.startswith("_")
    )


def load_json_document(path: Path) -> dict[str, Any]:
    """Load one ARN JSON document."""
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError("JSON-roten är inte ett objekt.")
    return payload


class ArnBulkIndexer:
    """Index ARN JSON files into the namnder collection."""

    def __init__(
        self,
        *,
        input_dir: str | Path = INPUT_DIR,
        chroma_path: str | Path = CHROMA_PATH,
        collection_name: str = COLLECTION_NAME,
        batch_size: int = BATCH_SIZE,
        chunker: ForarbeteChunker | None = None,
        embedder: ArnEmbedder | None = None,
        client_factory: Any | None = None,
    ) -> None:
        self.repo_root = Path(__file__).resolve().parent.parent
        self.input_dir = self._resolve_path(input_dir)
        self.chroma_path = self._resolve_path(chroma_path)
        self.collection_name = collection_name
        self.batch_size = max(1, int(batch_size))
        self.chunker = chunker or ForarbeteChunker(config=CHUNK_CONFIG)
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
        """Execute the ARN indexing pipeline."""
        json_files = collect_json_files(self.input_dir)
        if max_docs is not None:
            json_files = json_files[:max_docs]

        count_before = int(self.collection.count())
        if count_before > 0:
            raise ExistingCollectionError(count_before)

        stats = {
            "json_files_read": 0,
            "documents_indexed": 0,
            "skipped": 0,
            "total_chunks": 0,
        }
        pending_chunks: list[PreparedChunk] = []

        for index, json_path in enumerate(json_files, start=1):
            document = load_json_document(json_path)
            stats["json_files_read"] += 1

            try:
                prepared = self.prepare_document(document)
            except SkipDocument as exc:
                stats["skipped"] += 1
                if verbose:
                    print(f"[SKIP] {json_path.name} -> {exc}")
                continue

            pending_chunks.extend(prepared)
            stats["documents_indexed"] += 1
            stats["total_chunks"] += len(prepared)

            if verbose:
                namespace = prepared[0].chunk_id.rsplit("_chunk_", 1)[0]
                print(f"[OK]   {json_path.name} -> {namespace} ({len(prepared)} chunks)")

            while len(pending_chunks) >= self.batch_size:
                batch = pending_chunks[: self.batch_size]
                self.process_batch(batch, dry_run=dry_run)
                pending_chunks = pending_chunks[self.batch_size :]

            if verbose and index % 100 == 0:
                print(f"[INFO] {index}/{len(json_files)} JSON-filer behandlade")

        if pending_chunks:
            self.process_batch(pending_chunks, dry_run=dry_run)

        return stats

    def prepare_document(self, document: dict[str, Any]) -> list[PreparedChunk]:
        """Chunk one ARN JSON document."""
        dnr = str(document.get("dnr") or "").strip()
        if not dnr:
            raise SkipDocument("missing dnr")

        text_content = str(document.get("text_content") or "").strip()
        if len(text_content) < 1:
            raise SkipDocument("empty_text")

        sections = [
            Section(
                section_key="other",
                section_title="other",
                text=text_content,
                level=1,
            )
        ]
        chunked_sections = self.chunker.chunk_sections(sections)
        if not chunked_sections:
            raise SkipDocument("empty_text")

        title = str(document.get("title") or f"Ärendereferat {dnr}").strip()
        source_format = str(document.get("source_format") or "").strip().lower()

        prepared_chunks: list[PreparedChunk] = []
        for chunk_index, chunk in enumerate(chunked_sections):
            chunk_text = chunk.chunk_text.strip()
            if not chunk_text:
                continue
            chunk_id = build_chunk_id(dnr, chunk_index)
            metadata = {
                "chunk_id": chunk_id,
                "namespace": chunk_id,
                "source_type": "namnder",
                "document_subtype": DOCUMENT_SUBTYPE,
                "authority_level": AUTHORITY_LEVEL,
                "dnr": dnr,
                "title": title,
                "source_format": source_format,
                "content_hash": hashlib.sha256(chunk_text.encode("utf-8")).hexdigest(),
                "schema_version": SCHEMA_VERSION,
                "license": "public_domain",
                "is_active": True,
                "chunk_index": chunk_index,
                "section_type": "other",
            }
            prepared_chunks.append(
                PreparedChunk(
                    chunk_id=chunk_id,
                    text=chunk_text,
                    metadata=metadata,
                )
            )

        if not prepared_chunks:
            raise SkipDocument("empty_text")
        return prepared_chunks

    def process_batch(self, prepared_chunks: list[PreparedChunk], *, dry_run: bool) -> None:
        """Embed and optionally upsert one batch."""
        texts = [chunk.text for chunk in prepared_chunks]
        embeddings = self.embedder.embed(texts)
        if len(embeddings) != len(texts):
            raise RuntimeError(
                f"fel antal embeddings ({len(embeddings)} av {len(texts)})"
            )
        if dry_run:
            return
        self.collection.upsert(
            ids=[chunk.chunk_id for chunk in prepared_chunks],
            embeddings=embeddings,
            documents=texts,
            metadatas=[chunk.metadata for chunk in prepared_chunks],
        )

    @property
    def embedder(self) -> ArnEmbedder:
        """Lazily initialize the ARN embedder."""
        if self._embedder is None:
            self._embedder = ArnEmbedder()
        return self._embedder

    @property
    def collection(self) -> Any:
        """Lazily initialize the target Chroma collection."""
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
    """Print the required end-of-run summary."""
    print("ARN bulk-indexering klar.")
    print(f"  JSON-filer lästa:    {stats['json_files_read']}")
    print(f"  Dokument indexerade: {stats['documents_indexed']}")
    print(f"  Skippade:            {stats['skipped']} (tom text)")
    print(f"  Chunks totalt:       {stats['total_chunks']}")
    print(f"  Collection:          {COLLECTION_NAME}")
    print(f"  Chroma-sökväg:       {CHROMA_PATH}")
    print(f"  Körläge:             {'DRY-RUN' if dry_run else 'LIVE'}")


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Bulk-indexera ARN-JSON till ChromaDB")
    parser.add_argument("--max-docs", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s:%(name)s:%(message)s",
    )

    indexer = ArnBulkIndexer()
    try:
        stats = indexer.run(
            max_docs=args.max_docs,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )
    except ExistingCollectionError as exc:
        print(f"FEL: {COLLECTION_NAME} innehåller redan {exc.count} chunks.")
        print("     Avbryt. Kör manuellt: collection.delete_collection() om avsett.")
        return 1

    print_summary(stats, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
