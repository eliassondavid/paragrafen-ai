"""Bulk-index JK raw JSON documents into ChromaDB."""

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
from typing import Any

import chromadb

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from index.embedder import Embedder
from pipelines.common.chunk_base import ChunkConfig, ForarbeteChunker
from pipelines.common.parse_base import Section

os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")

logger = logging.getLogger("jk_bulk_indexer")

COLLECTION_NAME = "paragrafen_jk_v1"
CHROMA_PATH = "data/index/chroma/jk"
INPUT_DIR = "data/raw/jk/decisions"
DOCUMENT_SUBTYPE = "jk"
SOURCE_TYPE = "myndighetsbeslut"
SCHEMA_VERSION = "v0.15"
LICENSE = "public_domain"
FAIL_RATE_WARNING_THRESHOLD = 0.01
BATCH_SIZE = 100
CHUNK_CONFIG = ChunkConfig(min_tokens=150, max_tokens=350, overlap_tokens=35)

KATEGORI_AUTHORITY = {
    "Skadeståndsärenden": "binding",
    "Ersättning vid frihetsinskränkning": "binding",
    "Tillsynsärenden": "guiding",
    "Tryck- och yttrandefrihetsärenden": "binding",
    "Remissyttranden": "persuasive",
}

SECTION_PATTERNS = [
    ("beslut", r"(?i)^(?:Justitiekanslerns?\s+beslut|Beslut)$"),
    ("arendet", r"(?i)^(?:Ärendet|Bakgrund)$"),
    ("bedomning", r"(?i)^(?:Justitiekanslerns?\s+bedömning|Bedömning|Skälen?)$"),
]


@dataclass
class PreparedChunk:
    """One chunk ready for optional embedding and Chroma upsert."""

    chunk_id: str
    text: str
    metadata: dict[str, Any]


class SkipDocument(Exception):
    """Raised when one JK JSON document should be skipped."""


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def normalize_dnr_for_namespace(dnr: str) -> str:
    candidate = normalize_whitespace(dnr).replace("/", "_")
    candidate = re.sub(r"[^0-9A-Za-z_]+", "_", candidate)
    return re.sub(r"_+", "_", candidate).strip("_")


def build_chunk_id(dnr: str, chunk_index: int) -> str:
    return f"jk::{normalize_dnr_for_namespace(dnr)}_chunk_{chunk_index:03d}"


def collect_json_files(input_dir: Path) -> list[Path]:
    return sorted(
        path
        for path in input_dir.glob("*.json")
        if path.is_file() and not path.name.startswith("_")
    )


def load_json_document(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("JSON-roten är inte ett objekt.")
    return payload


def authority_level_for_document(raw: dict[str, Any]) -> str:
    return KATEGORI_AUTHORITY.get(str(raw.get("kategori") or "").strip(), "guiding")


def classify_section(line: str) -> tuple[str, str] | None:
    normalized = normalize_whitespace(line)
    for section_key, pattern in SECTION_PATTERNS:
        if re.fullmatch(pattern, normalized):
            return section_key, normalized
    return None


def parse_sections(text: str) -> list[Section]:
    lines = [
        normalize_whitespace(line)
        for line in (text or "").splitlines()
        if normalize_whitespace(line)
    ]
    if not lines:
        return []

    sections: list[Section] = []
    current_key = "other"
    current_title = "Övrigt"
    current_parts: list[str] = []

    def flush() -> None:
        body = "\n".join(part for part in current_parts if part).strip()
        if not body:
            return
        sections.append(
            Section(
                section_key=current_key,
                section_title=current_title,
                text=body,
                level=1,
            )
        )

    for line in lines:
        section_match = classify_section(line)
        if section_match:
            flush()
            current_parts = [line]
            current_key, current_title = section_match
            continue
        current_parts.append(line)

    flush()
    return sections or [
        Section(
            section_key="other",
            section_title="Övrigt",
            text="\n".join(lines),
            level=1,
        )
    ]


class JKBulkIndexer:
    """Read JK raw JSON, chunk it, and upsert the chunks into ChromaDB."""

    def __init__(
        self,
        *,
        input_dir: str | Path = INPUT_DIR,
        chroma_path: str | Path = CHROMA_PATH,
        collection_name: str = COLLECTION_NAME,
        batch_size: int = BATCH_SIZE,
        chunker: ForarbeteChunker | None = None,
        embedder: Embedder | None = None,
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
        json_files = collect_json_files(self.input_dir)
        if max_docs is not None:
            json_files = json_files[:max_docs]

        stats = {
            "json_files_read": 0,
            "documents_indexed": 0,
            "skipped": 0,
            "failed": 0,
            "total_chunks": 0,
            "count_before": self.collection.count() if not dry_run else 0,
            "count_after": self.collection.count() if not dry_run else 0,
        }

        pending_chunks: list[PreparedChunk] = []

        for index, json_path in enumerate(json_files, start=1):
            try:
                document = load_json_document(json_path)
            except Exception as exc:
                logger.error("[ERROR] %s — kunde inte läsa JSON (%s)", json_path.name, exc)
                stats["failed"] += 1
                continue

            stats["json_files_read"] += 1

            try:
                prepared_chunks = self.prepare_document(document)
            except SkipDocument as exc:
                logger.warning("[SKIP] %s — %s", json_path.name, exc)
                stats["skipped"] += 1
                continue
            except Exception as exc:
                logger.error("[ERROR] %s — %s", json_path.name, exc)
                stats["failed"] += 1
                continue

            stats["documents_indexed"] += 1
            stats["total_chunks"] += len(prepared_chunks)

            if dry_run:
                if verbose or index % 100 == 0:
                    namespace = prepared_chunks[0].chunk_id.rsplit("_chunk_", 1)[0]
                    logger.info("[OK] %s — %s chunks", namespace, len(prepared_chunks))
                continue

            pending_chunks.extend(prepared_chunks)
            while len(pending_chunks) >= self.batch_size:
                batch = pending_chunks[: self.batch_size]
                self.upsert_chunks(batch)
                pending_chunks = pending_chunks[self.batch_size :]

            if verbose or index % 100 == 0:
                namespace = prepared_chunks[0].chunk_id.rsplit("_chunk_", 1)[0]
                logger.info("[OK] %s — %s chunks", namespace, len(prepared_chunks))

        if pending_chunks and not dry_run:
            self.upsert_chunks(pending_chunks)

        if not dry_run:
            stats["count_after"] = self.collection.count()

        fail_rate = (
            stats["failed"] / stats["json_files_read"] if stats["json_files_read"] else 0.0
        )
        if fail_rate > FAIL_RATE_WARNING_THRESHOLD:
            logger.warning("Felkvot %.2f%% överstiger 1%%.", fail_rate * 100.0)

        return stats

    def prepare_document(self, raw: dict[str, Any]) -> list[PreparedChunk]:
        dnr = str(raw.get("dnr") or "").strip()
        if not dnr:
            raise SkipDocument("missing dnr")

        text_content = str(raw.get("text_content") or "").strip()
        if not text_content:
            raise SkipDocument("empty_text")

        sections = parse_sections(text_content)
        if not sections:
            raise SkipDocument("empty_text")

        chunked_sections = self.chunker.chunk_sections(sections)
        if not chunked_sections:
            raise SkipDocument("empty_text")

        authority_level = authority_level_for_document(raw)
        title = str(raw.get("titel") or "").strip()
        beslutsdatum = str(raw.get("beslutsdatum") or "").strip()
        kategori = str(raw.get("kategori") or "").strip()
        source_url = str(raw.get("source_url") or "").strip()

        prepared: list[PreparedChunk] = []
        fallback_section = sections[0].section_key if len(sections) == 1 else "other"
        for chunk_index, chunk in enumerate(chunked_sections):
            chunk_text = str(chunk.chunk_text or "").strip()
            if not chunk_text:
                continue

            chunk_id = build_chunk_id(dnr, chunk_index)
            section = chunk.section_path if chunk.section_path != "document" else fallback_section
            metadata = {
                "chunk_id": chunk_id,
                "namespace": chunk_id,
                "source_type": SOURCE_TYPE,
                "document_subtype": DOCUMENT_SUBTYPE,
                "authority_level": authority_level,
                "kategori": kategori,
                "dnr": dnr,
                "titel": title,
                "beslutsdatum": beslutsdatum,
                "section": section,
                "content_hash": hashlib.sha256(chunk_text.encode("utf-8")).hexdigest(),
                "source_url": source_url,
                "schema_version": SCHEMA_VERSION,
                "license": LICENSE,
                "is_active": True,
            }
            prepared.append(PreparedChunk(chunk_id=chunk_id, text=chunk_text, metadata=metadata))

        if not prepared:
            raise SkipDocument("empty_text")
        return prepared

    def upsert_chunks(self, prepared_chunks: list[PreparedChunk]) -> None:
        texts = [chunk.text for chunk in prepared_chunks]
        embeddings = self.embedder.embed(texts)
        if len(embeddings) != len(texts):
            raise RuntimeError(
                f"fel antal embeddings ({len(embeddings)} av {len(texts)})"
            )
        self.collection.upsert(
            ids=[chunk.chunk_id for chunk in prepared_chunks],
            documents=texts,
            embeddings=embeddings,
            metadatas=[chunk.metadata for chunk in prepared_chunks],
        )

    @property
    def embedder(self) -> Embedder:
        if self._embedder is None:
            self._embedder = Embedder()
        return self._embedder

    @property
    def collection(self) -> Any:
        if self._collection is None:
            self.chroma_path.mkdir(parents=True, exist_ok=True)
            client = self.client_factory(path=str(self.chroma_path))
            self._collection = client.get_or_create_collection(name=self.collection_name)
        return self._collection

    def _resolve_path(self, path_value: str | Path) -> Path:
        candidate = Path(path_value)
        if candidate.is_absolute():
            return candidate
        return self.repo_root / candidate


def print_summary(stats: dict[str, int], *, dry_run: bool) -> None:
    total = stats["json_files_read"]
    fail_rate = (stats["failed"] / total * 100.0) if total else 0.0
    print("JK-indexering klar.")
    print(f"  JSON-filer lästa:    {stats['json_files_read']}")
    print(f"  Dokument indexerade: {stats['documents_indexed']}")
    print(f"  Skippade:            {stats['skipped']}")
    print(f"  Failed:              {stats['failed']}")
    print(f"  Chunks totalt:       {stats['total_chunks']}")
    print(f"  Felkvot:             {fail_rate:.2f}%")
    print(f"  Körläge:             {'DRY-RUN' if dry_run else 'LIVE'}")
    if not dry_run:
        print(f"  Collection före:     {stats['count_before']}")
        print(f"  Collection efter:    {stats['count_after']}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bulk-indexera JK-JSON till ChromaDB.")
    parser.add_argument("--max-docs", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    setup_logging(args.verbose)
    indexer = JKBulkIndexer()
    stats = indexer.run(max_docs=args.max_docs, dry_run=args.dry_run, verbose=args.verbose)
    print_summary(stats, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
