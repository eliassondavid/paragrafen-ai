"""Bulk-index Ds raw documents directly into ChromaDB."""

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
import time
from typing import Any

import chromadb
import yaml

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from index.embedder import Embedder
from normalize.sou_parser import parse_sou_html
from pipelines.common.chunk_base import ChunkConfig, ForarbeteChunker
from pipelines.common.parse_base import Section

os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")

logger = logging.getLogger("paragrafenai.noop")

COLLECTION_NAME = "paragrafen_ds_v1"
CHROMA_PATH = "data/index/chroma/ds"
RAW_DIR = "data/raw/ds"
EMBEDDING_MODEL = "KBLab/sentence-bert-swedish-cased"
SCHEMA_VERSION = "v0.15"
AUTHORITY_LEVEL = "preparatory"
SOURCE_TYPE = "forarbete"
DOCUMENT_SUBTYPE = "ds"
PAGE_FIELD_WARNING = (
    "Section/ChunkedSection saknar page_start/page_end; Ds-indexern bär därför sidinformation separat."
)


@dataclass
class PreparedChunk:
    """One chunk ready for embedding and indexing."""

    chunk_id: str
    text: str
    metadata: dict[str, Any]


def extract_metadata(raw: dict) -> dict:
    """Extrahera metadata från Ds-råfilens nästlade format."""
    dok = raw.get("status_json", {}).get("dokumentstatus", {}).get("dokument", {})

    rm = dok.get("rm", "")
    bet_nr = dok.get("beteckning", "")
    titel = dok.get("titel", "")
    organ = dok.get("organ", "")
    datum = dok.get("datum", "") or raw.get("metadata", {}).get("datum", "")
    dok_id = raw.get("dok_id", "") or dok.get("dok_id", "")

    # Bygg fullständig beteckning — API:et ger bara löpnummer
    beteckning = f"Ds {rm}:{bet_nr}" if rm and bet_nr else ""

    return {
        "dok_id": dok_id,
        "rm": rm,
        "nummer": int(bet_nr) if bet_nr.isdigit() else 0,
        "beteckning": beteckning,
        "titel": titel,
        "organ": organ,
        "datum": datum,
    }


def build_chunk_id(rm: str, nummer: int, filename: str, chunk_index: int) -> str:
    """Bygg namespace-ID för Ds-chunk."""
    part_match = re.search(r"_d(\d+)", filename or "")
    part_suffix = f"_d{part_match.group(1)}" if part_match else ""
    return f"ds::{rm}_{nummer}{part_suffix}_chunk_{chunk_index:03d}"


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Bulk-indexera Ds till ChromaDB")
    parser.add_argument("--max-docs", type=int, default=None)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parsa och chunka men skriv inte till Chroma",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s:%(name)s:%(message)s",
    )

    indexer = DsBulkIndexer()
    stats = indexer.run(
        dry_run=args.dry_run,
        max_docs=args.max_docs,
        verbose=args.verbose,
    )
    print_summary(stats)

    if stats["aborted"]:
        return 1
    return 0


class DsBulkIndexer:
    """Read raw Ds JSON, parse, chunk, embed and upsert to Chroma."""

    def __init__(
        self,
        *,
        raw_dir: str | Path = RAW_DIR,
        chroma_path: str | Path = CHROMA_PATH,
        collection_name: str = COLLECTION_NAME,
        batch_size: int = 100,
        config_path: str | Path = "config/embedding_config.yaml",
        rank_config_path: str | Path = "config/forarbete_rank.yaml",
    ) -> None:
        self.repo_root = Path(__file__).resolve().parent.parent
        self.raw_dir = self._resolve_path(raw_dir)
        self.chroma_path = self._resolve_path(chroma_path)
        self.collection_name = collection_name
        self.batch_size = max(1, int(batch_size))
        self.config_path = self._resolve_path(config_path)
        self.rank_config_path = self._resolve_path(rank_config_path)

        self.chunker = ForarbeteChunker(
            config=ChunkConfig(
                min_tokens=150,
                max_tokens=350,
                overlap_tokens=35,
            )
        )
        self.ds_rank = load_ds_rank(self.rank_config_path)
        self._embedder: Embedder | None = None
        self._collection: Any | None = None
        self._warned_page_fields = False

    def run(
        self,
        *,
        dry_run: bool = False,
        max_docs: int | None = None,
        verbose: bool = False,
    ) -> dict[str, int | bool]:
        """Execute the full pipeline over all raw Ds files."""
        raw_files = collect_raw_files(self.raw_dir)
        if max_docs is not None:
            raw_files = raw_files[:max_docs]

        stats: dict[str, int | bool] = {
            "total_files": len(raw_files),
            "ok": 0,
            "skipped": 0,
            "failed": 0,
            "total_chunks": 0,
            "aborted": False,
        }
        total = len(raw_files)

        for index, raw_path in enumerate(raw_files, start=1):
            try:
                raw = load_raw_json(raw_path)
            except Exception as exc:
                log_error(raw_path.name, f"kunde inte läsa JSON ({exc})")
                stats["failed"] += 1
                if should_abort(stats):
                    stats["aborted"] = True
                    break
                continue

            try:
                prepared_chunks = self.prepare_document(raw, raw_path=raw_path)
            except SkipDocument as exc:
                log_skip(raw_path.name, str(exc))
                stats["skipped"] += 1
                continue
            except Exception as exc:
                log_error(raw_path.name, str(exc))
                stats["failed"] += 1
                if should_abort(stats):
                    stats["aborted"] = True
                    break
                continue

            if dry_run:
                stats["ok"] += 1
                stats["total_chunks"] += len(prepared_chunks)
                if verbose or index % 100 == 0:
                    log_progress(index, total, prepared_chunks)
                continue

            texts = [chunk.text for chunk in prepared_chunks]
            try:
                embeddings = self.embedder.embed(texts)
            except Exception as exc:
                log_error(raw_path.name, f"embedding-fel ({exc})")
                stats["failed"] += 1
                if should_abort(stats):
                    stats["aborted"] = True
                    break
                continue

            if len(embeddings) != len(texts):
                log_error(
                    raw_path.name,
                    f"fel antal embeddings ({len(embeddings)} av {len(texts)})",
                )
                stats["failed"] += 1
                if should_abort(stats):
                    stats["aborted"] = True
                    break
                continue

            if not self.upsert_chunks(prepared_chunks, embeddings):
                stats["failed"] += 1
                if should_abort(stats):
                    stats["aborted"] = True
                    break
                continue

            stats["ok"] += 1
            stats["total_chunks"] += len(prepared_chunks)
            if verbose or index % 100 == 0:
                log_progress(index, total, prepared_chunks)

        return stats

    def prepare_document(
        self,
        raw: dict[str, Any],
        *,
        raw_path: Path | None = None,
    ) -> list[PreparedChunk]:
        """Prepare one raw Ds document for indexing."""
        if raw_path is None:
            raise SkipDocument("missing raw path")

        html_content = str(raw.get("html_content", "") or "")
        if not html_content or len(html_content) < 10_000:
            raise SkipDocument("html_stub_no_text")

        meta = extract_metadata(raw)
        if not meta["dok_id"]:
            raise SkipDocument("missing dok_id")
        if not meta["rm"] or meta["nummer"] <= 0:
            raise SkipDocument("missing rm_or_nummer")
        if not meta["beteckning"]:
            raise SkipDocument("missing beteckning")

        sections = parse_sou_html(html_content, str(meta["dok_id"]))
        if not sections:
            raise SkipDocument("parser returned no sections")

        chunked_sections = self.chunk_sections(sections)
        if not chunked_sections:
            raise SkipDocument("chunker returned no chunks")

        content_hash = hashlib.sha256(
            html_content.encode("utf-8")
        ).hexdigest()

        prepared_chunks: list[PreparedChunk] = []
        filename = str(raw.get("filename") or raw_path.stem)
        for chunk_index, chunk in enumerate(chunked_sections):
            chunk_text = chunk.chunk_text
            chunk_id = build_chunk_id(meta["rm"], meta["nummer"], filename, chunk_index)
            metadata = {
                "namespace": chunk_id,
                "chunk_id": chunk_id,
                "chunk_index": chunk_index,
                "source_type": SOURCE_TYPE,
                "document_subtype": DOCUMENT_SUBTYPE,
                "dok_id": meta["dok_id"],
                "source_document_id": meta["dok_id"],
                "beteckning": meta["beteckning"],
                "titel": meta["titel"],
                "organ": meta["organ"],
                "rm": meta["rm"],
                "datum": meta["datum"],
                "authority_level": AUTHORITY_LEVEL,
                "forarbete_rank": self.ds_rank,
                "citation": f"Ds {meta['rm']}:{meta['nummer']}",
                "section": chunk.section_path,
                "token_count": chunk.token_count,
                "chunk_text": chunk_text,
                "content_hash": content_hash,
                "schema_version": SCHEMA_VERSION,
                "legal_area": json.dumps([], ensure_ascii=False),
                "references_to": json.dumps([], ensure_ascii=False),
            }

            prepared_chunks.append(
                PreparedChunk(
                    chunk_id=chunk_id,
                    text=chunk_text,
                    metadata=metadata,
                )
            )

        return prepared_chunks

    def chunk_sections(self, parsed_sections: list[dict[str, Any]]) -> list[Any]:
        """Convert parsed sections and chunk them."""
        section_objects: list[Section] = []
        for section in parsed_sections:
            text = str(section.get("text") or "").strip()
            if not text:
                continue
            if not self._warned_page_fields:
                logger.warning(PAGE_FIELD_WARNING)
                self._warned_page_fields = True

            section_objects.append(
                Section(
                    section_key=str(section.get("section") or "other"),
                    section_title=str(section.get("section_title") or "other"),
                    text=text,
                    level=2,
                )
            )

        return self.chunker.chunk_sections(section_objects)

    def upsert_chunks(
        self,
        prepared_chunks: list[PreparedChunk],
        embeddings: list[list[float]],
    ) -> bool:
        """Upsert chunks to Chroma in batches with one retry."""
        collection = self.collection
        ids = [chunk.chunk_id for chunk in prepared_chunks]
        texts = [chunk.text for chunk in prepared_chunks]
        metadatas = [chunk.metadata for chunk in prepared_chunks]

        for start in range(0, len(ids), self.batch_size):
            end = start + self.batch_size
            batch_ids = ids[start:end]
            batch_texts = texts[start:end]
            batch_embeddings = embeddings[start:end]
            batch_metadatas = metadatas[start:end]
            try:
                collection.upsert(
                    ids=batch_ids,
                    embeddings=batch_embeddings,
                    documents=batch_texts,
                    metadatas=batch_metadatas,
                )
            except Exception as exc:
                logger.error("Upsert-fel för batch %s-%s: %s", start, end - 1, exc)
                time.sleep(0.5)
                try:
                    collection.upsert(
                        ids=batch_ids,
                        embeddings=batch_embeddings,
                        documents=batch_texts,
                        metadatas=batch_metadatas,
                    )
                except Exception as retry_exc:
                    logger.error(
                        "Retry misslyckades för batch %s-%s: %s",
                        start,
                        end - 1,
                        retry_exc,
                    )
                    return False
        return True

    @property
    def embedder(self) -> Embedder:
        """Lazily initialize the shared embedder."""
        if self._embedder is None:
            self._embedder = Embedder(config_path=self.config_path)
        return self._embedder

    @property
    def collection(self) -> Any:
        """Lazily initialize the target Chroma collection."""
        if self._collection is None:
            self.chroma_path.mkdir(parents=True, exist_ok=True)
            client = chromadb.PersistentClient(path=str(self.chroma_path.resolve()))
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


class SkipDocument(Exception):
    """Raised when a document should be skipped without aborting the run."""


def collect_raw_files(raw_dir: Path) -> list[Path]:
    """Collect raw Ds files."""
    return sorted(raw_dir.glob("*.json"))


def load_raw_json(raw_path: Path) -> dict[str, Any]:
    """Load one raw JSON file."""
    with raw_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError("råfilen innehåller inte ett JSON-objekt")
    return data


def load_ds_rank(config_path: Path) -> int:
    """Load forarbete_rank for Ds from YAML."""
    with config_path.open("r", encoding="utf-8") as fh:
        rank_config = yaml.safe_load(fh) or {}

    forarbete_types = rank_config.get("forarbete_types")
    if not isinstance(forarbete_types, dict) or "ds" not in forarbete_types:
        raise KeyError("Nyckeln 'forarbete_types.ds' saknas i config/forarbete_rank.yaml")

    rank = forarbete_types["ds"].get("rank")
    if not isinstance(rank, int):
        raise ValueError("forarbete_rank för ds saknas eller är inte ett heltal.")
    return rank


def should_abort(stats: dict[str, int | bool]) -> bool:
    """Abort if failure ratio exceeds 1%."""
    total_files = int(stats["total_files"])
    failed = int(stats["failed"])
    if total_files <= 0:
        return False
    return (failed / total_files) > 0.01


def log_ok(filename: str, message: str) -> None:
    """Log success messages consistently."""
    logger.info("[OK] %s — %s", filename, message)


def log_skip(filename: str, message: str) -> None:
    """Log skipped files consistently."""
    logger.warning("[SKIP] %s — %s", filename, message)


def log_error(filename: str, message: str) -> None:
    """Log failures consistently."""
    logger.error("[ERROR] %s — %s", filename, message)


def log_progress(current: int, total: int, prepared_chunks: list[PreparedChunk]) -> None:
    """Log progress in namespace format."""
    if not prepared_chunks:
        return
    document_namespace = prepared_chunks[0].chunk_id.rsplit("_chunk_", 1)[0]
    log_ok(document_namespace, f"[{current}/{total}] {len(prepared_chunks)} chunks")


def print_summary(stats: dict[str, int | bool]) -> None:
    """Print the required end-of-run summary."""
    total_files = int(stats["total_files"])
    failed = int(stats["failed"])
    failed_ratio = (failed / total_files * 100) if total_files else 0.0
    print("Indexering klar:")
    print(f"  Filer: {total_files}")
    print(f"  OK: {stats['ok']}")
    print(f"  Skippade: {stats['skipped']}")
    print(f"  Failed: {failed}")
    print(f"  Chunks: {stats['total_chunks']}")
    print(f"  Felkvot: {failed_ratio:.2f}%")
    if stats["aborted"]:
        print("  Avbruten: ja (felkvot > 1%)")


if __name__ == "__main__":
    raise SystemExit(main())
