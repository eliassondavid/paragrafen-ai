"""Bulk-index dir raw documents directly into ChromaDB."""

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

COLLECTION_NAME = "paragrafen_riksdag_v1"
CHROMA_PATH = "data/index/chroma/riksdag"
INPUT_DIR = "data/raw/dir"
SCHEMA_VERSION = "v0.18"
LICENSE = "public_domain"
SOURCE_TYPE = "forarbete"
DOCUMENT_SUBTYPE = "dir"
AUTHORITY_LEVEL = "preparatory"
PAGE_FIELD_WARNING = (
    "Section/ChunkedSection saknar page_start/page_end; dir-indexern bar darfor sidinformation separat."
)


@dataclass
class PreparedChunk:
    """One chunk ready for embedding and indexing."""

    chunk_id: str
    text: str
    metadata: dict[str, Any]


class SkipDocument(Exception):
    """Raised when a document should be skipped without failing the run."""


def extract_nested_dokument(raw: dict[str, Any]) -> dict[str, Any]:
    """Return the nested dokument payload used by dir raw files."""
    status_json = raw.get("status_json")
    if isinstance(status_json, dict):
        dokumentstatus = status_json.get("dokumentstatus")
        if isinstance(dokumentstatus, dict):
            dokument = dokumentstatus.get("dokument")
            if isinstance(dokument, dict):
                return dokument

    dokumentstatus = raw.get("dokumentstatus")
    if isinstance(dokumentstatus, dict):
        dokument = dokumentstatus.get("dokument")
        if isinstance(dokument, dict):
            return dokument

    return {}


def normalize_beteckning(beteckning: str, rm: str) -> str:
    """Normalize to canonical dir citation style."""
    value = (beteckning or "").strip()
    if re.search(r"(?i)\bdir\.?\b", value):
        return value
    if rm and value:
        return f"Dir. {rm}:{value}"
    return value


def extract_metadata(raw: dict[str, Any]) -> dict[str, Any]:
    """Extract metadata from nested dir raw format."""
    dokument = extract_nested_dokument(raw)
    html_content = str(raw.get("html_content") or "")
    dok_id = str(raw.get("dok_id") or dokument.get("dok_id") or "").strip()
    rm = str(dokument.get("rm") or "").strip()
    raw_beteckning = str(dokument.get("beteckning") or "").strip()
    beteckning = normalize_beteckning(raw_beteckning, rm)
    titel = str(dokument.get("titel") or "").strip()
    datum = str(dokument.get("datum") or raw.get("metadata", {}).get("datum") or "").strip()[:10]
    organ = str(dokument.get("organ") or "").strip()

    nummer = 0
    match = re.search(r"(\d+)\s*$", beteckning)
    if match:
        nummer = int(match.group(1))

    html_available = bool(html_content and html_content != "HTML saknas" and len(html_content) > 100)
    return {
        "dok_id": dok_id,
        "beteckning": beteckning,
        "rm": rm,
        "nummer": nummer,
        "titel": titel,
        "datum": datum,
        "organ": organ,
        "html_content": html_content,
        "html_available": bool(raw.get("html_available", html_available)),
        "source_url": f"https://data.riksdagen.se/dokument/{dok_id}" if dok_id else "",
    }


def build_chunk_id(meta: dict[str, Any], chunk_index: int) -> str:
    """Build the mandated dir namespace."""
    rm = str(meta.get("rm") or "").strip()
    nummer = coerce_int(meta.get("nummer"))
    dok_id = str(meta.get("dok_id") or "").strip().lower()
    if rm and nummer > 0:
        rm_norm = rm.replace("/", "-")
        return f"dir::{rm_norm}_{nummer}_chunk_{chunk_index:03d}"
    return f"dir::{dok_id}_chunk_{chunk_index:03d}"


def build_pinpoint(page_start: int, page_end: int) -> str:
    """Build pinpoint string from parser page range."""
    if page_start <= 0:
        return ""
    if page_end <= 0 or page_start == page_end:
        return f"s. {page_start}"
    return f"s. {page_start}–{page_end}"


def serialize_json_list(value: Any) -> str:
    """Serialize metadata lists to JSON strings for Chroma."""
    if value is None:
        return json.dumps([], ensure_ascii=False)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError, ValueError):
            return json.dumps([value] if value else [], ensure_ascii=False)
        if isinstance(parsed, list):
            return json.dumps(parsed, ensure_ascii=False)
        return json.dumps([parsed], ensure_ascii=False)
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False)
    return json.dumps([value], ensure_ascii=False)


def build_typed_references(references_raw: Any) -> str:
    """Convert references_to into a typed JSON string."""
    if not isinstance(references_raw, list):
        references_raw = []
    typed = [
        {"target": reference, "relation_type": "cites"}
        for reference in references_raw
        if isinstance(reference, str) and reference.strip()
    ]
    return json.dumps(typed, ensure_ascii=False)


def coerce_int(value: Any) -> int:
    """Best-effort integer coercion."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def should_abort(stats: dict[str, int | bool]) -> bool:
    """Abort once failure ratio exceeds 1%."""
    total_files = int(stats.get("total_files", 0) or 0)
    failed = int(stats.get("failed", 0) or 0)
    return total_files > 0 and (failed / total_files) > 0.01


def load_yaml(path: Path) -> dict[str, Any]:
    """Load a YAML config file."""
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config root must be a mapping: {path}")
    return data


def load_forarbete_rank(config_path: Path) -> int:
    """Load the dir forarbete rank from YAML."""
    rank = load_yaml(config_path).get("forarbete_types", {}).get("dir", {}).get("rank")
    if not isinstance(rank, int):
        raise ValueError("forarbete_rank for dir saknas eller ar inte ett heltal.")
    return rank


def load_embedding_model_name(config_path: Path, fallback_path: Path) -> str:
    """Load embedding model name from config, with fallback to embedding_config."""
    primary = load_yaml(config_path)
    model_name = str(primary.get("embedding", {}).get("production_model", "")).strip()
    if model_name:
        return model_name
    fallback = load_yaml(fallback_path)
    model_name = str(fallback.get("embedding", {}).get("production_model", "")).strip()
    if not model_name:
        raise ValueError("embedding.production_model saknas i embedding-konfigurationen.")
    return model_name


def collect_raw_files(input_dir: Path) -> list[Path]:
    """Collect raw dir files."""
    return sorted(path for path in input_dir.glob("*.json") if not path.name.startswith("_"))


def load_raw_json(raw_path: Path) -> dict[str, Any]:
    """Load one raw JSON file."""
    with raw_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError("rafilen innehaller inte ett JSON-objekt")
    return data


def log_progress(current: int, total: int, prepared_chunks: list[PreparedChunk]) -> None:
    """Log progress using the document namespace."""
    if not prepared_chunks:
        return
    namespace = prepared_chunks[0].chunk_id.rsplit("_chunk_", 1)[0]
    logger.info("[%s/%s] %s — %s chunks", current, total, namespace, len(prepared_chunks))


def print_summary(stats: dict[str, int | bool]) -> None:
    """Print end-of-run summary."""
    total_files = int(stats.get("total_files", 0) or 0)
    failed = int(stats.get("failed", 0) or 0)
    failed_ratio = (failed / total_files * 100.0) if total_files else 0.0
    print("Dir-indexering klar:")
    print(f"  Filer totalt:   {total_files}")
    print(f"  OK:             {stats['ok']}")
    print(f"  Skippade:       {stats['skipped']}")
    print(f"  Failed:         {failed}")
    print(f"  Chunks totalt:  {stats['total_chunks']}")
    print(f"  Felkvot:        {failed_ratio:.2f}%")
    if stats.get("aborted"):
        print("  Status:         AVBRUTEN (felkvot over 1%)")


class DirBulkIndexer:
    """Read raw dir JSON, parse, chunk, embed and upsert to Chroma."""

    def __init__(
        self,
        *,
        input_dir: str | Path = INPUT_DIR,
        chroma_path: str | Path = CHROMA_PATH,
        collection_name: str = COLLECTION_NAME,
        batch_size: int = 100,
        config_path: str | Path = "config/sources.yaml",
        rank_config_path: str | Path = "config/forarbete_rank.yaml",
        fallback_embedding_config_path: str | Path = "config/embedding_config.yaml",
    ) -> None:
        self.repo_root = Path(__file__).resolve().parent.parent
        self.input_dir = self._resolve_path(input_dir)
        self.chroma_path = self._resolve_path(chroma_path)
        self.collection_name = collection_name
        self.batch_size = max(1, int(batch_size))
        self.config_path = self._resolve_path(config_path)
        self.rank_config_path = self._resolve_path(rank_config_path)
        self.fallback_embedding_config_path = self._resolve_path(fallback_embedding_config_path)
        self.skip_list_path = self.input_dir / "_skip_list.jsonl"

        self.chunker = ForarbeteChunker(
            config=ChunkConfig(
                min_tokens=150,
                max_tokens=350,
                overlap_tokens=35,
            )
        )
        self.forarbete_rank = load_forarbete_rank(self.rank_config_path)
        self.embedding_model_name = load_embedding_model_name(
            self.config_path,
            self.fallback_embedding_config_path,
        )
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
        """Execute the full pipeline over all raw dir files."""
        raw_files = collect_raw_files(self.input_dir)
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

        if not dry_run:
            count_before = int(self.collection.count())
            logger.info("paragrafen_riksdag_v1 fore ingest: %s chunks", count_before)
            if count_before > 0:
                logger.warning("Collectionen innehaller redan chunks; fortsatter med upsert.")

        for index, raw_path in enumerate(raw_files, start=1):
            try:
                raw = load_raw_json(raw_path)
            except (OSError, ValueError, json.JSONDecodeError, TypeError) as exc:
                logger.error("FAIL: %s — kunde inte lasa JSON (%s)", raw_path.name, exc)
                stats["failed"] += 1
                if should_abort(stats):
                    stats["aborted"] = True
                    break
                continue

            try:
                prepared_chunks = self.prepare_document(raw, raw_path=raw_path)
            except SkipDocument as exc:
                logger.warning("SKIP: %s — %s", raw_path.name, exc)
                stats["skipped"] += 1
                continue
            except (OSError, TypeError, ValueError, KeyError) as exc:
                logger.error("FAIL: %s — %s", raw_path.name, exc)
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
            embeddings = self.embedder.embed(texts)
            if len(embeddings) != len(texts):
                logger.error(
                    "FAIL: %s — fel antal embeddings (%s av %s)",
                    raw_path.name,
                    len(embeddings),
                    len(texts),
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
        raw_path: Path,
    ) -> list[PreparedChunk]:
        """Prepare one raw dir document for indexing."""
        meta = extract_metadata(raw)
        html_content = str(meta.get("html_content") or "")

        if not html_content or html_content == "HTML saknas":
            self._append_skip(raw_path, meta, "html_missing")
            raise SkipDocument("html_content tom eller HTML saknas")
        if len(html_content) < 100:
            self._append_skip(raw_path, meta, "html_stub")
            raise SkipDocument("html_content kortare an 100 tecken")
        if not meta.get("dok_id"):
            self._append_skip(raw_path, meta, "missing_dok_id")
            raise SkipDocument("missing dok_id")

        sections = parse_sou_html(html_content, str(meta.get("dok_id") or ""))
        if not sections:
            self._append_skip(raw_path, meta, "parser_no_sections")
            raise SkipDocument("parser returned no sections")

        chunked_sections, page_ranges = self.chunk_sections(sections)
        if not chunked_sections:
            self._append_skip(raw_path, meta, "chunker_no_chunks")
            raise SkipDocument("chunker returned no chunks")

        content_hash = hashlib.sha256(html_content.encode("utf-8")).hexdigest()
        legal_area = serialize_json_list(raw.get("legal_area", []))
        references_to = build_typed_references(raw.get("references_to", []))

        prepared_chunks: list[PreparedChunk] = []
        for chunk_index, (chunk, page_range) in enumerate(zip(chunked_sections, page_ranges)):
            page_start, page_end = page_range
            chunk_id = build_chunk_id(meta, chunk_index)
            metadata = {
                "chunk_id": chunk_id,
                "namespace": chunk_id,
                "source_type": SOURCE_TYPE,
                "document_subtype": DOCUMENT_SUBTYPE,
                "authority_level": AUTHORITY_LEVEL,
                "forarbete_rank": self.forarbete_rank,
                "dok_id": str(meta.get("dok_id") or ""),
                "beteckning": str(meta.get("beteckning") or ""),
                "rm": str(meta.get("rm") or ""),
                "nummer": coerce_int(meta.get("nummer")),
                "titel": str(meta.get("titel") or ""),
                "datum": str(meta.get("datum") or ""),
                "source_url": str(meta.get("source_url") or ""),
                "legal_area": legal_area,
                "references_to": references_to,
                "chunk_index": chunk_index,
                "chunk_text": chunk.chunk_text,
                "section_type": chunk.section_path or "other",
                "section_title": chunk.section_title,
                "pinpoint": build_pinpoint(page_start, page_end),
                "content_hash": content_hash,
                "schema_version": SCHEMA_VERSION,
                "license": LICENSE,
                "embedding_model": self.embedding_model_name,
            }
            prepared_chunks.append(
                PreparedChunk(
                    chunk_id=chunk_id,
                    text=chunk.chunk_text,
                    metadata=metadata,
                )
            )

        return prepared_chunks

    def chunk_sections(
        self,
        parsed_sections: list[dict[str, Any]],
    ) -> tuple[list[Any], list[tuple[int, int]]]:
        """Convert parser output to Section objects and preserve page ranges."""
        section_objects: list[Section] = []
        usable_page_ranges: list[tuple[int, int]] = []
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
            usable_page_ranges.append(
                (
                    coerce_int(section.get("page_start")),
                    coerce_int(section.get("page_end")),
                )
            )

        chunked_sections = self.chunker.chunk_sections(section_objects)
        if not chunked_sections:
            return [], []

        page_ranges = self._build_page_ranges(section_objects, usable_page_ranges)
        if len(page_ranges) != len(chunked_sections):
            logger.warning(
                "Chunk/page-range mismatch (%s != %s); pageinfo nollstalls defensivt",
                len(chunked_sections),
                len(page_ranges),
            )
            return chunked_sections, [(0, 0)] * len(chunked_sections)
        return chunked_sections, page_ranges

    def _build_page_ranges(
        self,
        section_objects: list[Section],
        page_ranges: list[tuple[int, int]],
    ) -> list[tuple[int, int]]:
        usable_sections: list[tuple[Section, tuple[int, int]]] = []
        for section, page_range in zip(section_objects, page_ranges):
            if len((section.text or "").strip()) < self.chunker.config.min_chunk_chars:
                continue
            usable_sections.append((section, page_range))

        if not usable_sections:
            return []

        total_tokens = sum(self.chunker.count_tokens(section.text) for section, _ in usable_sections)
        if total_tokens < self.chunker.config.min_tokens:
            starts = [page_start for _, (page_start, _) in usable_sections if page_start > 0]
            ends = [page_end for _, (_, page_end) in usable_sections if page_end > 0]
            return [(min(starts) if starts else 0, max(ends) if ends else 0)]

        chunk_page_ranges: list[tuple[int, int]] = []
        for section, page_range in usable_sections:
            section_chunks = self.chunker._chunk_single_section(section)
            chunk_page_ranges.extend([page_range] * len(section_chunks))
        return chunk_page_ranges

    def upsert_chunks(
        self,
        prepared_chunks: list[PreparedChunk],
        embeddings: list[list[float]],
    ) -> bool:
        """Upsert chunks to Chroma in batches with one retry."""
        ids = [chunk.chunk_id for chunk in prepared_chunks]
        texts = [chunk.text for chunk in prepared_chunks]
        metadatas = [chunk.metadata for chunk in prepared_chunks]
        collection = self.collection

        for start in range(0, len(ids), self.batch_size):
            end = start + self.batch_size
            try:
                collection.upsert(
                    ids=ids[start:end],
                    embeddings=embeddings[start:end],
                    documents=texts[start:end],
                    metadatas=metadatas[start:end],
                )
            except Exception as exc:
                logger.error("Upsert-fel for batch %s-%s: %s", start, end - 1, exc)
                time.sleep(0.5)
                try:
                    collection.upsert(
                        ids=ids[start:end],
                        embeddings=embeddings[start:end],
                        documents=texts[start:end],
                        metadatas=metadatas[start:end],
                    )
                except Exception as retry_exc:
                    logger.error(
                        "Retry misslyckades for batch %s-%s: %s",
                        start,
                        end - 1,
                        retry_exc,
                    )
                    return False
        return True

    def _append_skip(self, raw_path: Path, meta: dict[str, Any], reason: str) -> None:
        payload = {
            "filename": raw_path.name,
            "dok_id": str(meta.get("dok_id") or ""),
            "reason": reason,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        with self.skip_list_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")

    @property
    def embedder(self) -> Embedder:
        """Lazily initialize the shared embedder."""
        if self._embedder is None:
            self._embedder = Embedder(config_path=self.fallback_embedding_config_path)
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


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Bulk-indexera dir till ChromaDB")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-docs", type=int, default=None)
    parser.add_argument("--input-dir", default=INPUT_DIR)
    parser.add_argument("--config", default="config/sources.yaml")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s:%(name)s:%(message)s",
    )

    indexer = DirBulkIndexer(
        input_dir=args.input_dir,
        config_path=args.config,
    )
    stats = indexer.run(
        dry_run=args.dry_run,
        max_docs=args.max_docs,
        verbose=args.verbose,
    )
    print_summary(stats)

    if stats.get("aborted"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
