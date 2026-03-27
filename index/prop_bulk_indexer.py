"""Bulk-index proposition raw documents directly into ChromaDB."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from html import escape
import hashlib
import json
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any

import chromadb
import yaml

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from index.embedder import Embedder
from normalize.prop_parser import parse_prop_html
from pipelines.common.chunk_base import ChunkConfig, ForarbeteChunker
from pipelines.common.parse_base import Section

os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")

logger = logging.getLogger("paragrafenai.noop")

COLLECTION_NAME = "paragrafen_prop_v1"
CHROMA_PATH = "data/index/chroma/prop"
RAW_DIR = "data/raw/prop"
EMBEDDING_MODEL = "KBLab/sentence-bert-swedish-cased"
SCHEMA_VERSION = "v0.15"
LICENSE = "public_domain"
AUTHORITY_LEVEL = "preparatory"
PAGE_FIELD_WARNING = (
    "Section/ChunkedSection saknar page_start/page_end; bulk-indexern bär därför sidinformation separat."
)


@dataclass
class PreparedChunk:
    """One chunk ready for embedding and indexing."""

    chunk_id: str
    text: str
    metadata: dict[str, Any]


def build_chunk_id(rm: str, nummer: int, part: int | None, chunk_index: int) -> str:
    """Build the mandated proposition chunk namespace."""
    rm_norm = (rm or "").replace("/", "-")
    part_suffix = f"_d{part}" if part else ""
    return f"prop::{rm_norm}_{nummer}{part_suffix}_chunk_{chunk_index:03d}"


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Bulk-indexera propositioner till ChromaDB")
    parser.add_argument("--raw-dir", default=RAW_DIR)
    parser.add_argument("--chroma-path", default=CHROMA_PATH)
    parser.add_argument("--collection", default=COLLECTION_NAME)
    parser.add_argument("--max-docs", type=int, default=None)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parsa och chunka men skriv inte till Chroma",
    )
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Chroma upsert batch-storlek",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s:%(name)s:%(message)s",
    )

    indexer = PropBulkIndexer(
        raw_dir=args.raw_dir,
        chroma_path=args.chroma_path,
        collection_name=args.collection,
        batch_size=args.batch_size,
    )
    stats = indexer.run(dry_run=args.dry_run, max_docs=args.max_docs, verbose=args.verbose)
    print_summary(stats)

    total_files = stats["total_files"]
    failed_ratio = (stats["failed"] / total_files) if total_files else 0.0
    if failed_ratio > 0.01:
        print("KRITISK VARNING: Felkvoten överstiger 1%")
        return 1
    return 0


class PropBulkIndexer:
    """Read raw proposition JSON, parse, chunk, embed and upsert to Chroma."""

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
        self.prop_rank = load_prop_rank(self.rank_config_path)
        self._embedder: Embedder | None = None
        self._collection: Any | None = None
        self._warned_page_fields = False

    def run(
        self,
        *,
        dry_run: bool = False,
        max_docs: int | None = None,
        verbose: bool = False,
    ) -> dict[str, int]:
        """Execute the full pipeline over all raw proposition files."""
        raw_files = collect_raw_files(self.raw_dir)
        if max_docs is not None:
            raw_files = raw_files[:max_docs]

        stats = {
            "total_files": len(raw_files),
            "ok": 0,
            "skipped": 0,
            "failed": 0,
            "total_chunks": 0,
        }
        total = len(raw_files)

        for index, raw_path in enumerate(raw_files, start=1):
            try:
                raw = load_raw_json(raw_path)
            except Exception as exc:
                logger.error("FAIL: %s — kunde inte läsa JSON (%s)", raw_path.name, exc)
                stats["failed"] += 1
                continue

            try:
                prepared_chunks = self.prepare_document(raw, raw_path=raw_path)
            except SkipDocument as exc:
                logger.warning("SKIP: %s — %s", raw_path.name, exc)
                stats["skipped"] += 1
                continue
            except Exception as exc:
                logger.error("FAIL: %s — %s", raw_path.name, exc)
                stats["failed"] += 1
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
                logger.error("FAIL: %s — embedding-fel (%s)", raw_path.name, exc)
                stats["failed"] += 1
                continue

            if len(embeddings) != len(texts):
                logger.error(
                    "FAIL: %s — fel antal embeddings (%s av %s)",
                    raw_path.name,
                    len(embeddings),
                    len(texts),
                )
                stats["failed"] += 1
                continue

            if not self.upsert_chunks(prepared_chunks, embeddings):
                stats["failed"] += 1
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
        """Prepare one raw proposition document for indexing."""
        html_content = resolve_html_content(raw)
        if not html_content:
            raise SkipDocument("no HTML content")

        rm = str(raw.get("rm") or raw.get("riksmote") or "").strip()
        nummer = coerce_int(raw.get("nummer"))
        part = coerce_optional_int(raw.get("part"))
        if not rm or nummer <= 0:
            logger.warning(
                "WARN: %s — saknar rm och/eller nummer, använder fallback-värden",
                raw_path.name if raw_path else raw.get("beteckning", "okänt"),
            )

        sections = parse_prop_html(html_content, str(raw.get("dok_id") or ""))
        if not sections:
            raise SkipDocument("parser returned no sections")

        chunked_sections, page_ranges = self.chunk_sections(sections)
        if not chunked_sections:
            raise SkipDocument("chunker returned no chunks")

        content_hash = hashlib.sha256(html_content.encode("utf-8")).hexdigest()
        references_typed = build_typed_references(raw.get("references_to", []))
        legal_area = serialize_json_list(raw.get("legal_area", []))
        beteckning = resolve_beteckning(raw.get("beteckning"), rm, nummer)
        canonical_citation = build_canonical_citation(rm, nummer, fallback=beteckning)

        prepared_chunks: list[PreparedChunk] = []
        total_chunks = len(chunked_sections)
        for chunk_index, (chunk, page_range) in enumerate(zip(chunked_sections, page_ranges)):
            page_start, page_end = page_range
            pinpoint = build_pinpoint(page_start, page_end)
            citation = f"{beteckning} {pinpoint}" if pinpoint else beteckning
            chunk_id = build_chunk_id(rm, nummer, part, chunk_index)

            metadata = {
                "chunk_id": chunk_id,
                "namespace": chunk_id,
                "source_document_id": str(raw.get("dok_id") or ""),
                "source_type": "forarbete",
                "document_subtype": "proposition",
                "forarbete_type": "proposition",
                "authority_level": AUTHORITY_LEVEL,
                "forarbete_rank": self.prop_rank,
                "beteckning": beteckning,
                "canonical_citation": canonical_citation,
                "short_citation": canonical_citation,
                "citation": citation,
                "pinpoint": pinpoint,
                "titel": str(raw.get("titel") or ""),
                "organ": str(raw.get("organ") or ""),
                "datum": str(raw.get("datum") or ""),
                "rm": rm,
                "nummer": nummer,
                "section": chunk.section_path,
                "section_title": chunk.section_title,
                "chunk_index": chunk_index,
                "chunk_total": total_chunks,
                "page_start": page_start if page_start else 0,
                "page_end": page_end if page_end else 0,
                "token_count": chunk.token_count,
                "legal_area": legal_area,
                "references_to": references_typed,
                "content_hash": content_hash,
                "source_url": str(raw.get("source_url") or ""),
                "html_url": str(raw.get("dokument_url_html") or ""),
                "ingest_method": str(raw.get("ingest_method") or "api"),
                "curated_by": raw.get("curated_by"),
                "curated_note": raw.get("curated_note"),
                "schema_version": SCHEMA_VERSION,
                "license": LICENSE,
                "is_active": True,
                "embedding_model": EMBEDDING_MODEL,
            }
            metadata = {key: value for key, value in metadata.items() if value is not None}

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
        """Convert parsed sections, chunk them and preserve page ranges separately."""
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
                "Chunk/page-range mismatch (%s != %s); pageinfo nollställs defensivt",
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
            starts = [page_start for page_start, _ in (page_range for _, page_range in usable_sections) if page_start > 0]
            ends = [page_end for _, page_end in (page_range for _, page_range in usable_sections) if page_end > 0]
            return [(
                min(starts) if starts else 0,
                max(ends) if ends else 0,
            )]

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
    """Collect raw and curated proposition files."""
    curated_dir = raw_dir / "curated"
    raw_files = sorted(raw_dir.glob("*.json"))
    if curated_dir.exists():
        raw_files += sorted(curated_dir.glob("*.json"))
    return raw_files


def load_raw_json(raw_path: Path) -> dict[str, Any]:
    """Load one raw JSON file."""
    with raw_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError("råfilen innehåller inte ett JSON-objekt")
    return data


def resolve_html_content(raw: dict[str, Any]) -> str:
    """Return html_content or synthesize it from curated pages."""
    html_content = str(raw.get("html_content") or "").strip()
    if html_content:
        return html_content

    pages = raw.get("pages")
    if not isinstance(pages, list) or not pages:
        return ""

    parts: list[str] = []
    for page in pages:
        if not isinstance(page, dict):
            continue
        page_num = coerce_int(page.get("page_num"))
        page_text = str(page.get("text") or "").strip()
        if page_num <= 0 or not page_text:
            continue
        text_html = escape(page_text).replace("\n", "</p>\n<p>")
        parts.append(f'<div id="page_{page_num}">\n<p>{text_html}</p>\n</div>')

    if not parts:
        return ""

    html_content = "\n".join(parts)
    raw["html_content"] = html_content
    return html_content


def load_prop_rank(config_path: Path) -> int:
    """Load forarbete_rank for proposition from YAML."""
    with config_path.open("r", encoding="utf-8") as fh:
        rank_config = yaml.safe_load(fh) or {}
    rank = rank_config.get("forarbete_types", {}).get("proposition", {}).get("rank")
    if not isinstance(rank, int):
        raise ValueError("forarbete_rank för proposition saknas eller är inte ett heltal.")
    return rank


def build_default_beteckning(rm: str, nummer: int) -> str:
    """Build fallback proposition label."""
    if rm and nummer > 0:
        return f"Prop. {rm}:{nummer}"
    if rm:
        return f"Prop. {rm}"
    return "Prop."


def build_canonical_citation(rm: str, nummer: int, *, fallback: str) -> str:
    """Build canonical proposition citation."""
    if rm and nummer > 0:
        return f"Prop. {rm}:{nummer}"
    return fallback


def resolve_beteckning(raw_beteckning: Any, rm: str, nummer: int) -> str:
    """Normalize beteckning so proposition metadata stays citation-safe."""
    candidate = str(raw_beteckning or "").strip()
    if candidate.lower().startswith("prop."):
        return candidate
    if rm and nummer > 0:
        return build_default_beteckning(rm, nummer)
    return candidate or build_default_beteckning(rm, nummer)


def build_pinpoint(page_start: int, page_end: int) -> str:
    """Build pinpoint string from page range."""
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
    """Convert raw references_to strings into typed JSON."""
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


def coerce_optional_int(value: Any) -> int | None:
    """Best-effort optional integer coercion."""
    parsed = coerce_int(value)
    return parsed if parsed > 0 else None


def log_progress(current: int, total: int, prepared_chunks: list[PreparedChunk]) -> None:
    """Log progress in the requested namespace format."""
    if not prepared_chunks:
        return
    document_namespace = prepared_chunks[0].chunk_id.rsplit("_chunk_", 1)[0]
    logger.info("[%s/%s] %s — %s chunks", current, total, document_namespace, len(prepared_chunks))


def print_summary(stats: dict[str, int]) -> None:
    """Print the required end-of-run summary."""
    total_files = stats["total_files"]
    failed_ratio = (stats["failed"] / total_files * 100) if total_files else 0.0
    print("Indexering klar:")
    print(f"  Filer: {total_files}")
    print(f"  OK: {stats['ok']}")
    print(f"  Skippade: {stats['skipped']}")
    print(f"  Failed: {stats['failed']}")
    print(f"  Chunks: {stats['total_chunks']}")
    print(f"  Felkvot: {failed_ratio:.2f}%")


if __name__ == "__main__":
    raise SystemExit(main())
