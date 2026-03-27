"""Bulk-index utskottsbetankanden raw documents directly into ChromaDB."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any

import chromadb
from transformers import AutoTokenizer
import yaml

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from index.embedder import Embedder
from normalize.sou_parser import parse_sou_html
from pipelines.common.chunk_base import ChunkConfig, ForarbeteChunker
from pipelines.common.parse_base import Section

os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")

logger = logging.getLogger("paragrafenai.noop")

COLLECTION_NAME = "paragrafen_bet_v1"
CHROMA_PATH = "data/index/chroma/bet"
INPUT_DIR = "data/raw/bet"
EMBEDDING_MODEL = "KBLab/sentence-bert-swedish-cased"
SCHEMA_VERSION = "v0.15"
SOURCE_TYPE = "forarbete"
DOCUMENT_SUBTYPE = "bet"
FORARBETE_TYPE = "utskottsbetankande"
AUTHORITY_LEVEL = "preparatory"
HTML_MIN_CHARS = 10_000
TOKEN_HARD_LIMIT = 384
PAGE_FIELD_WARNING = (
    "Section/ChunkedSection saknar page_start/page_end; bet-indexern bär därför sidinformation separat."
)

tokenizer = AutoTokenizer.from_pretrained(EMBEDDING_MODEL, use_fast=True)


@dataclass
class PreparedChunk:
    """One chunk ready for embedding and indexing."""

    chunk_id: str
    text: str
    metadata: dict[str, Any]


@dataclass
class PreparedSectionChunk:
    """Chunk plus its parser-derived page range."""

    chunk_text: str
    section_path: str
    section_title: str
    page_start: int
    page_end: int


def extract_metadata(raw: dict) -> dict | None:
    """Extract nested metadata and filter out non-betankanden."""
    dok = raw.get("dokumentstatus", {}).get("dokument", {})

    subtyp = (dok.get("subtyp") or "").strip()
    typ = (dok.get("typ") or "").strip()

    if subtyp in ("utl", "ap", "arbetsplenum"):
        return None
    if not (subtyp == "bet" or (not subtyp and typ == "bet")):
        return None

    rm = (dok.get("rm") or "").strip()
    beteckning_kod = (dok.get("beteckning") or "").strip()
    titel = (dok.get("titel") or "").strip()
    organ = (dok.get("organ") or "").strip()
    datum = str(dok.get("datum") or "")[:10]
    dok_id = (dok.get("dok_id") or "").strip()
    html = str(dok.get("html") or "")

    full_beteckning = f"bet. {rm}:{beteckning_kod}" if rm and beteckning_kod else ""

    return {
        "dok_id": dok_id,
        "rm": rm,
        "beteckning_kod": beteckning_kod,
        "beteckning": full_beteckning,
        "titel": titel,
        "organ": organ,
        "datum": datum,
        "html_content": html,
    }


def build_chunk_id(rm: str, beteckning_kod: str, chunk_index: int) -> str:
    """Build the mandated bet namespace."""
    rm_norm = (rm or "").replace("/", "-")
    bet_norm = (beteckning_kod or "").lower().replace(" ", "_")
    return f"bet::{rm_norm}_{bet_norm}_chunk_{chunk_index:03d}"


def load_forarbete_rank(yaml_path: str = "config/forarbete_rank.yaml") -> int:
    """Load the utskottsbetankande rank from YAML."""
    with Path(yaml_path).open("r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh) or {}
    rank_config = config["forarbete_types"]["utskottsbetankande"]
    return rank_config["rank"]


FORARBETE_RANK = load_forarbete_rank(
    str(Path(__file__).resolve().parent.parent / "config/forarbete_rank.yaml")
)
assert FORARBETE_RANK == 1, f"Fel rank: {FORARBETE_RANK} — eskalera"


def count_tokens(text: str) -> int:
    """Count actual model tokens for schema metadata."""
    return len(tokenizer.encode(text or "", add_special_tokens=False))


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


def coerce_int(value: Any) -> int:
    """Best-effort integer coercion."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def collect_raw_files(raw_dir: Path) -> list[Path]:
    """Collect raw bet files."""
    return sorted(raw_dir.glob("*.json"))


def log_ok(filename: str, message: str) -> None:
    """Log success messages consistently."""
    logger.info("[OK] %s — %s", filename, message)


def log_skip(filename: str, reason: str, detail: str = "") -> None:
    """Log skipped files consistently."""
    suffix = f" ({detail})" if detail else ""
    logger.warning("[SKIP] %s — %s%s", filename, reason, suffix)


def log_error(filename: str, message: str) -> None:
    """Log failures consistently."""
    logger.error("[ERROR] %s — %s", filename, message)


def log_progress(current: int, total: int, prepared_chunks: list[PreparedChunk]) -> None:
    """Log progress using the document namespace."""
    if not prepared_chunks:
        return
    document_namespace = prepared_chunks[0].chunk_id.rsplit("_chunk_", 1)[0]
    log_ok(document_namespace, f"[{current}/{total}] {len(prepared_chunks)} chunks")


def print_summary(stats: dict[str, int]) -> None:
    """Print the required end-of-run summary."""
    total_files = stats["total_files"]
    failed = stats["failed"]
    failed_ratio = (failed / total_files * 100) if total_files else 0.0
    print("Indexering klar:")
    print(f"  Filer totalt:          {total_files}")
    print(f"  OK (indexerade):       {stats['ok']}")
    print(f"  Skippade (subtyp-filter): {stats['skipped_subtyp']}")
    print(f"  Skippade (html-stub):  {stats['skipped_html_stub']}")
    print(f"  JSON-fel:              {stats['json_errors']}")
    if stats["skipped_other"]:
        print(f"  Skippade (ovrigt):     {stats['skipped_other']}")
    print(f"  Failed:                {failed}")
    print(f"  Felkvot:               {failed_ratio:.2f}%")
    print(f"  Chunks i Chroma:       {stats['chunks_in_chroma']}")


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Bulk-indexera utskottsbetankanden till ChromaDB")
    parser.add_argument("--max-docs", type=int, default=None)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parsa och chunka men skriv inte till Chroma",
    )
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--start-from",
        type=int,
        default=0,
        help="Hoppa over de N forsta filerna (resume)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s:%(name)s:%(message)s",
    )

    indexer = BetBulkIndexer()
    stats = indexer.run(
        dry_run=args.dry_run,
        max_docs=args.max_docs,
        verbose=args.verbose,
        start_from=args.start_from,
    )
    print_summary(stats)

    total_files = stats["total_files"]
    failed_ratio = (stats["failed"] / total_files) if total_files else 0.0
    if failed_ratio > 0.05:
        print("KRITISK VARNING: Fler an 5% av filerna gav Failed")
        return 1
    return 0


class BetBulkIndexer:
    """Read raw bet JSON, parse, chunk, embed and upsert to Chroma."""

    def __init__(
        self,
        *,
        raw_dir: str | Path = INPUT_DIR,
        chroma_path: str | Path = CHROMA_PATH,
        collection_name: str = COLLECTION_NAME,
        batch_size: int = 100,
        config_path: str | Path = "config/embedding_config.yaml",
    ) -> None:
        self.repo_root = Path(__file__).resolve().parent.parent
        self.raw_dir = self._resolve_path(raw_dir)
        self.chroma_path = self._resolve_path(chroma_path)
        self.collection_name = collection_name
        self.batch_size = max(1, int(batch_size))
        self.config_path = self._resolve_path(config_path)

        self.chunker = ForarbeteChunker(
            config=ChunkConfig(
                min_tokens=150,
                max_tokens=350,
                overlap_tokens=35,
            )
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
        start_from: int = 0,
    ) -> dict[str, int]:
        """Execute the full pipeline over all raw bet files."""
        raw_files = collect_raw_files(self.raw_dir)
        if start_from > 0:
            raw_files = raw_files[start_from:]
        if max_docs is not None:
            raw_files = raw_files[:max_docs]

        stats = {
            "total_files": len(raw_files),
            "ok": 0,
            "skipped_subtyp": 0,
            "skipped_html_stub": 0,
            "skipped_other": 0,
            "json_errors": 0,
            "failed": 0,
            "chunks_in_chroma": 0,
        }
        total = len(raw_files)

        for index, raw_path in enumerate(raw_files, start=1):
            try:
                raw = json.loads(raw_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                log_skip(raw_path.stem, "json_parse_error", str(exc))
                stats["json_errors"] += 1
                continue
            except Exception as exc:
                log_error(raw_path.name, f"kunde inte lasa JSON ({exc})")
                stats["failed"] += 1
                continue

            if not isinstance(raw, dict):
                log_error(raw_path.name, "rafilen innehaller inte ett JSON-objekt")
                stats["failed"] += 1
                continue

            try:
                prepared_chunks = self.prepare_document(raw, raw_path=raw_path)
            except SkipDocument as exc:
                if exc.reason == "subtyp_filter":
                    stats["skipped_subtyp"] += 1
                elif exc.reason == "html_stub_no_text":
                    stats["skipped_html_stub"] += 1
                else:
                    stats["skipped_other"] += 1
                log_skip(raw_path.name, exc.reason, exc.detail)
                continue
            except Exception as exc:
                log_error(raw_path.name, str(exc))
                stats["failed"] += 1
                continue

            if dry_run:
                stats["ok"] += 1
                if verbose or index % 100 == 0:
                    log_progress(index, total, prepared_chunks)
                continue

            texts = [chunk.text for chunk in prepared_chunks]
            try:
                embeddings = self.embedder.embed(texts)
            except Exception as exc:
                log_error(raw_path.name, f"embedding-fel ({exc})")
                stats["failed"] += 1
                continue

            if len(embeddings) != len(texts):
                log_error(
                    raw_path.name,
                    f"fel antal embeddings ({len(embeddings)} av {len(texts)})",
                )
                stats["failed"] += 1
                continue

            if not self.upsert_chunks(prepared_chunks, embeddings):
                stats["failed"] += 1
                continue

            stats["ok"] += 1
            if verbose or index % 100 == 0:
                log_progress(index, total, prepared_chunks)

        if not dry_run and self._collection is not None:
            stats["chunks_in_chroma"] = int(self.collection.count())
        return stats

    def prepare_document(
        self,
        raw: dict[str, Any],
        *,
        raw_path: Path | None = None,
    ) -> list[PreparedChunk]:
        """Prepare one raw bet document for indexing."""
        meta = extract_metadata(raw)
        if meta is None:
            raise SkipDocument("subtyp_filter")

        html_content = str(meta["html_content"] or "")
        if len(html_content) < HTML_MIN_CHARS:
            raise SkipDocument("html_stub_no_text")

        if not meta["dok_id"]:
            raise SkipDocument("missing_dok_id")
        if not meta["rm"] or not meta["beteckning_kod"]:
            raise SkipDocument("missing_rm_or_beteckning")
        if not meta["beteckning"]:
            raise SkipDocument("missing_beteckning")

        sections = parse_sou_html(html_content, str(meta["dok_id"]))
        if not sections:
            raise SkipDocument("parser returned no sections")

        chunked_sections = self.chunk_sections(sections)
        if not chunked_sections:
            raise SkipDocument("chunker returned no chunks")

        content_hash = hashlib.sha256(html_content.encode("utf-8")).hexdigest()
        citation = f"bet. {meta['rm']}:{meta['beteckning_kod']}"

        prepared_chunks: list[PreparedChunk] = []
        for chunk_index, chunk in enumerate(chunked_sections):
            chunk_id = build_chunk_id(meta["rm"], meta["beteckning_kod"], chunk_index)
            token_count = count_tokens(chunk.chunk_text)

            metadata = {
                "namespace": chunk_id,
                "chunk_id": chunk_id,
                "source_document_id": meta["dok_id"],
                "source_type": SOURCE_TYPE,
                "document_subtype": DOCUMENT_SUBTYPE,
                "forarbete_type": FORARBETE_TYPE,
                "forarbete_rank": FORARBETE_RANK,
                "schema_version": SCHEMA_VERSION,
                "rm": meta["rm"],
                "beteckning": meta["beteckning"],
                "beteckning_kod": meta["beteckning_kod"],
                "organ": meta["organ"],
                "dok_id": meta["dok_id"],
                "titel": meta["titel"],
                "datum": meta["datum"],
                "authority_level": AUTHORITY_LEVEL,
                "citation": citation,
                "chunk_index": chunk_index,
                "section_type": chunk.section_path or "other",
                "pinpoint": build_pinpoint(chunk.page_start, chunk.page_end),
                "token_count": token_count,
                "legal_area": serialize_json_list([]),
                "references_to": serialize_json_list([]),
                "content_hash": content_hash,
            }

            prepared_chunks.append(
                PreparedChunk(
                    chunk_id=chunk_id,
                    text=chunk.chunk_text,
                    metadata=metadata,
                )
            )

        return prepared_chunks

    def chunk_sections(self, parsed_sections: list[dict[str, Any]]) -> list[PreparedSectionChunk]:
        """Convert parsed sections, chunk them and preserve page ranges."""
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
                    section_title=str(section.get("section_title") or "Huvudtext"),
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
            return []

        page_ranges = self._build_page_ranges(section_objects, usable_page_ranges)
        if len(page_ranges) != len(chunked_sections):
            logger.warning(
                "Chunk/page-range mismatch (%s != %s); pageinfo nollstalls defensivt",
                len(chunked_sections),
                len(page_ranges),
            )
            page_ranges = [(0, 0)] * len(chunked_sections)

        prepared: list[PreparedSectionChunk] = []
        for chunk, page_range in zip(chunked_sections, page_ranges):
            split_chunks = self._split_chunk_to_token_limit(
                chunk.chunk_text,
                section_path=chunk.section_path,
                section_title=chunk.section_title,
                page_start=page_range[0],
                page_end=page_range[1],
            )
            prepared.extend(split_chunks)
        return prepared

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
            return [(
                min(starts) if starts else 0,
                max(ends) if ends else 0,
            )]

        chunk_page_ranges: list[tuple[int, int]] = []
        for section, page_range in usable_sections:
            section_chunks = self.chunker._chunk_single_section(section)
            chunk_page_ranges.extend([page_range] * len(section_chunks))
        return chunk_page_ranges

    def _split_chunk_to_token_limit(
        self,
        text: str,
        *,
        section_path: str,
        section_title: str,
        page_start: int,
        page_end: int,
    ) -> list[PreparedSectionChunk]:
        """Defensively re-split chunks if actual token counts exceed the hard limit."""
        normalized = " ".join((text or "").split()).strip()
        if not normalized:
            return []
        if count_tokens(normalized) <= TOKEN_HARD_LIMIT:
            return [
                PreparedSectionChunk(
                    chunk_text=normalized,
                    section_path=section_path,
                    section_title=section_title,
                    page_start=page_start,
                    page_end=page_end,
                )
            ]

        words = normalized.split()
        chunks: list[PreparedSectionChunk] = []
        start = 0
        overlap = self.chunker.config.overlap_tokens
        while start < len(words):
            end = start + min(self.chunker.config.max_tokens, len(words) - start)
            candidate = " ".join(words[start:end]).strip()
            while end > start + 1 and count_tokens(candidate) > TOKEN_HARD_LIMIT:
                end -= 1
                candidate = " ".join(words[start:end]).strip()

            if not candidate:
                break

            chunks.append(
                PreparedSectionChunk(
                    chunk_text=candidate,
                    section_path=section_path,
                    section_title=section_title,
                    page_start=page_start,
                    page_end=page_end,
                )
            )
            if end >= len(words):
                break
            start = max(end - overlap, start + 1)
        return chunks

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
    """Raised when a document should be skipped without failing the run."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


if __name__ == "__main__":
    raise SystemExit(main())
