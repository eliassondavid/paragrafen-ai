"""Bulk-index FK föreskrifter och allmänna råd till ChromaDB."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Any

import chromadb

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from index.embedder import Embedder
from pipelines.common.chunk_base import ChunkConfig, ForarbeteChunker
from pipelines.common.parse_base import Section

os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")

logger = logging.getLogger("paragrafenai.noop")

COLLECTION_NAME = "paragrafen_foreskrift_v1"
CHROMA_PATH = "data/index/chroma/foreskrift"
RAW_DIR = "data/raw/foreskrift/fk"
PDF_DIR = "data/raw/foreskrift/fk/pdf"
METADATA_DIR = "data/raw/foreskrift/fk/metadata"
SCHEMA_VERSION = "v0.15"
LICENSE = "public_domain"
BATCH_SIZE = 100
PRIORITY_ORDER = ["FKFS", "FKAR", "RFFS", "RAR"]
AUTHORITY_MAP = {
    "FKFS": "binding",
    "RFFS": "binding",
    "FKAR": "guiding",
    "RAR": "guiding",
}
SOURCE_TYPE_MAP = {
    "FKFS": "foreskrift",
    "RFFS": "foreskrift",
    "FKAR": "allmannarad",
    "RAR": "allmannarad",
}
PDFTOTEXT_BIN = (
    os.environ.get("PDFTOTEXT_BIN")
    or shutil.which("pdftotext")
    or "/opt/homebrew/bin/pdftotext"
)


@dataclass
class PreparedChunk:
    """One chunk ready for embedding."""

    chunk_id: str
    text: str
    metadata: dict[str, Any]


class SkipDocument(Exception):
    """Raised when a document should be skipped."""


def clean_scalar(value: Any) -> str:
    """Normalize a metadata scalar to string."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def serialize_json_list(value: Any) -> str:
    """Serialize list-like metadata for Chroma."""
    if value is None:
        return json.dumps([], ensure_ascii=False)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError, ValueError):
            cleaned = clean_scalar(value)
            return json.dumps([cleaned] if cleaned else [], ensure_ascii=False)
        if isinstance(parsed, list):
            return json.dumps(parsed, ensure_ascii=False)
        return json.dumps([parsed], ensure_ascii=False)
    if isinstance(value, list):
        return json.dumps([clean_scalar(item) for item in value if clean_scalar(item)], ensure_ascii=False)
    cleaned = clean_scalar(value)
    return json.dumps([cleaned] if cleaned else [], ensure_ascii=False)


def build_chunk_id(samling: str, arsutgava: str, lopnummer: str, chunk_index: int) -> str:
    """Build the required namespace."""
    return f"{samling.lower()}::{arsutgava}_{lopnummer}_chunk_{chunk_index:03d}"


def collect_metadata_files(metadata_dir: Path) -> list[Path]:
    """Collect metadata JSON files."""
    return sorted(path for path in metadata_dir.glob("*.json") if not path.name.startswith("_"))


def load_json(path: Path) -> dict[str, Any]:
    """Load one metadata JSON file."""
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError("metadatafilen innehaller inte ett JSON-objekt")
    return payload


class FkForeskriftBulkIndexer:
    """Convert PDFs to text, chunk them and upsert to Chroma."""

    def __init__(
        self,
        *,
        raw_dir: str | Path = RAW_DIR,
        pdf_dir: str | Path = PDF_DIR,
        metadata_dir: str | Path = METADATA_DIR,
        chroma_path: str | Path = CHROMA_PATH,
        collection_name: str = COLLECTION_NAME,
        batch_size: int = BATCH_SIZE,
        embedder: Embedder | None = None,
        client_factory: Any | None = None,
    ) -> None:
        self.repo_root = Path(__file__).resolve().parent.parent
        self.raw_dir = self._resolve_path(raw_dir)
        self.pdf_dir = self._resolve_path(pdf_dir)
        self.metadata_dir = self._resolve_path(metadata_dir)
        self.chroma_path = self._resolve_path(chroma_path)
        self.collection_name = collection_name
        self.batch_size = max(1, int(batch_size))
        self.chunker = ForarbeteChunker(
            config=ChunkConfig(
                min_tokens=35,
                max_tokens=350,
                overlap_tokens=150,
                min_chunk_chars=35,
            )
        )
        self._embedder = embedder
        self.client_factory = client_factory or chromadb.PersistentClient
        self._collection: Any | None = None

    def run(
        self,
        *,
        samling: str | None = None,
        limit: int | None = None,
        reset: bool = False,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> dict[str, int]:
        """Execute the bulk indexing pipeline."""
        if reset and not dry_run:
            self.reset_collection()

        metadata_files = collect_metadata_files(self.metadata_dir)
        if samling:
            selected = clean_scalar(samling).upper()
            metadata_files = [
                path
                for path in metadata_files
                if path.stem.startswith(f"{selected}_")
            ]
        metadata_files = self._sort_metadata_files(metadata_files)
        if limit is not None:
            metadata_files = metadata_files[: max(0, int(limit))]

        stats = {
            "metadata_files_read": 0,
            "documents_indexed": 0,
            "skipped": 0,
            "txt_created": 0,
            "total_chunks": 0,
        }
        pending_chunks: list[PreparedChunk] = []

        for index, metadata_path in enumerate(metadata_files, start=1):
            stats["metadata_files_read"] += 1
            try:
                document = load_json(metadata_path)
                txt_created, prepared = self.prepare_document(document, metadata_path=metadata_path)
                stats["txt_created"] += int(txt_created)
            except SkipDocument as exc:
                stats["skipped"] += 1
                if verbose:
                    print(f"[SKIP] {metadata_path.name} -> {exc}")
                continue

            pending_chunks.extend(prepared)
            stats["documents_indexed"] += 1
            stats["total_chunks"] += len(prepared)

            if verbose:
                namespace = prepared[0].chunk_id.rsplit("_chunk_", 1)[0]
                print(f"[OK]   {index}/{len(metadata_files)} {namespace} ({len(prepared)} chunks)")

            while len(pending_chunks) >= self.batch_size:
                batch = pending_chunks[: self.batch_size]
                self.process_batch(batch, dry_run=dry_run)
                pending_chunks = pending_chunks[self.batch_size :]

        if pending_chunks:
            self.process_batch(pending_chunks, dry_run=dry_run)

        return stats

    def prepare_document(
        self,
        document: dict[str, Any],
        *,
        metadata_path: Path,
    ) -> tuple[bool, list[PreparedChunk]]:
        """Prepare one fetched document for embedding."""
        samling = clean_scalar(document.get("samling")).upper()
        arsutgava = clean_scalar(document.get("arsutgava"))
        lopnummer = clean_scalar(document.get("lopnummer"))
        node_id = clean_scalar(document.get("node_id"))
        if not samling or not arsutgava or not lopnummer or not node_id:
            raise SkipDocument("missing_identifier")

        pdf_path = self.pdf_dir / f"{metadata_path.stem}.pdf"
        if not pdf_path.exists():
            raise SkipDocument("missing_pdf")

        txt_path = pdf_path.with_suffix(".txt")
        txt_created = self.ensure_text_file(pdf_path, txt_path)
        text_content = txt_path.read_text(encoding="utf-8", errors="replace").strip()
        if not text_content:
            raise SkipDocument("empty_text")

        sections = [
            Section(
                section_key="document",
                section_title=clean_scalar(document.get("titel") or document.get("headline") or "Dokument"),
                text=text_content,
                level=1,
            )
        ]
        chunked_sections = self.chunker.chunk_sections(sections)
        if not chunked_sections:
            raise SkipDocument("chunker_no_chunks")

        prepared_chunks: list[PreparedChunk] = []
        for chunk_number, chunk in enumerate(chunked_sections, start=1):
            chunk_text = chunk.chunk_text.strip()
            if not chunk_text:
                continue
            chunk_id = build_chunk_id(samling, arsutgava, lopnummer, chunk_number)
            metadata = {
                "source_type": SOURCE_TYPE_MAP.get(samling, "foreskrift"),
                "authority_level": AUTHORITY_MAP.get(samling, "guiding"),
                "myndighet": "Försäkringskassan",
                "samling": samling,
                "nummer": clean_scalar(document.get("nummer") or f"{arsutgava}:{lopnummer}"),
                "arsutgava": arsutgava,
                "lopnummer": lopnummer,
                "node_id": node_id,
                "titel": clean_scalar(document.get("aktuell_titel") or document.get("titel")),
                "bemyndigande": clean_scalar(document.get("bemyndigande")),
                "is_revoked": bool(document.get("is_revoked")),
                "is_change_document": bool(document.get("is_change_document")),
                "ikraftträdande": clean_scalar(document.get("ikraftträdande")),
                "uppslagsord": serialize_json_list(document.get("uppslagsord")),
                "chunk_index": chunk_number,
                "chunk_id": chunk_id,
                "content_hash": hashlib.sha256(chunk_text.encode("utf-8")).hexdigest(),
                "schema_version": SCHEMA_VERSION,
                "license": LICENSE,
                "is_active": not bool(document.get("is_revoked")),
            }
            prepared_chunks.append(PreparedChunk(chunk_id=chunk_id, text=chunk_text, metadata=metadata))

        if not prepared_chunks:
            raise SkipDocument("empty_text")
        return txt_created, prepared_chunks

    def ensure_text_file(self, pdf_path: Path, txt_path: Path) -> bool:
        """Create a sidecar .txt using pdftotext when needed."""
        if txt_path.exists() and txt_path.stat().st_size > 0:
            return False
        pdftotext_cmd = shutil.which(PDFTOTEXT_BIN) or PDFTOTEXT_BIN
        if shutil.which(pdftotext_cmd) is None and not Path(pdftotext_cmd).exists():
            raise SkipDocument(f"pdftotext_missing:{PDFTOTEXT_BIN}")
        completed = subprocess.run(
            [pdftotext_cmd, "-layout", str(pdf_path), str(txt_path)],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            stderr = completed.stderr.strip() or completed.stdout.strip() or "pdftotext failed"
            raise SkipDocument(stderr)
        if not txt_path.exists():
            raise SkipDocument("txt_missing_after_pdftotext")
        return True

    def process_batch(self, prepared_chunks: list[PreparedChunk], *, dry_run: bool) -> None:
        """Embed and upsert one batch."""
        texts = [chunk.text for chunk in prepared_chunks]
        if dry_run:
            return
        embeddings = self.embedder.embed(texts)
        if len(embeddings) != len(texts):
            raise RuntimeError(f"fel antal embeddings ({len(embeddings)} av {len(texts)})")
        self.collection.upsert(
            ids=[chunk.chunk_id for chunk in prepared_chunks],
            embeddings=embeddings,
            documents=texts,
            metadatas=[chunk.metadata for chunk in prepared_chunks],
        )

    def reset_collection(self) -> None:
        """Delete the target collection if it exists."""
        self.chroma_path.mkdir(parents=True, exist_ok=True)
        client = self.client_factory(path=str(self.chroma_path.resolve()))
        try:
            client.delete_collection(self.collection_name)
        except Exception:
            logger.info("Collection %s fanns inte att rensa", self.collection_name)
        self._collection = None

    @property
    def embedder(self) -> Embedder:
        """Lazily initialize the shared embedder."""
        if self._embedder is None:
            self._embedder = Embedder()
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

    def _sort_metadata_files(self, paths: list[Path]) -> list[Path]:
        priority = {samling: index for index, samling in enumerate(PRIORITY_ORDER)}

        def sort_key(path: Path) -> tuple[int, str]:
            samling = path.stem.split("_", 1)[0].upper()
            return (priority.get(samling, len(priority)), path.stem)

        return sorted(paths, key=sort_key)

    def _resolve_path(self, path_value: str | Path) -> Path:
        candidate = Path(path_value)
        if candidate.is_absolute():
            return candidate
        return self.repo_root / candidate


def print_summary(stats: dict[str, int], *, dry_run: bool) -> None:
    """Print end-of-run summary."""
    print("FK föreskrifts-indexering klar.")
    print(f"  Metadatafiler lästa:  {stats['metadata_files_read']}")
    print(f"  Dokument indexerade:  {stats['documents_indexed']}")
    print(f"  Skippade:             {stats['skipped']}")
    print(f"  TXT skapade:          {stats['txt_created']}")
    print(f"  Chunks totalt:        {stats['total_chunks']}")
    print(f"  Collection:           {COLLECTION_NAME}")
    print(f"  Chroma-sökväg:        {CHROMA_PATH}")
    print(f"  Körläge:              {'DRY-RUN' if dry_run else 'LIVE'}")


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Bulk-indexera FK föreskrifter till ChromaDB")
    parser.add_argument("--samling", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s:%(name)s:%(message)s",
    )

    indexer = FkForeskriftBulkIndexer()
    stats = indexer.run(
        samling=args.samling,
        limit=args.limit,
        reset=args.reset,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )
    print_summary(stats, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
