"""Index normalized doktrin documents into ChromaDB."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from index.embedder import Embedder
from index.vector_store import ChromaVectorStore

logger = logging.getLogger("paragrafenai.noop")

DOKTRIN_INSTANCE_KEY = "doktrin"
COLLECTION_NAME = "paragrafen_doktrin_v1"
EMBEDDING_MODEL = "KBLab/sentence-bert-swedish-cased"


@dataclass
class IndexingSummary:
    documents_seen: int = 0
    documents_indexed: int = 0
    documents_skipped: int = 0
    chunks_indexed: int = 0
    chunks_skipped: int = 0
    errors: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "documents_seen": self.documents_seen,
            "documents_indexed": self.documents_indexed,
            "documents_skipped": self.documents_skipped,
            "chunks_indexed": self.chunks_indexed,
            "chunks_skipped": self.chunks_skipped,
            "errors": self.errors,
        }


class DoktrinIndexer:
    """Indexes normalized doktrin chunk JSON files into Chroma."""

    def __init__(
        self,
        *,
        config_path: str | Path = "config/embedding_config.yaml",
        input_dir: str | Path = "data/norm/doktrin",
        errors_path: str | Path = "data/index/doktrin_indexing_errors.jsonl",
        collection_name: str = COLLECTION_NAME,
        vector_store: ChromaVectorStore | None = None,
        embedder: Embedder | None = None,
    ) -> None:
        self.repo_root = Path(__file__).resolve().parent.parent
        self.config_path = self._resolve_path(config_path)
        self.input_dir = self._resolve_path(input_dir)
        self.errors_path = self._resolve_path(errors_path)
        self.collection_name = collection_name
        self.vector_store = vector_store
        self.embedder = embedder
        self.error_rows: list[dict[str, Any]] = []

    def _resolve_path(self, path_value: str | Path) -> Path:
        path = Path(path_value)
        if path.is_absolute():
            return path
        return self.repo_root / path

    def _serialize_list_field(self, value: Any) -> str:
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except (json.JSONDecodeError, ValueError):
                parsed = [value] if value else []
            else:
                if isinstance(parsed, list):
                    return json.dumps(parsed, ensure_ascii=False)
                parsed = [str(parsed)]
            return json.dumps(parsed, ensure_ascii=False)

        if isinstance(value, list):
            return json.dumps(value, ensure_ascii=False)
        if value is None:
            return json.dumps([], ensure_ascii=False)
        return json.dumps([value], ensure_ascii=False)

    def _record_error(self, file_path: Path, message: str) -> None:
        self.error_rows.append(
            {
                "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
                "file": str(file_path),
                "error": message,
            }
        )
        logger.error("%s (%s)", message, file_path.name)

    def _write_errors(self) -> None:
        if not self.error_rows:
            return
        self.errors_path.parent.mkdir(parents=True, exist_ok=True)
        with self.errors_path.open("w", encoding="utf-8") as fh:
            for row in self.error_rows:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _namespace_exists(self, namespace: str) -> bool:
        if self.vector_store is None:
            return False
        metadata = self.vector_store.get_one_metadata(
            instance_key=DOKTRIN_INSTANCE_KEY,
            where_filter={"namespace": namespace},
        )
        return metadata is not None

    def _ensure_vector_store(self) -> ChromaVectorStore:
        if self.vector_store is None:
            self.vector_store = ChromaVectorStore(config_path=self.config_path)
        return self.vector_store

    def _ensure_embedder(self) -> Embedder:
        if self.embedder is None:
            self.embedder = Embedder(config_path=self.config_path)
        return self.embedder

    def _build_chunk_metadata(
        self,
        document: dict[str, Any],
        chunk: dict[str, Any],
    ) -> tuple[str, dict[str, Any]] | None:
        text = str(chunk.get("text") or "").strip()
        if not text:
            logger.warning("Skippade tom doktrin-chunk för %s", document.get("title", "okänt"))
            return None

        authority_level = str(chunk.get("authority_level") or document.get("authority_level") or "")
        assert authority_level == "persuasive", "authority_level must be persuasive"

        namespace = str(chunk.get("id") or chunk.get("namespace") or "").strip()
        if not namespace:
            raise ValueError("Doktrinchunk saknar namespace/id.")

        metadata: dict[str, Any] = {
            "namespace": namespace,
            "chunk_id": namespace,
            "source_type": "doktrin",
            "source_subtype": str(chunk.get("source_subtype") or document.get("source_subtype") or ""),
            "title": str(chunk.get("title") or document.get("title") or ""),
            "author": str(chunk.get("author") or document.get("author") or ""),
            "author_last": str(chunk.get("author_last") or document.get("author_last") or ""),
            "authors": str(chunk.get("authors") or json.dumps(document.get("authors") or [], ensure_ascii=False)),
            "is_edited_volume": bool(chunk.get("is_edited_volume", document.get("is_edited_volume", False))),
            "year": int(chunk.get("year") or document.get("year") or 0),
            "edition": int(chunk.get("edition") or document.get("edition") or 1),
            "authority_level": authority_level,
            "citation_hd": str(chunk.get("citation_hd") or document.get("citation_hd") or ""),
            "citation_academic": str(chunk.get("citation_academic") or document.get("citation_academic") or ""),
            "pinpoint": str(chunk.get("pinpoint") or ""),
            "page_start": int(chunk.get("page_start") or 0),
            "page_end": int(chunk.get("page_end") or 0),
            "legal_area": self._serialize_list_field(chunk.get("legal_area", document.get("legal_area"))),
            "references_to": self._serialize_list_field(chunk.get("references_to", "[]")),
            "excluded_at_retrieval": bool(
                chunk.get("excluded_at_retrieval", document.get("excluded_at_retrieval", False))
            ),
            "embedding_model": EMBEDDING_MODEL,
            "chunk_index": int(chunk.get("chunk_index") or 0),
            "chunk_total": len(document.get("chunks") or []),
            "avg_quality": float(chunk.get("avg_quality") or 0.0),
            "extraction_method": str(chunk.get("extraction_method") or document.get("source_subtype") or ""),
            "filename": str(chunk.get("filename") or document.get("filename") or ""),
            "source_url": str(chunk.get("source_url") or document.get("source_url") or ""),
            "urn": str(chunk.get("urn") or document.get("urn") or ""),
            "license": str(chunk.get("license") or document.get("license") or ""),
            "license_url": str(chunk.get("license_url") or document.get("license_url") or ""),
            "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        }

        for optional_key in ("isbn", "publisher", "series", "work_type"):
            optional_value = str(chunk.get(optional_key) or document.get(optional_key) or "").strip()
            if optional_value:
                metadata[optional_key] = optional_value

        return text, metadata

    def index_all(
        self,
        *,
        dry_run: bool = False,
        max_docs: int | None = None,
    ) -> IndexingSummary:
        summary = IndexingSummary()
        files = sorted(self.input_dir.glob("*.json"))
        if max_docs is not None:
            files = files[:max_docs]

        for file_path in files:
            summary.documents_seen += 1
            try:
                with file_path.open("r", encoding="utf-8") as fh:
                    document = json.load(fh)
            except Exception as exc:
                self._record_error(file_path, f"Kunde inte läsa JSON: {exc}")
                summary.errors += 1
                continue

            chunks = document.get("chunks") or []
            if not isinstance(chunks, list):
                self._record_error(file_path, "Dokumentet saknar chunk-lista.")
                summary.errors += 1
                continue

            first_namespace = str(chunks[0].get("id") or chunks[0].get("namespace") or "").strip() if chunks else ""
            if first_namespace and self._namespace_exists(first_namespace):
                summary.documents_skipped += 1
                continue

            texts: list[str] = []
            metadatas: list[dict[str, Any]] = []
            for chunk in chunks:
                try:
                    built = self._build_chunk_metadata(document, chunk)
                except Exception as exc:
                    self._record_error(file_path, f"Ogiltig chunk-metadata: {exc}")
                    summary.errors += 1
                    built = None
                if built is None:
                    summary.chunks_skipped += 1
                    continue
                text, metadata = built
                texts.append(text)
                metadatas.append(metadata)

            if not texts:
                summary.documents_skipped += 1
                continue

            if dry_run:
                summary.documents_indexed += 1
                summary.chunks_indexed += len(texts)
                continue

            embeddings = self._ensure_embedder().embed(texts)
            if len(embeddings) != len(texts):
                self._record_error(file_path, "Fel antal embeddings returnerades.")
                summary.errors += 1
                continue

            added = self._ensure_vector_store().add_chunks(
                instance_key=DOKTRIN_INSTANCE_KEY,
                chunks=texts,
                embeddings=embeddings,
                metadatas=metadatas,
            )
            if added != len(texts):
                self._record_error(file_path, f"Endast {added}/{len(texts)} chunks indexerades.")
                summary.errors += 1
                continue

            summary.documents_indexed += 1
            summary.chunks_indexed += added

        self._write_errors()
        return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Index doktrin norm documents into ChromaDB.")
    parser.add_argument("--norm-dir", default="data/norm/doktrin")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--max-docs", type=int, default=None)
    args = parser.parse_args(argv)

    if args.verbose:
        logging.basicConfig(level=logging.INFO)

    indexer = DoktrinIndexer(input_dir=args.norm_dir)
    summary = indexer.index_all(dry_run=args.dry_run, max_docs=args.max_docs)
    print(summary.as_dict())
    failed_ratio = summary.errors / summary.documents_seen if summary.documents_seen else 0.0
    return 1 if failed_ratio > 0.01 else 0


if __name__ == "__main__":
    raise SystemExit(main())
