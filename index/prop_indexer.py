"""Index normalized proposition documents into ChromaDB."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re
import tempfile
from typing import Any

import yaml

from index.embedder import Embedder
from index.vector_store import ChromaVectorStore
from normalize.prop_normalizer import build_citation

logger = logging.getLogger("paragrafenai.noop")

COLLECTION_NAME = "paragrafen_forarbete_v1"
EMBEDDING_MODEL = "KBLab/sentence-bert-swedish-cased"


def build_prop_namespace(rm: str, nummer: int, chunk_index: int, part: int | None = None) -> str:
    rm_norm = rm.replace("/", "-")
    part_suffix = f"_d{part}" if part else ""
    return f"forarbete::prop_{rm_norm}_{nummer}{part_suffix}_chunk_{chunk_index:03d}"


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


class PropIndexer:
    """Indexes normalized proposition chunk JSON files into Chroma."""

    def __init__(
        self,
        *,
        config_path: str | Path = "config/embedding_config.yaml",
        forarbete_rank_path: str | Path = "config/forarbete_rank.yaml",
        input_dir: str | Path = "data/norm/forarbete/prop",
        errors_path: str | Path = "data/index/prop_indexing_errors.jsonl",
        collection_name: str = COLLECTION_NAME,
        vector_store: ChromaVectorStore | None = None,
        embedder: Embedder | None = None,
    ) -> None:
        self.repo_root = Path(__file__).resolve().parent.parent
        self.config_path = self._resolve_path(config_path)
        self.forarbete_rank_path = self._resolve_path(forarbete_rank_path)
        self.input_dir = self._resolve_path(input_dir)
        self.errors_path = self._resolve_path(errors_path)
        self.collection_name = collection_name
        self.vector_store = vector_store or ChromaVectorStore(config_path=self.config_path)
        self.embedder = embedder or Embedder(config_path=self.config_path)
        self.prop_rank = self._load_prop_rank(self.forarbete_rank_path)
        self.error_rows: list[dict[str, Any]] = []

    def _resolve_path(self, path_value: str | Path) -> Path:
        path = Path(path_value)
        if path.is_absolute():
            return path
        return self.repo_root / path

    def _load_yaml(self, path: Path) -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}

    def _load_prop_rank(self, path: Path) -> int:
        rank = self._load_yaml(path).get("forarbete_types", {}).get("proposition", {}).get("rank")
        if not isinstance(rank, int):
            raise ValueError("forarbete_rank för proposition saknas eller är inte int.")
        return rank

    def _extract_part(self, file_path: Path) -> int | None:
        match = re.search(r"_d(\d+)\.json$", file_path.name)
        if match:
            return int(match.group(1))
        return None

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
        metadata = self.vector_store.get_one_metadata(
            collection_name=self.collection_name,
            where_filter={"namespace": namespace},
        )
        return metadata is not None

    def _build_chunk_metadata(
        self,
        document: dict[str, Any],
        chunk: dict[str, Any],
        *,
        part: int | None,
    ) -> tuple[str, dict[str, Any]] | None:
        text = str(chunk.get("text") or "").strip()
        if not text:
            logger.warning("Skippade tom proposition-chunk för %s", document.get("beteckning", "okänt"))
            return None

        authority_level = str(document.get("authority_level") or "")
        assert authority_level == "preparatory", "authority_level must be preparatory"
        assert int(document.get("forarbete_rank", -1)) == self.prop_rank, "forarbete_rank mismatch"

        rm = str(document.get("rm") or "")
        nummer = int(document.get("nummer") or 0)
        chunk_index = int(chunk.get("chunk_index") or 0)
        namespace = build_prop_namespace(rm, nummer, chunk_index, part=part)
        pinpoint = str(chunk.get("pinpoint") or "")
        citation = str(chunk.get("citation") or "")
        if not citation:
            citation = build_citation(rm, nummer, pinpoint) if rm and nummer else str(document.get("beteckning") or "")

        metadata = {
            "namespace": namespace,
            "source_type": "forarbete",
            "forarbete_type": "proposition",
            "beteckning": str(document.get("beteckning") or ""),
            "citation": citation,
            "dok_id": str(document.get("dok_id") or ""),
            "authority_level": authority_level,
            "forarbete_rank": self.prop_rank,
            "title": str(document.get("titel") or ""),
            "section": str(chunk.get("section") or "other"),
            "section_title": str(chunk.get("section_title") or "other"),
            "pinpoint": pinpoint,
            "page_start": int(chunk.get("page_start") or 0),
            "page_end": int(chunk.get("page_end") or 0),
            "legal_area": self._serialize_list_field(document.get("legal_area")),
            "references_to": self._serialize_list_field(document.get("references_to")),
            "rm": rm,
            "nummer": nummer,
            "datum": str(document.get("datum") or ""),
            "organ": str(document.get("organ") or ""),
            "source_url": str(document.get("source_url") or ""),
            "pdf_url": str(document.get("pdf_url") or ""),
            "embedding_model": EMBEDDING_MODEL,
            "chunk_index": chunk_index,
            "chunk_total": len(document.get("chunks") or []),
            "fetched_at": str(document.get("fetched_at") or ""),
            "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        }
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

            part = self._extract_part(file_path)
            first_chunk_index = int(chunks[0].get("chunk_index") or 0) if chunks else 0
            first_namespace = build_prop_namespace(
                str(document.get("rm") or ""),
                int(document.get("nummer") or 0),
                first_chunk_index,
                part=part,
            )
            if chunks and self._namespace_exists(first_namespace):
                summary.documents_skipped += 1
                continue

            texts: list[str] = []
            metadatas: list[dict[str, Any]] = []
            for chunk in chunks:
                built = self._build_chunk_metadata(document, chunk, part=part)
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

            embeddings = self.embedder.embed(texts)
            if len(embeddings) != len(texts):
                self._record_error(file_path, "Fel antal embeddings returnerades.")
                summary.errors += 1
                continue

            added = self.vector_store.add_chunks(
                collection_name=self.collection_name,
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
    parser = argparse.ArgumentParser(description="Index proposition norm documents into ChromaDB.")
    parser.add_argument("--norm-dir", default="data/norm/forarbete/prop")
    parser.add_argument("--chroma-path", default=None)
    parser.add_argument("--collection", default=COLLECTION_NAME)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--max-docs", type=int, default=None)
    args = parser.parse_args(argv)

    if args.verbose:
        logging.basicConfig(level=logging.INFO)

    config_path = Path("config/embedding_config.yaml")
    temp_config_path: Path | None = None
    if args.chroma_path or args.collection != COLLECTION_NAME:
        with config_path.open("r", encoding="utf-8") as fh:
            config_payload = yaml.safe_load(fh) or {}
        if args.chroma_path:
            config_payload.setdefault("chroma", {})["persistent_path"] = args.chroma_path
        if args.collection:
            config_payload.setdefault("chroma", {}).setdefault("collections", {})["forarbete"] = args.collection
        temp_file = tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".yaml", delete=False)
        yaml.safe_dump(config_payload, temp_file, sort_keys=False, allow_unicode=True)
        temp_file.close()
        temp_config_path = Path(temp_file.name)
        config_path = temp_config_path

    try:
        indexer = PropIndexer(
            config_path=config_path,
            input_dir=args.norm_dir,
            collection_name=args.collection,
        )
        summary = indexer.index_all(dry_run=args.dry_run, max_docs=args.max_docs)
    finally:
        if temp_config_path and temp_config_path.exists():
            temp_config_path.unlink()

    print(summary.as_dict())
    failed_ratio = summary.errors / summary.documents_seen if summary.documents_seen else 0.0
    return 1 if failed_ratio > 0.01 else 0


if __name__ == "__main__":
    raise SystemExit(main())
