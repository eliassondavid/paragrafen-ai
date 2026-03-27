"""Index normalized SOU documents into ChromaDB (paragrafen_sou_v1)."""

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

logger = logging.getLogger("paragrafenai.noop")

COLLECTION_NAME = "paragrafen_sou_v1"
CHROMA_PATH = "data/index/chroma/sou"
EMBEDDING_MODEL = "KBLab/sentence-bert-swedish-cased"


def build_sou_namespace(år: str, nr: int, chunk_index: int) -> str:
    return f"forarbete::sou_{år}_{nr}_chunk_{chunk_index:03d}"


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


class SouIndexer:
    """Indexes normalized SOU chunk JSON files into Chroma (paragrafen_sou_v1)."""

    def __init__(
        self,
        *,
        config_path: str | Path = "config/embedding_config.yaml",
        forarbete_rank_path: str | Path = "config/forarbete_rank.yaml",
        input_dir: str | Path = "data/norm/sou",
        errors_path: str | Path = "data/index/sou_indexing_errors.jsonl",
        collection_name: str = COLLECTION_NAME,
        chroma_path: str = CHROMA_PATH,
        vector_store: ChromaVectorStore | None = None,
        embedder: Embedder | None = None,
    ) -> None:
        self.repo_root = Path(__file__).resolve().parent.parent
        self.config_path = self._resolve(config_path)
        self.forarbete_rank_path = self._resolve(forarbete_rank_path)
        self.input_dir = self._resolve(input_dir)
        self.errors_path = self._resolve(errors_path)
        self.collection_name = collection_name
        self.chroma_path = chroma_path

        # Bygg config med rätt chroma-sökväg
        self._temp_config_path: Path | None = None
        effective_config = self._build_config(self.config_path, chroma_path, collection_name)

        self.vector_store = vector_store or ChromaVectorStore(config_path=effective_config)
        self.embedder = embedder or Embedder(config_path=effective_config)
        self.sou_rank = self._load_sou_rank(self.forarbete_rank_path)
        self.error_rows: list[dict[str, Any]] = []

    def _resolve(self, path: str | Path) -> Path:
        p = Path(path)
        return p if p.is_absolute() else self.repo_root / p

    def _build_config(self, config_path: Path, chroma_path: str, collection_name: str) -> Path:
        """Skapa temporär config med rätt chroma-sökväg för SOU."""
        with config_path.open("r", encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh) or {}

        # Sätt absolut sökväg för SOU-instansen
        abs_chroma = str(self.repo_root / chroma_path)
        cfg.setdefault("chroma", {})["persistent_path"] = abs_chroma
        cfg["chroma"].setdefault("collections", {})["forarbete"] = collection_name

        tmp = tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".yaml", delete=False)
        yaml.safe_dump(cfg, tmp, sort_keys=False, allow_unicode=True)
        tmp.close()
        self._temp_config_path = Path(tmp.name)
        return self._temp_config_path

    def _load_yaml(self, path: Path) -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}

    def _load_sou_rank(self, path: Path) -> int:
        rank = self._load_yaml(path).get("forarbete_types", {}).get("sou", {}).get("rank")
        if not isinstance(rank, int):
            raise ValueError("forarbete_rank för sou saknas eller är inte int.")
        return rank

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
        self.error_rows.append({
            "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
            "file": str(file_path),
            "error": message,
        })
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

    def _parse_beteckning(self, beteckning: str) -> tuple[str, int] | None:
        match = re.search(r"(?i)\bSOU\s+(\d{4})\s*:\s*(\d+)\b", beteckning or "")
        if match:
            return match.group(1), int(match.group(2))
        return None

    def _build_chunk_metadata(
        self,
        document: dict[str, Any],
        chunk: dict[str, Any],
    ) -> tuple[str, dict[str, Any]] | None:
        text = str(chunk.get("text") or "").strip()
        if not text:
            return None

        år = str(document.get("år") or "")
        nr = int(document.get("nr") or 0)
        chunk_index = int(chunk.get("chunk_index") or 0)
        namespace = chunk.get("namespace") or build_sou_namespace(år, nr, chunk_index)
        pinpoint = str(chunk.get("pinpoint") or "")
        citation = str(chunk.get("citation") or f"SOU {år}:{nr}")

        metadata = {
            "namespace":        namespace,
            "source_type":      "forarbete",
            "forarbete_type":   "sou",
            "document_subtype": "sou",
            "beteckning":       str(document.get("beteckning") or ""),
            "citation":         citation,
            "short_citation":   f"SOU {år}:{nr}",
            "dok_id":           str(document.get("dok_id") or ""),
            "authority_level":  "preparatory",
            "forarbete_rank":   self.sou_rank,
            "title":            str(document.get("titel") or ""),
            "section":          str(chunk.get("section") or "other"),
            "section_title":    str(chunk.get("section_title") or "other"),
            "pinpoint":         pinpoint,
            "page_start":       int(chunk.get("page_start") or 0),
            "page_end":         int(chunk.get("page_end") or 0),
            "legal_area":       self._serialize_list_field(document.get("legal_area")),
            "references_to":    self._serialize_list_field(document.get("references_to")),
            "år":               år,
            "nr":               nr,
            "riksmote":         str(document.get("riksmote") or ""),
            "datum":            str(document.get("datum") or ""),
            "organ":            str(document.get("organ") or ""),
            "source_url":       str(document.get("source_url") or ""),
            "embedding_model":  EMBEDDING_MODEL,
            "chunk_index":      chunk_index,
            "chunk_total":      len(document.get("chunks") or []),
            "fetched_at":       str(document.get("fetched_at") or ""),
            "sha256":           hashlib.sha256(text.encode("utf-8")).hexdigest(),
        }
        return text, metadata

    def index_all(
        self,
        *,
        dry_run: bool = False,
        max_docs: int | None = None,
    ) -> IndexingSummary:
        summary = IndexingSummary()
        files = sorted(f for f in self.input_dir.glob("*.json") if not f.name.startswith("_"))
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

            # Kontrollera om redan indexerat
            år = str(document.get("år") or "")
            nr = int(document.get("nr") or 0)
            first_ns = build_sou_namespace(år, nr, 0)
            if chunks and self._namespace_exists(first_ns):
                summary.documents_skipped += 1
                continue

            texts: list[str] = []
            metadatas: list[dict[str, Any]] = []
            for chunk in chunks:
                built = self._build_chunk_metadata(document, chunk)
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
        if self._temp_config_path and self._temp_config_path.exists():
            self._temp_config_path.unlink()

        return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Index SOU norm documents into ChromaDB.")
    parser.add_argument("--norm-dir", default="data/norm/sou")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--max-docs", type=int, default=None)
    args = parser.parse_args(argv)

    if args.verbose:
        logging.basicConfig(level=logging.INFO)

    indexer = SouIndexer(input_dir=args.norm_dir)
    summary = indexer.index_all(dry_run=args.dry_run, max_docs=args.max_docs)
    print(summary.as_dict())

    failed_ratio = summary.errors / summary.documents_seen if summary.documents_seen else 0.0
    return 1 if failed_ratio > 0.01 else 0


if __name__ == "__main__":
    raise SystemExit(main())
