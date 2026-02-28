"""Index normalized SFS chunks into an intermediate record format."""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class SfsIndexer:
    """Build index records for SFS chunks with deterministic metadata."""

    DEFAULT_COLLECTION = "paragrafen_sfs_v1"

    def __init__(
        self,
        norm_dir: Path | str = "data/norm/sfs",
        output_path: Path | str = "data/chunks/sfs_index_records.jsonl",
        config_path: Path | str = "config/embedding_config.yaml",
        collection_name: str | None = None,
        embedding_model: str | None = None,
    ) -> None:
        self.norm_dir = Path(norm_dir)
        self.output_path = Path(output_path)
        self.config_path = Path(config_path)

        cfg = self._load_config()
        self.collection_name = (
            collection_name
            or self._extract_collection_name(cfg)
            or self.DEFAULT_COLLECTION
        )
        self.embedding_model = embedding_model or self._extract_embedding_model(cfg) or ""

    @staticmethod
    def build_source_id(sfs_nr: str) -> str:
        """Generate deterministic source UUID from SFS number."""
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"sfs:{sfs_nr}"))

    def index_all(self) -> dict[str, Any]:
        """Read normalized SFS files and write index-ready JSONL records."""
        files = sorted(self.norm_dir.glob("*.json"))
        records: list[dict[str, Any]] = []

        indexed_at = datetime.now(timezone.utc).isoformat()
        documents = 0
        skipped = 0

        for file_path in files:
            try:
                doc = json.loads(file_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                skipped += 1
                continue

            sfs_nr = self._norm(doc.get("sfs_nr"))
            if not sfs_nr:
                skipped += 1
                continue

            chunks = doc.get("chunks")
            if not isinstance(chunks, list):
                skipped += 1
                continue

            source_id = self.build_source_id(sfs_nr)
            documents += 1
            for chunk in chunks:
                if not isinstance(chunk, dict):
                    continue
                record = self._build_record(doc, chunk, source_id, indexed_at)
                if record is not None:
                    records.append(record)

        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with self.output_path.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, ensure_ascii=False))
                handle.write("\n")

        return {
            "collection": self.collection_name,
            "documents_indexed": documents,
            "chunks_indexed": len(records),
            "files_scanned": len(files),
            "files_skipped": skipped,
            "output_path": str(self.output_path),
        }

    def _build_record(
        self,
        doc: dict[str, Any],
        chunk: dict[str, Any],
        source_id: str,
        indexed_at: str,
    ) -> dict[str, Any] | None:
        text = self._norm(chunk.get("text"))
        if not text:
            return None

        sfs_nr = self._norm(doc.get("sfs_nr"))
        paragraf_nr = self._norm(chunk.get("paragraf_nr"))
        kapitel_nr = self._norm(chunk.get("kapitel_nr"))
        kapitel_titel = self._norm(chunk.get("kapitel_titel"))
        ikraft = self._norm(chunk.get("ikraftträdandedatum")) or self._norm(
            doc.get("ikraftträdandedatum")
        )

        namespace = self._build_namespace(sfs_nr=sfs_nr, kapitel_nr=kapitel_nr, paragraf_nr=paragraf_nr)

        legal_area = chunk.get("legal_area")
        if not isinstance(legal_area, list):
            legal_area = []

        metadata = {
            "namespace": namespace,
            "source_id": source_id,
            "source_type": "sfs",
            "sfs_nr": sfs_nr,
            "titel": self._norm(doc.get("titel")),
            "paragraf_nr": paragraf_nr,
            "kapitel_nr": kapitel_nr,
            "kapitel_titel": kapitel_titel,
            "ikraftträdandedatum": ikraft,
            "consolidation_source": self._norm(doc.get("consolidation_source")),
            "legal_area": legal_area,
            "authority_level": "binding",
            "source_url": self._norm(doc.get("source_url")),
            "embedding_model": self.embedding_model,
            "chunk_index": self._to_int(chunk.get("chunk_index")),
            "chunk_total": self._to_int(chunk.get("chunk_total")),
            "indexed_at": indexed_at,
        }

        return {"id": namespace, "text": text, "metadata": metadata}

    def _build_namespace(self, sfs_nr: str, kapitel_nr: str, paragraf_nr: str) -> str:
        safe_paragraf = paragraf_nr or "unknown"
        if kapitel_nr:
            return f"sfs::{sfs_nr}_{kapitel_nr}kap_{safe_paragraf}para"
        return f"sfs::{sfs_nr}_{safe_paragraf}para"

    def _load_config(self) -> dict[str, Any]:
        if not self.config_path.exists():
            return {}

        raw = self.config_path.read_text(encoding="utf-8")
        try:
            import yaml  # type: ignore

            parsed = yaml.safe_load(raw)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

        return self._parse_yaml_like_text(raw)

    def _parse_yaml_like_text(self, raw: str) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for line in raw.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or ":" not in stripped:
                continue
            key, value = stripped.split(":", 1)
            result[key.strip()] = value.strip().strip("\"'")
        return result

    def _extract_collection_name(self, config: dict[str, Any]) -> str | None:
        for key in ("sfs_collection", "collection", "collection_name"):
            value = config.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

        collections = config.get("collections")
        if isinstance(collections, dict):
            value = collections.get("sfs")
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _extract_embedding_model(self, config: dict[str, Any]) -> str | None:
        for key in ("embedding_model", "model", "embeddingModel"):
            value = config.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _norm(self, value: Any) -> str:
        if value is None:
            return ""
        text = str(value).strip()
        return re.sub(r"\s+", " ", text)

    def _to_int(self, value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0
