"""SOU indexing pipeline for F4."""

from __future__ import annotations

import json
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import yaml

from index.embedder import Embedder
from index.vector_store import ChromaVectorStore

logger = logging.getLogger("paragrafenai.noop")


REQUIRED_METADATA_FIELDS = {
    "namespace",
    "source_id",
    "source_type",
    "document_type",
    "beteckning",
    "title",
    "year",
    "department",
    "section_title",
    "legal_area",
    "authority_level",
    "pinpoint",
    "embedding_model",
    "chunk_index",
    "chunk_total",
    "source_url",
    "indexed_at",
}


@dataclass
class IndexingSummary:
    documents_seen: int = 0
    documents_indexed: int = 0
    documents_skipped: int = 0
    chunks_indexed: int = 0
    errors: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "documents_seen": self.documents_seen,
            "documents_indexed": self.documents_indexed,
            "documents_skipped": self.documents_skipped,
            "chunks_indexed": self.chunks_indexed,
            "errors": self.errors,
        }


class SouIndexer:
    """Indexes normalized SOU chunk JSON files into Chroma."""

    def __init__(
        self,
        config_path: str | Path = "config/embedding_config.yaml",
        legal_areas_path: str | Path = "config/legal_areas.yaml",
        input_dir: str | Path = "data/norm/forarbete",
        errors_path: str | Path = "data/index/indexing_errors.jsonl",
    ) -> None:
        self.repo_root = Path(__file__).resolve().parent.parent
        self.config_path = self._resolve_path(config_path)
        self.legal_areas_path = self._resolve_path(legal_areas_path)
        self.input_dir = self._resolve_path(input_dir)
        self.errors_path = self._resolve_path(errors_path)

        self.config = self._load_yaml(self.config_path)
        self.collection_name = str(self.config["chroma"]["collections"]["forarbete"])

        self.vector_store = ChromaVectorStore(config_path=self.config_path)
        self.embedder = Embedder(config_path=self.config_path)

        self.valid_area_ids, self.alias_to_area_id = self._load_legal_areas(self.legal_areas_path)
        self.error_rows: list[dict[str, Any]] = []
        self._last_progress_bucket = 0

    def _resolve_path(self, path_value: str | Path) -> Path:
        candidate = Path(path_value)
        if candidate.is_absolute():
            return candidate
        return self.repo_root / candidate

    def _load_yaml(self, path: Path) -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}

    def _load_legal_areas(self, path: Path) -> tuple[set[str], dict[str, str]]:
        payload = self._load_yaml(path)
        valid_ids: set[str] = set()
        alias_map: dict[str, str] = {}

        for item in payload.get("legal_areas", []):
            area_id = str(item.get("id", "")).strip()
            if not area_id:
                continue
            valid_ids.add(area_id)
            alias_map[area_id.lower()] = area_id
            for alias in item.get("aliases", []) or []:
                alias_map[str(alias).strip().lower()] = area_id

        return valid_ids, alias_map

    def _record_error(self, file_path: Path, message: str, payload: dict[str, Any] | None = None) -> None:
        row: dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
            "file": str(file_path),
            "error": message,
        }
        if payload is not None:
            row["payload"] = payload
        self.error_rows.append(row)
        logger.error("%s (%s)", message, file_path.name)

    def _write_errors(self) -> None:
        self.errors_path.parent.mkdir(parents=True, exist_ok=True)
        with self.errors_path.open("w", encoding="utf-8") as fh:
            for row in self.error_rows:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _normalize_sou_beteckning(self, beteckning: str) -> str:
        match = re.search(r"(\d{4})\s*:\s*(\d+)", beteckning)
        if match:
            year = int(match.group(1))
            number = int(match.group(2))
            return f"SOU_{year}_{number:03d}"
        return re.sub(r"[^A-Za-z0-9]+", "_", beteckning).strip("_") or "SOU_OKAND"

    def _normalize_section(self, section_title: str) -> str:
        lowered = (section_title or "").lower()
        lowered = lowered.replace("å", "a").replace("ä", "a").replace("ö", "o")
        slug = re.sub(r"[^a-z0-9]+", "_", lowered).strip("_")
        if not slug:
            return "avsnitt_okand"
        if slug[0].isdigit():
            return f"avsnitt_{slug}"
        return slug

    def _build_namespace(self, beteckning: str, section_title: str, chunk_index: int) -> str:
        sou_id = self._normalize_sou_beteckning(beteckning)
        section_id = self._normalize_section(section_title)
        return f"forarbete::{sou_id}_{section_id}_chunk_{chunk_index:03d}"

    def _normalize_legal_area(self, raw_value: Any, source_file: Path) -> list[str]:
        if raw_value is None:
            return ["okänt"]

        if isinstance(raw_value, str):
            values = [raw_value]
        elif isinstance(raw_value, list):
            values = [str(item) for item in raw_value if str(item).strip()]
        else:
            values = [str(raw_value)]

        normalized: list[str] = []
        seen: set[str] = set()
        for value in values:
            key = value.strip().lower()
            area_id = self.alias_to_area_id.get(key, value.strip())
            if area_id not in self.valid_area_ids:
                logger.warning("legal_area ej i legal_areas.yaml: %s (%s)", area_id, source_file.name)
            if area_id and area_id not in seen:
                seen.add(area_id)
                normalized.append(area_id)

        return normalized or ["okänt"]

    def _validate_metadata(self, metadata: dict[str, Any], source_file: Path) -> bool:
        missing = REQUIRED_METADATA_FIELDS.difference(metadata.keys())
        if missing:
            self._record_error(
                source_file,
                "Saknade metadata-fält",
                {"missing_fields": sorted(missing), "namespace": metadata.get("namespace", "")},
            )
            return False
        return True

    def _get_existing_source_id_for_beteckning(self, beteckning: str) -> str | None:
        metadata = self.vector_store.get_one_metadata(
            collection_name=self.collection_name,
            where_filter={"beteckning": beteckning},
        )
        if not metadata:
            return None
        source_id = metadata.get("source_id")
        return str(source_id) if source_id else None

    def _is_document_already_indexed(self, source_id: str, beteckning: str) -> bool:
        if source_id and self.vector_store.source_id_exists(self.collection_name, source_id):
            return True

        existing_source_id = self._get_existing_source_id_for_beteckning(beteckning)
        if existing_source_id and self.vector_store.source_id_exists(self.collection_name, existing_source_id):
            return True
        return False

    def _parse_chunk_index(self, chunk: dict[str, Any], fallback: int) -> int:
        value = chunk.get("chunk_index", fallback)
        try:
            return int(value)
        except (TypeError, ValueError):
            return fallback

    def _parse_chunk_total(self, chunk: dict[str, Any], fallback: int) -> int:
        value = chunk.get("chunk_total", fallback)
        try:
            return int(value)
        except (TypeError, ValueError):
            return fallback

    def _parse_year(self, document: dict[str, Any], beteckning: str) -> int:
        year = document.get("year")
        if isinstance(year, int):
            return year
        if isinstance(year, str) and year.isdigit():
            return int(year)

        match = re.search(r"(\d{4})\s*:", beteckning)
        if match:
            return int(match.group(1))
        return 0

    def _index_document(self, file_path: Path, summary: IndexingSummary) -> None:
        try:
            with file_path.open("r", encoding="utf-8") as fh:
                document = json.load(fh)
        except Exception as exc:
            self._record_error(file_path, f"Kunde inte läsa JSON: {exc}")
            summary.errors += 1
            return

        chunks = document.get("chunks")
        if not isinstance(chunks, list):
            self._record_error(file_path, "Dokument saknar chunks-lista")
            summary.errors += 1
            return

        raw_beteckning = str(document.get("beteckning", "")).strip()
        if not raw_beteckning:
            raw_beteckning = f"OKÄND_{uuid.uuid4().hex[:8]}"
            logger.warning("Saknar beteckning i %s, fallback används: %s", file_path.name, raw_beteckning)

        source_id = str(document.get("source_id", "")).strip() or str(uuid.uuid4())

        if self._is_document_already_indexed(source_id, raw_beteckning):
            summary.documents_skipped += 1
            logger.info("Hoppar över redan indexerat dokument: %s", raw_beteckning)
            return

        title = str(document.get("title", "")).strip()
        year = self._parse_year(document, raw_beteckning)
        department = str(document.get("department", "")).strip()
        source_url = str(document.get("source_url", "")).strip()
        indexed_at = date.today().isoformat()

        prepared_texts: list[str] = []
        prepared_metadatas: list[dict[str, Any]] = []

        total_chunks = len(chunks)
        for fallback_index, chunk in enumerate(chunks):
            text = str(chunk.get("text", "") or "").strip()
            if not text:
                self._record_error(file_path, "Chunk saknar text", {"chunk_index": fallback_index})
                summary.errors += 1
                continue

            chunk_index = self._parse_chunk_index(chunk, fallback_index)
            chunk_total = self._parse_chunk_total(chunk, total_chunks)
            section_title = str(chunk.get("section_title", "")).strip()
            pinpoint = str(chunk.get("pinpoint", "")).strip()
            legal_area = self._normalize_legal_area(chunk.get("legal_area"), file_path)

            metadata = {
                "namespace": self._build_namespace(raw_beteckning, section_title, chunk_index),
                "source_id": source_id,
                "source_type": "forarbete",
                "document_type": "SOU",
                "beteckning": raw_beteckning,
                "title": title,
                "year": year,
                "department": department,
                "section_title": section_title,
                "legal_area": legal_area,
                "authority_level": "persuasive",
                "pinpoint": pinpoint,
                "embedding_model": self.embedder.model_name,
                "chunk_index": chunk_index,
                "chunk_total": chunk_total,
                "source_url": source_url,
                "indexed_at": indexed_at,
            }

            if not self._validate_metadata(metadata, file_path):
                summary.errors += 1
                continue

            prepared_texts.append(text)
            prepared_metadatas.append(metadata)

        if not prepared_texts:
            logger.warning("Inga indexerbara chunks i %s", file_path.name)
            return

        indexed_for_document = 0
        for start in range(0, len(prepared_texts), 100):
            end = start + 100
            batch_texts = prepared_texts[start:end]
            batch_metadatas = prepared_metadatas[start:end]
            embeddings = self.embedder.embed(batch_texts)
            if len(embeddings) != len(batch_texts):
                self._record_error(
                    file_path,
                    "Embedding-resultat matchar inte batchstorlek",
                    {"expected": len(batch_texts), "actual": len(embeddings)},
                )
                summary.errors += 1
                continue

            added = self.vector_store.add_chunks(
                collection_name=self.collection_name,
                chunks=batch_texts,
                embeddings=embeddings,
                metadatas=batch_metadatas,
            )
            indexed_for_document += added
            summary.chunks_indexed += added

            progress_bucket = summary.chunks_indexed // 500
            if progress_bucket > self._last_progress_bucket:
                logger.info("Indexerade chunks: %s", summary.chunks_indexed)
                self._last_progress_bucket = progress_bucket

        if indexed_for_document > 0:
            summary.documents_indexed += 1

    def index_all(self) -> dict[str, int]:
        summary = IndexingSummary()
        files = sorted(self.input_dir.glob("*.json"))
        if not files:
            logger.info("Ingen indata hittad i %s", self.input_dir)
            self._write_errors()
            return summary.as_dict()

        for file_path in files:
            summary.documents_seen += 1
            self._index_document(file_path, summary)

        self._write_errors()
        return summary.as_dict()


def main() -> None:
    indexer = SouIndexer()
    result = indexer.index_all()
    logger.info("Indexering klar: %s", result)


if __name__ == "__main__":
    main()
