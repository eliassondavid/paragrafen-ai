"""Shared normalization primitives for forarbete pipelines."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from typing import Any

import yaml

from pipelines.common.chunk_base import ForarbeteChunker
from pipelines.common.fetch_base import RawDocument
from pipelines.common.parse_base import Section

logger = logging.getLogger("paragrafenai.noop")


@dataclass
class NormalizedChunk:
    chunk_id: str
    namespace: str
    source_document_id: str
    document_subtype: str
    chunk_index: int
    chunk_total: int
    section_path: str
    section_title: str
    chunk_text: str
    token_count: int
    authority_level: str
    forarbete_rank: int
    citation: str
    short_citation: str
    legal_area: str
    references_to: str
    source_url: str
    license: str
    schema_version: str
    is_active: bool
    extra_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class NormalizedDocument:
    source_document_id: str
    document_type: str
    document_subtype: str
    canonical_citation: str
    title: str
    session_or_year: str
    department_or_committee: str
    issued_at: str
    authority_level: str
    forarbete_rank: int
    source_url: str
    html_url: str
    content_hash: str
    status: str
    is_active: bool
    ingest_method: str
    curated_by: str | None
    curated_note: str | None
    schema_version: str
    ingested_at: str
    chunks: list[NormalizedChunk]
    extra_metadata: dict[str, Any] = field(default_factory=dict)


class ForarbeteNormalizer(ABC):
    """Bas-normalizer. Adaptern implementerar build_chunk_metadata()."""

    DOCUMENT_META_KEYS = {
        "document_subtype",
        "canonical_citation",
        "title",
        "session_or_year",
        "department_or_committee",
        "issued_at",
        "source_url",
        "html_url",
        "status",
        "is_active",
        "ingest_method",
        "curated_by",
        "curated_note",
        "legal_area",
        "references_to",
    }

    CHUNK_META_KEYS = {
        "citation",
        "short_citation",
        "legal_area",
        "references_to",
        "source_url",
    }

    def __init__(
        self,
        *,
        rank_config_path: str | Path = "config/forarbete_rank.yaml",
        chunker: ForarbeteChunker | None = None,
        schema_version: str = "v0.14",
    ) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.rank_config_path = self._resolve_path(rank_config_path)
        self.chunker = chunker or ForarbeteChunker()
        self.schema_version = schema_version

    def normalize(self, raw: RawDocument, sections: list[Section]) -> NormalizedDocument:
        """
        Bygger NormalizedDocument från rådata och parsade sektioner.
        Anropar self.build_document_metadata() och self.build_chunk_id().
        """
        document_metadata = self.build_document_metadata(raw) or {}
        subtype = str(document_metadata.get("document_subtype") or "").strip()
        if not subtype:
            raise ValueError("build_document_metadata måste returnera document_subtype.")

        forarbete_rank = self.load_forarbete_rank(subtype)
        chunked_sections = self.chunker.chunk_sections(sections)
        chunk_total = len(chunked_sections)

        chunks: list[NormalizedChunk] = []
        for chunk_index, chunked in enumerate(chunked_sections):
            chunk_id = self.build_chunk_id(raw, chunk_index)
            if not chunk_id:
                logger.warning("Skippade chunk %s för %s utan chunk_id.", chunk_index, raw.dok_id)
                continue

            chunk_metadata = self.build_chunk_metadata(
                raw,
                chunked,
                chunk_index=chunk_index,
                chunk_total=chunk_total,
                document_metadata=document_metadata,
            ) or {}
            citation = str(
                chunk_metadata.get("citation")
                or document_metadata.get("canonical_citation")
                or raw.metadata.get("beteckning")
                or raw.dok_id
            )
            short_citation = str(
                chunk_metadata.get("short_citation")
                or document_metadata.get("canonical_citation")
                or citation
            )
            legal_area = self.serialize_list_field(
                chunk_metadata.get("legal_area", document_metadata.get("legal_area"))
            )
            references_to = self.serialize_list_field(
                chunk_metadata.get("references_to", document_metadata.get("references_to"))
            )
            source_url = str(
                chunk_metadata.get("source_url")
                or document_metadata.get("source_url")
                or raw.metadata.get("source_url")
                or ""
            )
            extra_chunk_metadata = {
                key: value
                for key, value in chunk_metadata.items()
                if key not in self.CHUNK_META_KEYS
            }
            chunks.append(
                NormalizedChunk(
                    chunk_id=chunk_id,
                    namespace=chunk_id,
                    source_document_id=raw.dok_id,
                    document_subtype=subtype,
                    chunk_index=chunk_index,
                    chunk_total=chunk_total,
                    section_path=chunked.section_path,
                    section_title=chunked.section_title,
                    chunk_text=chunked.chunk_text,
                    token_count=chunked.token_count,
                    authority_level="preparatory",
                    forarbete_rank=forarbete_rank,
                    citation=citation,
                    short_citation=short_citation,
                    legal_area=legal_area,
                    references_to=references_to,
                    source_url=source_url,
                    license="public_domain",
                    schema_version=self.schema_version,
                    is_active=True,
                    extra_metadata=extra_chunk_metadata,
                )
            )

        source_url = str(document_metadata.get("source_url") or raw.metadata.get("source_url") or "")
        html_url = str(
            document_metadata.get("html_url")
            or raw.metadata.get("dokument_url_html")
            or raw.metadata.get("html_url")
            or ""
        )
        extra_document_metadata = {
            key: value
            for key, value in document_metadata.items()
            if key not in self.DOCUMENT_META_KEYS
        }

        return NormalizedDocument(
            source_document_id=raw.dok_id,
            document_type="forarbete",
            document_subtype=subtype,
            canonical_citation=str(
                document_metadata.get("canonical_citation")
                or raw.metadata.get("beteckning")
                or raw.dok_id
            ),
            title=str(document_metadata.get("title") or raw.metadata.get("titel") or raw.filename),
            session_or_year=str(
                document_metadata.get("session_or_year")
                or raw.metadata.get("rm")
                or raw.metadata.get("ar")
                or ""
            ),
            department_or_committee=str(
                document_metadata.get("department_or_committee")
                or raw.metadata.get("organ")
                or ""
            ),
            issued_at=str(
                document_metadata.get("issued_at")
                or raw.metadata.get("datum")
                or ""
            ),
            authority_level="preparatory",
            forarbete_rank=forarbete_rank,
            source_url=source_url,
            html_url=html_url,
            content_hash=raw.content_hash,
            status=str(document_metadata.get("status") or "active"),
            is_active=bool(document_metadata.get("is_active", True)),
            ingest_method=str(document_metadata.get("ingest_method") or "api"),
            curated_by=document_metadata.get("curated_by"),
            curated_note=document_metadata.get("curated_note"),
            schema_version=self.schema_version,
            ingested_at=datetime.now(timezone.utc).isoformat(),
            chunks=chunks,
            extra_metadata=extra_document_metadata,
        )

    @abstractmethod
    def build_document_metadata(self, raw: RawDocument) -> dict[str, Any]:
        """Implementeras av adaptern — dokumentspecifik metadata."""

    @abstractmethod
    def build_chunk_id(self, raw: RawDocument, chunk_index: int) -> str:
        """Implementeras av adaptern — namespace-format per dokumenttyp."""

    def build_chunk_metadata(
        self,
        raw: RawDocument,
        chunked_section: Any,
        *,
        chunk_index: int,
        chunk_total: int,
        document_metadata: dict[str, Any],
    ) -> dict[str, Any]:
        """Kan överlagras av adaptern för chunk-specifik metadata."""
        return {
            "citation": document_metadata.get("canonical_citation", ""),
            "short_citation": document_metadata.get("canonical_citation", ""),
            "legal_area": document_metadata.get("legal_area"),
            "references_to": document_metadata.get("references_to"),
            "source_url": document_metadata.get("source_url"),
        }

    def serialize_list_field(self, value: Any) -> str:
        """
        Konvertera list/str/None till JSON-sträng.
        Kopiera _serialize_list_field från prop_indexer.py — samma logik.
        """
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

    def load_forarbete_rank(self, subtype: str) -> int:
        """Läs rank från config/forarbete_rank.yaml — aldrig hårdkoda."""
        try:
            with self.rank_config_path.open("r", encoding="utf-8") as fh:
                payload = yaml.safe_load(fh) or {}
        except (OSError, yaml.YAMLError) as exc:
            raise ValueError(f"Kunde inte läsa rank-config: {exc}") from exc

        type_cfg = payload.get("forarbete_types", {})
        if not isinstance(type_cfg, dict):
            raise ValueError("forarbete_rank.yaml saknar forarbete_types.")

        normalized_subtype = subtype.strip().casefold()
        for key, entry in type_cfg.items():
            if not isinstance(entry, dict):
                continue
            candidates = {
                str(key).strip().casefold(),
                str(entry.get("namespace_prefix", "")).strip().casefold(),
                str(entry.get("forarbete_type", "")).strip().casefold(),
            }
            if normalized_subtype not in candidates:
                continue
            rank = entry.get("rank")
            if not isinstance(rank, int):
                raise ValueError(f"Rank saknas eller är ogiltig för subtype={subtype}.")
            return rank

        raise ValueError(f"Kunde inte hitta forarbete_rank för subtype={subtype}.")

    def _resolve_path(self, value: str | Path) -> Path:
        path = Path(value)
        if path.is_absolute():
            return path
        return self.repo_root / path


__all__ = [
    "ForarbeteNormalizer",
    "NormalizedChunk",
    "NormalizedDocument",
]
