"""Shared validation primitives for forarbete pipelines."""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
from pathlib import Path
from typing import Any

import yaml

from pipelines.common.fetch_base import RawDocument
from pipelines.common.normalize_base import NormalizedDocument

logger = logging.getLogger("paragrafenai.noop")


@dataclass
class ValidationResult:
    passed: bool
    level: str
    errors: list[str]
    warnings: list[str]


class ForarbeteValidator:
    """
    Treskiktad validering per beslut V7.

    Nivå A — Source: HTTP 200, payload komplett, content_hash finns
    Nivå B — Schema: alla obligatoriska fält, forarbete_rank från yaml,
                     legal_area och references_to är JSON-strängar (inte listor)
    Nivå C — Content: chunk_text >= 50 tecken, section_path ej tom,
                      token_count > 0, minst 1 chunk per dokument
    """

    REQUIRED_CHUNK_FIELDS = [
        "chunk_id",
        "namespace",
        "source_document_id",
        "document_subtype",
        "authority_level",
        "forarbete_rank",
        "citation",
        "legal_area",
        "references_to",
        "chunk_text",
        "token_count",
        "schema_version",
    ]

    REQUIRED_DOCUMENT_FIELDS = [
        "source_document_id",
        "document_type",
        "document_subtype",
        "canonical_citation",
        "title",
        "authority_level",
        "forarbete_rank",
        "content_hash",
        "schema_version",
    ]

    def __init__(
        self,
        *,
        rank_config_path: str | Path = "config/forarbete_rank.yaml",
    ) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.rank_config_path = self._resolve_path(rank_config_path)
        self.allowed_ranks = self._load_allowed_ranks()

    def validate_source(self, raw: RawDocument) -> ValidationResult:
        errors: list[str] = []
        warnings: list[str] = []

        if not getattr(raw, "dok_id", "").strip():
            errors.append("dok_id saknas.")
        if not getattr(raw, "filename", "").strip():
            errors.append("filename saknas.")
        metadata = getattr(raw, "metadata", None)
        if not isinstance(metadata, dict) or not metadata:
            errors.append("metadata saknas eller är ogiltig.")
        status_json = getattr(raw, "status_json", None)
        if not isinstance(status_json, dict) or not status_json:
            errors.append("status_json saknas eller är ogiltig.")
        content_hash = getattr(raw, "content_hash", "")
        if not isinstance(content_hash, str) or not content_hash.strip():
            errors.append("content_hash saknas.")

        html_available = bool(getattr(raw, "html_available", False))
        html_content = str(getattr(raw, "html_content", "") or "")
        if html_available and not html_content.strip():
            errors.append("html_available=True men html_content är tom.")
        if not html_available:
            warnings.append("HTML saknas för dokumentet.")

        return ValidationResult(
            passed=not errors,
            level="source",
            errors=errors,
            warnings=warnings,
        )

    def validate_schema(self, doc: NormalizedDocument) -> ValidationResult:
        errors: list[str] = []
        warnings: list[str] = []

        for field_name in self.REQUIRED_DOCUMENT_FIELDS:
            value = getattr(doc, field_name, None)
            if value in ("", None):
                errors.append(f"Dokumentfält saknas: {field_name}.")

        if doc.document_type != "forarbete":
            errors.append("document_type måste vara 'forarbete'.")
        if doc.authority_level != "preparatory":
            errors.append("authority_level måste vara 'preparatory'.")
        if self.allowed_ranks and doc.forarbete_rank not in self.allowed_ranks:
            errors.append("forarbete_rank finns inte i forarbete_rank.yaml.")

        for index, chunk in enumerate(doc.chunks):
            for field_name in self.REQUIRED_CHUNK_FIELDS:
                value = getattr(chunk, field_name, None)
                if value in ("", None):
                    errors.append(f"Chunk {index} saknar fältet {field_name}.")
            for field_name in ("legal_area", "references_to"):
                raw_value = getattr(chunk, field_name, None)
                if not isinstance(raw_value, str):
                    errors.append(f"Chunk {index} har {field_name} som inte är JSON-sträng.")
                    continue
                try:
                    parsed = json.loads(raw_value)
                except (json.JSONDecodeError, TypeError):
                    errors.append(f"Chunk {index} har ogiltig JSON i {field_name}.")
                    continue
                if not isinstance(parsed, list):
                    warnings.append(f"Chunk {index} har {field_name} som JSON men inte lista.")
            if chunk.chunk_total != len(doc.chunks):
                warnings.append(f"Chunk {index} har chunk_total={chunk.chunk_total}, väntat {len(doc.chunks)}.")

        return ValidationResult(
            passed=not errors,
            level="schema",
            errors=errors,
            warnings=warnings,
        )

    def validate_content(self, doc: NormalizedDocument) -> ValidationResult:
        errors: list[str] = []
        warnings: list[str] = []

        if not doc.chunks:
            errors.append("Dokumentet saknar chunks.")

        for index, chunk in enumerate(doc.chunks):
            if len((chunk.chunk_text or "").strip()) < 50:
                errors.append(f"Chunk {index} är kortare än 50 tecken.")
            if not (chunk.section_path or "").strip():
                errors.append(f"Chunk {index} saknar section_path.")
            if int(chunk.token_count or 0) <= 0:
                errors.append(f"Chunk {index} har token_count <= 0.")
            if not (chunk.section_title or "").strip():
                warnings.append(f"Chunk {index} saknar section_title.")

        return ValidationResult(
            passed=not errors,
            level="content",
            errors=errors,
            warnings=warnings,
        )

    def validate_all(self, raw: RawDocument, doc: NormalizedDocument) -> list[ValidationResult]:
        """Kör alla tre nivåer. Stoppa vid första fel på source-nivå."""
        source_result = self.validate_source(raw)
        results = [source_result]
        if not source_result.passed:
            return results
        results.append(self.validate_schema(doc))
        results.append(self.validate_content(doc))
        return results

    def _load_allowed_ranks(self) -> set[int]:
        try:
            with self.rank_config_path.open("r", encoding="utf-8") as fh:
                payload = yaml.safe_load(fh) or {}
        except (OSError, yaml.YAMLError) as exc:
            logger.warning("Kunde inte läsa rank-config för validatorn: %s", exc)
            return set()

        type_cfg = payload.get("forarbete_types", {})
        if not isinstance(type_cfg, dict):
            return set()
        return {
            int(entry["rank"])
            for entry in type_cfg.values()
            if isinstance(entry, dict) and isinstance(entry.get("rank"), int)
        }

    def _resolve_path(self, value: str | Path) -> Path:
        path = Path(value)
        if path.is_absolute():
            return path
        return self.repo_root / path


__all__ = [
    "ForarbeteValidator",
    "ValidationResult",
]
