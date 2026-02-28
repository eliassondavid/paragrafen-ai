from __future__ import annotations

import logging
from typing import Any

LOGGER = logging.getLogger("paragrafenai.noop")


class ConfidenceGate:
    pass_threshold: float = 0.5
    only_persuasive_penalty: float = 0.3
    conflicting_authority_penalty: float = 0.2
    sparse_results_penalty: float = 0.15
    single_source_type_penalty: float = 0.1
    conflict_distance_threshold: float = 0.2
    default_distance: float = 0.5

    def evaluate(self, chunks: list[dict] | None) -> dict[str, Any]:
        """
        Utvärdera en lista RAG-chunks och returnera ett confidence-beslut.

        Returnerar:
        {
            "pass": bool,           # True om tillräcklig confidence
            "score": float,         # 0.0–1.0
            "reason": str | None,   # Förklaringstext om ej godkänd
            "flags": list[str],     # Varningsflaggor
        }
        """
        normalized_chunks: list[dict[str, Any]] = list(chunks or [])
        if not normalized_chunks:
            return {"pass": False, "score": 0.0, "reason": "No retrieval results.", "flags": ["no_results"]}

        score = 1.0
        flags: list[str] = []

        authority_levels = [self._get_authority_level(chunk) for chunk in normalized_chunks]
        source_types = [self._get_source_type(chunk) for chunk in normalized_chunks]

        if authority_levels and all(level == "persuasive" for level in authority_levels):
            flags.append("only_persuasive")
            score -= self.only_persuasive_penalty

        if self._has_conflicting_authority(normalized_chunks):
            flags.append("conflicting_authority")
            score -= self.conflicting_authority_penalty

        if len(normalized_chunks) < 2:
            flags.append("sparse_results")
            score -= self.sparse_results_penalty

        if source_types and len(set(source_types)) == 1:
            flags.append("single_source_type")
            score -= self.single_source_type_penalty

        score = max(0.0, round(score, 4))
        passed = score >= self.pass_threshold

        reason = None
        if not passed:
            reason = self._build_reason(flags)

        return {"pass": passed, "score": score, "reason": reason, "flags": flags}

    def _has_conflicting_authority(self, chunks: list[dict[str, Any]]) -> bool:
        ranked = sorted(chunks, key=self._get_distance)
        low_distance_chunks = [chunk for chunk in ranked if self._get_distance(chunk) <= self.conflict_distance_threshold]
        if not low_distance_chunks:
            return False

        levels = {self._get_authority_level(chunk) for chunk in low_distance_chunks}
        return "binding" in levels and "guiding" in levels

    def _build_reason(self, flags: list[str]) -> str:
        if not flags:
            return "Confidence below threshold."
        return "Confidence below threshold due to: " + ", ".join(flags)

    @staticmethod
    def _get_metadata(chunk: dict[str, Any]) -> dict[str, Any]:
        metadata = chunk.get("metadata", {})
        if isinstance(metadata, dict):
            return metadata
        return {}

    def _get_distance(self, chunk: dict[str, Any]) -> float:
        metadata = self._get_metadata(chunk)
        distance = metadata.get("distance", self.default_distance)
        if isinstance(distance, (int, float)):
            return float(distance)
        return self.default_distance

    def _get_authority_level(self, chunk: dict[str, Any]) -> str:
        metadata = self._get_metadata(chunk)
        authority = metadata.get("authority_level", "persuasive")
        if isinstance(authority, str):
            return authority.strip().lower() or "persuasive"
        return "persuasive"

    @staticmethod
    def _get_source_type(chunk: dict[str, Any]) -> str:
        metadata = chunk.get("metadata", {})
        if isinstance(metadata, dict):
            source_type = metadata.get("source_type", "unknown")
            if isinstance(source_type, str):
                normalized = source_type.strip().lower()
                return normalized or "unknown"
        return "unknown"
