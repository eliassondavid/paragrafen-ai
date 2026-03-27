from __future__ import annotations

import logging
from typing import Any

from guard.area_blocker import AreaBlocker
from rag.models import RAGHit

logger = logging.getLogger(__name__)


class GuardPipeline:
    def __init__(self, config_path: str | None = None):
        self.area_blocker = AreaBlocker(config_path)

        try:
            from index.norm_boost import NormBoost
        except Exception:  # noqa: BLE001
            self.norm_boost = None
        else:
            self.norm_boost = NormBoost()

        try:
            from guard.confidence_gate import ConfidenceGate
        except Exception:  # noqa: BLE001
            self.confidence_gate = None
        else:
            self.confidence_gate = ConfidenceGate()

        try:
            from publish.disclaimer_injector import DisclaimerInjector
        except Exception:  # noqa: BLE001
            self.disclaimer_injector = None
        else:
            self.disclaimer_injector = DisclaimerInjector()

    def check_query(self, query: str) -> tuple[bool, str | None]:
        return self.area_blocker.is_blocked(query)

    def filter_hits(self, hits: list[RAGHit]) -> list[RAGHit]:
        allowed_hits: list[RAGHit] = []
        excluded_areas = {"straffrätt_exkl", "skatterätt_exkl", "migrationsrätt_exkl"}

        for hit in hits:
            legal_area = hit.metadata.get("legal_area", "")
            if isinstance(legal_area, list):
                normalized_areas = {str(value).strip().lower() for value in legal_area}
            else:
                normalized_areas = {
                    part.strip().lower()
                    for part in str(legal_area).split(",")
                    if part.strip()
                }

            if normalized_areas & excluded_areas:
                continue

            namespace = str(hit.metadata.get("namespace", "")).strip().lower()
            if namespace.startswith("sfs::"):
                sfs_nr = str(hit.metadata.get("sfs_nr", "")).strip()
                if sfs_nr:
                    blocked, _ = self.area_blocker.is_sfs_blocked(sfs_nr)
                    if blocked:
                        continue

            allowed_hits.append(hit)

        return allowed_hits

    def assess_confidence(self, hits: list[RAGHit]) -> str:
        if not hits:
            return "low"

        if self.confidence_gate is not None and hasattr(self.confidence_gate, "evaluate"):
            gate_input = [
                {
                    "metadata": {
                        **hit.metadata,
                        "distance": hit.distance,
                    }
                }
                for hit in hits
            ]
            result = self.confidence_gate.evaluate(gate_input)
            if not result.get("pass", False):
                return "low"
            score = float(result.get("score", 0.0))
            if score >= 0.75:
                return "high"
            return "medium"

        average_score = sum(hit.score for hit in hits) / len(hits)
        has_binding = any(hit.metadata.get("authority_level") == "binding" for hit in hits)
        has_guiding = any(hit.metadata.get("authority_level") == "guiding" for hit in hits)

        if has_binding and average_score > 0.7:
            return "high"
        if has_guiding or average_score > 0.4:
            return "medium"
        return "low"

    def get_disclaimer(self, module: str, confidence: str) -> str:
        base_messages = {
            "high": (
                "§AI har hittat relevanta rättskällor. "
                "Svaret är inte juridisk rådgivning."
            ),
            "medium": (
                "§AI har hittat delvis relevanta källor. "
                "Verifiera med jurist."
            ),
            "low": "§AI har låg konfidens. Kontakta jurist.",
        }
        base_message = base_messages.get(confidence, base_messages["low"])

        if self.disclaimer_injector is not None and hasattr(self.disclaimer_injector, "inject"):
            return self.disclaimer_injector.inject(base_message)

        return base_message

