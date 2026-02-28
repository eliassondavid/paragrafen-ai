# index/norm_boost.py
from __future__ import annotations

import logging
from typing import Any


logger = logging.getLogger("paragrafenai.noop")


class NormBoost:
    """
    Omrangordnar RAG-chunks enligt normhierarki (authority_level) + relevans (distance).
    """

    AUTHORITY_WEIGHTS = {
        "binding": 1.0,
        "guiding": 0.7,
        "persuasive": 0.4,
    }

    def rerank(self, chunks: list[dict] | None) -> list[dict]:
        """
        Omrangordna RAG-chunks enligt normhierarki + relevans.

        Indata: Lista av chunk-dicts från Chroma-retrieval.
        Utdata: Samma lista, sorterad med bästa chunks först.
        Chunk-ordningen i input ändras ej — returnerar ny lista.
        """
        if not chunks:
            return []

        decorated: list[tuple[float, int, int, dict]] = []

        for idx, chunk in enumerate(chunks):
            # Defensive, but keep function pure (no raises for malformed chunks).
            if not isinstance(chunk, dict):
                continue

            metadata = chunk.get("metadata") or {}
            if not isinstance(metadata, dict):
                metadata = {}

            authority_level = metadata.get("authority_level")
            if authority_level not in self.AUTHORITY_WEIGHTS:
                authority_level = "persuasive"

            authority_weight = self.AUTHORITY_WEIGHTS[authority_level]

            distance = metadata.get("distance", None)
            relevance_weight = self._relevance_weight(distance)

            norm_score = authority_weight * relevance_weight

            # Copy chunk dict and add norm_score (do not mutate input)
            out_chunk = dict(chunk)
            out_chunk["norm_score"] = norm_score

            # Sort order:
            # 1) norm_score desc
            # 2) authority_level: binding > guiding > persuasive
            # 3) stable: preserve original order
            authority_rank = self._authority_rank(authority_level)
            decorated.append((norm_score, authority_rank, idx, out_chunk))

        # Python sort is stable; we also include idx explicitly to guarantee stability
        decorated.sort(key=lambda t: (-t[0], t[1], t[2]))
        return [t[3] for t in decorated]

    @staticmethod
    def _relevance_weight(distance: Any) -> float:
        """
        relevance_weight = 1.0 - min(distance, 1.0)
        If distance missing/invalid -> 0.5
        """
        if distance is None:
            return 0.5
        try:
            d = float(distance)
        except (TypeError, ValueError):
            return 0.5

        # Conservatively clamp: distance < 0 treated as 0, distance > 1 treated as 1
        if d < 0.0:
            d = 0.0
        if d > 1.0:
            d = 1.0
        return 1.0 - d

    @staticmethod
    def _authority_rank(level: str) -> int:
        """
        Lower rank value = higher priority in tie-breaks.
        binding (0) > guiding (1) > persuasive (2)
        """
        if level == "binding":
            return 0
        if level == "guiding":
            return 1
        return 2
