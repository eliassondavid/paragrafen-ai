from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class RAGHit:
    text: str
    metadata: dict[str, Any]
    distance: float
    collection: str
    weight: float
    score: float = 0.0


@dataclass
class RAGResult:
    hits: list[RAGHit]
    confidence: str
    disclaimer: str
    total_candidates: int
    filtered_count: int

    @property
    def has_binding_source(self) -> bool:
        return any(h.metadata.get("authority_level") == "binding" for h in self.hits)

    @property
    def source_types(self) -> set[str]:
        return {h.metadata.get("source_type", "unknown") for h in self.hits}

    def citations(self) -> list[str]:
        cites: list[str] = []
        for hit in self.hits:
            citation = hit.metadata.get("citation") or hit.metadata.get("short_citation")
            if citation and citation not in cites:
                cites.append(citation)
        return cites

