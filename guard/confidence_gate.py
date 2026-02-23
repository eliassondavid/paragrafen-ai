"""Flaggar svar med låg konfidensgrad."""
from dataclasses import dataclass

@dataclass
class ConfidenceResult:
    score: float
    flagged: bool
    reason: str | None = None

def gate(retrieval_scores: list[float], threshold: float = 0.6) -> ConfidenceResult:
    if not retrieval_scores:
        return ConfidenceResult(score=0.0, flagged=True, reason="Inga källdokument hittades")
    avg = sum(retrieval_scores) / len(retrieval_scores)
    if avg < threshold:
        return ConfidenceResult(score=avg, flagged=True, reason=f"Låg källmatchning ({avg:.2f})")
    return ConfidenceResult(score=avg, flagged=False)
