from __future__ import annotations

from modules.second_opinion.engine import SecondOpinionEngine
from rag.llm_client import MockLLMClient
from rag.models import RAGHit, RAGResult


class FakeGuard:
    def __init__(self, blocked: bool = False, message: str | None = None) -> None:
        self.blocked = blocked
        self.message = message
        self.calls: list[str] = []

    def check_query(self, query: str) -> tuple[bool, str | None]:
        self.calls.append(query)
        return self.blocked, self.message


class FakeRAG:
    def __init__(self, result: RAGResult) -> None:
        self.result = result
        self.calls: list[tuple[str, str, int | None]] = []

    def query(
        self,
        query: str,
        module: str,
        n_results: int | None = None,
        user_context: list[RAGHit] | None = None,
    ) -> RAGResult:
        self.calls.append((query, module, n_results))
        return self.result


class FakeLLM:
    def __init__(self, responses: list[str]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, list[dict], int]] = []

    def chat(
        self,
        system_prompt: str,
        messages: list[dict],
        max_tokens: int = 2000,
    ) -> str:
        self.calls.append((system_prompt, messages, max_tokens))
        return self.responses.pop(0)


def _rag_result(num_hits: int = 5) -> RAGResult:
    hits = [
        RAGHit(
            text=f"Textutdrag {index}",
            metadata={
                "authority_level": "binding" if index == 1 else "guiding",
                "source_type": "sfs" if index == 1 else "praxis",
                "citation": f"Referens {index}",
                "short_citation": f"Ref {index}",
            },
            distance=0.1,
            collection="sfs" if index == 1 else "praxis",
            weight=1.0,
            score=1.0,
        )
        for index in range(1, num_hits + 1)
    ]
    return RAGResult(
        hits=hits,
        confidence="high",
        disclaimer="§AI har hittat relevanta rättskällor.",
        total_candidates=num_hits,
        filtered_count=num_hits,
    )


def test_engine_returns_structured_report_from_json_backticks() -> None:
    rag = FakeRAG(_rag_result())
    llm = FakeLLM(
        responses=[
            "Den centrala rättsfrågan är om hyresgästen riskerar att förlora hyresrätten.",
            """```json
{
  "legal_analysis": "Rådet har visst stöd men behöver fler förbehåll.",
  "strengths": [
    {
      "description": "Viss betalningsfrist kan följa av lag.",
      "legal_ref": "Referens 1",
      "severity": "medium"
    }
  ],
  "weaknesses": [
    {
      "description": "Den angivna tidsfristen verkar vara för generell.",
      "legal_ref": "Referens 2",
      "severity": "high"
    }
  ],
  "gaps": [
    {
      "description": "Rådet nämner inte betydelsen av störningarnas art.",
      "legal_ref": "Referens 3",
      "severity": "medium"
    }
  ],
  "overall_assessment": "delvis_korrekt",
  "confidence": "medium"
}
```""",
            """```json
{
  "outcome_prognosis": "Utgången beror starkt på bevisning och störningarnas allvar.",
  "burden_of_proof": "Hyresvärden måste visa störningarna och att rättelseanmaning skett när det krävs.",
  "practical_obstacles": "Bevisning, dokumentation och tidsutdräkt kan påverka utfallet.",
  "prognosis_level": "osäkert",
  "follow_up_questions": [
    "Vilken dokumentation finns om störningarna?"
  ]
}
```""",
        ]
    )
    engine = SecondOpinionEngine(
        rag=rag,
        llm=llm,
        guard=FakeGuard(),
    )

    report = engine.analyze(
        situation="Min hyresvärd har sagt upp mig för störningar.",
        advice_received="Du har tre veckor på dig att betala.",
        legal_area="Hyresrätt",
    )

    assert report.overall_assessment == "delvis_korrekt"
    assert report.confidence == "medium"
    assert len(report.strengths) == 1
    assert len(report.weaknesses) == 1
    assert report.weaknesses[0].source_text == "Textutdrag 2"
    assert report.prognosis_level == "osäkert"
    assert rag.calls[0][1] == "second_opinion"


def test_engine_handles_empty_advice() -> None:
    rag = FakeRAG(_rag_result())
    engine = SecondOpinionEngine(
        rag=rag,
        llm=FakeLLM(responses=[]),
        guard=FakeGuard(),
    )

    report = engine.analyze(
        situation="Jag har en fråga om min uppsägning.",
        advice_received="",
    )

    assert report.confidence == "low"
    assert "ofullständigt" in report.summary.lower()
    assert rag.calls == []


def test_engine_returns_structured_fallback_with_mock_llm() -> None:
    engine = SecondOpinionEngine(
        rag=FakeRAG(_rag_result()),
        llm=MockLLMClient(),
        guard=FakeGuard(),
    )

    report = engine.analyze(
        situation="Jag fick råd om att inte bestrida en avgift.",
        advice_received="Du bör bara betala direkt.",
        legal_area="Konsumenträtt",
    )

    assert report.confidence == "low"
    assert report.overall_assessment == "tveksamt"
    assert "demo-läge" in report.summary.lower()
    assert report.citations
