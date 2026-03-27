from rag.models import RAGHit, RAGResult
from rag.prompt_builder import PromptBuilder


def test_build_context_formats_citations() -> None:
    builder = PromptBuilder()
    result = RAGResult(
        hits=[
            RAGHit(
                text="Avtalslagen gäller.",
                metadata={
                    "source_type": "sfs",
                    "authority_level": "binding",
                    "citation": "Avtalslagen 1 §",
                },
                distance=0.1,
                collection="sfs",
                weight=1.0,
            )
        ],
        confidence="high",
        disclaimer="Standarddisclaimer",
        total_candidates=1,
        filtered_count=1,
    )

    context = builder.build_context(result)

    assert "[Källa 1] (sfs, binding)" in context
    assert "Referens: Avtalslagen 1 §" in context


def test_system_prompt_contains_disclaimer() -> None:
    builder = PromptBuilder()
    result = RAGResult(
        hits=[],
        confidence="low",
        disclaimer="Verifiera med jurist.",
        total_candidates=0,
        filtered_count=0,
    )

    prompt = builder.build_system_prompt(result)

    assert "Verifiera med jurist." in prompt


def test_module_specific_addition() -> None:
    builder = PromptBuilder()
    result = RAGResult(
        hits=[],
        confidence="medium",
        disclaimer="Basdisclaimer.",
        total_candidates=0,
        filtered_count=0,
    )

    prompt = builder.build_system_prompt(result, module="framtidsfullmakt")

    assert "2017:310" in prompt
