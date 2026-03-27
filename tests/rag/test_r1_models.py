from rag.models import RAGHit, RAGResult


def test_raghit_default_values() -> None:
    hit = RAGHit(
        text="Ett chunk",
        metadata={"source_type": "sfs"},
        distance=0.12,
        collection="paragrafen_sfs_v1",
        weight=1.5,
    )

    assert hit.text == "Ett chunk"
    assert hit.metadata == {"source_type": "sfs"}
    assert hit.distance == 0.12
    assert hit.collection == "paragrafen_sfs_v1"
    assert hit.weight == 1.5
    assert hit.score == 0.0


def test_ragresult_has_binding_source_true_for_binding_hit() -> None:
    binding_hit = RAGHit(
        text="Bindande källa",
        metadata={"authority_level": "binding"},
        distance=0.1,
        collection="paragrafen_sfs_v1",
        weight=2.0,
    )
    result = RAGResult(
        hits=[binding_hit],
        confidence="high",
        disclaimer="",
        total_candidates=1,
        filtered_count=1,
    )

    assert result.has_binding_source is True


def test_ragresult_citations_deduplicates() -> None:
    hits = [
        RAGHit(
            text="Första träffen",
            metadata={"citation": "NJA 2024 s. 1"},
            distance=0.1,
            collection="paragrafen_praxis_v1",
            weight=1.0,
        ),
        RAGHit(
            text="Andra träffen",
            metadata={"citation": "NJA 2024 s. 1"},
            distance=0.2,
            collection="paragrafen_praxis_v1",
            weight=0.9,
        ),
        RAGHit(
            text="Tredje träffen",
            metadata={"short_citation": "Prop. 2023/24:10"},
            distance=0.3,
            collection="paragrafen_forarbete_v1",
            weight=0.8,
        ),
    ]
    result = RAGResult(
        hits=hits,
        confidence="medium",
        disclaimer="",
        total_candidates=3,
        filtered_count=3,
    )

    assert result.citations() == ["NJA 2024 s. 1", "Prop. 2023/24:10"]
