import pytest

from index.norm_boost import NormBoost


def test_preparatory_weight_between_guiding_and_persuasive():
    """preparatory (0.6) ska rangordnas efter guiding (0.7) men före persuasive (0.4)."""
    nb = NormBoost()
    chunks = [
        {"metadata": {"authority_level": "persuasive", "distance": 0.0}},
        {"metadata": {"authority_level": "preparatory", "distance": 0.0}},
        {"metadata": {"authority_level": "guiding", "distance": 0.0}},
        {"metadata": {"authority_level": "binding", "distance": 0.0}},
    ]
    result = nb.rerank(chunks)
    levels = [r["metadata"]["authority_level"] for r in result]
    assert levels == ["binding", "guiding", "preparatory", "persuasive"]


def test_preparatory_norm_score():
    """preparatory med distance=0.0 ska ge norm_score = 0.6."""
    nb = NormBoost()
    chunks = [{"metadata": {"authority_level": "preparatory", "distance": 0.0}}]
    result = nb.rerank(chunks)
    assert result[0]["norm_score"] == pytest.approx(0.6)


def test_unknown_authority_level_falls_back_to_persuasive():
    """Okänd authority_level ska behandlas som persuasive (rank 3, weight 0.4)."""
    nb = NormBoost()
    chunks = [
        {"metadata": {"authority_level": "okänd_nivå", "distance": 0.0}},
        {"metadata": {"authority_level": "preparatory", "distance": 0.0}},
    ]
    result = nb.rerank(chunks)
    assert result[0]["metadata"]["authority_level"] == "preparatory"


def test_tie_break_preparatory_vs_persuasive_same_score():
    """Vid lika norm_score: preparatory (rank 2) ska slå persuasive (rank 3)."""
    # Välj värden som ger exakt samma flyttal (0.4) för robust tie-break-test.
    nb = NormBoost()
    chunks = [
        {"metadata": {"authority_level": "persuasive", "distance": 0.0}},  # 0.4 * 1.0 = 0.4
        {"metadata": {"authority_level": "preparatory", "distance": 0.3333333333333333}},  # 0.6 * 0.666... = 0.4
    ]
    result = nb.rerank(chunks)
    assert result[0]["metadata"]["authority_level"] == "preparatory"
