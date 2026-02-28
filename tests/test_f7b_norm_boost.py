# tests/test_f7b_norm_boost.py
import pytest

from index.norm_boost import NormBoost


def _chunk(cid: str, authority_level=None, distance=None):
    md = {}
    if authority_level is not None:
        md["authority_level"] = authority_level
    if distance is not None:
        md["distance"] = distance
    return {
        "id": cid,
        "text": f"text::{cid}",
        "metadata": md,
    }


def test_1_binding_before_guiding_same_distance():
    nb = NormBoost()
    chunks = [
        _chunk("g1", "guiding", 0.2),
        _chunk("b1", "binding", 0.2),
    ]
    out = nb.rerank(chunks)
    assert [c["id"] for c in out] == ["b1", "g1"]


def test_2_guiding_before_persuasive_same_distance():
    nb = NormBoost()
    chunks = [
        _chunk("p1", "persuasive", 0.2),
        _chunk("g1", "guiding", 0.2),
    ]
    out = nb.rerank(chunks)
    assert [c["id"] for c in out] == ["g1", "p1"]


def test_3_relevance_compensates_partly_but_not_enough_for_persuasive_over_binding_same_distance():
    nb = NormBoost()

    # Part A: Higher relevance can make guiding outrank binding if distance differs enough
    # binding @ 0.9 => relevance 0.1, score 1.0*0.1 = 0.10
    # guiding @ 0.0 => relevance 1.0, score 0.7*1.0 = 0.70
    out_a = nb.rerank([_chunk("b", "binding", 0.9), _chunk("g", "guiding", 0.0)])
    assert [c["id"] for c in out_a] == ["g", "b"]

    # Part B: With identical distance, persuasive must never beat binding
    out_b = nb.rerank([_chunk("p", "persuasive", 0.2), _chunk("b", "binding", 0.2)])
    assert [c["id"] for c in out_b] == ["b", "p"]


def test_4_missing_distance_uses_neutral_relevance_weight():
    nb = NormBoost()
    chunks = [
        _chunk("b1", "binding", None),
        _chunk("p1", "persuasive", 0.0),
    ]
    out = nb.rerank(chunks)
    # binding with missing distance => relevance 0.5 => score 0.5
    # persuasive with distance 0.0 => relevance 1.0 => score 0.4
    assert [c["id"] for c in out] == ["b1", "p1"]
    assert out[0]["norm_score"] == pytest.approx(0.5)


def test_5_missing_authority_level_treated_as_persuasive():
    nb = NormBoost()
    chunks = [
        _chunk("x1", None, 0.0),          # treated as persuasive => 0.4
        _chunk("g1", "guiding", 0.9),     # guiding => 0.7*0.1=0.07
    ]
    out = nb.rerank(chunks)
    assert [c["id"] for c in out] == ["x1", "g1"]
    assert out[0]["norm_score"] == pytest.approx(0.4)


def test_6_empty_list_returns_empty_list():
    nb = NormBoost()
    assert nb.rerank([]) == []
    assert nb.rerank(None) == []


def test_7_norm_score_present_in_all_output_chunks():
    nb = NormBoost()
    chunks = [
        _chunk("a", "binding", 0.2),
        _chunk("b", "guiding", None),
        _chunk("c", None, 0.8),
    ]
    out = nb.rerank(chunks)
    assert len(out) == 3
    assert all("norm_score" in c for c in out)


def test_8_stable_sort_preserves_original_order_on_equal_scores():
    nb = NormBoost()
    # Same authority and same distance => equal norm_score; must preserve input order
    chunks = [
        _chunk("p1", "persuasive", 0.5),
        _chunk("p2", "persuasive", 0.5),
        _chunk("p3", "persuasive", 0.5),
    ]
    out = nb.rerank(chunks)
    assert [c["id"] for c in out] == ["p1", "p2", "p3"]
