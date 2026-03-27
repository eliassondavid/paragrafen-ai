from __future__ import annotations

from guard.guard_pipeline import GuardPipeline
from rag.chroma_pool import ChromaClientPool
from rag.models import RAGHit, RAGResult
from rag.rag_query import RAGQueryEngine


def test_blocked_query_returns_empty(monkeypatch) -> None:
    monkeypatch.setenv("RAG_DRY_RUN", "1")
    engine = RAGQueryEngine()

    result = engine.query("straffrätt mord", module="allman")

    assert result.hits == []
    assert result.disclaimer


def test_rank_prefers_binding() -> None:
    engine = RAGQueryEngine.__new__(RAGQueryEngine)
    engine.guard = GuardPipeline()
    module_config = {"authority_boost": {
        "binding": 2.0, "persuasive": 0.8
    }}
    binding_hit = RAGHit(
        text="x", metadata={"authority_level": "binding"},
        distance=0.3, collection="sfs", weight=1.0
    )
    persuasive_hit = RAGHit(
        text="y", metadata={"authority_level": "persuasive"},
        distance=0.3, collection="doktrin", weight=1.0
    )

    ranked = engine._rank([persuasive_hit, binding_hit], module_config)

    assert ranked[0].metadata["authority_level"] == "binding"


def test_dry_run_returns_empty(monkeypatch) -> None:
    monkeypatch.setenv("RAG_DRY_RUN", "1")
    engine = RAGQueryEngine()

    result = engine.query("vad säger avtalslagen", module="allman")

    assert isinstance(result, RAGResult)
    assert result.hits == []


def test_build_pools_shares_default_pool_and_overrides(monkeypatch) -> None:
    engine = RAGQueryEngine.__new__(RAGQueryEngine)
    engine.config = {
        "chroma_paths": {
            "upphandling": "/tmp/external-upphandling-chroma",
        }
    }

    pools = engine._build_pools("data/index/chroma")

    assert pools["sfs"] is pools["prop"]
    assert pools["sfs"] is pools["foreskrift"]
    assert pools["upphandling"] is not pools["sfs"]
    assert isinstance(pools["sfs"], ChromaClientPool)
    assert pools["sfs"].base_path.as_posix().endswith("data/index/chroma")
    assert pools["upphandling"].base_path.as_posix() == "/tmp/external-upphandling-chroma"
