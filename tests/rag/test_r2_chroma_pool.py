from __future__ import annotations

from pathlib import Path

import pytest

from rag.chroma_pool import ChromaClientPool, CollectionNotFoundError
import rag.chroma_pool as chroma_pool_module


class FakeCollection:
    def __init__(self, name: str) -> None:
        self.name = name


class FakePersistentClient:
    def __init__(self, path: str) -> None:
        self.path = path
        self._collections: dict[str, FakeCollection] = {}

    def get_collection(self, name: str) -> FakeCollection:
        if name not in self._collections:
            self._collections[name] = FakeCollection(name)
        return self._collections[name]


def _mkdir(base: Path, *names: str) -> None:
    for name in names:
        (base / name).mkdir(parents=True, exist_ok=True)


def test_pool_evicts_on_max(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _mkdir(tmp_path, "prop", "sou", "bet")
    monkeypatch.setattr(chroma_pool_module.chromadb, "PersistentClient", FakePersistentClient)
    monkeypatch.setattr(ChromaClientPool, "MAX_OPEN", 2)

    current_time = {"value": 10.0}
    monkeypatch.setattr(chroma_pool_module.time, "time", lambda: current_time["value"])

    pool = ChromaClientPool(chroma_base_path=str(tmp_path))

    pool.get_collection("prop")
    current_time["value"] = 20.0
    pool.get_collection("sou")
    current_time["value"] = 30.0
    pool.get_collection("bet")

    assert set(pool._clients) == {"sou", "bet"}


def test_pool_refreshes_timestamp(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _mkdir(tmp_path, "prop")
    monkeypatch.setattr(chroma_pool_module.chromadb, "PersistentClient", FakePersistentClient)

    current_time = {"value": 100.0}
    monkeypatch.setattr(chroma_pool_module.time, "time", lambda: current_time["value"])

    pool = ChromaClientPool(chroma_base_path=str(tmp_path))

    first_collection = pool.get_collection("prop")
    first_timestamp = pool._clients["prop"][1]

    current_time["value"] = 150.0
    second_collection = pool.get_collection("prop")
    second_timestamp = pool._clients["prop"][1]

    assert first_collection is second_collection
    assert second_timestamp > first_timestamp


def test_collection_not_found_raises(tmp_path: Path) -> None:
    pool = ChromaClientPool(chroma_base_path=str(tmp_path))

    with pytest.raises(CollectionNotFoundError):
        pool.get_collection("prop")
