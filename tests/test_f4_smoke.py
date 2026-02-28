"""Smoke test for F4 Chroma setup and retrieval."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any
import sys

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from index.vector_store import ChromaVectorStore


def _build_test_config(tmp_path: Path) -> tuple[Path, dict[str, Any]]:
    with Path("config/embedding_config.yaml").open("r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh) or {}

    config["chroma"]["persistent_path"] = str(tmp_path / "chroma")
    config_path = tmp_path / "embedding_config.yaml"
    with config_path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(config, fh, sort_keys=False, allow_unicode=True)

    return config_path, config


def _make_embedding(index: int, dim: int = 16) -> list[float]:
    vector = [0.0] * dim
    vector[index % dim] = 1.0
    return vector


def test_f4_smoke(tmp_path: Path) -> None:
    config_path, config = _build_test_config(tmp_path)
    vector_store = ChromaVectorStore(config_path=config_path)
    test_collection = "paragrafen_test_v1"

    try:
        # 1) Chroma instance and configured collections should exist.
        stats = vector_store.get_collection_stats()
        expected_collections = set(config["chroma"]["collections"].values())
        assert expected_collections.issubset(set(stats.keys()))

        # 2) Index 10 synthetic chunks with required schema.
        chunks: list[str] = []
        embeddings: list[list[float]] = []
        metadatas: list[dict[str, Any]] = []
        today = date.today().isoformat()

        for i in range(10):
            chunk_text = f"syntetisk juridisk testtext {i}"
            chunks.append(chunk_text)
            embeddings.append(_make_embedding(i))
            metadatas.append(
                {
                    "namespace": f"forarbete::SOU_2026_001_avsnitt_1_chunk_{i:03d}",
                    "source_id": "c5a4d9ba-2f7f-4387-8f00-b6693f14b300",
                    "source_type": "forarbete",
                    "document_type": "SOU",
                    "beteckning": "SOU 2026:1",
                    "title": "Syntetiskt testdokument",
                    "year": 2026,
                    "department": "",
                    "section_title": "1. Inledning",
                    "legal_area": ["avtalsrätt"],
                    "authority_level": "persuasive",
                    "pinpoint": f"s. {i + 1}",
                    "embedding_model": "KBLab/sentence-bert-swedish-cased",
                    "chunk_index": i,
                    "chunk_total": 10,
                    "source_url": "",
                    "indexed_at": today,
                }
            )

        added = vector_store.add_chunks(
            collection_name=test_collection,
            chunks=chunks,
            embeddings=embeddings,
            metadatas=metadatas,
        )
        assert added == 10

        # 3) Query and verify recall@10 >= 0.20.
        query_embedding = _make_embedding(3)
        documents, returned_metadatas, _distances = vector_store.query(
            collection_name=test_collection,
            query_embedding=query_embedding,
            n_results=10,
            where_filter=None,
        )
        assert len(documents) > 0
        recall_at_10 = 1.0 if "syntetisk juridisk testtext 3" in documents[:10] else 0.0
        assert recall_at_10 >= 0.20

        # 4) Verify source_type where-filter.
        filtered_documents, _, _ = vector_store.query(
            collection_name=test_collection,
            query_embedding=query_embedding,
            n_results=10,
            where_filter={"source_type": "forarbete"},
        )
        assert len(filtered_documents) > 0

        mismatched_documents, _, _ = vector_store.query(
            collection_name=test_collection,
            query_embedding=query_embedding,
            n_results=10,
            where_filter={"source_type": "praxis"},
        )
        assert len(mismatched_documents) == 0

        # 5) Returned metadata should include embedding_model.
        assert returned_metadatas
        assert all("embedding_model" in metadata for metadata in returned_metadatas)
    finally:
        vector_store.client.delete_collection(test_collection)
