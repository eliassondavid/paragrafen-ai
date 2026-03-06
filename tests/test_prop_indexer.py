from __future__ import annotations

import json
from pathlib import Path

import yaml

from index.prop_indexer import PropIndexer, build_prop_namespace


class FakeVectorStore:
    def __init__(self) -> None:
        self.metadata_queries: list[tuple[str, dict]] = []
        self.add_calls: list[dict] = []

    def get_one_metadata(self, collection_name: str, where_filter: dict) -> dict | None:
        self.metadata_queries.append((collection_name, where_filter))
        return None

    def add_chunks(self, collection_name: str, chunks: list[str], embeddings: list[list[float]], metadatas: list[dict]) -> int:
        self.add_calls.append(
            {
                "collection_name": collection_name,
                "chunks": chunks,
                "embeddings": embeddings,
                "metadatas": metadatas,
            }
        )
        return len(chunks)


class FakeEmbedder:
    def embed(self, texts: list[str]) -> list[list[float]]:
        return [[float(index)] for index, _ in enumerate(texts, start=1)]


def _rank_config(tmp_path: Path) -> Path:
    path = tmp_path / "forarbete_rank.yaml"
    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump({"forarbete_types": {"proposition": {"rank": 2}}}, fh, sort_keys=False)
    return path


def _write_norm_doc(tmp_path: Path, filename: str = "prop_2016-17_180.json") -> Path:
    norm_dir = tmp_path / "norm"
    norm_dir.mkdir(parents=True, exist_ok=True)
    doc = {
        "beteckning": "Prop. 2016/17:180",
        "dok_id": "HC03180",
        "rm": "2016/17",
        "nummer": 180,
        "titel": "Titel",
        "datum": "2017-03-16",
        "organ": "Justitiedepartementet",
        "source_url": "https://data.riksdagen.se/dokument/HC03180",
        "pdf_url": "https://data.riksdagen.se/dokument/HC03180.pdf",
        "authority_level": "preparatory",
        "forarbete_rank": 2,
        "legal_area": ["civilrätt"],
        "references_to": ["sfs::1986:223"],
        "fetched_at": "2026-03-06T14:00:00Z",
        "chunks": [
            {
                "chunk_index": 0,
                "section": "rationale",
                "section_title": "Skälen för regeringens förslag",
                "text": "Chunktext",
                "page_start": 45,
                "page_end": 47,
                "pinpoint": "s. 45–47",
                "citation": "Prop. 2016/17:180 s. 45–47",
            }
        ],
    }
    path = norm_dir / filename
    path.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    return norm_dir


def test_build_prop_namespace() -> None:
    assert build_prop_namespace("2016/17", 180, 42) == "forarbete::prop_2016-17_180_chunk_042"


def test_build_prop_namespace_for_multipart_proposition() -> None:
    assert build_prop_namespace("2016/17", 180, 7, part=2) == "forarbete::prop_2016-17_180_d2_chunk_007"


def test_prop_indexer_serializes_list_fields_with_json_dumps(tmp_path: Path) -> None:
    indexer = PropIndexer(
        input_dir=_write_norm_doc(tmp_path),
        forarbete_rank_path=_rank_config(tmp_path),
        vector_store=FakeVectorStore(),
        embedder=FakeEmbedder(),
    )
    summary = indexer.index_all()

    assert summary.chunks_indexed == 1
    metadata = indexer.vector_store.add_calls[0]["metadatas"][0]
    assert metadata["legal_area"] == json.dumps(["civilrätt"], ensure_ascii=False)
    assert metadata["references_to"] == json.dumps(["sfs::1986:223"], ensure_ascii=False)


def test_prop_indexer_skips_empty_text_chunks(tmp_path: Path) -> None:
    norm_dir = _write_norm_doc(tmp_path)
    doc_path = norm_dir / "prop_2016-17_180.json"
    payload = json.loads(doc_path.read_text(encoding="utf-8"))
    payload["chunks"][0]["text"] = ""
    doc_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    indexer = PropIndexer(
        input_dir=norm_dir,
        forarbete_rank_path=_rank_config(tmp_path),
        vector_store=FakeVectorStore(),
        embedder=FakeEmbedder(),
    )
    summary = indexer.index_all()

    assert summary.chunks_skipped == 1
    assert indexer.vector_store.add_calls == []


def test_prop_indexer_dry_run_does_not_write_to_chroma(tmp_path: Path) -> None:
    vector_store = FakeVectorStore()
    indexer = PropIndexer(
        input_dir=_write_norm_doc(tmp_path),
        forarbete_rank_path=_rank_config(tmp_path),
        vector_store=vector_store,
        embedder=FakeEmbedder(),
    )

    summary = indexer.index_all(dry_run=True)

    assert summary.chunks_indexed == 1
    assert vector_store.add_calls == []


def test_prop_indexer_uses_preparatory_authority_and_rank_2(tmp_path: Path) -> None:
    indexer = PropIndexer(
        input_dir=_write_norm_doc(tmp_path),
        forarbete_rank_path=_rank_config(tmp_path),
        vector_store=FakeVectorStore(),
        embedder=FakeEmbedder(),
    )

    indexer.index_all()
    metadata = indexer.vector_store.add_calls[0]["metadatas"][0]

    assert metadata["authority_level"] == "preparatory"
    assert metadata["forarbete_rank"] == 2


def test_prop_indexer_keeps_pdf_url_in_metadata(tmp_path: Path) -> None:
    indexer = PropIndexer(
        input_dir=_write_norm_doc(tmp_path),
        forarbete_rank_path=_rank_config(tmp_path),
        vector_store=FakeVectorStore(),
        embedder=FakeEmbedder(),
    )

    indexer.index_all()
    metadata = indexer.vector_store.add_calls[0]["metadatas"][0]

    assert metadata["pdf_url"] == "https://data.riksdagen.se/dokument/HC03180.pdf"
