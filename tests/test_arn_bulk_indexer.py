from __future__ import annotations

import json

import pytest

from index.arn_bulk_indexer import (
    ArnBulkIndexer,
    ExistingCollectionError,
    dnr_to_namespace_prefix,
)


class FakeChunk:
    def __init__(self, text: str) -> None:
        self.chunk_text = text


class FakeChunker:
    def chunk_sections(self, sections):
        return [FakeChunk(sections[0].text)]


class FakeEmbedder:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def embed(self, texts: list[str]) -> list[list[float]]:
        self.calls.append(texts)
        return [[0.1, 0.2] for _ in texts]


class FakeCollection:
    def __init__(self, *, count_value: int = 0) -> None:
        self.count_value = count_value
        self.upsert_calls: list[dict] = []

    def count(self) -> int:
        return self.count_value

    def upsert(self, **kwargs) -> None:
        self.upsert_calls.append(kwargs)


class FakeClient:
    def __init__(self, collection: FakeCollection) -> None:
        self.collection = collection

    def get_or_create_collection(self, **kwargs):
        return self.collection


def test_dnr_to_namespace_prefix() -> None:
    assert dnr_to_namespace_prefix("2023-10393") == "arn::2023_10393"


def test_prepare_document_builds_expected_chunk_metadata(tmp_path) -> None:
    indexer = ArnBulkIndexer(
        input_dir=tmp_path,
        chroma_path=tmp_path / "chroma",
        chunker=FakeChunker(),
        embedder=FakeEmbedder(),
        client_factory=lambda path: FakeClient(FakeCollection()),
    )

    prepared = indexer.prepare_document(
        {
            "dnr": "2023-10393",
            "title": "Ärendereferat 2023-10393",
            "source_format": "pdf",
            "text_content": "Det här är en ARN-text som ska chunkas.",
        }
    )

    assert len(prepared) == 1
    chunk = prepared[0]
    assert chunk.chunk_id == "arn::2023_10393_chunk_000"
    assert chunk.metadata["namespace"] == "arn::2023_10393_chunk_000"
    assert chunk.metadata["source_type"] == "namnder"
    assert chunk.metadata["document_subtype"] == "arn"
    assert chunk.metadata["authority_level"] == "persuasive"
    assert chunk.metadata["section_type"] == "other"
    assert chunk.metadata["license"] == "public_domain"


def test_run_aborts_when_collection_already_contains_chunks(tmp_path) -> None:
    collection = FakeCollection(count_value=5)
    indexer = ArnBulkIndexer(
        input_dir=tmp_path,
        chroma_path=tmp_path / "chroma",
        chunker=FakeChunker(),
        embedder=FakeEmbedder(),
        client_factory=lambda path: FakeClient(collection),
    )

    with pytest.raises(ExistingCollectionError) as exc_info:
        indexer.run(dry_run=True)

    assert exc_info.value.count == 5


def test_run_dry_run_embeds_without_upsert(tmp_path) -> None:
    input_dir = tmp_path / "json"
    input_dir.mkdir()
    json_path = input_dir / "arn_2023_10393.json"
    json_path.write_text(
        json.dumps(
            {
                "dnr": "2023-10393",
                "title": "Ärendereferat 2023-10393",
                "source_format": "pdf",
                "text_content": "Det här är en ARN-text som ska embedas i dry-run.",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    collection = FakeCollection()
    embedder = FakeEmbedder()
    indexer = ArnBulkIndexer(
        input_dir=input_dir,
        chroma_path=tmp_path / "chroma",
        chunker=FakeChunker(),
        embedder=embedder,
        client_factory=lambda path: FakeClient(collection),
    )

    stats = indexer.run(dry_run=True)

    assert stats["json_files_read"] == 1
    assert stats["documents_indexed"] == 1
    assert stats["skipped"] == 0
    assert stats["total_chunks"] == 1
    assert embedder.calls == [["Det här är en ARN-text som ska embedas i dry-run."]]
    assert collection.upsert_calls == []
