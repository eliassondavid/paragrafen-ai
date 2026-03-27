from __future__ import annotations

import json

from index.jk_bulk_indexer import JKBulkIndexer, authority_level_for_document, build_chunk_id


class FakeChunk:
    def __init__(self, text: str, section_path: str = "bedomning") -> None:
        self.chunk_text = text
        self.section_path = section_path
        self.section_title = "Justitiekanslerns bedömning"
        self.token_count = 180


class FakeChunker:
    def chunk_sections(self, sections):
        return [FakeChunk(sections[0].text, section_path=sections[0].section_key)]


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


def test_build_chunk_id_uses_required_namespace_format() -> None:
    assert build_chunk_id("2025/7175", 0) == "jk::2025_7175_chunk_000"


def test_authority_level_varies_by_category() -> None:
    assert authority_level_for_document({"kategori": "Skadeståndsärenden"}) == "binding"
    assert authority_level_for_document({"kategori": "Tillsynsärenden"}) == "guiding"


def test_prepare_document_builds_expected_metadata(tmp_path) -> None:
    indexer = JKBulkIndexer(
        input_dir=tmp_path,
        chroma_path=tmp_path / "chroma",
        chunker=FakeChunker(),
        embedder=FakeEmbedder(),
        client_factory=lambda path: FakeClient(FakeCollection()),
    )

    prepared = indexer.prepare_document(
        {
            "dnr": "2025/7175",
            "titel": "JK-beslut",
            "kategori": "Skadeståndsärenden",
            "beslutsdatum": "2026-03-04",
            "source_url": "https://www.jk.se/beslut-och-yttranden/2026/03/20257175/",
            "text_content": " ".join(f"ord{i}" for i in range(220)),
        }
    )

    assert len(prepared) == 1
    chunk = prepared[0]
    assert chunk.chunk_id == "jk::2025_7175_chunk_000"
    assert chunk.metadata["namespace"] == "jk::2025_7175_chunk_000"
    assert chunk.metadata["source_type"] == "myndighetsbeslut"
    assert chunk.metadata["document_subtype"] == "jk"
    assert chunk.metadata["authority_level"] == "binding"
    assert chunk.metadata["section"] == "other"
    assert chunk.metadata["license"] == "public_domain"


def test_run_dry_run_chunks_without_chroma_writes(tmp_path) -> None:
    input_dir = tmp_path / "decisions"
    input_dir.mkdir()
    (input_dir / "jk_2025_7175.json").write_text(
        json.dumps(
            {
                "dnr": "2025/7175",
                "titel": "JK-beslut",
                "kategori": "Tillsynsärenden",
                "beslutsdatum": "2026-03-04",
                "source_url": "https://www.jk.se/beslut-och-yttranden/2026/03/20257175/",
                "text_content": " ".join(f"ord{i}" for i in range(220)),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    collection = FakeCollection()
    embedder = FakeEmbedder()
    indexer = JKBulkIndexer(
        input_dir=input_dir,
        chroma_path=tmp_path / "chroma",
        chunker=FakeChunker(),
        embedder=embedder,
        client_factory=lambda path: FakeClient(collection),
    )

    stats = indexer.run(dry_run=True)

    assert stats["json_files_read"] == 1
    assert stats["documents_indexed"] == 1
    assert stats["failed"] == 0
    assert stats["total_chunks"] == 1
    assert embedder.calls == []
    assert collection.upsert_calls == []
