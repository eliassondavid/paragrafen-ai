from __future__ import annotations

import json

from index.jo_bulk_indexer import JOBulkIndexer, make_namespace


class FakeTokenizer:
    def encode(self, text: str, add_special_tokens: bool = False) -> list[int]:
        del add_special_tokens
        tokens = [token for token in text.split() if token]
        return list(range(len(tokens)))

    def decode(self, token_ids: list[int], skip_special_tokens: bool = True) -> str:
        del skip_special_tokens
        return " ".join(f"tok{token_id}" for token_id in token_ids)


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


def test_make_namespace() -> None:
    assert make_namespace("6533-2025", 0) == "jo::6533-2025_chunk_000"


def test_prepare_document_preserves_section_metadata(tmp_path) -> None:
    indexer = JOBulkIndexer(
        input_dir=tmp_path,
        chroma_path=tmp_path / "chroma",
        tokenizer=FakeTokenizer(),
        embedder=FakeEmbedder(),
        client_factory=lambda path: FakeClient(FakeCollection()),
    )

    prepared = indexer.prepare_document(
        {
            "dnr": "6533-2025",
            "title": "JO-beslut",
            "beslutsdatum": "2025-01-15",
            "source_url": "https://www.jo.se/jo-beslut/sokresultat/",
            "sections": [
                {
                    "section": "bedomning",
                    "section_title": "JO:s bedömning",
                    "text": " ".join(f"ord{i}" for i in range(160)),
                }
            ],
        }
    )

    assert len(prepared) == 1
    chunk = prepared[0]
    assert chunk.chunk_id == "jo::6533-2025_chunk_000"
    assert chunk.metadata["namespace"] == "jo::6533-2025_chunk_000"
    assert chunk.metadata["source_type"] == "myndighetsbeslut"
    assert chunk.metadata["document_subtype"] == "jo"
    assert chunk.metadata["authority_level"] == "guiding"
    assert chunk.metadata["section"] == "bedomning"
    assert chunk.metadata["section_title"] == "JO:s bedömning"
    assert chunk.metadata["schema_version"] == "v0.15"
    assert chunk.metadata["coverage_note"] == "publicerat urval från jo.se, ej komplett beslutsmassa"


def test_run_dry_run_embeds_without_upsert(tmp_path) -> None:
    input_dir = tmp_path / "json"
    input_dir.mkdir()
    (input_dir / "jo_6533_2025.json").write_text(
        json.dumps(
            {
                "dnr": "6533-2025",
                "title": "JO-beslut",
                "beslutsdatum": "2025-01-15",
                "source_url": "https://www.jo.se/jo-beslut/sokresultat/",
                "sections": [
                    {
                        "section": "other",
                        "section_title": "Övrigt",
                        "text": " ".join(f"ord{i}" for i in range(160)),
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    collection = FakeCollection()
    embedder = FakeEmbedder()
    indexer = JOBulkIndexer(
        input_dir=input_dir,
        chroma_path=tmp_path / "chroma",
        tokenizer=FakeTokenizer(),
        embedder=embedder,
        client_factory=lambda path: FakeClient(collection),
    )

    stats = indexer.run(dry_run=True)

    assert stats["json_files_read"] == 1
    assert stats["documents_indexed"] == 1
    assert stats["failed"] == 0
    assert stats["total_chunks"] == 1
    assert len(embedder.calls) == 1
    assert collection.upsert_calls == []


def test_run_upserts_in_batches_of_64(tmp_path) -> None:
    input_dir = tmp_path / "json"
    input_dir.mkdir()
    for index in range(65):
        (input_dir / f"jo_{index:04d}_2025.json").write_text(
            json.dumps(
                {
                    "dnr": f"{index:04d}-2025",
                    "title": f"JO {index}",
                    "beslutsdatum": "2025-01-15",
                    "source_url": "https://www.jo.se/jo-beslut/sokresultat/",
                    "sections": [
                        {
                            "section": "other",
                            "section_title": "Övrigt",
                            "text": " ".join(f"ord{i}" for i in range(160)),
                        }
                    ],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    collection = FakeCollection(count_value=10)
    indexer = JOBulkIndexer(
        input_dir=input_dir,
        chroma_path=tmp_path / "chroma",
        tokenizer=FakeTokenizer(),
        embedder=FakeEmbedder(),
        client_factory=lambda path: FakeClient(collection),
    )

    stats = indexer.run(dry_run=False)

    assert stats["json_files_read"] == 65
    assert stats["documents_indexed"] == 65
    assert stats["total_chunks"] == 65
    assert len(collection.upsert_calls) == 2
    assert len(collection.upsert_calls[0]["ids"]) == 64
    assert len(collection.upsert_calls[1]["ids"]) == 1
