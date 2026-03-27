from __future__ import annotations

import json
from pathlib import Path

from index.doktrin_indexer import DoktrinIndexer


class FakeVectorStore:
    def __init__(self, *, existing_namespace: str | None = None) -> None:
        self.existing_namespace = existing_namespace
        self.metadata_queries: list[tuple[str | None, dict]] = []
        self.add_calls: list[dict] = []

    def get_one_metadata(self, instance_key: str | None = None, where_filter: dict | None = None, *, collection_name: str | None = None) -> dict | None:
        self.metadata_queries.append((instance_key or collection_name, where_filter or {}))
        namespace = (where_filter or {}).get("namespace")
        if namespace and namespace == self.existing_namespace:
            return {"namespace": namespace}
        return None

    def add_chunks(
        self,
        instance_key: str | None = None,
        chunks: list[str] | None = None,
        embeddings: list[list[float]] | None = None,
        metadatas: list[dict] | None = None,
        *,
        collection_name: str | None = None,
    ) -> int:
        chunks = chunks or []
        embeddings = embeddings or []
        metadatas = metadatas or []
        self.add_calls.append(
            {
                "instance_key": instance_key or collection_name,
                "chunks": chunks,
                "embeddings": embeddings,
                "metadatas": metadatas,
            }
        )
        return len(chunks)


class FakeEmbedder:
    def embed(self, texts: list[str]) -> list[list[float]]:
        return [[float(index)] for index, _ in enumerate(texts, start=1)]


def _write_norm_doc(tmp_path: Path, *, filename: str = "2003 - Akademisk avhandling - Engelbrekt - Fair trading law in flux - 1 uppl.json") -> Path:
    norm_dir = tmp_path / "norm"
    norm_dir.mkdir(parents=True, exist_ok=True)
    doc = {
        "filename": filename,
        "source_type": "doktrin",
        "source_subtype": "monografi_digital",
        "title": "Fair trading law in flux?",
        "author": "Antonina Bakardjieva Engelbrekt",
        "author_last": "engelbrekt",
        "authors": [{"first": "Antonina Bakardjieva", "last": "Engelbrekt", "role": "author"}],
        "is_edited_volume": False,
        "year": 2003,
        "edition": 1,
        "authority_level": "persuasive",
        "legal_area": ["eu_rätt", "civilrätt", "marknadsrätt"],
        "excluded_at_retrieval": False,
        "citation_hd": "Antonina Bakardjieva Engelbrekt, Fair trading law in flux?, 2003",
        "citation_academic": "Engelbrekt, Antonina Bakardjieva, Fair trading law in flux?, Stockholms universitet, 2003",
        "work_type": "Akademisk avhandling",
        "publisher": "Stockholms universitet",
        "source_url": "https://juridikbok.se/book/9172657510",
        "urn": "urn:nbn:se:juridikbokse-f45d2",
        "license": "CC BY-NC 4.0",
        "license_url": "https://creativecommons.org/licenses/by-nc/4.0/",
        "chunks": [
            {
                "id": "doktrin::engelbrekt_2003_s005_chunk_000",
                "source_type": "doktrin",
                "source_subtype": "monografi_digital",
                "text": "Chunktext",
                "title": "Fair trading law in flux?",
                "author": "Antonina Bakardjieva Engelbrekt",
                "author_last": "engelbrekt",
                "authors": json.dumps([{"first": "Antonina Bakardjieva", "last": "Engelbrekt", "role": "author"}], ensure_ascii=False),
                "is_edited_volume": False,
                "year": 2003,
                "edition": 1,
                "authority_level": "persuasive",
                "legal_area": json.dumps(["eu_rätt", "civilrätt", "marknadsrätt"], ensure_ascii=False),
                "citation_hd": "Antonina Bakardjieva Engelbrekt, Fair trading law in flux?, 2003",
                "citation_academic": "Engelbrekt, Antonina Bakardjieva, Fair trading law in flux?, Stockholms universitet, 2003",
                "chunk_index": 0,
                "page_start": 5,
                "page_end": 6,
                "pinpoint": "s. 5–6",
                "references_to": "[]",
                "avg_quality": 1.0,
                "extraction_method": "native",
                "filename": filename,
                "source_url": "https://juridikbok.se/book/9172657510",
                "urn": "urn:nbn:se:juridikbokse-f45d2",
                "license": "CC BY-NC 4.0",
                "license_url": "https://creativecommons.org/licenses/by-nc/4.0/",
            }
        ],
    }
    (norm_dir / filename).write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    return norm_dir


def test_doktrin_indexer_serializes_list_fields_and_uses_doktrin_instance(tmp_path: Path) -> None:
    vector_store = FakeVectorStore()
    indexer = DoktrinIndexer(
        input_dir=_write_norm_doc(tmp_path),
        vector_store=vector_store,
        embedder=FakeEmbedder(),
    )

    summary = indexer.index_all()

    assert summary.chunks_indexed == 1
    assert vector_store.add_calls[0]["instance_key"] == "doktrin"
    metadata = vector_store.add_calls[0]["metadatas"][0]
    assert metadata["legal_area"] == json.dumps(["eu_rätt", "civilrätt", "marknadsrätt"], ensure_ascii=False)
    assert metadata["references_to"] == json.dumps([], ensure_ascii=False)
    assert metadata["chunk_id"] == "doktrin::engelbrekt_2003_s005_chunk_000"


def test_doktrin_indexer_dry_run_does_not_write_to_chroma(tmp_path: Path) -> None:
    vector_store = FakeVectorStore()
    indexer = DoktrinIndexer(
        input_dir=_write_norm_doc(tmp_path),
        vector_store=vector_store,
        embedder=FakeEmbedder(),
    )

    summary = indexer.index_all(dry_run=True)

    assert summary.documents_indexed == 1
    assert summary.chunks_indexed == 1
    assert vector_store.add_calls == []


def test_doktrin_indexer_skips_existing_document(tmp_path: Path) -> None:
    existing_namespace = "doktrin::engelbrekt_2003_s005_chunk_000"
    vector_store = FakeVectorStore(existing_namespace=existing_namespace)
    indexer = DoktrinIndexer(
        input_dir=_write_norm_doc(tmp_path),
        vector_store=vector_store,
        embedder=FakeEmbedder(),
    )

    summary = indexer.index_all()

    assert summary.documents_skipped == 1
    assert summary.chunks_indexed == 0
    assert vector_store.add_calls == []


def test_doktrin_indexer_skips_empty_text_chunks(tmp_path: Path) -> None:
    norm_dir = _write_norm_doc(tmp_path)
    doc_path = norm_dir / "2003 - Akademisk avhandling - Engelbrekt - Fair trading law in flux - 1 uppl.json"
    payload = json.loads(doc_path.read_text(encoding="utf-8"))
    payload["chunks"][0]["text"] = ""
    doc_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    indexer = DoktrinIndexer(
        input_dir=norm_dir,
        vector_store=FakeVectorStore(),
        embedder=FakeEmbedder(),
    )

    summary = indexer.index_all()

    assert summary.chunks_skipped == 1
    assert summary.documents_skipped == 1
