from __future__ import annotations

import json
from pathlib import Path

from index.praxis_indexer import index_directory


class FakeVectorStore:
    def __init__(self) -> None:
        self.add_calls: list[dict] = []
        self._count = 0

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
        resolved_instance = instance_key or collection_name
        self.add_calls.append(
            {
                "instance_key": resolved_instance,
                "chunks": chunks,
                "embeddings": embeddings,
                "metadatas": metadatas,
            }
        )
        self._count += len(chunks)
        return len(chunks)

    def get_collection_stats(self) -> dict[str, int]:
        return {"paragrafen_praxis_v1": self._count}


class FakeSentenceTransformer:
    def encode(
        self,
        texts: list[str],
        *,
        normalize_embeddings: bool,
        convert_to_numpy: bool,
        show_progress_bar: bool,
        batch_size: int,
    ) -> list[list[float]]:
        assert normalize_embeddings is True
        assert convert_to_numpy is True
        assert show_progress_bar is False
        assert batch_size == 32
        return [[float(index), 0.0] for index, _ in enumerate(texts, start=1)]


def _write_norm_doc(tmp_path: Path) -> Path:
    norm_dir = tmp_path / "praxis" / "HFD"
    norm_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "source_type": "praxis",
        "domstol": "HFD",
        "year": 2011,
        "ref_no": 1,
        "ref_no_padded": "001",
        "malnummer": "4033-09",
        "avgorandedatum": "2011-01-26",
        "ar_vagledande": True,
        "authority_level": "binding",
        "citation": "HFD 2011 ref. 1",
        "legal_area": ["forvaltningsratt"],
        "references_to": ["sfs::1999:1229"],
        "api_id": "3fa7430a-test",
        "harvest_source": "rattspraxis.etjanst.domstol.se",
        "chunks": [
            {
                "chunk_id": "praxis::HFD_2011_ref-001_chunk_003",
                "namespace": "praxis::HFD_2011_ref-001_chunk_003",
                "pinpoint": "domskal",
                "chunk_index": 3,
                "chunk_text": "Domskalen utvecklar proportionalitetsprincipen.",
            }
        ],
    }
    (norm_dir / "HFD_2011_ref_001.json").write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )
    return norm_dir


def test_index_directory_dry_run_counts_chunks_without_chroma_writes(tmp_path: Path) -> None:
    vector_store = FakeVectorStore()

    report = index_directory(
        _write_norm_doc(tmp_path),
        vector_store,
        dry_run=True,
    )

    assert report.total_files == 1
    assert report.indexed_files == 1
    assert report.total_chunks == 1
    assert report.indexed_chunks == 1
    assert vector_store.add_calls == []


def test_index_directory_live_uses_praxis_instance_and_explicit_embeddings(tmp_path: Path) -> None:
    vector_store = FakeVectorStore()

    report = index_directory(
        _write_norm_doc(tmp_path),
        vector_store,
        dry_run=False,
        embedding_model=FakeSentenceTransformer(),
        normalize_embeddings=True,
        embedding_batch_size=32,
    )

    assert report.collection_count_before == 0
    assert report.collection_count_after == 1
    assert report.indexed_chunks == 1
    assert len(vector_store.add_calls) == 1
    assert vector_store.add_calls[0]["instance_key"] == "praxis"
    assert vector_store.add_calls[0]["embeddings"] == [[1.0, 0.0]]
    metadata = vector_store.add_calls[0]["metadatas"][0]
    assert metadata["legal_area"] == json.dumps(["forvaltningsratt"], ensure_ascii=False)
    assert metadata["references_to"] == json.dumps(["sfs::1999:1229"], ensure_ascii=False)
