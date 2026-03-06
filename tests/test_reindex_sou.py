from __future__ import annotations

import copy

from scripts import reindex_sou


class FakeCollection:
    def __init__(self, results_a, results_b):
        self._results = [results_a, results_b]
        self.get_calls = []
        self.update_calls = []
        self.delete_calls = []
        self.upsert_calls = []

    def get(self, **kwargs):
        self.get_calls.append(kwargs)
        return self._results[len(self.get_calls) - 1]

    def update(self, **kwargs):
        self.update_calls.append(kwargs)

    def delete(self, **kwargs):
        self.delete_calls.append(kwargs)

    def upsert(self, **kwargs):
        self.upsert_calls.append(kwargs)


def make_result(*records):
    return {
        "ids": [record["id"] for record in records],
        "metadatas": [copy.deepcopy(record["metadata"]) for record in records],
        "documents": [record.get("document") for record in records],
        "embeddings": [record.get("embedding") for record in records],
    }


def test_main_dry_run_smoke(monkeypatch):
    metadata_only = {
        "source_type": "forarbete",
        "forarbete_type": "sou",
        "authority_level": "persuasive",
        "forarbete_rank": None,
        "citation": "SOU 2015:14",
        "chunk_index": 7,
        "legal_area": "[\"arbetsratt\"]",
        "references_to": "[]",
    }
    collection = FakeCollection(
        make_result(
            {
                "id": "forarbete::sou_2015_14_chunk_007",
                "metadata": metadata_only,
                "document": "text",
                "embedding": [0.1, 0.2],
            }
        ),
        make_result(),
    )

    monkeypatch.setattr(reindex_sou, "connect_collection", lambda: collection)
    monkeypatch.setattr(reindex_sou, "load_forarbete_rank", lambda: {"sou": 3})

    exit_code = reindex_sou.main(["--dry-run", "--limit", "10"])

    assert exit_code == 0
    assert len(collection.get_calls) == 2
    assert collection.get_calls[0]["where"] == {"forarbete_type": {"$eq": "sou"}}
    assert collection.get_calls[1]["where"] == {"authority_level": {"$eq": "persuasive"}}
    assert collection.update_calls == []
    assert collection.delete_calls == []
    assert collection.upsert_calls == []


def test_build_corrected_metadata_fixes_required_fields():
    original = {
        "source_type": "forarbete",
        "forarbete_type": "betankande",
        "authority_level": "persuasive",
        "forarbete_rank": "3",
        "citation": "SOU 2015:14",
        "chunk_index": 42,
        "legal_area": "[\"arbetsratt\"]",
        "references_to": "[]",
        "title": "Original title",
    }

    corrected = reindex_sou.build_corrected_metadata(original, sou_rank=3)

    assert corrected["authority_level"] == "preparatory"
    assert corrected["forarbete_rank"] == 3
    assert corrected["forarbete_type"] == "sou"
    assert corrected["title"] == "Original title"
    assert corrected["legal_area"] == "[\"arbetsratt\"]"


def test_build_expected_chunk_id_for_multipart_sou():
    metadata = {
        "source_type": "forarbete",
        "forarbete_type": "sou",
        "authority_level": "preparatory",
        "forarbete_rank": 3,
        "citation": "SOU 2003:33",
        "chunk_index": 42,
        "del": 2,
        "legal_area": "[\"arbetsratt\"]",
        "references_to": "[]",
    }

    corrected_id = reindex_sou.build_expected_chunk_id(
        "forarbete::sou_2003_33_chunk_000",
        metadata,
    )

    assert corrected_id == "forarbete::sou_2003_33_d2_chunk_042"


def test_process_chunks_repairs_namespace_with_delete_and_upsert():
    metadata = {
        "source_type": "forarbete",
        "forarbete_type": "sou",
        "authority_level": "persuasive",
        "forarbete_rank": None,
        "citation": "SOU 2003:33",
        "chunk_index": 42,
        "del": 2,
        "legal_area": "[\"arbetsratt\"]",
        "references_to": "[]",
    }
    collection = FakeCollection(make_result(), make_result())
    record = reindex_sou.ChunkRecord(
        chunk_id="forarbete::sou_2003_33_chunk_000",
        metadata=metadata,
        document="chunk body",
        embedding=[0.1, 0.2],
    )

    metadata_updates, namespace_repairs, failures = reindex_sou.process_chunks(
        collection=collection,
        records=[record],
        sou_rank=3,
        dry_run=False,
    )

    assert metadata_updates == 0
    assert namespace_repairs == 1
    assert failures == 0
    # Ett enda delete-anrop med gamla ID:t
    assert collection.delete_calls == [{"ids": ["forarbete::sou_2003_33_chunk_000"]}]
    assert collection.upsert_calls == [
        {
            "ids": ["forarbete::sou_2003_33_d2_chunk_042"],
            "metadatas": [
                {
                    "source_type": "forarbete",
                    "forarbete_type": "sou",
                    "authority_level": "preparatory",
                    "forarbete_rank": 3,
                    "citation": "SOU 2003:33",
                    "chunk_index": 42,
                    "del": 2,
                    "legal_area": "[\"arbetsratt\"]",
                    "references_to": "[]",
                }
            ],
            "documents": ["chunk body"],
            "embeddings": [[0.1, 0.2]],
        }
    ]
