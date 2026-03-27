from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def load_module():
    import sys
    module_name = "scripts_ingest_nja_ii"
    if module_name in sys.modules:
        return sys.modules[module_name]
    script_path = Path("/Users/davideliasson/Projects/paragrafen-ai/scripts/ingest_nja_ii.py")
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Registrera i sys.modules innan exec_module så att @dataclass kan
    # slå upp annotationsnamnet "int | None" via sys.modules.get(cls.__module__).
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def ingest_module():
    return load_module()


@pytest.fixture
def sample_config() -> dict:
    return {
        "embedding_model": "KBLab/sentence-bert-swedish-cased",
        "max_tokens": 600,
        "lag_legal_area_fallback": {
            "rb": ["processrätt"],
            "upphl": ["immaterialrätt", "upphovsrätt"],
            "jb": ["fastighetsrätt"],
            "brb": ["straffrätt"],
            "ub": ["processrätt"],
            "diverse": ["civilrätt"],
        },
        "volym_lag_default": {
            1943: "rb",
            1961: "upphl",
            1962: "brb",
            1964: "brb",
            1966: "jb",
            1972: "jb",
            1973: "rb",
        },
        "volym_lag_fallback": {1968: {"primary": "jb", "secondary": "ub"}},
        "mixed_volume_rules": {
            1968: {
                "primary_keywords": ["hyreslag", "hyresreglering", "hyresnämnd"],
                "secondary_keywords": ["utmätning", "införsel", "utsökningslag"],
            }
        },
        "legal_areas_path": "/Users/davideliasson/Projects/paragrafen-ai/config/legal_areas.yaml",
    }


class DummyClassifier:
    def classify(self, text: str, lag: str) -> list[str]:
        if lag == "brb":
            return ["straffrätt"]
        if lag == "upphl":
            return ["immaterialrätt", "upphovsrätt"]
        return ["processrätt"]


def test_namespace_format(ingest_module):
    namespace = ingest_module.build_namespace("rb", 1943, 42, 1)
    assert namespace == "nja_ii::rb_1943_s042_chunk_001"


def test_no_forarbete_rank_in_metadata(ingest_module):
    chunk = ingest_module.make_chunk_metadata(
        lag="rb",
        volume_year=1943,
        page_number=42,
        text="Testtext",
        chunk_index=0,
        chunk_total=1,
        legal_area=["processrätt"],
        embedding_model="KBLab/sentence-bert-swedish-cased",
        citation_precision="exact",
        fetched_at="2026-03-07T12:00:00+00:00",
    )
    assert "forarbete_rank" not in chunk


def test_source_type_is_nja_ii(ingest_module):
    chunk = ingest_module.make_chunk_metadata(
        lag="rb",
        volume_year=1943,
        page_number=1,
        text="Text",
        chunk_index=0,
        chunk_total=1,
        legal_area=["processrätt"],
        embedding_model="KBLab/sentence-bert-swedish-cased",
        citation_precision="exact",
        fetched_at="2026-03-07T12:00:00+00:00",
    )
    assert chunk["source_type"] == "nja_ii"


def test_authority_level_is_persuasive(ingest_module):
    chunk = ingest_module.make_chunk_metadata(
        lag="rb",
        volume_year=1943,
        page_number=1,
        text="Text",
        chunk_index=0,
        chunk_total=1,
        legal_area=["processrätt"],
        embedding_model="KBLab/sentence-bert-swedish-cased",
        citation_precision="exact",
        fetched_at="2026-03-07T12:00:00+00:00",
    )
    assert chunk["authority_level"] == "persuasive"


def test_empty_text_skipped(ingest_module, sample_config, caplog):
    volume = ingest_module.VolumeDocument(
        file_path=Path("nja_ii_1943.md"),
        volume_year=1943,
        metadata={},
        pages=[ingest_module.PageBlock(page_number=1, text="", citation_precision="exact")],
    )
    chunks, summary = ingest_module.build_chunks_for_volume(
        volume,
        sample_config,
        DummyClassifier(),
    )
    assert chunks == []
    assert summary.blocks_skipped == 1
    assert "empty text" in caplog.text


def test_citation_source_format(ingest_module):
    assert ingest_module.build_citation_source(1943, 42) == "njaii_1943_s042"


def test_dry_run_does_not_write(ingest_module, monkeypatch, sample_config, tmp_path):
    data_dir = tmp_path / "data" / "curated" / "nja_ii"
    data_dir.mkdir(parents=True)
    (data_dir / "nja_ii_1943.md").write_text(
        """# NJA II 1943
<!-- volym_år: 1943 -->

## s. 42

Detta är ett testblock om rättegångsbalken.
""",
        encoding="utf-8",
    )
    config = dict(sample_config)
    config["data_dir"] = str(data_dir)

    monkeypatch.setattr(ingest_module, "load_config", lambda _: config)

    def fail_collection(_config):
        raise AssertionError("create_collection ska inte anropas i dry-run")

    monkeypatch.setattr(ingest_module, "create_collection", fail_collection)
    result = ingest_module.run_ingest(config_path="unused", dry_run=True)
    assert result["summary"].chunks_indexed == 0
    assert result["summary"].chunks_produced == 1


def test_upsert_idempotent(ingest_module, sample_config):
    class FakeCollection:
        def __init__(self):
            self.rows = {}

        def upsert(self, ids, embeddings, documents, metadatas):
            for chunk_id, embedding, document, metadata in zip(
                ids, embeddings, documents, metadatas, strict=False
            ):
                self.rows[chunk_id] = {
                    "embedding": embedding,
                    "document": document,
                    "metadata": metadata,
                }

    chunks = [
        ingest_module.make_chunk_metadata(
            lag="rb",
            volume_year=1943,
            page_number=1,
            text="Ett chunk",
            chunk_index=0,
            chunk_total=1,
            legal_area=["processrätt"],
            embedding_model=sample_config["embedding_model"],
            citation_precision="exact",
            fetched_at="2026-03-07T12:00:00+00:00",
        )
    ]
    embeddings = [[0.1, 0.2]]
    collection = FakeCollection()
    ingest_module.upsert_chunks(collection, chunks, embeddings, batch_size=100)
    ingest_module.upsert_chunks(collection, chunks, embeddings, batch_size=100)
    assert len(collection.rows) == 1


def test_brb_chunks_have_straffratt_legal_area(ingest_module, sample_config):
    volume = ingest_module.VolumeDocument(
        file_path=Path("nja_ii_1964.md"),
        volume_year=1964,
        metadata={},
        pages=[
            ingest_module.PageBlock(
                page_number=102,
                text="Brottsbalken och straff för brott behandlas här.",
                citation_precision="exact",
            )
        ],
    )
    chunks, _summary = ingest_module.build_chunks_for_volume(
        volume,
        sample_config,
        DummyClassifier(),
    )
    assert chunks
    assert "straffrätt" in chunks[0]["legal_area"]


def test_brb_legal_area_set_by_classifier(ingest_module, sample_config):
    # Verifierar att BrB-chunks får legal_area: ["straffrätt"] via klassificeraren.
    # area_blocker.py-integrationstestet tillhör qa/ — inte detta testsvit.
    # Det som testas här är att ingest-steget producerar rätt metadata för
    # att area_blocker.py ska kunna fatta korrekt beslut vid retrieval.
    volume = ingest_module.VolumeDocument(
        file_path=Path("nja_ii_1962.md"),
        volume_year=1962,
        metadata={},
        pages=[
            ingest_module.PageBlock(
                page_number=5,
                text="Brottsbalken reglerar brott och straff.",
                citation_precision="exact",
            )
        ],
    )
    chunks, _summary = ingest_module.build_chunks_for_volume(
        volume,
        sample_config,
        DummyClassifier(),
    )
    assert chunks
    assert chunks[0]["legal_area"] == ["straffrätt"]
    assert chunks[0]["lag"] == "brb"
