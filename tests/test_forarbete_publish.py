import json
from pathlib import Path

import pytest

from publish.forarbete_publish import (
    PublishAbortError,
    PublishPartialFailureError,
    canonicalize_sou_number,
    extract_sou_number,
    generate_source_id,
    publish_forarbete,
)


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _write_schema(path: Path) -> None:
    schema = {
        "type": "object",
        "required": [
            "source_id",
            "source_type",
            "document_subtype",
            "sou_number",
            "title",
            "year",
            "authority_level",
            "published_at",
            "publish_version",
            "norm_sha256",
        ],
        "properties": {
            "source_id": {"type": "string"},
            "source_type": {"type": "string", "enum": ["forarbete"]},
            "document_subtype": {"type": "string", "enum": ["sou"]},
            "sou_number": {"type": "string"},
            "title": {"type": "string"},
            "year": {"type": "integer"},
            "authority_level": {"type": "string", "enum": ["preparatory"]},
            "published_at": {"type": "string"},
            "publish_version": {"type": "string", "enum": ["1.0"]},
            "norm_sha256": {"type": "string"},
        },
    }
    _write_json(path, schema)


def _write_config(path: Path, norm_dir: Path, published_dir: Path, schema_path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "forarbete_publish:",
                f'  norm_dir: "{norm_dir}"',
                f'  published_dir: "{published_dir}"',
                f'  schema_path: "{schema_path}"',
                '  idempotency_strategy: "sha256"',
                '  log_level: "noop"',
                "  batch_size: 100",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _setup(tmp_path: Path) -> tuple[Path, Path, Path]:
    norm_dir = tmp_path / "data" / "norm" / "forarbete" / "sou"
    published_dir = tmp_path / "data" / "published" / "forarbete" / "sou"
    schema_path = tmp_path / "schemas" / "chunk_metadata_schema.json"
    config_path = tmp_path / "config" / "pipeline_config.yaml"

    _write_schema(schema_path)
    _write_config(config_path, norm_dir, published_dir, schema_path)
    return config_path, norm_dir, published_dir


def _minimal_doc(
    sou_number: str = "SOU 2023:45",
    title: str = "En testtitel",
    front_page_text: str = "Första raden\nAndra raden",
) -> dict:
    return {
        "metadata": {"sou_number": sou_number, "title": title},
        "front_page_text": front_page_text,
        "body": [{"type": "paragraph", "text": "Innehåll"}],
    }


def test_sou_number_normalization():
    """10+ varianter -> kanonisk form."""
    filename_cases = [
        ("sou_2023_45.json", "SOU 2023:45"),
        ("SOU_2023_045.json", "SOU 2023:45"),
        ("SOU-2023-45.json", "SOU 2023:45"),
        ("sou 2023:45a.json", "SOU 2023:45a"),
    ]
    for filename, expected in filename_cases:
        assert extract_sou_number({"metadata": {}}, filename) == expected

    metadata_cases = [
        ("sou_2023_45", "SOU 2023:45"),
        ("SOU 2023:045", "SOU 2023:45"),
        ("SOU-2023-0045", "SOU 2023:45"),
        ("SOU 2023:45a", "SOU 2023:45a"),
        ("sou 2023 45", "SOU 2023:45"),
        ("SOU 2023:45 Bilaga A", "SOU 2023:45 Bilaga A"),
        ("2023:45", "SOU 2023:45"),
    ]
    for raw, expected in metadata_cases:
        assert canonicalize_sou_number(raw) == expected


def test_source_id_determinism():
    """Samma SOU-nummer -> alltid samma UUID."""
    sou_number = "SOU 2023:45"
    assert generate_source_id(sou_number) == generate_source_id(sou_number)


def test_source_id_uniqueness():
    """Olika SOU-nummer -> olika UUID."""
    assert generate_source_id("SOU 2023:45") != generate_source_id("SOU 2023:46")


def test_title_extraction_priority(tmp_path: Path):
    """metadata["title"] > metadata["titel"] > front_page_text > fallback."""
    config_path, norm_dir, published_dir = _setup(tmp_path)

    d1 = _minimal_doc(title="Primär titel")
    d1["metadata"]["titel"] = "Sekundär titel"
    _write_json(norm_dir / "sou_2023_45.json", d1)

    d2 = _minimal_doc(title="")
    d2["metadata"].pop("title", None)
    d2["metadata"]["titel"] = "Titel på svenska"
    _write_json(norm_dir / "sou_2023_46.json", d2)

    d3 = _minimal_doc(title="")
    d3["metadata"] = {"sou_number": "SOU 2023:47"}
    d3["front_page_text"] = "Framsidans rad ett\nrad två"
    _write_json(norm_dir / "sou_2023_47.json", d3)

    d4 = _minimal_doc(title="")
    d4["metadata"] = {"sou_number": "SOU 2023:48"}
    d4["front_page_text"] = ""
    _write_json(norm_dir / "sou_2023_48.json", d4)

    publish_forarbete(config_path=str(config_path))

    out1 = json.loads((published_dir / "sou_2023_45.json").read_text(encoding="utf-8"))
    out2 = json.loads((published_dir / "sou_2023_46.json").read_text(encoding="utf-8"))
    out3 = json.loads((published_dir / "sou_2023_47.json").read_text(encoding="utf-8"))
    out4 = json.loads((published_dir / "sou_2023_48.json").read_text(encoding="utf-8"))

    assert out1["front_matter"]["title"] == "Primär titel"
    assert out2["front_matter"]["title"] == "Titel på svenska"
    assert out3["front_matter"]["title"] == "Framsidans rad ett"
    assert out4["front_matter"]["title"] == "[Titel saknas — SOU 2023:48]"


def test_idempotency_no_change(tmp_path: Path):
    """Oförändrad norm-fil -> published skrivs ej om."""
    config_path, norm_dir, _ = _setup(tmp_path)
    _write_json(norm_dir / "sou_2023_45.json", _minimal_doc())

    first = publish_forarbete(config_path=str(config_path))
    second = publish_forarbete(config_path=str(config_path))

    assert first["created"] == 1
    assert second["skipped"] == 1
    assert second["updated"] == 0


def test_idempotency_on_change(tmp_path: Path):
    """Modifierad norm-fil -> published skrivs om."""
    config_path, norm_dir, _ = _setup(tmp_path)
    path = norm_dir / "sou_2023_45.json"

    _write_json(path, _minimal_doc(title="Version 1"))
    first = publish_forarbete(config_path=str(config_path))
    assert first["created"] == 1

    _write_json(path, _minimal_doc(title="Version 2"))
    second = publish_forarbete(config_path=str(config_path))
    assert second["updated"] == 1


def test_partial_run_recovery(tmp_path: Path):
    """Simulera avbruten körning -> omstart ger korrekt resultat."""
    config_path, norm_dir, published_dir = _setup(tmp_path)
    _write_json(norm_dir / "sou_2023_45.json", _minimal_doc(sou_number="SOU 2023:45"))
    (norm_dir / "sou_2023_46.json").write_text("{invalid json", encoding="utf-8")

    with pytest.raises(PublishPartialFailureError) as exc:
        publish_forarbete(config_path=str(config_path))
    assert exc.value.results["failed"] == 1
    assert (published_dir / "sou_2023_45.json").exists()

    _write_json(norm_dir / "sou_2023_46.json", _minimal_doc(sou_number="SOU 2023:46"))
    results = publish_forarbete(config_path=str(config_path))
    assert results["created"] == 1
    assert results["skipped"] == 1


def test_corrupt_file_graceful(tmp_path: Path):
    """Korrupt JSON -> failure loggad, körning fortsätter."""
    config_path, norm_dir, published_dir = _setup(tmp_path)
    _write_json(norm_dir / "sou_2023_45.json", _minimal_doc())
    (norm_dir / "sou_2023_46.json").write_text("{", encoding="utf-8")

    with pytest.raises(PublishPartialFailureError) as exc:
        publish_forarbete(config_path=str(config_path))

    assert exc.value.results["failed"] == 1
    assert (published_dir / "sou_2023_45.json").exists()


def test_unconventional_sou_number(tmp_path: Path):
    """SOU 2023:45a, SOU 2023:45 Bilaga A -> ingen krasch."""
    config_path, norm_dir, published_dir = _setup(tmp_path)
    _write_json(norm_dir / "sou_2023_45a.json", _minimal_doc(sou_number="SOU 2023:45a"))
    _write_json(
        norm_dir / "sou_2023_45_bilaga_a.json",
        _minimal_doc(sou_number="SOU 2023:45 Bilaga A"),
    )

    results = publish_forarbete(config_path=str(config_path))

    assert results["created"] == 2
    out = json.loads((published_dir / "sou_2023_45_bilaga_a.json").read_text(encoding="utf-8"))
    assert out["front_matter"]["sou_number"] == "SOU 2023:45 Bilaga A"


def test_missing_title_fallback(tmp_path: Path):
    """Ingen titel -> fallback-sträng med SOU-nummer."""
    config_path, norm_dir, published_dir = _setup(tmp_path)
    doc = _minimal_doc(title="")
    doc["metadata"] = {"sou_number": "SOU 2023:77"}
    doc["front_page_text"] = ""
    _write_json(norm_dir / "sou_2023_77.json", doc)

    publish_forarbete(config_path=str(config_path))

    out = json.loads((published_dir / "sou_2023_77.json").read_text(encoding="utf-8"))
    assert out["front_matter"]["title"] == "[Titel saknas — SOU 2023:77]"


def test_dry_run_no_writes(tmp_path: Path):
    """--dry-run -> ingen fil skapas i published_dir."""
    config_path, norm_dir, published_dir = _setup(tmp_path)
    _write_json(norm_dir / "sou_2023_45.json", _minimal_doc())

    results = publish_forarbete(config_path=str(config_path), dry_run=True)

    assert results["created"] == 1
    assert not published_dir.exists()


def test_abort_on_excessive_failures(tmp_path: Path):
    """>100 failures -> PublishAbortError."""
    config_path, norm_dir, _ = _setup(tmp_path)
    norm_dir.mkdir(parents=True, exist_ok=True)
    for idx in range(101):
        (norm_dir / f"sou_2023_{idx:03d}.json").write_text("{invalid", encoding="utf-8")

    with pytest.raises(PublishAbortError):
        publish_forarbete(config_path=str(config_path))


def test_published_dir_created_if_missing(tmp_path: Path):
    """published_dir saknas -> skapas automatiskt."""
    config_path, norm_dir, published_dir = _setup(tmp_path)
    _write_json(norm_dir / "sou_2023_45.json", _minimal_doc())

    assert not published_dir.exists()
    publish_forarbete(config_path=str(config_path))
    assert published_dir.exists()

