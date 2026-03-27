from __future__ import annotations

import json
from pathlib import Path

from normalize import doktrin_normalizer


def _metadata(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "source_url": "https://juridikbok.se/book/9172657510",
        "title": "Fair trading law in flux?",
        "author": "Antonina Bakardjieva Engelbrekt",
        "author_last": "Engelbrekt",
        "year": 2003,
        "edition": 1,
        "work_type": "Akademisk avhandling",
        "isbn": "9172657510",
        "urn": "urn:nbn:se:juridikbokse-f45d2",
        "publisher": "Stockholms universitet",
        "series": "",
        "subjects": ["Europarätt", "Marknadsrätt"],
        "hd_citation": "Antonina Bakardjieva Engelbrekt, Fair trading law in flux?, 2003",
        "filename": "2003 - Akademisk avhandling - Engelbrekt - Fair trading law in flux - 1 uppl.pdf",
        "downloaded": True,
    }
    base.update(overrides)
    return base


def _extracted(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "title": "Fair trading law in flux?",
        "author": "Antonina Bakardjieva Engelbrekt",
        "year": 2003,
        "work_type": "Akademisk avhandling",
        "subjects": ["Europarätt", "Marknadsrätt"],
        "filename": "2003 - Akademisk avhandling - Engelbrekt - Fair trading law in flux - 1 uppl.pdf",
        "total_pages": 6,
        "extraction_stats": {"native_pages": 2, "ocr_pages": 0, "avg_quality": 1.0},
        "pages": [
            {
                "page_num": 1,
                "text": "Creative Commons juridikbok.se",
                "char_count": 35,
                "word_count": 3,
                "method": "native",
                "quality_score": 1.0,
            },
            {
                "page_num": 5,
                "text": " ".join(["alpha"] * 260),
                "char_count": 1400,
                "word_count": 260,
                "method": "native",
                "quality_score": 1.0,
            },
            {
                "page_num": 6,
                "text": " ".join(["beta"] * 260),
                "char_count": 1200,
                "word_count": 260,
                "method": "pdftotext",
                "quality_score": 0.95,
            },
        ],
    }
    base.update(overrides)
    return base


def test_normalize_one_builds_expected_namespace_and_metadata() -> None:
    result = doktrin_normalizer.normalize_one(_metadata(), _extracted())

    assert result is not None
    assert result["output_basename"] == "2003 - Akademisk avhandling - Engelbrekt - Fair trading law in flux - 1 uppl"
    assert result["source_subtype"] == "monografi_digital"
    assert result["chunk_count"] == 1

    chunk = result["chunks"][0]
    assert chunk["id"] == "doktrin::engelbrekt_2003_s005_chunk_000"
    assert chunk["pinpoint"] == "s. 5–6"
    assert chunk["citation_hd"] == "Antonina Bakardjieva Engelbrekt, Fair trading law in flux?, 2003"
    assert chunk["extraction_method"] == "native"
    assert "isbn" in chunk
    assert json.loads(chunk["authors"]) == [
        {"first": "Antonina Bakardjieva", "last": "Engelbrekt", "role": "author"}
    ]
    assert json.loads(chunk["legal_area"]) == ["eu_rätt", "civilrätt", "marknadsrätt"]


def test_normalize_all_names_output_file_from_metadata_filename(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw" / "doktrin"
    extracted_dir = raw_dir / "extracted_text"
    norm_dir = tmp_path / "norm" / "doktrin"
    extracted_dir.mkdir(parents=True)

    metadata = [_metadata()]
    (raw_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False), encoding="utf-8")
    (extracted_dir / "2003 - Akademisk avhandling - Engelbrekt - Fair trading law in flux - 1 uppl.json").write_text(
        json.dumps(_extracted(), ensure_ascii=False),
        encoding="utf-8",
    )

    counts = doktrin_normalizer.normalize_all(raw_dir=raw_dir, norm_dir=norm_dir, force=True)

    assert counts == {"ok": 1, "skipped": 0, "failed": 0}
    expected = norm_dir / "2003 - Akademisk avhandling - Engelbrekt - Fair trading law in flux - 1 uppl.json"
    assert expected.exists()


def test_normalize_all_adds_urn_suffix_on_filename_collision(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw" / "doktrin"
    extracted_dir = raw_dir / "extracted_text"
    norm_dir = tmp_path / "norm" / "doktrin"
    extracted_dir.mkdir(parents=True)

    metadata = [
        _metadata(urn="urn:nbn:se:juridikbokse-f45d2"),
        _metadata(urn="urn:nbn:se:juridikbokse-f9999"),
    ]
    (raw_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False), encoding="utf-8")
    (extracted_dir / "2003 - Akademisk avhandling - Engelbrekt - Fair trading law in flux - 1 uppl.json").write_text(
        json.dumps(_extracted(), ensure_ascii=False),
        encoding="utf-8",
    )

    counts = doktrin_normalizer.normalize_all(raw_dir=raw_dir, norm_dir=norm_dir, force=True)

    assert counts == {"ok": 2, "skipped": 0, "failed": 0}
    assert (norm_dir / "2003 - Akademisk avhandling - Engelbrekt - Fair trading law in flux - 1 uppl_f45d2.json").exists()
    assert (norm_dir / "2003 - Akademisk avhandling - Engelbrekt - Fair trading law in flux - 1 uppl_f9999.json").exists()


def test_normalize_one_flags_excluded_subjects_and_omits_missing_isbn() -> None:
    metadata = _metadata(
        title="Om farebegreppet i straffrätten",
        author="Erik Sjödin (red.)",
        author_last="Sjödin",
        subjects=["Straffrätt"],
        isbn="",
        filename="1967 - Monografi - Sjödin - Om farebegreppet i straffrätten - 1 uppl.pdf",
    )
    extracted = _extracted(
        title="Om farebegreppet i straffrätten",
        author="Erik Sjödin (red.)",
        subjects=["Straffrätt"],
        filename="1967 - Monografi - Sjödin - Om farebegreppet i straffrätten - 1 uppl.pdf",
        extraction_stats={"native_pages": 0, "ocr_pages": 2, "avg_quality": 0.9},
        pages=[
            {
                "page_num": 10,
                "text": " ".join(["gamma"] * 200),
                "char_count": 1000,
                "word_count": 200,
                "method": "ocr",
                "quality_score": 0.9,
            }
        ],
    )

    result = doktrin_normalizer.normalize_one(metadata, extracted)

    assert result is not None
    assert result["source_subtype"] == "monografi_ocr"
    assert result["excluded_at_retrieval"] is True
    chunk = result["chunks"][0]
    assert chunk["excluded_at_retrieval"] is True
    assert chunk["extraction_method"] == "ocr"
    assert "isbn" not in chunk
    assert json.loads(chunk["authors"]) == [{"first": "Erik", "last": "Sjödin", "role": "editor"}]


def test_normalize_one_falls_back_to_publisher_then_okand() -> None:
    result = doktrin_normalizer.normalize_one(
        _metadata(author="", author_last="", publisher="Juridiska föreningen i Lund", hd_citation=""),
        _extracted(author="", hd_citation=""),
    )

    assert result is not None
    assert result["author"] == "Juridiska föreningen i Lund"
    assert result["citation_hd"] == "Juridiska föreningen i Lund, Fair trading law in flux?, 2003"

    result_okand = doktrin_normalizer.normalize_one(
        _metadata(author="", author_last="", publisher="", hd_citation=""),
        _extracted(author="", hd_citation=""),
    )

    assert result_okand is not None
    assert result_okand["author"] == "okand"


def test_parse_authors_supports_multiple_authors() -> None:
    authors, is_edited_volume = doktrin_normalizer.parse_authors(
        "Jennie Johansson, Pia Kjellbom och Susanne Kelfve"
    )

    assert is_edited_volume is False
    assert authors == [
        {"first": "Jennie", "last": "Johansson", "role": "author"},
        {"first": "Pia", "last": "Kjellbom", "role": "author"},
        {"first": "Susanne", "last": "Kelfve", "role": "author"},
    ]
