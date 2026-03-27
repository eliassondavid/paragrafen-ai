from __future__ import annotations

from pathlib import Path

import yaml

from normalize import prop_normalizer


def _rank_config(tmp_path: Path) -> Path:
    path = tmp_path / "forarbete_rank.yaml"
    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump({"forarbete_types": {"proposition": {"rank": 2}}}, fh, sort_keys=False)
    return path


def test_prop_normalizer_chunking_respects_600_token_limit() -> None:
    long_text = "\n\n".join([" ".join(["ord"] * 250), " ".join(["ord"] * 250), " ".join(["ord"] * 250)])

    chunks = prop_normalizer._chunk_section_text(long_text)

    assert chunks
    assert all(len(chunk.split()) <= 600 for chunk in chunks)


def test_prop_normalizer_populates_required_metadata_without_none(tmp_path: Path) -> None:
    raw = {
        "beteckning": "Prop. 2016/17:180",
        "dok_id": "HC03180",
        "rm": "2016/17",
        "nummer": 180,
        "titel": "En titel",
        "datum": "2017-03-16",
        "organ": "Justitiedepartementet",
        "source_url": "https://data.riksdagen.se/dokument/HC03180",
        "pdf_url": "https://data.riksdagen.se/dokument/HC03180.pdf",
        "fetched_at": "2026-03-06T14:00:00Z",
        "html_available": True,
        "html_content": """
            <div id="page_1">Bakgrund Detta är bakgrund.</div>
            <div id="page_2">Skälen för regeringens förslag Detta hänvisar till 1986:223.</div>
        """,
    }

    result = prop_normalizer.normalize_one(raw, rank_config_path=_rank_config(tmp_path))

    assert result is not None
    assert result["authority_level"] == "preparatory"
    assert result["forarbete_rank"] == 2
    assert all(value is not None for key, value in result.items() if key not in {"chunks", "part"})
    assert result["part"] is None
    assert all(all(chunk.get(field) is not None for field in chunk) for chunk in result["chunks"])


def test_extract_sfs_references() -> None:
    refs = prop_normalizer.extract_sfs_references("Se 1986:223 och 1986:223 samt 2024:451.")
    assert refs == ["sfs::1986:223", "sfs::2024:451"]


def test_build_pinpoint() -> None:
    assert prop_normalizer.build_pinpoint(45, 47) == "s. 45–47"
    assert prop_normalizer.build_pinpoint(45, 45) == "s. 45"
    assert prop_normalizer.build_pinpoint(0, 0) == ""


def test_build_citation() -> None:
    assert prop_normalizer.build_citation("2016/17", 180, "s. 45–47") == "Prop. 2016/17:180 s. 45–47"
    assert prop_normalizer.build_citation("2016/17", 180, "") == "Prop. 2016/17:180"


def test_normalize_prop_preserves_part_value(tmp_path: Path) -> None:
    raw = {
        "beteckning": "Prop. 2010/11:165",
        "dok_id": "GY03165d2",
        "rm": "2010/11",
        "nummer": 165,
        "titel": "Del 2",
        "datum": "2011-06-01",
        "organ": "Justitiedepartementet",
        "source_url": "https://data.riksdagen.se/dokument/GY03165d2",
        "pdf_url": "https://data.riksdagen.se/dokument/GY03165d2.pdf",
        "fetched_at": "2026-03-06T14:00:00Z",
        "html_available": True,
        "html_content": "<div id=\"page_1\">Bakgrund Del 2-text.</div>",
    }

    result = prop_normalizer.normalize_prop(raw, part=2, rank_config_path=_rank_config(tmp_path))

    assert result is not None
    assert result["part"] == 2
