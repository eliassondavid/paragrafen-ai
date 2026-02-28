"""Unit tests for F-5b forarbete parser and legal area normalizer."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from normalize.forarbete_parser import ForarbeteParser
from normalize.legal_area_normalizer import LegalAreaNormalizer


def _words(prefix: str, count: int) -> str:
    return " ".join(f"{prefix}{idx}" for idx in range(count))


def _raw_doc(html: str, *, html_available: bool = True) -> dict:
    return {
        "beteckning": "SOU 2017:14",
        "dok_id": "H9B3sou14",
        "titel": "Bättre skydd mot diskriminering",
        "datum": "2017-02-14",
        "organ": "Arbetsmarknadsdepartementet",
        "source_url": "https://data.riksdagen.se/dokument/H9B3sou14",
        "html_content": html,
        "html_available": html_available,
    }


def test_header_strategy_extracts_sections() -> None:
    parser = ForarbeteParser()
    html = f"""
    <html><body>
      <h2>1. Inledning</h2>
      <p>{_words("intro", 80)}</p>
      <h2>2. Förslag</h2>
      <p>{_words("forslag", 80)}</p>
    </body></html>
    """
    parsed = parser.parse(_raw_doc(html))
    assert parsed is not None

    section_titles = {chunk["section_title"] for chunk in parsed["chunks"]}
    assert "1. Inledning" in section_titles
    assert "2. Förslag" in section_titles


def test_paragraph_fallback_creates_sections_from_p_tags() -> None:
    parser = ForarbeteParser()
    html = f"""
    <html><body>
      <p>{_words("paraA", 80)}</p>
      <p>{_words("paraB", 80)}</p>
    </body></html>
    """
    parsed = parser.parse(_raw_doc(html))
    assert parsed is not None
    assert len(parsed["chunks"]) == 2
    assert all(chunk["section_title"].endswith("...") for chunk in parsed["chunks"])


def test_chunking_splits_over_800_tokens_with_overlap() -> None:
    parser = ForarbeteParser()
    p1 = "P1MARK " + _words("p1", 250)
    p2 = "P2MARK " + _words("p2", 250)
    p3 = "P3MARK " + _words("p3", 250)
    p4 = "P4MARK " + _words("p4", 250)
    html = f"""
    <html><body>
      <h2>3. Överväganden</h2>
      <p>{p1}</p>
      <p>{p2}</p>
      <p>{p3}</p>
      <p>{p4}</p>
    </body></html>
    """
    parsed = parser.parse(_raw_doc(html))
    assert parsed is not None

    chunks = parsed["chunks"]
    assert len(chunks) >= 3
    assert "P2MARK" in chunks[0]["text"]
    assert "P2MARK" in chunks[1]["text"]


def test_page_number_extraction_from_comment() -> None:
    parser = ForarbeteParser()
    html = f"""
    <html><body>
      <!-- Page 45 -->
      <h2>4. Konsekvenser</h2>
      <p>{_words("konsekvens", 80)}</p>
    </body></html>
    """
    parsed = parser.parse(_raw_doc(html))
    assert parsed is not None
    assert parsed["chunks"][0]["pinpoint"] == "s. 45"


def test_output_schema_matches_spec() -> None:
    parser = ForarbeteParser()
    html = f"<html><body><h2>5. Titel</h2><p>{_words('schema', 80)}</p></body></html>"
    parsed = parser.parse(_raw_doc(html))
    assert parsed is not None

    assert set(parsed.keys()) == {"beteckning", "title", "year", "department", "source_url", "chunks"}
    assert parsed["year"] == 2017
    assert parsed["chunks"]
    for index, chunk in enumerate(parsed["chunks"]):
        assert set(chunk.keys()) == {"text", "section_title", "pinpoint", "legal_area", "chunk_index", "chunk_total"}
        assert chunk["chunk_index"] == index
        assert chunk["chunk_total"] == len(parsed["chunks"])
        assert chunk["legal_area"] == []


def test_html_available_false_returns_none() -> None:
    parser = ForarbeteParser()
    html = "<html><body><p>irrelevant</p></body></html>"
    assert parser.parse(_raw_doc(html, html_available=False)) is None


def test_text_cleaning_removes_nav_and_script_content() -> None:
    parser = ForarbeteParser()
    html = f"""
    <html><body>
      <div class="nav">SKA_INTE_SYNAS</div>
      <script>window.BAD = true;</script>
      <h2>6. Rensning</h2>
      <p>{_words("clean", 80)}</p>
    </body></html>
    """
    parsed = parser.parse(_raw_doc(html))
    assert parsed is not None
    combined_text = "\n".join(chunk["text"] for chunk in parsed["chunks"])
    assert "SKA_INTE_SYNAS" not in combined_text
    assert "window.BAD" not in combined_text


def test_legal_area_normalizer_keeps_unknown_and_maps_alias() -> None:
    normalizer = LegalAreaNormalizer()
    normalized = normalizer.normalize(["AvtL", "specialområde"])
    assert normalized[0] == "avtalsrätt"
    assert "specialområde" in normalized


def test_is_excluded_matches_config() -> None:
    normalizer = LegalAreaNormalizer()
    assert normalizer.is_excluded("straffrätt") is True
    assert normalizer.is_excluded("avtalsrätt") is False
