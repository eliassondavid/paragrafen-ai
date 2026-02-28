"""Tests for F-6b SFS parser and indexer primitives."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from index.sfs_indexer import SfsIndexer
from normalize.sfs_parser import SfsParser


def _raw_doc(html: str, html_available: bool = True) -> dict[str, object]:
    return {
        "sfs_nr": "1949:381",
        "dok_id": "X123",
        "titel": "Föräldrabalk (1949:381)",
        "datum": "1949-06-10",
        "ikraftträdandedatum": "1950-01-01",
        "consolidation_source": "rk",
        "source_url": "https://example.org/sfs/1949-381",
        "html_content": html,
        "html_available": html_available,
        "fetched_at": "2026-02-28T12:00:00+00:00",
    }


def test_paragraf_strategy_creates_expected_chunks() -> None:
    parser = SfsParser()
    html = """
    <html><body>
      <h2>1 kap. Om faderskapet</h2>
      <p>1 § Första paragrafen.</p>
      <p>2 § Andra paragrafen.</p>
    </body></html>
    """

    parsed = parser.parse(_raw_doc(html))
    assert parsed is not None
    assert len(parsed["chunks"]) == 2


def test_paragraf_number_extraction_numeric() -> None:
    parser = SfsParser()
    html = "<html><body><p>3 § Detta är tredje paragrafen.</p></body></html>"

    parsed = parser.parse(_raw_doc(html))
    assert parsed is not None
    assert parsed["chunks"][0]["paragraf_nr"] == "3"


def test_paragraf_number_extraction_alphanumeric() -> None:
    parser = SfsParser()
    html = "<html><body><p>1a § Detta är en tillagd paragraf.</p></body></html>"

    parsed = parser.parse(_raw_doc(html))
    assert parsed is not None
    assert parsed["chunks"][0]["paragraf_nr"] == "1a"


def test_chapter_identification() -> None:
    parser = SfsParser()
    html = """
    <html><body>
      <p>2 kap. Adoption</p>
      <p>7 § Adoption får ske under vissa villkor.</p>
    </body></html>
    """

    parsed = parser.parse(_raw_doc(html))
    assert parsed is not None
    chunk = parsed["chunks"][0]
    assert chunk["kapitel_nr"] == "2"
    assert chunk["kapitel_titel"] == "Adoption"


def test_output_schema_contains_expected_fields() -> None:
    parser = SfsParser()
    html = "<html><body><p>1 § Testtext.</p></body></html>"

    parsed = parser.parse(_raw_doc(html))
    assert parsed is not None

    expected_doc_keys = {
        "sfs_nr",
        "titel",
        "ikraftträdandedatum",
        "consolidation_source",
        "source_url",
        "chunks",
    }
    assert expected_doc_keys.issubset(parsed.keys())

    expected_chunk_keys = {
        "paragraf_nr",
        "kapitel_nr",
        "kapitel_titel",
        "text",
        "legal_area",
        "chunk_index",
        "chunk_total",
        "ikraftträdandedatum",
    }
    first_chunk = parsed["chunks"][0]
    assert expected_chunk_keys.issubset(first_chunk.keys())
    assert first_chunk["legal_area"] == []


def test_html_unavailable_returns_none() -> None:
    parser = SfsParser()
    html = "<html><body><p>1 § Ska inte parseas.</p></body></html>"

    assert parser.parse(_raw_doc(html, html_available=False)) is None


def test_law_without_chapters_sets_null_chapter_fields() -> None:
    parser = SfsParser()
    html = """
    <html><body>
      <p>1 § Inledande bestämmelse.</p>
      <p>2 § Fortsatt bestämmelse.</p>
    </body></html>
    """

    parsed = parser.parse(_raw_doc(html))
    assert parsed is not None
    assert parsed["chunks"][0]["kapitel_nr"] is None
    assert parsed["chunks"][1]["kapitel_nr"] is None


def test_transition_heading_is_used_as_kapitel_titel() -> None:
    parser = SfsParser()
    html = """
    <html><body>
      <h2>Övergångsbestämmelser</h2>
      <p>1 § Denna lag träder i kraft den 1 juli.</p>
      <p>2 § Äldre bestämmelser gäller fortfarande.</p>
    </body></html>
    """

    parsed = parser.parse(_raw_doc(html))
    assert parsed is not None
    assert parsed["chunks"][0]["kapitel_titel"] == "Övergångsbestämmelser"
    assert parsed["chunks"][1]["kapitel_titel"] == "Övergångsbestämmelser"


def test_source_id_is_deterministic_for_same_sfs_number() -> None:
    first = SfsIndexer.build_source_id("1949:381")
    second = SfsIndexer.build_source_id("1949:381")
    third = SfsIndexer.build_source_id("1970:994")

    assert first == second
    assert first != third
