from __future__ import annotations

from bs4 import BeautifulSoup

from ingest.jk_fetcher import (
    DecisionListing,
    build_catalog_entry,
    extract_beslutsdatum,
    extract_dnr,
    normalize_dnr,
    parse_search_results,
)


def test_normalize_dnr_for_filename() -> None:
    assert normalize_dnr("2025/7175") == "2025_7175"
    assert normalize_dnr("1023-19-3.1.1") == "1023_19_3_1_1"


def test_extract_dnr_and_date_from_metadata_line() -> None:
    line = "Diarienr: 2025/7175 / Beslutsdatum: 04 mar 2026"
    assert extract_dnr(line) == "2025/7175"
    assert extract_beslutsdatum(line) == "2026-03-04"


def test_parse_search_results_extracts_listing_shape() -> None:
    soup = BeautifulSoup(
        """
        <div class="ruling-results">
          <div class="results">
            <div class="date">Diarienr: 2025/7175 <span>/</span> Beslutsdatum: 04 mar 2026</div>
            <h2><a href="/beslut-och-yttranden/2026/03/20257175/">Titel A</a></h2>
            <br />
          </div>
        </div>
        """,
        "html.parser",
    )

    listings = parse_search_results(soup, "Skadeståndsärenden")

    assert listings == [
        DecisionListing(
            dnr="2025/7175",
            titel="Titel A",
            beslutsdatum="2026-03-04",
            kategori="Skadeståndsärenden",
            source_url="https://www.jk.se/beslut-och-yttranden/2026/03/20257175/",
        )
    ]


def test_build_catalog_entry_uses_relative_file_path(tmp_path) -> None:
    file_path = tmp_path / "jk_2025_7175.json"
    payload = {
        "dok_id": "jk_2025_7175",
        "dnr": "2025/7175",
        "titel": "Titel A",
        "beslutsdatum": "2026-03-04",
        "kategori": "Skadeståndsärenden",
        "authority_level": "binding",
        "source_url": "https://www.jk.se/beslut-och-yttranden/2026/03/20257175/",
        "fetched_at": "2026-03-22T00:00:00+00:00",
    }

    entry = build_catalog_entry(payload, file_path)

    assert entry["dok_id"] == "jk_2025_7175"
    assert entry["authority_level"] == "binding"
    assert entry["source_url"].endswith("/20257175/")
