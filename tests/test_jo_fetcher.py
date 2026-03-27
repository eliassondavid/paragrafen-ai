from __future__ import annotations

import pytest

from ingest.jo_fetcher import (
    PauseExecution,
    extract_dnr_from_filename,
    normalize_dnr,
    parse_total_hits,
    validate_decision_payload,
)


def test_normalize_dnr_replaces_slash() -> None:
    assert normalize_dnr("6533/2025") == "6533-2025"


def test_parse_total_hits_from_status_label() -> None:
    assert parse_total_hits("Visar: 3586 träffar") == 3586


def test_extract_dnr_from_filename_standard() -> None:
    assert extract_dnr_from_filename("1956869_5825-2025.pdf") == ("5825-2025", [])


def test_extract_dnr_from_filename_with_suffix() -> None:
    assert extract_dnr_from_filename("1935062_4810-2024beslut.pdf") == ("4810-2024", [])


def test_extract_dnr_from_filename_with_short_dnr() -> None:
    assert extract_dnr_from_filename("1829798_59-2025.pdf") == ("59-2025", [])


def test_extract_dnr_from_filename_with_multi_dnr() -> None:
    assert extract_dnr_from_filename("1829915_7632-2023och944-2024beslutwebbversion.pdf") == (
        "7632-2023",
        ["944-2024"],
    )


def test_extract_dnr_from_filename_without_dnr() -> None:
    assert extract_dnr_from_filename("1637166_Webbversion.pdf") == (None, [])


def test_extract_dnr_from_filename_with_prefix_suffix_before_dnr() -> None:
    assert extract_dnr_from_filename("1826721_beslut9915-2024webbversion.pdf") == (
        "9915-2024",
        [],
    )


def test_validate_decision_payload_builds_metadata_shape() -> None:
    decision = validate_decision_payload(
        {
            "dnr": "6533/2025",
            "titel": "Testbeslut",
            "beslutsdatum": "2025-01-15",
            "pdf_url": "https://www.jo.se/app/uploads/resolve_pdfs/2024074_6533-2025.pdf",
        }
    )

    assert decision == {
        "dnr": "6533-2025",
        "titel": "Testbeslut",
        "beslutsdatum": "2025-01-15",
        "pdf_url": "https://www.jo.se/app/uploads/resolve_pdfs/2024074_6533-2025.pdf",
        "pdf_filename": "jo_6533-2025.pdf",
        "source_url": "https://www.jo.se/jo-beslut/sokresultat/",
        "myndighet": "JO",
        "document_type": "beslut",
    }


def test_validate_decision_payload_rejects_non_pdf_url() -> None:
    with pytest.raises(PauseExecution):
        validate_decision_payload(
            {
                "dnr": "6533-2025",
                "titel": "Testbeslut",
                "beslutsdatum": "2025-01-15",
                "pdf_url": "https://www.jo.se/besluten/test/",
            }
        )
