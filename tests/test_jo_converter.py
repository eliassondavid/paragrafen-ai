from __future__ import annotations

import logging
from pathlib import Path

import pytest

from ingest.jo_converter import (
    build_document_payload,
    count_bedomning_documents,
    dnr_to_dok_id,
    dnr_to_output_filename,
    extract_dnr_from_pdf_path,
    log_empty_text_escalation,
    split_into_sections,
)


def test_split_into_sections_identifies_standard_jo_sections() -> None:
    text = (
        "Försättsblad\n"
        "Anmälan\nBakgrundstext.\n"
        "Utredning\nUtredningstext.\n"
        "JO:s bedömning\nBedömningstext.\n"
        "Beslut\nBeslutstext.\n"
    )

    sections = split_into_sections(text)

    assert [section["section"] for section in sections] == [
        "other",
        "bakgrund",
        "utredning",
        "bedomning",
        "atgard",
    ]
    assert sections[3]["section_title"] == "JO:s bedömning"


def test_split_into_sections_falls_back_to_other() -> None:
    sections = split_into_sections("Bara löptext utan rubriker.")

    assert sections == [
        {
            "section": "other",
            "section_title": "Övrigt",
            "text": "Bara löptext utan rubriker.",
        }
    ]


def test_dnr_helpers_build_expected_names() -> None:
    assert extract_dnr_from_pdf_path(Path("jo_6533-2025.pdf")) == "6533-2025"
    assert dnr_to_dok_id("6533/2025") == "jo_6533_2025"
    assert dnr_to_output_filename("6533-2025") == "jo_6533_2025.json"


def test_build_document_payload_reads_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    import ingest.jo_converter as jo_converter

    monkeypatch.setattr(
        jo_converter,
        "load_metadata",
        lambda dnr: {
            "titel": "JO-beslut",
            "beslutsdatum": "2025-01-15",
            "pdf_url": "https://www.jo.se/app/uploads/resolve_pdfs/2024074_6533-2025.pdf",
            "source_url": "https://www.jo.se/jo-beslut/sokresultat/",
        },
    )

    payload = build_document_payload(
        Path("jo_6533-2025.pdf"),
        text="Textinnehåll",
        sections=[{"section": "other", "section_title": "Övrigt", "text": "Textinnehåll"}],
    )

    assert payload["dok_id"] == "jo_6533_2025"
    assert payload["title"] == "JO-beslut"
    assert payload["beslutsdatum"] == "2025-01-15"
    assert payload["extraction_method"] == "pdftotext"


def test_count_bedomning_documents() -> None:
    results = [
        {"sections": [{"section": "other"}]},
        {"sections": [{"section": "bedomning"}]},
        {"sections": [{"section": "utredning"}, {"section": "bedomning"}]},
    ]

    assert count_bedomning_documents(results) == 2


def test_log_empty_text_escalation(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.ERROR):
        log_empty_text_escalation(empty_count=3, processed_count=10)

    assert "ESKALERA" in caplog.text
