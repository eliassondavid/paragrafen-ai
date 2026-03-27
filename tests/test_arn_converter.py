from __future__ import annotations

import json

from ingest.arn_converter import (
    ArnConverter,
    build_document_payload,
    collect_source_files,
    extract_dnr,
)


def test_extract_dnr_examples() -> None:
    assert extract_dnr("0008-Ärendereferat 2023-10393.pdf") == "2023-10393"
    assert extract_dnr("001 - Ärendereferat - 2010-8943.pdf") == "2010-8943"
    assert extract_dnr("utan-dnr.pdf") is None


def test_collect_source_files_filters_supported_suffixes(tmp_path) -> None:
    (tmp_path / "one.pdf").write_text("x", encoding="utf-8")
    (tmp_path / "two.doc").write_text("x", encoding="utf-8")
    (tmp_path / "three.wpd").write_text("x", encoding="utf-8")
    (tmp_path / ".DS_Store").write_text("x", encoding="utf-8")
    (tmp_path / ".~lock.test.wpd#").write_text("x", encoding="utf-8")

    names = [path.name for path in collect_source_files(tmp_path)]
    assert names == ["one.pdf", "three.wpd", "two.doc"]


def test_process_file_skips_existing_json_and_logs_reason(tmp_path) -> None:
    input_dir = tmp_path / "raw"
    output_dir = tmp_path / "json"
    error_log = output_dir / "_conversion_errors.jsonl"
    input_dir.mkdir()
    output_dir.mkdir()

    source_path = input_dir / "Ärendereferat - 2023-10393.pdf"
    source_path.write_text("ignored", encoding="utf-8")
    (output_dir / "arn_2023_10393.json").write_text("{}", encoding="utf-8")

    converter = ArnConverter(
        input_dir=input_dir,
        output_dir=output_dir,
        error_log_path=error_log,
    )

    result = converter.process_file(source_path)

    assert result.status == "skipped"
    assert result.reason == "already_exists"
    logged = [json.loads(line) for line in error_log.read_text(encoding="utf-8").splitlines()]
    assert logged == [
        {
            "source_file": "Ärendereferat - 2023-10393.pdf",
            "reason": "already_exists",
            "timestamp": logged[0]["timestamp"],
        }
    ]


def test_build_document_payload_matches_schema() -> None:
    payload = build_document_payload(
        dnr="2023-10393",
        source_file="0008-Ärendereferat 2023-10393.pdf",
        source_format="pdf",
        text_content="  Hej världen  ",
        extraction_method="pdftotext",
        fetched_at="2026-03-21T12:00:00+00:00",
    )

    assert payload["dok_id"] == "arn_2023_10393"
    assert payload["title"] == "Ärendereferat 2023-10393"
    assert payload["text_content"] == "Hej världen"
    assert payload["text_length"] == len("Hej världen")
    assert payload["license"] == "public_domain"
