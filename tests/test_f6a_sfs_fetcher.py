"""Unit tests for F-6a SFS fetcher with mocked HTTP calls."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock, patch

import requests
import yaml

from ingest.sfs_fetcher import IKRAFT_KEY, SfsFetcher


def _write_sources_config(tmp_path: Path) -> Path:
    config = {
        "riksdagen_api": {
            "base_url": "https://example.test",
            "sfs": {
                "list_endpoint": "/dokumentlista/",
                "doktyp": "SFS",
                "utformat": "json",
                "pagesize": 200,
                "document_html_endpoint": "/dokument/{dok_id}",
                "output_dir": str(tmp_path / "sfs"),
                "errors_file": str(tmp_path / "sfs_errors.jsonl"),
                "consolidation_source": "rk",
                "only_active": False,
            },
        },
        "rate_limiting": {
            "delay_between_requests_s": 0.0,
            "max_retries": 3,
            "retry_backoff_base_s": 0.0,
            "request_timeout_s": 30,
        },
        "http": {"user_agent": "test-agent", "accept_encoding": "gzip"},
        "progress": {"log_every_n_documents": 100},
    }
    config_path = tmp_path / "sources.yaml"
    with config_path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(config, fh, sort_keys=False)
    return config_path


def _response(
    *,
    json_data: dict | None = None,
    text: str = "",
    status_code: int = 200,
) -> Mock:
    response = Mock()
    response.text = text
    if status_code >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(f"HTTP {status_code}")
    else:
        response.raise_for_status.return_value = None
    response.json.return_value = json_data
    return response


def _sfs_list_payload(remaining: int, documents: list[dict] | dict) -> dict:
    return {
        "dokumentlista": {
            "@\u00e5terst\u00e5ende": str(remaining),
            "dokument": documents,
        }
    }


def test_sfs_fetcher_paginates_two_pages_and_stops_on_remaining_zero(tmp_path: Path) -> None:
    config_path = _write_sources_config(tmp_path)

    page_1 = _response(
        json_data=_sfs_list_payload(
            1,
            [{"beteckning": "SFS 1962:700", "dok_id": "D1", "titel": "Brottsbalk", "datum": "1962-12-21"}],
        )
    )
    page_2 = _response(
        json_data=_sfs_list_payload(
            0,
            {"beteckning": "1949:381", "dok_id": "D2", "titel": "F\u00f6r\u00e4ldrabalk", "datum": "1949-06-10"},
        )
    )
    html_1 = _response(text="<html>d1</html>")
    html_2 = _response(text="<html>d2</html>")

    session = Mock(spec=requests.Session)
    session.get.side_effect = [page_1, html_1, page_2, html_2]

    with patch("ingest.sfs_fetcher.time.sleep", return_value=None):
        summary = SfsFetcher(config_path=config_path, session=session).fetch_all()

    assert summary["saved"] == 2
    assert summary["pages_fetched"] == 2
    assert session.get.call_count == 4


def test_sfs_fetcher_is_idempotent_when_file_exists(tmp_path: Path) -> None:
    config_path = _write_sources_config(tmp_path)

    out_dir = tmp_path / "sfs"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "1962-700.json").write_text("{}", encoding="utf-8")

    page_1 = _response(
        json_data=_sfs_list_payload(
            0,
            [{"beteckning": "SFS 1962:700", "dok_id": "D1", "titel": "Brottsbalk"}],
        )
    )

    session = Mock(spec=requests.Session)
    session.get.side_effect = [page_1]

    with patch("ingest.sfs_fetcher.time.sleep", return_value=None):
        summary = SfsFetcher(config_path=config_path, session=session).fetch_all()

    assert summary["saved"] == 0
    assert summary["skipped_existing"] == 1
    assert session.get.call_count == 1


def test_sfs_fetcher_logs_http_error_and_continues_with_next_document(tmp_path: Path) -> None:
    config_path = _write_sources_config(tmp_path)

    page_1 = _response(
        json_data=_sfs_list_payload(
            0,
            [
                {"beteckning": "SFS 2020:1", "dok_id": "BAD", "titel": "bad"},
                {"beteckning": "SFS 2020:2", "dok_id": "GOOD", "titel": "good"},
            ],
        )
    )
    err = _response(status_code=500)
    html_good = _response(text="<html>good</html>")

    session = Mock(spec=requests.Session)
    session.get.side_effect = [page_1, err, err, err, html_good]

    with patch("ingest.sfs_fetcher.time.sleep", return_value=None):
        summary = SfsFetcher(config_path=config_path, session=session).fetch_all()

    assert summary["saved"] == 1
    assert summary["errors"] >= 1
    assert (tmp_path / "sfs" / "2020-2.json").exists()

    errors_file = tmp_path / "sfs_errors.jsonl"
    assert errors_file.exists()
    entries = [json.loads(line) for line in errors_file.read_text(encoding="utf-8").splitlines()]
    assert any(entry.get("dok_id") == "BAD" for entry in entries)


def test_sfs_number_normalization_and_filename(tmp_path: Path) -> None:
    config_path = _write_sources_config(tmp_path)

    page_1 = _response(
        json_data=_sfs_list_payload(
            0,
            [{"beteckning": "SFS 1962:700", "dok_id": "D1", "titel": "Brottsbalk", "datum": "1962-12-21"}],
        )
    )
    html_1 = _response(text="<html>d1</html>")

    session = Mock(spec=requests.Session)
    session.get.side_effect = [page_1, html_1]

    with patch("ingest.sfs_fetcher.time.sleep", return_value=None):
        SfsFetcher(config_path=config_path, session=session).fetch_all()

    out_file = tmp_path / "sfs" / "1962-700.json"
    assert out_file.exists()
    payload = json.loads(out_file.read_text(encoding="utf-8"))
    assert payload["sfs_nr"] == "1962:700"


def test_ikrafttradandedatum_zero_date_becomes_null(tmp_path: Path) -> None:
    config_path = _write_sources_config(tmp_path)

    page_1 = _response(
        json_data=_sfs_list_payload(
            0,
            [
                {
                    "beteckning": "SFS 2000:1",
                    "dok_id": "D1",
                    "titel": "test",
                    IKRAFT_KEY: "0000-00-00",
                }
            ],
        )
    )
    html_1 = _response(text="<html>d1</html>")

    session = Mock(spec=requests.Session)
    session.get.side_effect = [page_1, html_1]

    with patch("ingest.sfs_fetcher.time.sleep", return_value=None):
        SfsFetcher(config_path=config_path, session=session).fetch_all()

    payload = json.loads((tmp_path / "sfs" / "2000-1.json").read_text(encoding="utf-8"))
    assert IKRAFT_KEY in payload
    assert payload[IKRAFT_KEY] is None


def test_ikrafttradandedatum_missing_becomes_null_and_key_exists(tmp_path: Path) -> None:
    config_path = _write_sources_config(tmp_path)

    page_1 = _response(
        json_data=_sfs_list_payload(
            0,
            [{"beteckning": "SFS 2001:2", "dok_id": "D1", "titel": "test"}],
        )
    )
    html_1 = _response(text="<html>d1</html>")

    session = Mock(spec=requests.Session)
    session.get.side_effect = [page_1, html_1]

    with patch("ingest.sfs_fetcher.time.sleep", return_value=None):
        SfsFetcher(config_path=config_path, session=session).fetch_all()

    payload = json.loads((tmp_path / "sfs" / "2001-2.json").read_text(encoding="utf-8"))
    assert IKRAFT_KEY in payload
    assert payload[IKRAFT_KEY] is None
