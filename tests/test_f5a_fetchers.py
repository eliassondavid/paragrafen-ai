"""Unit tests for F-5a fetchers with mocked HTTP calls."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock, patch

import requests
import yaml

from ingest import prop_fetcher, sou_fetcher


def _write_sources_config(tmp_path: Path) -> Path:
    config = {
        "riksdagen_api": {
            "base_url": "https://example.test",
            "sou": {
                "list_endpoint": "/dokumentlista/",
                "doktyp": "sou",
                "utformat": "json",
                "pagesize": 200,
                "document_html_endpoint": "/dokument/{dok_id}",
                "output_dir": str(tmp_path / "sou"),
                "errors_file": str(tmp_path / "sou_errors.jsonl"),
            },
            "prop": {
                "list_endpoint": "/dokumentlista/",
                "doktyp": "prop",
                "utformat": "json",
                "pagesize": 200,
                "document_html_endpoint": "/dokument/{dok_id}",
                "output_dir": str(tmp_path / "prop"),
                "errors_file": str(tmp_path / "prop_errors.jsonl"),
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
    headers: dict[str, str] | None = None,
) -> Mock:
    response = Mock()
    response.headers = headers or {}
    response.text = text
    if status_code >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(f"HTTP {status_code}")
    else:
        response.raise_for_status.return_value = None
    response.json.return_value = json_data
    return response


def _sou_list_payload(remaining: int, documents: list[dict]) -> dict:
    return {
        "dokumentlista": {
            "@\u00e5terst\u00e5ende": str(remaining),
            "dokument": documents,
        }
    }


def _prop_list_payload(remaining: int, documents: list[dict]) -> dict:
    return {
        "dokumentlista": {
            "@\u00e5terst\u00e5ende": str(remaining),
            "dokument": documents,
        }
    }


def test_sou_fetcher_paginates_three_pages(tmp_path: Path) -> None:
    config_path = _write_sources_config(tmp_path)

    page_1 = _response(
        json_data=_sou_list_payload(
            2,
            [{"beteckning": "SOU 2017:14", "dok_id": "A1", "titel": "t1", "datum": "2017-01-01"}],
        )
    )
    page_2 = _response(
        json_data=_sou_list_payload(
            1,
            [{"beteckning": "SOU 2018:2", "dok_id": "A2", "titel": "t2", "datum": "2018-01-01"}],
        )
    )
    page_3 = _response(
        json_data=_sou_list_payload(
            0,
            [{"beteckning": "SOU 2019:101", "dok_id": "A3", "titel": "t3", "datum": "2019-01-01"}],
        )
    )
    html_1 = _response(text="<html>sou-a1</html>", headers={"Content-Type": "text/html"})
    html_2 = _response(text="<html>sou-a2</html>", headers={"Content-Type": "text/html"})
    html_3 = _response(text="<html>sou-a3</html>", headers={"Content-Type": "text/html"})

    session = Mock(spec=requests.Session)
    session.get.side_effect = [page_1, html_1, page_2, html_2, page_3, html_3]

    with patch("ingest.sou_fetcher.time.sleep", return_value=None):
        saved = sou_fetcher.fetch_sou_documents(config_path=config_path, session=session)

    assert saved == 3
    output_dir = tmp_path / "sou"
    filenames = sorted(path.name for path in output_dir.glob("*.json"))
    assert filenames == ["SOU_2017_014.json", "SOU_2018_002.json", "SOU_2019_101.json"]


def test_sou_fetcher_is_idempotent_when_file_exists(tmp_path: Path) -> None:
    config_path = _write_sources_config(tmp_path)
    output_dir = tmp_path / "sou"
    output_dir.mkdir(parents=True, exist_ok=True)
    existing_file = output_dir / "SOU_2017_014.json"
    existing_file.write_text("{}", encoding="utf-8")

    page_1 = _response(
        json_data=_sou_list_payload(0, [{"beteckning": "SOU 2017:14", "dok_id": "A1", "titel": "t1"}])
    )

    session = Mock(spec=requests.Session)
    session.get.side_effect = [page_1]

    with patch("ingest.sou_fetcher.time.sleep", return_value=None):
        saved = sou_fetcher.fetch_sou_documents(config_path=config_path, session=session)

    assert saved == 0
    assert session.get.call_count == 1


def test_sou_fetcher_logs_error_and_continues_on_http_failure(tmp_path: Path) -> None:
    config_path = _write_sources_config(tmp_path)

    page_1 = _response(
        json_data=_sou_list_payload(
            0,
            [
                {"beteckning": "SOU 2020:1", "dok_id": "BAD1", "titel": "bad"},
                {"beteckning": "SOU 2020:2", "dok_id": "GOOD2", "titel": "good"},
            ],
        )
    )
    err = _response(status_code=500)
    html_good = _response(text="<html>good</html>", headers={"Content-Type": "text/html"})

    session = Mock(spec=requests.Session)
    session.get.side_effect = [page_1, err, err, err, html_good]

    with patch("ingest.sou_fetcher.time.sleep", return_value=None):
        saved = sou_fetcher.fetch_sou_documents(config_path=config_path, session=session)

    assert saved == 1
    saved_files = list((tmp_path / "sou").glob("*.json"))
    assert len(saved_files) == 1
    assert saved_files[0].name == "SOU_2020_002.json"

    errors_file = tmp_path / "sou_errors.jsonl"
    assert errors_file.exists()
    entries = [json.loads(line) for line in errors_file.read_text(encoding="utf-8").splitlines()]
    assert any(entry.get("dok_id") == "BAD1" for entry in entries)


def test_normalize_sou_beteckning() -> None:
    assert sou_fetcher.normalize_sou_beteckning("SOU 2017:14") == "SOU_2017_014"


def test_normalize_prop_beteckning() -> None:
    assert prop_fetcher.normalize_prop_beteckning("prop. 2016/17:180") == "prop_2016-17_180"


def test_prop_fetcher_paginates_and_saves_documents(tmp_path: Path) -> None:
    config_path = _write_sources_config(tmp_path)

    page_1 = _response(
        json_data=_prop_list_payload(
            1,
            [
                {
                    "beteckning": "prop. 2016/17:180",
                    "dok_id": "P1",
                    "rm": "2016/17",
                    "nummer": "180",
                    "titel": "prop 1",
                }
            ],
        )
    )
    page_2 = _response(
        json_data=_prop_list_payload(
            0,
            [
                {
                    "beteckning": "prop. 2017/18:1",
                    "dok_id": "P2",
                    "rm": "2017/18",
                    "nummer": "1",
                    "titel": "prop 2",
                }
            ],
        )
    )
    html_1 = _response(text="<html>prop-1</html>", headers={"Content-Type": "text/html"})
    html_2 = _response(text="<html>prop-2</html>", headers={"Content-Type": "text/html"})

    session = Mock(spec=requests.Session)
    session.get.side_effect = [page_1, html_1, page_2, html_2]

    with patch("ingest.prop_fetcher.time.sleep", return_value=None):
        saved = prop_fetcher.fetch_prop_documents(config_path=config_path, session=session)

    assert saved == 2
    filenames = sorted(path.name for path in (tmp_path / "prop").glob("*.json"))
    assert filenames == ["prop_2016-17_180.json", "prop_2017-18_1.json"]
