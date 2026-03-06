from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock, patch

import requests
import yaml

from ingest import prop_fetcher


def _write_sources_config(tmp_path: Path) -> Path:
    config = {
        "riksdagen_api": {
            "base_url": "https://data.riksdagen.se",
            "prop": {
                "list_endpoint": "/dokumentlista/",
                "doktyp": "prop",
                "utformat": "json",
                "pagesize": 200,
                "document_html_endpoint": "/dokument/{dok_id}.html",
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
    path = tmp_path / "sources.yaml"
    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(config, fh, sort_keys=False)
    return path


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
    response.json.return_value = json_data
    if status_code >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(f"HTTP {status_code}")
    else:
        response.raise_for_status.return_value = None
    return response


def _payload(page: int, pages: int, documents: list[dict]) -> dict:
    return {
        "dokumentlista": {
            "@sida": str(page),
            "@sidor": str(pages),
            "dokument": documents,
        }
    }


def test_prop_fetcher_paginates_three_pages(tmp_path: Path) -> None:
    config_path = _write_sources_config(tmp_path)
    session = Mock(spec=requests.Session)
    session.get.side_effect = [
        _response(json_data=_payload(1, 3, [{"beteckning": "Prop. 2016/17:180", "dok_id": "A1", "rm": "2016/17", "nummer": "180"}])),
        _response(text="<html>a1</html>", headers={"Content-Type": "text/html"}),
        _response(json_data=_payload(2, 3, [{"beteckning": "Prop. 2017/18:1", "dok_id": "A2", "rm": "2017/18", "nummer": "1"}])),
        _response(text="<html>a2</html>", headers={"Content-Type": "text/html"}),
        _response(json_data=_payload(3, 3, [{"beteckning": "Prop. 2018/19:99", "dok_id": "A3", "rm": "2018/19", "nummer": "99"}])),
        _response(text="<html>a3</html>", headers={"Content-Type": "text/html"}),
    ]

    with patch("ingest.prop_fetcher.time.sleep", return_value=None):
        saved = prop_fetcher.fetch_prop_documents(config_path=config_path, session=session)

    assert saved == 3
    filenames = sorted(path.name for path in (tmp_path / "prop").glob("*.json"))
    assert filenames == ["prop_2016-17_180.json", "prop_2017-18_1.json", "prop_2018-19_99.json"]


def test_prop_fetcher_is_idempotent_when_file_exists(tmp_path: Path) -> None:
    config_path = _write_sources_config(tmp_path)
    existing_path = tmp_path / "prop" / "prop_2016-17_180.json"
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    existing_path.write_text("{}", encoding="utf-8")

    session = Mock(spec=requests.Session)
    session.get.side_effect = [
        _response(
            json_data=_payload(
                1,
                1,
                [{"beteckning": "Prop. 2016/17:180", "dok_id": "A1", "rm": "2016/17", "nummer": "180"}],
            )
        )
    ]

    with patch("ingest.prop_fetcher.time.sleep", return_value=None):
        saved = prop_fetcher.fetch_prop_documents(config_path=config_path, session=session)

    assert saved == 0
    assert session.get.call_count == 1


def test_prop_fetcher_logs_error_and_continues_on_http_failure(tmp_path: Path) -> None:
    config_path = _write_sources_config(tmp_path)
    session = Mock(spec=requests.Session)
    error_response = _response(status_code=500)
    session.get.side_effect = [
        _response(
            json_data=_payload(
                1,
                1,
                [
                    {"beteckning": "Prop. 2016/17:180", "dok_id": "BAD", "rm": "2016/17", "nummer": "180"},
                    {"beteckning": "Prop. 2017/18:1", "dok_id": "GOOD", "rm": "2017/18", "nummer": "1"},
                ],
            )
        ),
        error_response,
        error_response,
        error_response,
        _response(text="<html>ok</html>", headers={"Content-Type": "text/html"}),
    ]

    with patch("ingest.prop_fetcher.time.sleep", return_value=None):
        saved = prop_fetcher.fetch_prop_documents(config_path=config_path, session=session)

    assert saved == 1
    assert sorted(path.name for path in (tmp_path / "prop").glob("*.json")) == ["prop_2017-18_1.json"]

    errors = [json.loads(line) for line in (tmp_path / "prop_errors.jsonl").read_text(encoding="utf-8").splitlines()]
    assert any(entry.get("dok_id") == "BAD" for entry in errors)


def test_normalize_prop_beteckning_uses_normalized_riksmote_year() -> None:
    assert prop_fetcher.normalize_prop_beteckning("Prop. 2016/17:180") == "prop_2016-17_180"


def test_prop_fetcher_normalizes_protocol_relative_urls_and_pdf_url(tmp_path: Path) -> None:
    config_path = _write_sources_config(tmp_path)
    session = Mock(spec=requests.Session)
    session.get.side_effect = [
        _response(
            json_data=_payload(
                1,
                1,
                [
                    {
                        "beteckning": "Prop. 2016/17:180",
                        "dok_id": "A1",
                        "rm": "2016/17",
                        "nummer": "180",
                        "dokument_url_html": "//data.riksdagen.se/dokument/A1.html",
                        "filbilaga": {"fil": {"url": "//data.riksdagen.se/dokument/A1.pdf"}},
                    }
                ],
            )
        ),
        _response(text="<html>a1</html>", headers={"Content-Type": "text/html"}),
    ]

    with patch("ingest.prop_fetcher.time.sleep", return_value=None):
        prop_fetcher.fetch_prop_documents(config_path=config_path, session=session)

    payload = json.loads((tmp_path / "prop" / "prop_2016-17_180.json").read_text(encoding="utf-8"))
    assert payload["dokument_url_html"] == "https://data.riksdagen.se/dokument/A1.html"
    assert payload["pdf_url"] == "https://data.riksdagen.se/dokument/A1.pdf"
    assert payload["html_available"] is True
