from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from qa.qa_runner import QaRunner


class DummyPipeline:
    def query(self, user_query: str) -> dict:
        raise NotImplementedError


def _make_expected(**overrides) -> dict:
    base = {
        "blocked": False,
        "low_confidence": False,
        "source_types_present": [],
        "answer_language": "sv",
        "disclaimer_present": False,
        "must_contain_refs": [],
        "must_not_contain": [],
    }
    base.update(overrides)
    return base


def _make_test_case(case_id: str, category: str, expected: dict) -> dict:
    return {
        "id": case_id,
        "query": f"query for {case_id}",
        "category": category,
        "expected": expected,
        "notes": "unit test",
    }


def _write_gold_standard(path: Path, test_cases: list[dict]) -> Path:
    target = path / "gold_standard.json"
    target.write_text(json.dumps(test_cases, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def test_run_one_passes_when_blocked_matches_expected(tmp_path):
    tc = _make_test_case("gs_001", "blockerad_straffrätt", _make_expected(blocked=True))
    gold_path = _write_gold_standard(tmp_path, [tc])
    pipeline = DummyPipeline()

    response = {
        "answer": "Denna tjänst täcker inte straffrättsliga frågor.",
        "blocked": True,
        "low_confidence": False,
        "sources": [],
    }

    runner = QaRunner(str(gold_path), pipeline)
    with patch.object(pipeline, "query", return_value=response) as mock_query:
        result = runner._run_one(tc)

    mock_query.assert_called_once_with(tc["query"])
    assert result.passed is True
    assert result.failures == []


def test_run_one_fails_when_blocked_does_not_match(tmp_path):
    tc = _make_test_case("gs_002", "blockerad_straffrätt", _make_expected(blocked=True))
    gold_path = _write_gold_standard(tmp_path, [tc])
    pipeline = DummyPipeline()

    response = {
        "answer": "Ej blockerat svar",
        "blocked": False,
        "low_confidence": False,
        "sources": [],
    }

    runner = QaRunner(str(gold_path), pipeline)
    with patch.object(pipeline, "query", return_value=response):
        result = runner._run_one(tc)

    assert result.passed is False
    assert "blocked: got False, expected True" in result.failures


def test_run_one_passes_when_disclaimer_exists(tmp_path):
    tc = _make_test_case(
        "gs_003",
        "hyresrätt",
        _make_expected(disclaimer_present=True, source_types_present=["sfs"]),
    )
    gold_path = _write_gold_standard(tmp_path, [tc])
    pipeline = DummyPipeline()

    response = {
        "answer": "Här är svaret.\n\n⚠️ Detta är juridisk information.",
        "blocked": False,
        "low_confidence": False,
        "sources": ["SFS 1970:994 12 kap. 2 §"],
    }

    runner = QaRunner(str(gold_path), pipeline)
    with patch.object(pipeline, "query", return_value=response):
        result = runner._run_one(tc)

    assert result.passed is True
    assert result.failures == []


def test_run_one_fails_when_required_reference_missing(tmp_path):
    tc = _make_test_case(
        "gs_004",
        "hyresrätt",
        _make_expected(must_contain_refs=["1970:994"]),
    )
    gold_path = _write_gold_standard(tmp_path, [tc])
    pipeline = DummyPipeline()

    response = {
        "answer": "Svar om hyresrätt.",
        "blocked": False,
        "low_confidence": False,
        "sources": ["SFS 1982:80 6 §"],
    }

    runner = QaRunner(str(gold_path), pipeline)
    with patch.object(pipeline, "query", return_value=response):
        result = runner._run_one(tc)

    assert result.passed is False
    assert "källreferens saknas: 1970:994" in result.failures


def test_run_one_fails_when_forbidden_string_exists_in_answer(tmp_path):
    tc = _make_test_case(
        "gs_005",
        "hyresrätt",
        _make_expected(must_not_contain=["asyl"]),
    )
    gold_path = _write_gold_standard(tmp_path, [tc])
    pipeline = DummyPipeline()

    response = {
        "answer": "Detta svar nämner asyl av misstag.",
        "blocked": False,
        "low_confidence": False,
        "sources": ["SFS 1970:994 12 kap. 2 §"],
    }

    runner = QaRunner(str(gold_path), pipeline)
    with patch.object(pipeline, "query", return_value=response):
        result = runner._run_one(tc)

    assert result.passed is False
    assert "förbjuden sträng i svar: asyl" in result.failures


def test_run_all_builds_correct_report_metrics(tmp_path):
    tc_pass = _make_test_case("gs_006", "kategori_a", _make_expected(blocked=True))
    tc_fail = _make_test_case("gs_007", "kategori_b", _make_expected(blocked=True))
    gold_path = _write_gold_standard(tmp_path, [tc_pass, tc_fail])
    pipeline = DummyPipeline()

    responses = [
        {
            "answer": "Denna fråga är blockerad.",
            "blocked": True,
            "low_confidence": False,
            "sources": [],
        },
        {
            "answer": "Ej blockerad trots förväntan.",
            "blocked": False,
            "low_confidence": False,
            "sources": [],
        },
    ]

    runner = QaRunner(str(gold_path), pipeline)
    with patch.object(pipeline, "query", side_effect=responses) as mock_query:
        report = runner.run_all()

    assert mock_query.call_count == 2
    assert report.total == 2
    assert report.passed == 1
    assert report.failed == 1
    assert report.pass_rate == 0.5
    assert report.failures_by_category == {"kategori_b": 1}
