"""
qa/qa_runner.py
§AI (paragrafen.ai) — F-9 QA Golden Standard
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from rag.rag_pipeline import RagPipeline

logger = logging.getLogger("paragrafenai.noop")

_SOURCE_TYPE_INDICATORS = {
    "sfs": "SFS",
    "forarbete": "prop.",
    "praxis": "NJA",
    "doktrin": ",",
}

_SWEDISH_MARKERS = {
    "och",
    "att",
    "det",
    "som",
    "är",
    "inte",
    "jag",
    "du",
    "för",
    "med",
    "på",
    "till",
    "kan",
    "ska",
    "vad",
    "hur",
    "om",
    "den",
    "denna",
    "en",
    "ett",
}


@dataclass
class TestResult:
    id: str
    query: str
    category: str
    passed: bool
    failures: list[str]
    raw_response: dict[str, Any]


@dataclass
class QaReport:
    total: int
    passed: int
    failed: int
    results: list[TestResult]
    pass_rate: float
    failures_by_category: dict[str, int]


class QaRunner:
    def __init__(self, gold_standard_path: str, pipeline: RagPipeline) -> None:
        self.gold_standard_path = Path(gold_standard_path)
        self.pipeline = pipeline
        self.test_cases = self._load_gold_standard(self.gold_standard_path)

    @staticmethod
    def _load_gold_standard(path: Path) -> list[dict[str, Any]]:
        with path.open(encoding="utf-8") as fh:
            payload = json.load(fh)

        if not isinstance(payload, list):
            raise ValueError("gold_standard.json måste innehålla en lista av testfall")
        return payload

    def run_all(self) -> QaReport:
        results = [self._run_one(test_case) for test_case in self.test_cases]

        total = len(results)
        passed = sum(1 for result in results if result.passed)
        failed = total - passed

        failures_by_category: dict[str, int] = {}
        for result in results:
            if result.passed:
                continue
            failures_by_category[result.category] = failures_by_category.get(result.category, 0) + 1

        pass_rate = passed / total if total else 0.0

        return QaReport(
            total=total,
            passed=passed,
            failed=failed,
            results=results,
            pass_rate=pass_rate,
            failures_by_category=failures_by_category,
        )

    def _run_one(self, tc: dict[str, Any]) -> TestResult:
        response = self.pipeline.query(tc["query"])

        failures: list[str] = []
        exp = tc["expected"]

        got_blocked = bool(response.get("blocked"))
        expected_blocked = bool(exp["blocked"])
        if got_blocked != expected_blocked:
            failures.append(f"blocked: got {got_blocked}, expected {expected_blocked}")

        got_low_confidence = bool(response.get("low_confidence"))
        expected_low_confidence = bool(exp.get("low_confidence", False))
        if not expected_blocked and got_low_confidence != expected_low_confidence:
            failures.append(
                "low_confidence: "
                f"got {got_low_confidence}, expected {expected_low_confidence}"
            )

        answer = str(response.get("answer", ""))
        if exp.get("answer_language") == "sv" and not _is_probably_swedish(answer):
            failures.append("answer_language: expected sv")

        if exp.get("disclaimer_present") and not expected_blocked and "⚠️" not in answer:
            failures.append("disclaimer saknas i answer")

        sources = _as_string_list(response.get("sources", []))

        for ref in exp.get("must_contain_refs", []):
            if not any(ref in source for source in sources):
                failures.append(f"källreferens saknas: {ref}")

        for forbidden in exp.get("must_not_contain", []):
            if forbidden in answer:
                failures.append(f"förbjuden sträng i svar: {forbidden}")

        if not expected_blocked and not expected_low_confidence:
            for source_type in exp.get("source_types_present", []):
                indicator = _SOURCE_TYPE_INDICATORS.get(source_type, source_type)
                if not any(indicator in source for source in sources):
                    failures.append(
                        "source_type saknas: "
                        f"{source_type} (letar efter '{indicator}' i sources)"
                    )

        return TestResult(
            id=tc["id"],
            query=tc["query"],
            category=tc["category"],
            passed=len(failures) == 0,
            failures=failures,
            raw_response=response,
        )


def _as_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        return [value]
    return []


def _is_probably_swedish(text: str) -> bool:
    tokens = re.findall(r"[A-Za-zÅÄÖåäö]+", text.lower())
    if not tokens:
        return False
    marker_count = sum(1 for token in tokens if token in _SWEDISH_MARKERS)
    has_swedish_chars = any(char in text.lower() for char in "åäö")
    return marker_count >= 2 or has_swedish_chars


def _print_report(report: QaReport) -> None:
    print("§AI QA — Golden Standard")
    print("========================")
    print(f"Kör {report.total} testfall mot RagPipeline...\n")

    for result in report.results:
        label = f"{result.id} [{result.category}]"
        if result.passed:
            outcome = "✅ PASS"
        else:
            outcome = f"❌ FAIL — {'; '.join(result.failures)}"
        print(f"{label:<30} {outcome}")

    print("\n========================")
    print("SAMMANFATTNING")
    print("========================")
    print(f"Totalt:  {report.total}")
    print(f"Godkänt: {report.passed} ({report.pass_rate * 100:.1f}%)")
    print(f"Underkänt: {report.failed}")

    if report.failures_by_category:
        print("\nMisslyckanden per kategori:")
        for category, count in report.failures_by_category.items():
            print(f"  {category}: {count}")


if __name__ == "__main__":
    import sys

    from rag.rag_pipeline import RagPipeline

    pipeline = RagPipeline(config_path="config/rag_config.yaml")
    runner = QaRunner(
        gold_standard_path="qa/gold_standard.json",
        pipeline=pipeline,
    )
    qa_report = runner.run_all()
    _print_report(qa_report)
    sys.exit(0 if qa_report.failed == 0 else 1)
