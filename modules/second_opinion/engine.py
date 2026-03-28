"""
Second Opinion Engine — analyserar juridiskt råd mot rättskällor.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

import yaml

from guard.guard_pipeline import GuardPipeline
from modules.second_opinion.report_builder import ReportBuilder, SecondOpinionReport
from modules.second_opinion.steelman_prompt import (
    build_identify_legal_issue_prompt,
    build_led1_prompt,
    build_led2_prompt,
)
from rag.llm_client import MockLLMClient, get_llm_client
from rag.models import RAGResult
from rag.prompt_builder import PromptBuilder
from rag.rag_query import RAGQueryEngine

logger = logging.getLogger(__name__)


class SecondOpinionEngine:
    """Huvudklass för Second Opinion-analys."""

    def __init__(
        self,
        config_path: str | Path | None = None,
        rag: RAGQueryEngine | None = None,
        prompt_builder: PromptBuilder | None = None,
        llm: Any | None = None,
        guard: GuardPipeline | None = None,
        report_builder: ReportBuilder | None = None,
    ):
        self.config = self._load_config(config_path)
        self.rag = rag or RAGQueryEngine()
        self.prompt_builder = prompt_builder or PromptBuilder()
        self.llm = llm or get_llm_client()
        self.guard = guard or GuardPipeline()
        self.report_builder = report_builder or ReportBuilder(
            minimum_rag_hits=int(self.config.get("minimum_rag_hits", 5))
        )

    def analyze(
        self,
        situation: str,
        advice_received: str,
        legal_area: str = "",
        advice_source: str = "",
    ) -> SecondOpinionReport:
        """
        Analysera juridiskt råd mot rättskällor.

        Args:
            situation: Användarens beskrivning av sin situation
            advice_received: Rådet som gavs
            legal_area: Rättsområde (om känt)
            advice_source: Vem som gav rådet (jurist, myndighet etc.)
        """
        situation = situation.strip()
        advice_received = advice_received.strip()
        legal_area = legal_area.strip()
        advice_source = advice_source.strip()

        if self._is_excluded_legal_area(legal_area):
            return self.report_builder.build_blocked_report(
                "Det angivna rättsområdet omfattas inte av denna second opinion-modul."
            )

        blocked, message = self.guard.check_query(
            " ".join(part for part in [legal_area, situation, advice_received] if part)
        )
        if blocked:
            return self.report_builder.build_blocked_report(
                message or "Frågan kan inte analyseras i denna modul."
            )

        if not situation:
            return self.report_builder.build_incomplete_input_report(
                "Beskriv situationen mer konkret för att en second opinion ska kunna göras."
            )

        if not advice_received:
            return self.report_builder.build_incomplete_input_report(
                "Du behöver ange vilket råd du fick för att rådet ska kunna granskas."
            )

        legal_issue = self._identify_legal_issue(
            situation=situation,
            advice_received=advice_received,
            legal_area=legal_area,
            advice_source=advice_source,
        )

        rag_query = " ".join(
            part for part in [legal_area, legal_issue, situation, advice_received] if part
        )
        rag_result = self.rag.query(
            rag_query,
            module=str(self.config.get("rag_module", "second_opinion")),
            n_results=int(self.config.get("n_results", 25)),
        )

        if len(rag_result.hits) < int(self.config.get("minimum_rag_hits", 5)):
            logger.warning(
                "Second Opinion hämtade endast %s träffar för frågan: %s",
                len(rag_result.hits),
                legal_issue,
            )

        if not rag_result.hits:
            return self.report_builder.build_insufficient_sources_report(legal_issue, rag_result)

        if self._is_mock_llm():
            return self.report_builder.build_mock_report(legal_issue, rag_result)

        led1_response = self._analyze_led1(
            situation=situation,
            advice_received=advice_received,
            legal_issue=legal_issue,
            legal_area=legal_area,
            advice_source=advice_source,
            rag_result=rag_result,
        )
        led2_response = self._analyze_led2(
            situation=situation,
            legal_issue=legal_issue,
            legal_area=legal_area,
            rag_result=rag_result,
        )

        return self.report_builder.build(led1_response, led2_response, rag_result)

    def _load_config(self, config_path: str | Path | None) -> dict[str, Any]:
        path = Path(config_path) if config_path else self._default_config_path()
        with path.open(encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
        if not isinstance(payload, dict):
            raise ValueError("second_opinion.yaml måste innehålla ett YAML-objekt")
        return payload

    @staticmethod
    def _default_config_path() -> Path:
        return Path(__file__).resolve().parent / "config" / "second_opinion.yaml"

    def _is_excluded_legal_area(self, legal_area: str) -> bool:
        if not legal_area:
            return False

        normalized_legal_area = self._normalize_area_name(legal_area)
        excluded_areas = {
            self._normalize_area_name(str(area))
            for area in self.config.get("excluded_areas", [])
        }
        return normalized_legal_area in excluded_areas

    def _is_mock_llm(self) -> bool:
        return isinstance(self.llm, MockLLMClient)

    def _identify_legal_issue(
        self,
        situation: str,
        advice_received: str,
        legal_area: str = "",
        advice_source: str = "",
    ) -> str:
        if self._is_mock_llm():
            return self._heuristic_legal_issue(situation, advice_received, legal_area)

        prompt = build_identify_legal_issue_prompt(
            situation=situation,
            advice_received=advice_received,
            legal_area=legal_area,
            advice_source=advice_source,
        )
        response = str(
            self.llm.chat(
                system_prompt="Du är en erfaren svensk jurist. Svara med exakt en mening.",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=200,
            )
        ).strip()

        if not response or "demo-läge" in response.lower():
            return self._heuristic_legal_issue(situation, advice_received, legal_area)

        return " ".join(response.split())

    def _analyze_led1(
        self,
        situation: str,
        advice_received: str,
        legal_issue: str,
        legal_area: str,
        advice_source: str,
        rag_result: RAGResult,
    ) -> dict[str, Any]:
        context = self.prompt_builder.build_context(rag_result)
        prompt = build_led1_prompt(
            situation=situation,
            advice_received=advice_received,
            legal_issue=legal_issue,
            context=context,
            legal_area=legal_area,
            advice_source=advice_source,
        )

        response = self.llm.chat(
            system_prompt=(
                "Du är en erfaren svensk jurist som granskar juridiska råd. "
                "Svara enbart med giltig JSON."
            ),
            messages=[{"role": "user", "content": prompt}],
            max_tokens=3000,
        )

        payload = self._parse_json_object(response)
        if payload is not None:
            return payload

        return {
            "legal_analysis": str(response).strip(),
            "strengths": [],
            "weaknesses": [],
            "gaps": [],
            "overall_assessment": "tveksamt",
            "confidence": "low",
        }

    def _analyze_led2(
        self,
        situation: str,
        legal_issue: str,
        legal_area: str,
        rag_result: RAGResult,
    ) -> dict[str, Any]:
        context = self.prompt_builder.build_context(rag_result)
        prompt = build_led2_prompt(
            situation=situation,
            legal_issue=legal_issue,
            context=context,
            legal_area=legal_area,
        )

        response = self.llm.chat(
            system_prompt="Du är en erfaren processförande jurist. Svara enbart med giltig JSON.",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2000,
        )

        payload = self._parse_json_object(response)
        if payload is not None:
            return payload

        return {
            "outcome_prognosis": str(response).strip(),
            "burden_of_proof": "Kunde inte bedömas.",
            "practical_obstacles": "Kunde inte bedömas.",
            "prognosis_level": "osäkert",
            "follow_up_questions": [],
        }

    def _parse_json_object(self, response: Any) -> dict[str, Any] | None:
        text = self._strip_code_fences(str(response).strip())
        if not text:
            return None

        direct = self._json_loads_if_dict(text)
        if direct is not None:
            return direct

        decoder = json.JSONDecoder()
        for index, char in enumerate(text):
            if char != "{":
                continue
            try:
                payload, _ = decoder.raw_decode(text[index:])
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                return payload
        return None

    @staticmethod
    def _strip_code_fences(text: str) -> str:
        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\s*```$", "", cleaned)
        return cleaned.strip()

    @staticmethod
    def _json_loads_if_dict(text: str) -> dict[str, Any] | None:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return None
        if isinstance(payload, dict):
            return payload
        return None

    @staticmethod
    def _heuristic_legal_issue(
        situation: str,
        advice_received: str,
        legal_area: str = "",
    ) -> str:
        raw_basis = situation or advice_received
        basis = " ".join(raw_basis.split()).rstrip(".")
        if not basis:
            basis = "rådet i den beskrivna situationen"
        if legal_area:
            return f"Inom {legal_area.lower()} är kärnfrågan om {basis[:180]}."
        return f"Den centrala rättsfrågan är om {basis[:180]}."

    @staticmethod
    def _normalize_area_name(value: str) -> str:
        return (
            value.strip()
            .lower()
            .replace("å", "a")
            .replace("ä", "a")
            .replace("ö", "o")
            .replace("-", "")
            .replace(" ", "")
        )
