from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from rag.models import RAGResult


@dataclass
class Finding:
    """En enskild styrka, svaghet eller saknad aspekt."""

    category: str
    description: str
    legal_ref: str
    severity: str
    source_text: str


@dataclass
class SecondOpinionReport:
    """Komplett Second Opinion-rapport."""

    summary: str
    overall_assessment: str
    confidence: str
    legal_analysis: str
    strengths: list[Finding]
    weaknesses: list[Finding]
    gaps: list[Finding]
    outcome_prognosis: str
    burden_of_proof: str
    practical_obstacles: str
    prognosis_level: str
    recommendation: str
    follow_up_questions: list[str]
    citations: list[str]


class ReportBuilder:
    def __init__(self, minimum_rag_hits: int = 5):
        self.minimum_rag_hits = minimum_rag_hits

    def build(
        self,
        led1: dict[str, Any],
        led2: dict[str, Any],
        rag_result: RAGResult,
    ) -> SecondOpinionReport:
        assessment = self._normalize_assessment(led1.get("overall_assessment"))
        confidence = self._normalize_confidence(led1.get("confidence"), rag_result)

        strengths = self._build_findings(
            led1.get("strengths"),
            category="strength",
            default_severity="medium",
            rag_result=rag_result,
        )
        weaknesses = self._build_findings(
            led1.get("weaknesses"),
            category="weakness",
            default_severity="medium",
            rag_result=rag_result,
        )
        gaps = self._build_findings(
            led1.get("gaps"),
            category="gap",
            default_severity="medium",
            rag_result=rag_result,
        )

        follow_up_questions = self._normalize_questions(led2.get("follow_up_questions"))
        if len(rag_result.hits) < self.minimum_rag_hits:
            scarcity_question = (
                "Vilka ytterligare rättskällor eller fakta behöver utredas innan rådet kan bedömas säkert?"
            )
            if scarcity_question not in follow_up_questions:
                follow_up_questions.append(scarcity_question)

        return SecondOpinionReport(
            summary=self._build_summary(assessment, confidence, rag_result),
            overall_assessment=assessment,
            confidence=confidence,
            legal_analysis=str(led1.get("legal_analysis", "")).strip(),
            strengths=strengths,
            weaknesses=weaknesses,
            gaps=gaps,
            outcome_prognosis=str(led2.get("outcome_prognosis", "")).strip(),
            burden_of_proof=str(led2.get("burden_of_proof", "")).strip(),
            practical_obstacles=str(led2.get("practical_obstacles", "")).strip(),
            prognosis_level=self._normalize_prognosis(led2.get("prognosis_level")),
            recommendation=self._build_recommendation(
                assessment=assessment,
                confidence=confidence,
                strengths=strengths,
                weaknesses=weaknesses,
                gaps=gaps,
                rag_result=rag_result,
            ),
            follow_up_questions=follow_up_questions,
            citations=rag_result.citations(),
        )

    def build_incomplete_input_report(self, message: str) -> SecondOpinionReport:
        return SecondOpinionReport(
            summary="Det gick inte att göra en second opinion eftersom underlaget är ofullständigt.",
            overall_assessment="tveksamt",
            confidence="low",
            legal_analysis=message,
            strengths=[],
            weaknesses=[],
            gaps=[],
            outcome_prognosis="Utfallsprognos kan inte lämnas utan ett konkret råd att granska.",
            burden_of_proof="Kunde inte bedömas.",
            practical_obstacles="Det saknas tillräcklig information för att bedöma processrisk och bevisläge.",
            prognosis_level="osäkert",
            recommendation="Beskriv både situationen och det råd du vill få granskat innan analysen körs igen.",
            follow_up_questions=[
                "Vad var det konkreta rådet du fick?",
                "Vilka fakta i situationen är avgörande för bedömningen?",
            ],
            citations=[],
        )

    def build_blocked_report(self, message: str) -> SecondOpinionReport:
        return SecondOpinionReport(
            summary="Den här frågan ligger utanför tjänstens tillåtna rättsområden och har därför inte analyserats.",
            overall_assessment="tveksamt",
            confidence="low",
            legal_analysis=message,
            strengths=[],
            weaknesses=[],
            gaps=[],
            outcome_prognosis="Ingen utfallsprognos lämnas eftersom frågan inte ska analyseras i denna modul.",
            burden_of_proof="Ej bedömt.",
            practical_obstacles="Du behöver vända dig till kvalificerad rådgivning inom rätt område.",
            prognosis_level="osäkert",
            recommendation=message,
            follow_up_questions=[],
            citations=[],
        )

    def build_insufficient_sources_report(
        self,
        legal_issue: str,
        rag_result: RAGResult,
    ) -> SecondOpinionReport:
        legal_issue_text = legal_issue.strip() or "den aktuella frågan"
        return SecondOpinionReport(
            summary="Det gick inte att förankra analysen i tillräckligt många relevanta rättskällor.",
            overall_assessment="tveksamt",
            confidence="low",
            legal_analysis=(
                f"Second Opinion kunde inte genomföras med tillräcklig säkerhet för {legal_issue_text}. "
                "RAG-sökningen gav för få användbara träffar för en balanserad steelman-bedömning."
            ),
            strengths=[],
            weaknesses=[],
            gaps=[
                Finding(
                    category="gap",
                    description="Underlaget behöver kompletteras med fler relevanta rättskällor eller mer precisa fakta.",
                    legal_ref="Otillräckligt källunderlag",
                    severity="high",
                    source_text="",
                )
            ],
            outcome_prognosis="Utfallsprognosen är för osäker eftersom källunderlaget är tunt.",
            burden_of_proof="Bevisbördan kan inte bedömas säkert utan bättre rättskällestöd.",
            practical_obstacles="Största hindret är att frågan behöver preciseras eller kompletteras med mer källmaterial.",
            prognosis_level="osäkert",
            recommendation=(
                "Komplettera med fler fakta, tydligare tidslinje och om möjligt fler rättskällor "
                "innan du litar på en andra bedömning."
            ),
            follow_up_questions=[
                "Vilka ytterligare omständigheter eller handlingar finns som kan påverka rättsfrågan?",
                "Finns det fler rättskällor eller beslut som direkt rör samma situation?",
            ],
            citations=rag_result.citations(),
        )

    def build_mock_report(
        self,
        legal_issue: str,
        rag_result: RAGResult,
    ) -> SecondOpinionReport:
        legal_issue_text = legal_issue.strip() or "den aktuella rättsfrågan"
        return SecondOpinionReport(
            summary="Second Opinion kördes i demo-läge och kunde därför inte generera en full steelman-analys.",
            overall_assessment="tveksamt",
            confidence="low",
            legal_analysis=(
                f"Rättsfrågan bedömdes preliminärt som {legal_issue_text}, "
                "men språkmodellen är inte konfigurerad och kunde därför inte göra den djupare analysen."
            ),
            strengths=[],
            weaknesses=[],
            gaps=[
                Finding(
                    category="gap",
                    description="Fullständig steelman-analys kräver en konfigurerad LLM-klient.",
                    legal_ref="Demo-läge",
                    severity="high",
                    source_text="",
                )
            ],
            outcome_prognosis="Utfallsprognos kan inte genereras i demo-läge.",
            burden_of_proof="Ej bedömd i demo-läge.",
            practical_obstacles="Miljön saknar konfigurerad LLM, så analysen stannar vid ett strukturerat fallback-svar.",
            prognosis_level="osäkert",
            recommendation=(
                "Koppla en LLM-klient för full second opinion, eller använd källistan som utgångspunkt "
                "för manuell vidare granskning."
            ),
            follow_up_questions=[
                "Vilka delar av rådet behöver du särskilt få prövade när full analys är tillgänglig?"
            ],
            citations=rag_result.citations(),
        )

    def _build_summary(
        self,
        assessment: str,
        confidence: str,
        rag_result: RAGResult,
    ) -> str:
        if not rag_result.hits:
            return "Det finns inte tillräckligt med relevanta träffar för att ge en trygg second opinion."

        summary_map = {
            "korrekt": "Rådet du fick verkar i huvudsak stämma med gällande rätt.",
            "delvis_korrekt": "Rådet du fick verkar delvis stämma men behöver viktiga nyanser eller förbehåll.",
            "tveksamt": "Rådet du fick är osäkert bedömt och väcker flera invändningar.",
            "felaktigt": "Rådet du fick verkar inte stämma med det rättsliga underlaget som hittats.",
        }
        summary = summary_map.get(assessment, "Bedömningen är osäker.")

        if len(rag_result.hits) < self.minimum_rag_hits:
            return f"{summary} Underlaget är dock tunt, vilket sänker tillförlitligheten."
        if confidence == "low":
            return f"{summary} Konfidensen är låg och rådet bör dubbelkontrolleras."
        return summary

    def _build_recommendation(
        self,
        assessment: str,
        confidence: str,
        strengths: list[Finding],
        weaknesses: list[Finding],
        gaps: list[Finding],
        rag_result: RAGResult,
    ) -> str:
        if not rag_result.hits:
            return "Sök en ny bedömning med mer fullständigt underlag innan du agerar på rådet."

        if confidence == "low" or len(rag_result.hits) < self.minimum_rag_hits:
            return (
                "Be om en kompletterad bedömning från en specialist och be uttryckligen om rättskällor "
                "som stöder slutsatsen."
            )

        if assessment in {"tveksamt", "felaktigt"} or len(weaknesses) > len(strengths):
            return (
                "Ta in en ny oberoende bedömning innan du agerar, och be den rådgivare du vänder dig till "
                "att bemöta invändningarna punkt för punkt."
            )

        if gaps:
            return (
                "Rådet verkar möjligt att bygga vidare på, men be om ett förtydligande kring de delar som saknas "
                "innan du fattar slutligt beslut."
            )

        return "Rådet verkar i huvudsak hållbart utifrån det underlag som hittats, men följ upp om nya fakta tillkommer."

    def _build_findings(
        self,
        entries: Any,
        category: str,
        default_severity: str,
        rag_result: RAGResult,
    ) -> list[Finding]:
        if not isinstance(entries, list):
            return []

        findings: list[Finding] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            legal_ref = str(entry.get("legal_ref", "")).strip()
            findings.append(
                Finding(
                    category=category,
                    description=str(entry.get("description", "")).strip(),
                    legal_ref=legal_ref,
                    severity=self._normalize_severity(entry.get("severity"), default=default_severity),
                    source_text=self._resolve_source_text(legal_ref, rag_result),
                )
            )
        return findings

    def _resolve_source_text(self, legal_ref: str, rag_result: RAGResult) -> str:
        reference = legal_ref.strip().casefold()
        if not reference:
            return ""

        for hit in rag_result.hits:
            citation = str(
                hit.metadata.get("citation")
                or hit.metadata.get("short_citation")
                or ""
            ).casefold()
            if citation and (reference in citation or citation in reference):
                return self._truncate_text(hit.text)

        return ""

    @staticmethod
    def _truncate_text(text: str, limit: int = 280) -> str:
        cleaned = " ".join(text.split())
        if len(cleaned) <= limit:
            return cleaned
        return cleaned[: limit - 3].rstrip() + "..."

    @staticmethod
    def _normalize_questions(value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if str(item).strip()]

    @staticmethod
    def _normalize_assessment(value: Any) -> str:
        allowed = {"korrekt", "delvis_korrekt", "tveksamt", "felaktigt"}
        candidate = str(value or "tveksamt").strip().lower()
        if candidate not in allowed:
            return "tveksamt"
        return candidate

    @staticmethod
    def _normalize_prognosis(value: Any) -> str:
        allowed = {"sannolikt_bifall", "osäkert", "sannolikt_avslag"}
        candidate = str(value or "osäkert").strip().lower()
        if candidate not in allowed:
            return "osäkert"
        return candidate

    def _normalize_confidence(self, value: Any, rag_result: RAGResult) -> str:
        if len(rag_result.hits) < self.minimum_rag_hits:
            return "low"

        allowed = {"high", "medium", "low"}
        candidate = str(value or rag_result.confidence or "medium").strip().lower()
        if candidate not in allowed:
            return "medium"
        return candidate

    @staticmethod
    def _normalize_severity(value: Any, default: str = "medium") -> str:
        allowed = {"high", "medium", "low"}
        candidate = str(value or default).strip().lower()
        if candidate not in allowed:
            return default
        return candidate
