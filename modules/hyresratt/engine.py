"""Hyresrättsassistenten med regelbaserad analys och RAG-stöd."""

from __future__ import annotations

import logging
from typing import Any

from modules.hyresratt.decision_tree import (
    analysera_brist,
    analysera_hyreshojning,
    analysera_uppsagning,
)
from modules.hyresratt.document_generator import (
    generera_bestridandebrev,
    generera_hyreshojningssvar,
    generera_reklamationsbrev,
)
from modules.hyresratt.models import (
    ArendeTyp,
    BristInfo,
    HyresgastInfo,
    HyreshojningsInfo,
    HyresrattsAnalys,
    UpsagningsInfo,
)
from modules.hyresratt.timeline import sortera_tidsfrister
from rag.llm_client import get_llm_client
from rag.models import RAGResult
from rag.rag_query import RAGQueryEngine

logger = logging.getLogger(__name__)


def _empty_rag_result() -> RAGResult:
    return RAGResult(
        hits=[],
        confidence="low",
        disclaimer="Rättskällor kunde inte hämtas just nu.",
        total_candidates=0,
        filtered_count=0,
    )


class _NullRAGQueryEngine:
    def query(
        self,
        query: str,
        module: str,
        n_results: int | None = None,
        user_context: list | None = None,
    ) -> RAGResult:
        del query, module, n_results, user_context
        return _empty_rag_result()


class HyresrattsEngine:
    """Huvudklass för hyresrättsassistenten."""

    def __init__(
        self,
        rag: RAGQueryEngine | None = None,
        llm: Any | None = None,
    ) -> None:
        self.rag = rag or self._build_default_rag()
        self.llm = llm or get_llm_client()

    def analysera(
        self,
        arende_typ: ArendeTyp,
        uppsagning: UpsagningsInfo | None = None,
        hyresgast: HyresgastInfo | None = None,
        hyreshojning: HyreshojningsInfo | None = None,
        brist: BristInfo | None = None,
    ) -> HyresrattsAnalys:
        """Analysera en hyresrättsfråga."""

        hyresgast = hyresgast or HyresgastInfo()

        if arende_typ == ArendeTyp.UPPSAGNING:
            if uppsagning is None:
                raise ValueError("uppsagning måste anges för uppsägningsärenden")
            return self._analysera_uppsagning(uppsagning, hyresgast)

        if arende_typ == ArendeTyp.HYRESHOJNING:
            if hyreshojning is None:
                raise ValueError("hyreshojning måste anges för hyreshöjningsärenden")
            return self._analysera_hyreshojning(hyreshojning, hyresgast)

        if arende_typ == ArendeTyp.BRIST:
            if brist is None:
                raise ValueError("brist måste anges för bristärenden")
            return self._analysera_brist(brist, hyresgast)

        raise ValueError(f"Okänd ärendetyp: {arende_typ}")

    def _build_default_rag(self) -> RAGQueryEngine | _NullRAGQueryEngine:
        try:
            return RAGQueryEngine()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Kunde inte initiera RAG för hyresrättsmodulen: %s", exc)
            return _NullRAGQueryEngine()

    def _query_rag(self, query: str) -> RAGResult:
        try:
            return self.rag.query(query, module="allman", n_results=10)
        except Exception as exc:  # noqa: BLE001
            logger.warning("RAG-fråga misslyckades i hyresrättsmodulen: %s", exc)
            return _empty_rag_result()

    def _analysera_uppsagning(
        self,
        uppsagning: UpsagningsInfo,
        hyresgast: HyresgastInfo,
    ) -> HyresrattsAnalys:
        analys = analysera_uppsagning(uppsagning, hyresgast)
        sokfraga = (
            "uppsägning hyresrätt "
            f"{uppsagning.grund.value if uppsagning.grund else ''} "
            "besittningsskydd hyresnämnden"
        ).strip()
        rag_result = self._query_rag(sokfraga)
        brev = generera_bestridandebrev(
            hyresgast=hyresgast,
            uppsagning=uppsagning,
            argument=analys.get("argument_for_hyresgast", []),
        )
        hanvisa, motivering = self._juristbedomning(
            bedomning=analys["bedomning"],
            bostadstyp=hyresgast.bostadstyp,
            varningar=analys.get("varningar", []),
        )

        grund_info = analys.get("grund_info") or {}
        return HyresrattsAnalys(
            arende_typ=ArendeTyp.UPPSAGNING,
            sammanfattning=self._sammanfatta_uppsagning(analys),
            bedomning=analys["bedomning"],
            rattslig_grund=str(grund_info.get("lagrum", "JB 12 kap. 46 §")),
            relevanta_lagrum=self._extrahera_lagrum(analys),
            tidsfrister=sortera_tidsfrister(analys.get("tidsfrister", [])),
            rekommenderade_atgarder=self._bygg_atgarder_uppsagning(analys),
            genererat_brev=brev,
            brev_typ="bestridande",
            relevanta_rattsfall=self._relevanta_rattsfall(rag_result),
            kallhanvisningar=rag_result.citations()[:10],
            varningar=analys.get("varningar", []),
            hanvisa_till_jurist=hanvisa,
            jurist_motivering=motivering,
        )

    def _analysera_hyreshojning(
        self,
        hyreshojning: HyreshojningsInfo,
        hyresgast: HyresgastInfo,
    ) -> HyresrattsAnalys:
        analys = analysera_hyreshojning(hyreshojning)
        hyreshojning.hojning_procent = float(analys["hojning_procent"])
        rag_result = self._query_rag("hyreshöjning bruksvärde skälig hyra hyresnämnden")
        brev = generera_hyreshojningssvar(
            hyresgast=hyresgast,
            hojning=hyreshojning,
            argument=analys["argument"],
        )

        return HyresrattsAnalys(
            arende_typ=ArendeTyp.HYRESHOJNING,
            sammanfattning=self._sammanfatta_hyreshojning(analys),
            bedomning=analys["bedomning"],
            rattslig_grund="JB 12 kap. 55 §",
            relevanta_lagrum=["JB 12 kap. 55 §", "JB 12 kap. 55 c §"],
            tidsfrister=sortera_tidsfrister(analys.get("tidsfrister", [])),
            rekommenderade_atgarder=[
                "Svara skriftligt att du inte godtar höjningen.",
                "Begär jämförelsematerial för likvärdiga lägenheter.",
                "Kontakta Hyresgästföreningen om det finns förhandlingsordning.",
                "Om ni inte enas kan frågan prövas av hyresnämnden.",
                "Hyresnämndens prövning är kostnadsfri.",
            ],
            genererat_brev=brev,
            brev_typ="svar_hyreshojning",
            relevanta_rattsfall=self._relevanta_rattsfall(rag_result),
            kallhanvisningar=rag_result.citations()[:10],
            varningar=analys.get("varningar", []),
        )

    def _analysera_brist(
        self,
        brist: BristInfo,
        hyresgast: HyresgastInfo,
    ) -> HyresrattsAnalys:
        analys = analysera_brist(brist)
        rag_result = self._query_rag(
            f"brist lägenhet {brist.typ} underhållsskyldighet hyresnedsättning hyresnämnden"
        )
        brev = generera_reklamationsbrev(hyresgast=hyresgast, brist=brist)

        return HyresrattsAnalys(
            arende_typ=ArendeTyp.BRIST,
            sammanfattning=self._sammanfatta_brist(brist, analys),
            bedomning=analys["bedomning"],
            rattslig_grund="JB 12 kap. 11 §, 15 § och 16 §",
            relevanta_lagrum=["JB 12 kap. 11 §", "JB 12 kap. 15 §", "JB 12 kap. 16 §"],
            tidsfrister=sortera_tidsfrister(analys.get("tidsfrister", [])),
            rekommenderade_atgarder=list(analys["atgarder"])
            + ["Hyresnämndens prövning är kostnadsfri."],
            genererat_brev=brev,
            brev_typ="reklamation",
            relevanta_rattsfall=self._relevanta_rattsfall(rag_result),
            kallhanvisningar=rag_result.citations()[:10],
            varningar=analys.get("varningar", []),
        )

    def _juristbedomning(
        self,
        *,
        bedomning: str,
        bostadstyp: str,
        varningar: list[str],
    ) -> tuple[bool, str]:
        if bostadstyp.lower() == "lokal":
            return True, "Lokalhyra följer andra regler och bör bedömas av jurist."
        if bedomning == "svag_position":
            return True, "Din position verkar svag och det är klokt att låta jurist granska underlaget."
        if any("stöds inte" in varning.lower() for varning in varningar):
            return True, "Situationen ligger utanför MVP:ns säkra stöd och bör granskas av jurist."
        return False, ""

    def _relevanta_rattsfall(self, rag_result: RAGResult) -> list[str]:
        citations: list[str] = []
        for hit in rag_result.hits:
            citation = hit.metadata.get("short_citation") or hit.metadata.get("citation")
            if citation and citation not in citations:
                citations.append(str(citation))
        return citations[:5]

    def _extrahera_lagrum(self, analys: dict) -> list[str]:
        lagrum: list[str] = []
        grund_info = analys.get("grund_info")
        if grund_info:
            lagrum.append(str(grund_info["lagrum"]))
            rattelse_lagrum = str(grund_info.get("rattelse_lagrum", "")).strip()
            if rattelse_lagrum:
                lagrum.append(rattelse_lagrum)
        lagrum.append("JB 12 kap. 46 §")
        return lagrum

    def _bygg_atgarder_uppsagning(self, analys: dict) -> list[str]:
        atgarder = [
            "Bestrid uppsägningen skriftligt direkt.",
            "Spara all dokumentation om betalningar, kontakter och händelser.",
        ]
        if analys.get("rattelse_mojlig"):
            atgarder.append("Rätta det som hyresvärden påstår så snabbt som möjligt om det går.")
        atgarder.append("Avvakta att hyresvärden hänskjuter ärendet till hyresnämnden.")
        atgarder.append("Hyresnämndens prövning är kostnadsfri.")
        return atgarder

    def _sammanfatta_uppsagning(self, analys: dict) -> str:
        bedomning_text = {
            "stark_position": "Du verkar ha en stark position just nu.",
            "medel": "Situationen behöver bedömas noggrant, men det finns tydliga steg att ta.",
            "svag_position": "Risken är förhöjd och du bör agera snabbt samt överväga juriststöd.",
        }
        grund_info = analys.get("grund_info")
        grundtext = ""
        if grund_info:
            grundtext = f" Påstådd grund: {grund_info['beskrivning']}"
        return bedomning_text.get(analys["bedomning"], "Situationen behöver bedömas.") + grundtext

    def _sammanfatta_hyreshojning(self, analys: dict) -> str:
        return (
            f"Föreslagen höjning är {analys['hojning_procent']:.1f}%. "
            f"{analys['argument'][0] if analys['argument'] else ''}"
        ).strip()

    def _sammanfatta_brist(self, brist: BristInfo, analys: dict) -> str:
        if not brist.anmald_till_hyresvard:
            return "Första steget är att reklamera bristen skriftligt till hyresvärden."
        if analys["bedomning"] == "stark_position":
            return "Bristen är redan anmäld utan tillräcklig åtgärd. Du kan gå vidare mot hyresnämnden."
        return "Bristen behöver följas upp och dokumenteras vidare."
