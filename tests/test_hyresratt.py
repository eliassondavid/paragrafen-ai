from __future__ import annotations

from datetime import date

from modules.hyresratt.decision_tree import (
    analysera_brist,
    analysera_hyreshojning,
    analysera_uppsagning,
)
from modules.hyresratt.document_generator import generera_bestridandebrev
from modules.hyresratt.engine import HyresrattsEngine
from modules.hyresratt.models import (
    ArendeTyp,
    BristInfo,
    ForverkandeGrund,
    HyresgastInfo,
    HyreshojningsInfo,
    UpsagningsInfo,
)
from rag.models import RAGHit, RAGResult


class FakeRAG:
    def __init__(self, result: RAGResult) -> None:
        self.result = result
        self.calls: list[tuple[str, str, int | None]] = []

    def query(
        self,
        query: str,
        module: str,
        n_results: int | None = None,
        user_context: list[RAGHit] | None = None,
    ) -> RAGResult:
        del user_context
        self.calls.append((query, module, n_results))
        return self.result


def _rag_result(num_hits: int = 3) -> RAGResult:
    hits = [
        RAGHit(
            text=f"Textutdrag {index}",
            metadata={
                "authority_level": "binding" if index == 1 else "guiding",
                "source_type": "praxis",
                "citation": f"Referens {index}",
                "short_citation": f"NJA 20{index:02d} s. {index}",
            },
            distance=0.1,
            collection="praxis",
            weight=1.0,
            score=1.0,
        )
        for index in range(1, num_hits + 1)
    ]
    return RAGResult(
        hits=hits,
        confidence="high",
        disclaimer="Relevanta källor hittades.",
        total_candidates=num_hits,
        filtered_count=num_hits,
    )


def test_obetald_hyra_rattelse_mojlig() -> None:
    uppsagning = UpsagningsInfo(
        datum_mottagen=date.today(),
        grund=ForverkandeGrund.OBETALD_HYRA,
    )
    hyresgast = HyresgastInfo(antal_forseningar_12man=1)

    analys = analysera_uppsagning(uppsagning, hyresgast)
    assert analys["rattelse_mojlig"] is True
    assert len(analys["tidsfrister"]) > 0


def test_storningar_krav_pa_tillsagelse() -> None:
    uppsagning = UpsagningsInfo(grund=ForverkandeGrund.STORNINGAR)
    hyresgast = HyresgastInfo()

    analys = analysera_uppsagning(uppsagning, hyresgast)
    assert analys["rattelse_mojlig"] is True
    assert "tillsägelse" in analys["grund_info"]["rattelse_beskrivning"].lower()


def test_brottslig_verksamhet_ingen_rattelse() -> None:
    uppsagning = UpsagningsInfo(grund=ForverkandeGrund.BROTTSLIG_VERKSAMHET)
    hyresgast = HyresgastInfo()

    analys = analysera_uppsagning(uppsagning, hyresgast)
    assert analys["rattelse_mojlig"] is False
    assert analys["bedomning"] == "svag_position"


def test_bestridandebrev_genereras() -> None:
    hyresgast = HyresgastInfo(namn="Anna Andersson")
    uppsagning = UpsagningsInfo(
        datum_mottagen=date(2026, 3, 15),
        grund=ForverkandeGrund.OBETALD_HYRA,
    )

    brev = generera_bestridandebrev(
        hyresgast,
        uppsagning,
        ["Betalningen var försenad på grund av sjukdom."],
    )

    assert "Anna Andersson" in brev
    assert "bestrider" in brev.lower()
    assert "återvinningsfristen" in brev.lower()


def test_hyreshojning_over_10_procent() -> None:
    info = HyreshojningsInfo(
        nuvarande_hyra=8000,
        foreslagen_hyra=9500,
    )

    analys = analysera_hyreshojning(info)
    assert analys["bedomning"] == "stark_position"


def test_brist_ej_anmald() -> None:
    info = BristInfo(
        beskrivning="Fukt i badrummet",
        typ="fukt",
        anmald_till_hyresvard=False,
    )

    analys = analysera_brist(info)
    assert any("anmäl" in atgard.lower() for atgard in analys["atgarder"])


def test_engine_full_uppsagning() -> None:
    rag = FakeRAG(_rag_result())
    engine = HyresrattsEngine(rag=rag)

    resultat = engine.analysera(
        arende_typ=ArendeTyp.UPPSAGNING,
        uppsagning=UpsagningsInfo(
            datum_mottagen=date.today(),
            grund=ForverkandeGrund.OBETALD_HYRA,
        ),
        hyresgast=HyresgastInfo(namn="Test Testsson"),
    )

    assert resultat.arende_typ == ArendeTyp.UPPSAGNING
    assert resultat.genererat_brev is not None
    assert "bestrider" in resultat.genererat_brev.lower()
    assert len(resultat.relevanta_lagrum) > 0
    assert resultat.kallhanvisningar
    assert rag.calls[0][1] == "allman"
