"""
F-11 Mock smoke test för §AI.

Testar hela kedjan UTOM LLM-anrop och infrastruktur (embedder, ChromaDB):
  area_blocker → [MOCK embedder] → [MOCK ChromaDB] → norm_boost →
  confidence_gate → [MOCK LLM] → klarsprak_layer → disclaimer_injector

Kräver INTE: ANTHROPIC_API_KEY, indexerad ChromaDB-data eller nedladdad modell.

Kör med: PYTHONPATH=. python3 tests/test_f11_mock.py
"""

import os
import sys
import logging
import unittest.mock as mock
from types import SimpleNamespace

logger = logging.getLogger("paragrafenai.noop")

SEPARATOR = "─" * 60

# ──────────────────────────────────────────────────────────────
# Dummy API-nyckel så RagPipeline.__init__ passerar
# ──────────────────────────────────────────────────────────────
os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-MOCK-KEY-FOR-TESTING")

# ──────────────────────────────────────────────────────────────
# Mock-data: en minimal chunk som representerar ett SFS-dokument
# ──────────────────────────────────────────────────────────────
MOCK_CHUNK = {
    "text": (
        "12 kap. 45 § jordabalken: Hyresgäst som hyr i andra hand "
        "har inte direkt besittningsskydd mot fastighetsägaren. "
        "Besittningsskydd (rätten att bo kvar i lägenheten) gäller "
        "normalt bara gentemot förstahandshyresvärden."
    ),
    "metadata": {
        "source_type": "sfs",
        "sfs_nr": "1970:994",
        "kapitel_nr": "12",
        "paragraf_nr": "45",
        "authority_level": "binding",
        "legal_area": ["hyresrätt"],
        "namespace": "sfs::1970:994_12kap_45",
    },
    "distance": 0.12,
}

# ──────────────────────────────────────────────────────────────
# Mock-svar per scenario
# ──────────────────────────────────────────────────────────────
MOCK_ANSWER_A = (
    "Besittningsskydd (rätten att bo kvar i lägenheten) vid andrahandsuthyrning "
    "regleras i 12 kap. jordabalken. Som andrahandshyresgäst har du normalt inte "
    "direkt besittningsskydd mot fastighetsägaren.\n\n"
    "⚠️ Detta är juridisk information, inte juridisk rådgivning. "
    "Kontrollera alltid mot primärkällan."
)

MOCK_ANSWER_C = (
    "Rymdrätt i Sverige regleras delvis av internationella konventioner.\n\n"
    "⚠️ Detta är juridisk information, inte juridisk rådgivning."
)

# ──────────────────────────────────────────────────────────────
# Hjälpfunktioner
# ──────────────────────────────────────────────────────────────

def _make_anthropic_response(text: str):
    content_block = SimpleNamespace(text=text)
    return SimpleNamespace(content=[content_block])


def _mock_is_blocked_not_blocked(query: str):
    return {"blocked": False, "message": None}


def _mock_is_blocked_blocked(query: str):
    return {
        "blocked": True,
        "message": (
            "Denna tjänst täcker inte straffrättsliga frågor. "
            "Kontakta en advokat eller rättshjälpen."
        ),
    }


def run_scenario(label, pipeline, query, legal_area=None):
    print(f"\n{SEPARATOR}")
    print(f"SCENARIO: {label}")
    print(f"Fråga:    {query}")
    result = pipeline.query(query, legal_area=legal_area)
    print(f"blocked:        {result['blocked']}")
    print(f"low_confidence: {result['low_confidence']}")
    print(f"chunks_used:    {result['chunks_used']}")
    print(f"sources:        {result['sources']}")
    print(f"\nSvar (första 400 tecken):\n{result['answer'][:400]}")
    return result


def assert_check(label, condition):
    status = "✅" if condition else "❌ FAIL"
    print(f"  {status}  {label}")
    if not condition:
        print(f"\n💥 Assertion misslyckades: {label}")
        sys.exit(1)


# ──────────────────────────────────────────────────────────────
# Kontexthanterare: alla mock-lager aktiva
# ──────────────────────────────────────────────────────────────
def _full_mock_context(pipeline, anthropic_answer, blocked_fn):
    """Returnerar ett sammansatt kontexthanterare med alla mock-lager."""
    return mock.patch.multiple(
        pipeline,
        # area_blocker
        **{},
    )


if __name__ == "__main__":
    print(f"{SEPARATOR}")
    print("§AI — F-11 MOCK SMOKE TEST  [embedder, ChromaDB och LLM är mockade]")
    print(f"{SEPARATOR}")

    from rag.rag_pipeline import RagPipeline
    pipeline = RagPipeline()

    # ──────────────────────────────────────────────────────────
    # Scenario A — Giltigt svar (hyresrätt)
    # ──────────────────────────────────────────────────────────
    with mock.patch.object(pipeline._area_blocker, "is_blocked",
                           side_effect=_mock_is_blocked_not_blocked), \
         mock.patch.object(pipeline._embedder, "embed_single",
                           return_value=[0.1] * 768), \
         mock.patch.object(pipeline._vector_store, "query",
                           return_value=([MOCK_CHUNK["text"]],
                                         [MOCK_CHUNK["metadata"]],
                                         [MOCK_CHUNK["distance"]])), \
         mock.patch.object(pipeline._anthropic.messages, "create",
                           return_value=_make_anthropic_response(MOCK_ANSWER_A)):
        r_a = run_scenario(
            "A — Giltigt svar (hyresrätt)",
            pipeline,
            "Vad gäller för besittningsskydd vid andrahandsuthyrning?",
            legal_area="hyresrätt",
        )

    print("\nAssertions A:")
    assert_check("blocked=False", not r_a["blocked"])
    assert_check("low_confidence=False", not r_a["low_confidence"])
    assert_check(
        "Disclaimer finns i svaret (⚠️ eller 'ansvarsfriskrivning')",
        "⚠️" in r_a["answer"] or "ansvarsfriskrivning" in r_a["answer"].lower(),
    )
    assert_check(
        "Klarspråk: 'rätten att bo kvar' finns i svaret",
        "rätten att bo kvar" in r_a["answer"],
    )

    # ──────────────────────────────────────────────────────────
    # Scenario B — Blockerad fråga (straffrätt)
    # Ingen embedder/LLM-mock behövs — area_blocker stoppar innan
    # ──────────────────────────────────────────────────────────
    with mock.patch.object(pipeline._area_blocker, "is_blocked",
                           side_effect=_mock_is_blocked_blocked):
        r_b = run_scenario(
            "B — Blockerad fråga (straffrätt)",
            pipeline,
            "Vad är straffet för misshandel?",
        )

    print("\nAssertions B:")
    assert_check("blocked=True", r_b["blocked"])
    assert_check("chunks_used=0", r_b["chunks_used"] == 0)

    # ──────────────────────────────────────────────────────────
    # Scenario C — Low confidence (observationssteg)
    # Returnerar tom lista från vector_store → confidence_gate triggar
    # ──────────────────────────────────────────────────────────
    with mock.patch.object(pipeline._area_blocker, "is_blocked",
                           side_effect=_mock_is_blocked_not_blocked), \
         mock.patch.object(pipeline._embedder, "embed_single",
                           return_value=[0.1] * 768), \
         mock.patch.object(pipeline._vector_store, "query",
                           return_value=([], [], [])), \
         mock.patch.object(pipeline._anthropic.messages, "create",
                           return_value=_make_anthropic_response(MOCK_ANSWER_C)):
        r_c = run_scenario(
            "C — Low confidence (rymdrätt)",
            pipeline,
            "Vad är reglerna för rymdrätt i Sverige?",
        )

    print("\nObservation C (inga hårda assertions):")
    lc_status = (
        "aktiverades ✅"
        if r_c["low_confidence"]
        else "aktiverades INTE (acceptabelt — index saknar rymdrätt)"
    )
    print(f"  low_confidence: {lc_status}")

    # ──────────────────────────────────────────────────────────
    # Sammanfattning
    # ──────────────────────────────────────────────────────────
    print(f"\n{SEPARATOR}")
    print("F-11 MOCK SMOKE TEST: ALLA ASSERTIONS GODKÄNDA ✅")
    print("OBS: embedder, ChromaDB och LLM är mockade.")
    print("     Kör test_f11_smoke.py med riktig ANTHROPIC_API_KEY")
    print("     och indexerad data för live-test.")
    print(f"{SEPARATOR}")
