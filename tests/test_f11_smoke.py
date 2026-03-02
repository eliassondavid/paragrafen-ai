"""
F-11 End-to-end smoke test för §AI.
Kräver: ANTHROPIC_API_KEY i miljön, indexerad ChromaDB-data.
Kör med: PYTHONPATH=. python tests/test_f11_smoke.py
"""

import os
import sys
import logging

logger = logging.getLogger("paragrafenai.noop")

from rag.rag_pipeline import RagPipeline

SEPARATOR = "─" * 60


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


if __name__ == "__main__":
    print(f"{SEPARATOR}")
    print("§AI — F-11 END-TO-END SMOKE TEST")
    print(f"{SEPARATOR}")

    pipeline = RagPipeline()

    # ──────────────────────────────────────────────────────────
    # Scenario A — Giltigt svar (hyresrätt)
    # ──────────────────────────────────────────────────────────
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
    # ──────────────────────────────────────────────────────────
    r_b = run_scenario(
        "B — Blockerad fråga (straffrätt)",
        pipeline,
        "Vad är straffet för misshandel?",
    )
    print("\nAssertions B:")
    assert_check("blocked=True", r_b["blocked"])
    assert_check("chunks_used=0", r_b["chunks_used"] == 0)

    # ──────────────────────────────────────────────────────────
    # Scenario C — Low confidence (okänt område)
    # Inga hårda assertions — observationssteg
    # ──────────────────────────────────────────────────────────
    r_c = run_scenario(
        "C — Low confidence (rymdrätt)",
        pipeline,
        "Vad är reglerna för rymdrätt i Sverige?",
    )
    print("\nObservation C (inga hårda assertions):")
    lc_status = "aktiverades ✅" if r_c["low_confidence"] else "aktiverades INTE (acceptabelt om index saknar data)"
    print(f"  low_confidence: {lc_status}")

    # ──────────────────────────────────────────────────────────
    # Sammanfattning
    # ──────────────────────────────────────────────────────────
    print(f"\n{SEPARATOR}")
    print("F-11 SMOKE TEST: ALLA ASSERTIONS GODKÄNDA ✅")
    print(f"{SEPARATOR}")
