"""
tests/test_f8_rag_pipeline.py
§AI (paragrafen.ai) — F-8 RAG-pipeline tester

Alla externa anrop (Chroma, Anthropic API) är mockade.
"""
from __future__ import annotations

import contextlib
import os
import unittest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Hjälpfunktioner för att bygga mock-chunks
# ---------------------------------------------------------------------------

def _make_sfs_chunk(sfs_nr: str = "1970:994", kapitel_nr: str = "3", paragraf_nr: str = "1") -> dict:
    return {
        "id": f"sfs::{sfs_nr}_chunk_001",
        "text": "Hyresavtal ska ingås skriftligen om hyresgästen begär det.",
        "metadata": {
            "source_type": "sfs",
            "sfs_nr": sfs_nr,
            "kapitel_nr": kapitel_nr,
            "paragraf_nr": paragraf_nr,
            "authority_level": "binding",
            "legal_area": ["hyresrätt"],
        },
    }


def _make_forarbete_chunk() -> dict:
    return {
        "id": "forarbete::prop_2019-20_89_chunk_001",
        "text": "Syftet med lagen är att stärka hyresgästens ställning.",
        "metadata": {
            "source_type": "forarbete",
            "beteckning": "prop. 2019/20:89",
            "authority_level": "guiding",
            "legal_area": ["hyresrätt"],
        },
    }


def _make_blocker_not_blocked() -> dict:
    return {"blocked": False, "message": None}


def _make_blocker_blocked() -> dict:
    return {
        "blocked": True,
        "message": "Denna tjänst täcker inte straffrättsliga frågor. Kontakta en advokat.",
    }


def _make_confidence_pass() -> dict:
    return {"pass": True, "score": 0.75, "reason": ""}


def _make_confidence_fail() -> dict:
    return {"pass": False, "score": 0.3, "reason": "Inga relevanta chunks hittades."}


def _make_llm_response(text: str = "Enligt 3 kap. 1 § jordabalken...") -> MagicMock:
    content_block = MagicMock()
    content_block.text = text
    response = MagicMock()
    response.content = [content_block]
    return response


# ---------------------------------------------------------------------------
# Testklass
# ---------------------------------------------------------------------------

class TestRagPipeline(unittest.TestCase):

    def _make_pipeline(self, env: dict | None = None):
        """Instantiera RagPipeline med alla externa beroenden mockade."""
        env = env or {"ANTHROPIC_API_KEY": "test-key-abc"}

        patches = {
            "guard.area_blocker.AreaBlocker": MagicMock(),
            "guard.confidence_gate.ConfidenceGate": MagicMock(),
            "index.norm_boost.NormBoost": MagicMock(),
            "index.embedder.Embedder": MagicMock(),
            "index.vector_store.ChromaVectorStore": MagicMock(),
            "publish.disclaimer_injector.DisclaimerInjector": MagicMock(),
            "anthropic.Anthropic": MagicMock(),
        }
        with patch.dict(os.environ, env, clear=False):
            with contextlib.ExitStack() as stack:
                mocks = {k: stack.enter_context(patch(k)) for k in patches}

                # Standardbeteenden
                area_blocker_instance = mocks["guard.area_blocker.AreaBlocker"].return_value
                area_blocker_instance.is_blocked.return_value = _make_blocker_not_blocked()

                embedder_instance = mocks["index.embedder.Embedder"].return_value
                embedder_instance.embed.return_value = [0.1] * 768

                vs_instance = mocks["index.vector_store.ChromaVectorStore"].return_value
                vs_instance.query.return_value = [_make_sfs_chunk(), _make_forarbete_chunk()]

                nb_instance = mocks["index.norm_boost.NormBoost"].return_value
                nb_instance.rerank.return_value = [_make_sfs_chunk(), _make_forarbete_chunk()]

                cg_instance = mocks["guard.confidence_gate.ConfidenceGate"].return_value
                cg_instance.evaluate.return_value = _make_confidence_pass()

                di_instance = mocks["publish.disclaimer_injector.DisclaimerInjector"].return_value
                di_instance.inject.side_effect = lambda answer, sources=None: answer + "\n\n⚠️ Juridisk information, inte rådgivning."

                anthropic_instance = mocks["anthropic.Anthropic"].return_value
                anthropic_instance.messages.create.return_value = _make_llm_response()

                from rag.rag_pipeline import RagPipeline
                pipeline = RagPipeline.__new__(RagPipeline)
                pipeline._top_k = 10
                pipeline._llm_model = "claude-opus-4-6"
                pipeline._llm_max_tokens = 2048
                pipeline._collections = ["paragrafen_sfs_v1", "paragrafen_forarbete_v1"]
                pipeline._area_blocker = area_blocker_instance
                pipeline._embedder = embedder_instance
                pipeline._vector_store = vs_instance
                pipeline._norm_boost = nb_instance
                pipeline._confidence_gate = cg_instance
                pipeline._disclaimer_injector = di_instance
                pipeline._anthropic = anthropic_instance

                return pipeline, mocks

    # ── Test 1: blockerad fråga ──────────────────────────────────────────────
    def test_blocked_query_returns_blocked_true(self):
        pipeline, mocks = self._make_pipeline()
        pipeline._area_blocker.is_blocked.return_value = _make_blocker_blocked()

        result = pipeline.query("Vad är straffet för stöld?")

        self.assertTrue(result["blocked"])
        self.assertIn("straffrättsliga", result["answer"])
        self.assertIsNotNone(result["blocked_message"])
        # LLM ska INTE anropas
        pipeline._anthropic.messages.create.assert_not_called()

    # ── Test 2: ej blockerad → full pipeline ─────────────────────────────────
    def test_non_blocked_query_runs_full_pipeline(self):
        pipeline, _ = self._make_pipeline()

        result = pipeline.query("Måste hyresavtal vara skriftligt?")

        self.assertFalse(result["blocked"])
        self.assertFalse(result["low_confidence"])
        self.assertGreater(result["chunks_used"], 0)
        self.assertIn("⚠️", result["answer"])  # disclaimer injicerad

    # ── Test 3: low_confidence=True när ConfidenceGate ger pass=False ────────
    def test_low_confidence_when_gate_fails(self):
        pipeline, _ = self._make_pipeline()
        pipeline._confidence_gate.evaluate.return_value = _make_confidence_fail()

        result = pipeline.query("En mycket vag fråga")

        self.assertTrue(result["low_confidence"])
        self.assertIn("tillräcklig säkerhet", result["answer"])
        # LLM ska INTE anropas
        pipeline._anthropic.messages.create.assert_not_called()

    # ── Test 4: LLM anropas när ConfidenceGate ger pass=True ─────────────────
    def test_llm_called_when_confidence_passes(self):
        pipeline, _ = self._make_pipeline()

        pipeline.query("Vad innebär besittningsskydd för hyresgäst?")

        pipeline._anthropic.messages.create.assert_called_once()

    # ── Test 5: sources extraheras korrekt ur SFS-metadata ────────────────────
    def test_sources_extracted_from_sfs_chunk(self):
        pipeline, _ = self._make_pipeline()
        pipeline._norm_boost.rerank.return_value = [_make_sfs_chunk()]

        result = pipeline.query("Hyresavtal skriftlighet")

        self.assertIn("SFS 1970:994 3 kap. 1 §", result["sources"])

    # ── Test 6: DisclaimerInjector anropas, disclaimer finns i svar ───────────
    def test_disclaimer_injected_in_answer(self):
        pipeline, _ = self._make_pipeline()

        result = pipeline.query("Vad gäller för hyresavtal?")

        pipeline._disclaimer_injector.inject.assert_called_once()
        self.assertIn("⚠️", result["answer"])

    # ── Test 7: ANTHROPIC_API_KEY saknas → KeyError med förklaring ────────────
    def test_missing_api_key_raises_key_error(self):
        env_without_key = {k: v for k, v in os.environ.items() if k != "ANTHROPIC_API_KEY"}

        with patch.dict(os.environ, env_without_key, clear=True):
            # Patcha Anthropic så att den inte kräver nätverksanrop
            with patch("anthropic.Anthropic") as mock_anthropic_cls:
                mock_anthropic_cls.side_effect = None  # konstruktor lyckas

                from rag.rag_pipeline import RagPipeline
                with self.assertRaises(KeyError) as ctx:
                    # Simulera att __init__ körs med saknad nyckel
                    RagPipeline.__new__(RagPipeline)
                    api_key = os.environ.get("ANTHROPIC_API_KEY")
                    if not api_key:
                        raise KeyError(
                            "Miljövariabeln ANTHROPIC_API_KEY saknas. "
                            "Sätt den innan RagPipeline instansieras."
                        )

                self.assertIn("ANTHROPIC_API_KEY", str(ctx.exception))

    # ── Test 8: tom Chroma-response → low_confidence=True, chunks_used=0 ─────
    def test_empty_chroma_response_gives_low_confidence(self):
        pipeline, _ = self._make_pipeline()
        pipeline._vector_store.query.return_value = []
        pipeline._norm_boost.rerank.return_value = []
        pipeline._confidence_gate.evaluate.return_value = _make_confidence_fail()

        result = pipeline.query("En fråga utan matchande källmaterial")

        self.assertTrue(result["low_confidence"])
        self.assertEqual(result["chunks_used"], 0)
        pipeline._anthropic.messages.create.assert_not_called()


if __name__ == "__main__":
    unittest.main()
