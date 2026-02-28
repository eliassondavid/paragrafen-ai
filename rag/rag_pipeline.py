"""
rag/rag_pipeline.py
§AI (paragrafen.ai) — F-8 RAG-pipeline

Kopplar ihop guard, embedder, vector store, norm_boost,
confidence_gate och disclaimer_injector till ett komplett
frågesvarssystem.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import anthropic
import yaml

from guard.area_blocker import AreaBlocker
from guard.confidence_gate import ConfidenceGate
from index.embedder import Embedder
from index.norm_boost import NormBoost
from index.vector_store import ChromaVectorStore
from normalize.klarsprak_layer import KlarsprakLayer
from publish.disclaimer_injector import DisclaimerInjector

logger = logging.getLogger("paragrafenai.noop")

LOW_CONFIDENCE_ANSWER = (
    "Jag hittade inte tillräckligt med relevant information för att besvara "
    "din fråga med tillräcklig säkerhet. Försök omformulera frågan eller "
    "kontakta en jurist för rådgivning."
)

_SYSTEM_PROMPT = (
    "Du är §AI, en juridisk AI-assistent för allmänheten i Sverige.\n"
    "Besvara frågan baserat ENBART på de angivna källorna.\n"
    "Skriv på svenska i klarspråk. Förklara juridiska termer vid första användning.\n"
    "Om källorna inte ger tillräckligt underlag: säg det tydligt.\n"
    "Avsluta alltid med en källförteckning."
)


def _extract_source_ref(chunk: dict) -> str | None:
    """Extrahera en läsbar källreferens ur chunk-metadata."""
    meta = chunk.get("metadata", {})
    source_type = meta.get("source_type", "")

    if source_type == "sfs":
        sfs_nr = meta.get("sfs_nr", "")
        paragraf = meta.get("paragraf_nr", "")
        kapitel = meta.get("kapitel_nr", "")
        if kapitel and paragraf:
            return f"SFS {sfs_nr} {kapitel} kap. {paragraf} §"
        elif paragraf:
            return f"SFS {sfs_nr} {paragraf} §"
        return f"SFS {sfs_nr}"

    elif source_type == "praxis":
        return meta.get("citation", meta.get("namespace", ""))

    elif source_type == "forarbete":
        return meta.get("beteckning", meta.get("namespace", ""))

    elif source_type == "doktrin":
        return meta.get("citation_format", meta.get("namespace", ""))

    return meta.get("namespace") or None


def _build_context(chunks: list[dict]) -> str:
    """Bygg en kontextsträng av chunks för LLM-prompten."""
    parts: list[str] = []
    for i, chunk in enumerate(chunks, start=1):
        source_ref = _extract_source_ref(chunk) or f"källa {i}"
        text = chunk.get("text", "").strip()
        parts.append(f"[{i}] {source_ref}\n{text}")
    return "\n\n".join(parts)


class RagPipeline:
    """Komplett RAG-pipeline för §AI."""

    def __init__(self, config_path: str = "config/rag_config.yaml") -> None:
        cfg_file = Path(config_path)
        with cfg_file.open(encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
        cfg: dict[str, Any] = raw.get("rag", {})

        self._config_dir: str = str(cfg_file.parent)
        self._top_k: int = cfg.get("top_k", 10)
        self._llm_model: str = cfg.get("llm_model", "claude-opus-4-6")
        self._llm_max_tokens: int = cfg.get("llm_max_tokens", 2048)
        self._collections: list[str] = cfg.get(
            "collections", ["paragrafen_sfs_v1", "paragrafen_forarbete_v1"]
        )

        self._area_blocker = AreaBlocker()
        self._embedder = Embedder(config_path=config_path)
        self._vector_store = ChromaVectorStore(config_path=config_path)
        self._norm_boost = NormBoost()
        self._confidence_gate = ConfidenceGate()
        self._klarsprak = KlarsprakLayer(config_dir=self._config_dir)
        self._disclaimer_injector = DisclaimerInjector()

        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise KeyError(
                "Miljövariabeln ANTHROPIC_API_KEY saknas. "
                "Sätt den innan RagPipeline instansieras."
            )
        self._anthropic = anthropic.Anthropic(api_key=api_key)

        logger.debug("RagPipeline initialiserad med modell=%s top_k=%d", self._llm_model, self._top_k)

    # ------------------------------------------------------------------
    # Publik metod
    # ------------------------------------------------------------------

    def query(
        self,
        user_query: str,
        legal_area: str | None = None,
        top_k: int | None = None,
    ) -> dict:
        """
        Kör hela RAG-pipeline för en användarfråga.

        Returnerar ett dict med nycklarna:
            answer, blocked, blocked_message, sources,
            confidence, chunks_used, low_confidence
        """
        effective_top_k = top_k if top_k is not None else self._top_k

        # ── Steg 1: blockkontroll ────────────────────────────────────
        block_result = self._area_blocker.is_blocked(user_query)
        if block_result.get("blocked", False):
            logger.info("Fråga blockerad: %s", user_query[:80])
            return {
                "answer": block_result.get("message", ""),
                "blocked": True,
                "blocked_message": block_result.get("message"),
                "sources": [],
                "confidence": {},
                "chunks_used": 0,
                "low_confidence": False,
            }

        # ── Steg 2: embedding ────────────────────────────────────────
        query_vector: list[float] = self._embedder.embed(user_query)

        # ── Steg 3: hämta råchunks från alla collections ─────────────
        where: dict | None = None
        if legal_area:
            where = {"legal_area": {"$contains": legal_area}}

        raw_chunks: list[dict] = []
        for collection in self._collections:
            try:
                results = self._vector_store.query(
                    collection_name=collection,
                    query_embedding=query_vector,
                    n_results=effective_top_k,
                    where=where,
                )
                raw_chunks.extend(results)
            except Exception as exc:  # noqa: BLE001
                logger.warning("VectorStore query misslyckades för %s: %s", collection, exc)

        # ── Steg 4: omrangordna ──────────────────────────────────────
        reranked_chunks: list[dict] = self._norm_boost.rerank(raw_chunks)

        # ── Steg 5: confidence-kontroll ──────────────────────────────
        confidence_result: dict = self._confidence_gate.evaluate(reranked_chunks)
        if not confidence_result.get("pass", True):
            logger.info("Low confidence — returnerar standardsvar")
            return {
                "answer": LOW_CONFIDENCE_ANSWER,
                "blocked": False,
                "blocked_message": None,
                "sources": [],
                "confidence": confidence_result,
                "chunks_used": len(reranked_chunks),
                "low_confidence": True,
            }

        # ── Steg 6: extrahera källreferenser ─────────────────────────
        sources: list[str] = []
        for chunk in reranked_chunks:
            ref = _extract_source_ref(chunk)
            if ref and ref not in sources:
                sources.append(ref)

        # ── Steg 7: bygg LLM-prompt ──────────────────────────────────
        context = _build_context(reranked_chunks)
        user_message = (
            f"KÄLLOR:\n{context}\n\n"
            f"FRÅGA: {user_query}"
        )

        # ── Steg 8: anropa Anthropic API ─────────────────────────────
        response = self._anthropic.messages.create(
            model=self._llm_model,
            max_tokens=self._llm_max_tokens,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )
        llm_answer: str = response.content[0].text

        # ── Steg 9: klarspråkspostprocess ────────────────────────────
        if hasattr(self, "_klarsprak"):
            clarified_answer: str = self._klarsprak.process(
                answer=llm_answer,
                query=user_query,
                legal_area=legal_area,
            )
        else:
            clarified_answer = llm_answer

        # ── Steg 10: injicera disclaimer ─────────────────────────────
        final_answer: str = self._disclaimer_injector.inject(
            clarified_answer, sources=sources
        )

        logger.info("Fråga besvarad. chunks_used=%d sources=%d", len(reranked_chunks), len(sources))

        return {
            "answer": final_answer,
            "blocked": False,
            "blocked_message": None,
            "sources": sources,
            "confidence": confidence_result,
            "chunks_used": len(reranked_chunks),
            "low_confidence": False,
        }
