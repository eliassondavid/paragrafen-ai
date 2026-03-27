from __future__ import annotations

from dataclasses import replace
import logging
import os
from pathlib import Path
from typing import Any

import yaml

from guard.guard_pipeline import GuardPipeline
from rag.chroma_pool import ChromaClientPool, INSTANCE_TO_COLLECTION
from rag.models import RAGHit, RAGResult

logger = logging.getLogger(__name__)


class RAGQueryEngine:
    def __init__(
        self,
        config_path: str = "config/rag_config.yaml",
        chroma_base: str = "data/index/chroma",
    ):
        self.config = self._load_config(config_path)
        self.guard = GuardPipeline()
        self._pools = self._build_pools(chroma_base)

        if not os.environ.get("RAG_DRY_RUN"):
            from sentence_transformers import SentenceTransformer

            self.embedder = SentenceTransformer("KBLab/sentence-bert-swedish-cased")
        else:
            self.embedder = None

    def _load_config(self, config_path: str) -> dict[str, Any]:
        with Path(config_path).open(encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
        if not isinstance(payload, dict):
            raise ValueError("config/rag_config.yaml måste innehålla ett YAML-objekt")
        return payload

    def _build_pools(self, default_base: str) -> dict[str, ChromaClientPool]:
        default_pool = ChromaClientPool(default_base)
        pools: dict[str, ChromaClientPool] = {
            instance_key: default_pool
            for instance_key in INSTANCE_TO_COLLECTION
        }

        overrides = self.config.get("chroma_paths", {})
        if not isinstance(overrides, dict):
            overrides = {}

        override_pools: dict[str, ChromaClientPool] = {}
        for instance_key, override_path in overrides.items():
            if instance_key not in pools or not override_path:
                continue
            override_path = str(override_path)
            if override_path not in override_pools:
                override_pools[override_path] = ChromaClientPool(override_path)
            pools[instance_key] = override_pools[override_path]

        return pools

    def query(
        self,
        query: str,
        module: str,
        n_results: int | None = None,
        user_context: list[RAGHit] | None = None,
    ) -> RAGResult:
        blocked, message = self.guard.check_query(query)
        if blocked:
            return RAGResult(
                hits=[],
                confidence="low",
                disclaimer=message or self.guard.get_disclaimer(module, "low"),
                total_candidates=0,
                filtered_count=0,
            )

        if os.environ.get("RAG_DRY_RUN") == "1":
            return RAGResult(
                hits=[],
                confidence="low",
                disclaimer=self.guard.get_disclaimer(module, "low"),
                total_candidates=0,
                filtered_count=0,
            )

        if self.embedder is None:
            return RAGResult(
                hits=[],
                confidence="low",
                disclaimer=self.guard.get_disclaimer(module, "low"),
                total_candidates=0,
                filtered_count=0,
            )

        query_embedding = self.embedder.encode(query)
        modules = self.config["modules"]
        module_config = modules[module]

        defaults = self.config.get("defaults", {})
        per_collection_results = int(defaults.get("n_results_per_collection", 5))
        max_total_results = int(defaults.get("max_total_results", 15))
        effective_n_results = n_results if n_results is not None else max_total_results

        all_hits: list[RAGHit] = []

        for collection_config in module_config["collections"]:
            instance_key = collection_config["instance"]
            weight = float(collection_config.get("weight", 1.0))

            try:
                pool = self._pools[instance_key]
                collection = pool.get_collection(instance_key)
                result = collection.query(
                    query_embeddings=[query_embedding],
                    n_results=per_collection_results,
                )

                documents = (result.get("documents") or [[]])[0]
                metadatas = (result.get("metadatas") or [[]])[0]
                distances = (result.get("distances") or [[]])[0]

                for text, metadata, distance in zip(documents, metadatas, distances):
                    all_hits.append(
                        RAGHit(
                            text=str(text),
                            metadata=metadata if isinstance(metadata, dict) else {},
                            distance=float(distance),
                            collection=instance_key,
                            weight=weight,
                        )
                    )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Kunde inte läsa collection %s: %s", instance_key, exc)
                continue

        if user_context:
            for hit in user_context:
                all_hits.append(replace(hit, weight=0.5))

        filtered_hits = self.guard.filter_hits(all_hits)
        ranked = self._rank(filtered_hits, module_config)
        top_hits = ranked[:effective_n_results]
        confidence = self.guard.assess_confidence(top_hits)
        disclaimer = self.guard.get_disclaimer(module, confidence)

        return RAGResult(
            hits=top_hits,
            confidence=confidence,
            disclaimer=disclaimer,
            total_candidates=len(all_hits),
            filtered_count=len(filtered_hits),
        )

    def _rank(self, hits: list[RAGHit], module_config: dict[str, Any]) -> list[RAGHit]:
        authority_boost = module_config.get(
            "authority_boost",
            {
                "binding": 2.0,
                "guiding": 1.5,
                "preparatory": 1.0,
                "persuasive": 0.8,
                "none": 0.3,
            },
        )
        for hit in hits:
            auth = hit.metadata.get("authority_level", "persuasive")
            boost = authority_boost.get(auth, 0.8)
            hit.score = max(0.0, 1 - hit.distance) * boost * hit.weight
        return sorted(hits, key=lambda current_hit: current_hit.score, reverse=True)
