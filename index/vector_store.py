"""Chroma vector store setup and query utilities for F4."""

from __future__ import annotations

import logging
import uuid
from pathlib import Path
from typing import Any

import chromadb
import yaml

logger = logging.getLogger("paragrafenai.noop")


class ChromaVectorStore:
    """Wrapper around a persistent ChromaDB instance."""

    def __init__(self, config_path: str | Path = "config/embedding_config.yaml") -> None:
        self.repo_root = Path(__file__).resolve().parent.parent
        self.config_path = self._resolve_path(config_path)
        self.config = self._load_config(self.config_path)

        chroma_cfg = self.config.get("chroma", {})
        self.collection_names: dict[str, str] = dict(chroma_cfg.get("collections", {}))
        persistent_path = chroma_cfg["persistent_path"]
        self.persistent_path = self._resolve_path(persistent_path)
        self.persistent_path.mkdir(parents=True, exist_ok=True)

        try:
            self.client = chromadb.PersistentClient(path=str(self.persistent_path))
        except Exception as exc:
            logger.error("Kunde inte initiera Chroma PersistentClient: %s", exc)
            raise

        self._create_default_collections()

    def _resolve_path(self, path_value: str | Path) -> Path:
        candidate = Path(path_value)
        if candidate.is_absolute():
            return candidate
        return self.repo_root / candidate

    def _load_config(self, config_path: Path) -> dict[str, Any]:
        with config_path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        return data

    def _create_default_collections(self) -> None:
        for collection_name in self.collection_names.values():
            try:
                self.client.get_or_create_collection(name=collection_name)
            except Exception as exc:
                logger.error("Kunde inte skapa/hämta collection %s: %s", collection_name, exc)

    def _get_or_create_collection(self, collection_name: str):
        resolved_name = self.collection_names.get(collection_name, collection_name)
        return self.client.get_or_create_collection(name=resolved_name)

    def add_chunks(
        self,
        collection_name: str,
        chunks: list[str],
        embeddings: list[list[float]],
        metadatas: list[dict[str, Any]],
    ) -> int:
        """Add chunks to Chroma in batches of max 500 rows per call."""
        if not (len(chunks) == len(embeddings) == len(metadatas)):
            logger.error("add_chunks fick olika längder: chunks=%s, embeddings=%s, metadatas=%s", len(chunks), len(embeddings), len(metadatas))
            return 0

        collection = self._get_or_create_collection(collection_name)
        added = 0
        for start in range(0, len(chunks), 500):
            end = start + 500
            batch_chunks = chunks[start:end]
            batch_embeddings = embeddings[start:end]
            batch_metadatas = metadatas[start:end]
            ids: list[str] = []
            for metadata in batch_metadatas:
                namespace = str(metadata.get("namespace", "")).strip()
                ids.append(namespace if namespace else uuid.uuid4().hex)

            try:
                collection.add(
                    ids=ids,
                    documents=batch_chunks,
                    embeddings=batch_embeddings,
                    metadatas=batch_metadatas,
                )
                added += len(batch_chunks)
            except Exception as exc:
                logger.error("Fel vid add() till collection %s: %s", collection_name, exc)
        return added

    def query(
        self,
        collection_name: str,
        query_embedding: list[float],
        n_results: int = 10,
        where_filter: dict[str, Any] | None = None,
    ) -> tuple[list[str], list[dict[str, Any]], list[float]]:
        """Run an embedding query and return documents, metadatas and distances."""
        collection = self._get_or_create_collection(collection_name)
        try:
            result = collection.query(
                query_embeddings=[query_embedding],
                n_results=n_results,
                where=where_filter,
                include=["documents", "metadatas", "distances"],
            )
        except Exception as exc:
            logger.error("Fel vid query mot collection %s: %s", collection_name, exc)
            return [], [], []

        documents = result.get("documents", [[]])[0] or []
        metadatas = result.get("metadatas", [[]])[0] or []
        distances = result.get("distances", [[]])[0] or []
        return documents, metadatas, distances

    def source_id_exists(self, collection_name: str, source_id: str) -> bool:
        """Check if a source_id already exists in the target collection."""
        collection = self._get_or_create_collection(collection_name)
        try:
            result = collection.get(where={"source_id": source_id}, limit=1)
            return bool(result.get("ids"))
        except Exception as exc:
            logger.error("Fel vid source_id-kontroll (%s): %s", source_id, exc)
            return False

    def get_one_metadata(self, collection_name: str, where_filter: dict[str, Any]) -> dict[str, Any] | None:
        collection = self._get_or_create_collection(collection_name)
        try:
            result = collection.get(where=where_filter, limit=1, include=["metadatas"])
        except Exception as exc:
            logger.error("Fel vid metadata-hämtning: %s", exc)
            return None

        metadatas = result.get("metadatas") or []
        if not metadatas:
            return None
        return metadatas[0]

    def get_collection_stats(self) -> dict[str, int]:
        """Return collection row counts for configured collections."""
        stats: dict[str, int] = {}
        for key, collection_name in self.collection_names.items():
            try:
                collection = self.client.get_collection(collection_name)
                stats[collection_name] = collection.count()
            except Exception as exc:
                logger.error("Kunde inte läsa count för %s (%s): %s", key, collection_name, exc)
                stats[collection_name] = 0
        return stats
