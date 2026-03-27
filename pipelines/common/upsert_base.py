"""Shared Chroma upsert primitives for forarbete pipelines."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import logging
from pathlib import Path
from typing import Any

import chromadb

from pipelines.common.normalize_base import NormalizedChunk

logger = logging.getLogger("paragrafenai.noop")


@dataclass
class UpsertResult:
    upserted: int = 0
    skipped: int = 0
    errors: int = 0


class ForarbeteUpserter:
    """Idempotent upsert till ChromaDB. Stödjer tombstone."""

    def __init__(
        self,
        collection_name: str,
        chroma_path: str,
        config_path: str | Path = "config/embedding_config.yaml",
    ):
        self.config_path = config_path
        self.store = None
        self.collection_name = collection_name
        self.chroma_path = str(chroma_path)
        self.client: chromadb.PersistentClient | None = None
        self.collection = None

        try:
            self.client = chromadb.PersistentClient(path=self.chroma_path)
            self.collection = self.client.get_or_create_collection(
                name=self.collection_name,
                metadata={"hnsw:space": "cosine"},
            )
        except Exception as exc:
            logger.error("Kunde inte initiera Chroma-klient för %s: %s", self.collection_name, exc)

    def upsert_chunks(
        self,
        chunks: list[NormalizedChunk],
        embeddings: list[list[float]],
        *,
        dry_run: bool = False,
    ) -> UpsertResult:
        """
        Upserta chunks till ChromaDB.
        - Idempotent: samma chunk_id kan upserteras flera gånger
        - dry_run=True: räkna utan att skriva
        """
        result = UpsertResult()
        if len(chunks) != len(embeddings):
            logger.error("Mismatch mellan antal chunks (%s) och embeddings (%s).", len(chunks), len(embeddings))
            result.errors += 1
            return result

        if not chunks:
            return result

        texts = [chunk.chunk_text for chunk in chunks]
        metadatas = [self._chunk_to_metadata(chunk) for chunk in chunks]
        ids = [chunk.namespace or chunk.chunk_id for chunk in chunks]

        if dry_run:
            result.upserted = len(chunks)
            return result

        collection = self._get_collection()
        if collection is None:
            result.errors += len(chunks)
            return result

        try:
            collection.upsert(
                ids=ids,
                documents=texts,
                embeddings=embeddings,
                metadatas=metadatas,
            )
            result.upserted = len(chunks)
        except Exception as exc:
            logger.error("Fel vid upsert till collection %s: %s", self.collection_name, exc)
            result.errors += len(chunks)

        return result

    def tombstone(
        self,
        source_document_id: str,
        *,
        superseded_by: str | None = None,
    ) -> int:
        """
        Markera alla chunks för ett dokument som inaktiva.
        Uppdatera is_active=False, status="withdrawn".
        Returnerar antal berörda chunks.
        """
        collection = self._get_collection()
        if collection is None:
            return 0

        try:
            result = collection.get(
                where={"source_document_id": source_document_id},
                include=["metadatas"],
            )
        except Exception as exc:
            logger.error("Kunde inte läsa tombstone-underlag för %s: %s", source_document_id, exc)
            return 0

        ids = result.get("ids") or []
        metadatas = result.get("metadatas") or []
        if not ids or not metadatas:
            return 0

        updated_metadatas: list[dict[str, Any]] = []
        for metadata in metadatas:
            row = dict(metadata or {})
            row["is_active"] = False
            row["status"] = "withdrawn"
            if superseded_by:
                row["superseded_by"] = superseded_by
            updated_metadatas.append(row)

        try:
            collection.update(ids=ids, metadatas=updated_metadatas)
        except Exception as exc:
            logger.error("Kunde inte tombstone-markera %s: %s", source_document_id, exc)
            return 0
        return len(ids)

    def namespace_exists(self, namespace: str) -> bool:
        """Kolla om namespace redan finns i collection."""
        if not namespace.strip():
            return False

        collection = self._get_collection()
        if collection is None:
            return False

        try:
            result = collection.get(ids=[namespace], include=[])
            ids = result.get("ids") or []
            return bool(ids)
        except Exception:
            pass

        store = self._get_store()
        if store is None:
            return False

        try:
            metadata = store.get_one_metadata(
                collection_name=self.collection_name,
                where_filter={"namespace": namespace},
            )
        except Exception as exc:
            logger.error("Namespace-kontroll misslyckades för %s: %s", namespace, exc)
            return False
        return metadata is not None

    def _get_store(self):
        if self.store is not None:
            return self.store

        try:
            from index.vector_store import ChromaVectorStore

            self.store = ChromaVectorStore(config_path=self.config_path)
        except Exception as exc:
            logger.error(
                "Kunde inte initiera ChromaVectorStore för %s: %s",
                self.collection_name,
                exc,
            )
            return None
        return self.store

    def _get_collection(self):
        if self.collection is not None:
            return self.collection
        if self.client is None:
            return None
        try:
            self.collection = self.client.get_or_create_collection(
                name=self.collection_name,
                metadata={"hnsw:space": "cosine"},
            )
        except Exception as exc:
            logger.error("Kunde inte hämta collection %s: %s", self.collection_name, exc)
            return None
        return self.collection

    def _chunk_to_metadata(self, chunk: NormalizedChunk) -> dict[str, Any]:
        payload = asdict(chunk)
        extra_metadata = payload.pop("extra_metadata", {}) or {}
        payload.update(extra_metadata)
        payload.setdefault("status", "active")
        return payload


__all__ = [
    "ForarbeteUpserter",
    "UpsertResult",
]
