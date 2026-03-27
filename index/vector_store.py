"""Chroma vector store setup and query utilities for F4."""

from __future__ import annotations

import logging
import uuid
from pathlib import Path
from typing import Any

import chromadb
import yaml

logger = logging.getLogger("paragrafenai.noop")


class ConfigurationError(ValueError):
    """Raised when the Chroma configuration is invalid for the requested setup."""


class ChromaVectorStore:
    """Wrapper around a persistent ChromaDB instance.

    Supports two config formats in embedding_config.yaml:

    New (chroma.instances):
        chroma:
          instances:
            prop:
              path: "data/index/chroma/prop"
              collection: "paragrafen_prop_v1"
            sou:
              path: "data/index/chroma/sou"
              collection: "paragrafen_sou_v1"

    Legacy (chroma.persistent_path + collections):
        chroma:
          persistent_path: "data/index/chroma"
          collections:
            forarbete: "paragrafen_forarbete_v1"
    """

    def __init__(
        self,
        config_path: str | Path = "config/embedding_config.yaml",
        instance_key: str | None = None,
    ) -> None:
        self.repo_root = Path(__file__).resolve().parent.parent
        self.config_path = self._resolve_path(config_path)
        self.config = self._load_config(self.config_path)
        self.instance_key = instance_key

        chroma_cfg = self.config.get("chroma", {})

        # New instances format takes precedence
        instances = chroma_cfg.get("instances")
        if instances and isinstance(instances, dict):
            self._init_from_instances(instances, instance_key=instance_key)
        elif "persistent_path" in chroma_cfg:
            if instance_key is not None:
                raise ConfigurationError(
                    "instance_key kan bara användas när embedding_config.yaml "
                    "har chroma.instances konfigurerat."
                )
            self._init_from_legacy(chroma_cfg)
        else:
            raise ConfigurationError(
                "embedding_config.yaml saknar både chroma.instances och "
                "chroma.persistent_path — kan inte initiera ChromaVectorStore."
            )

    def _init_from_instances(self, instances: dict[str, Any], *, instance_key: str | None) -> None:
        """Initialize from chroma.instances format — one PersistentClient per instance."""
        self.clients: dict[str, Any] = {}
        self.collection_names: dict[str, str] = {}

        selected_instances = instances
        if instance_key is not None:
            if instance_key not in instances:
                available_keys = ", ".join(sorted(instances))
                raise ConfigurationError(
                    f"Okänd chroma.instances-nyckel: {instance_key!r}. "
                    f"Tillgängliga nycklar: {available_keys}."
                )
            selected_instances = {instance_key: instances[instance_key]}

        for key, inst_cfg in selected_instances.items():
            path = self._resolve_path(inst_cfg["path"])
            path.mkdir(parents=True, exist_ok=True)
            collection_name = inst_cfg["collection"]

            try:
                client = chromadb.PersistentClient(path=str(path))
                client.get_or_create_collection(name=collection_name)
            except Exception as exc:
                logger.error("Kunde inte initiera Chroma-instans %s: %s", key, exc)
                raise

            self.clients[collection_name] = client
            self.collection_names[key] = collection_name

        # Set self.client to first instance for backward compat
        if self.clients:
            self.client = next(iter(self.clients.values()))
        else:
            raise ConfigurationError("Inga chroma.instances konfigurerade.")

    def _init_from_legacy(self, chroma_cfg: dict[str, Any]) -> None:
        """Initialize from legacy persistent_path + collections format."""
        self.collection_names = dict(chroma_cfg.get("collections", {}))
        persistent_path = self._resolve_path(chroma_cfg["persistent_path"])
        persistent_path.mkdir(parents=True, exist_ok=True)

        try:
            self.client = chromadb.PersistentClient(path=str(persistent_path))
        except Exception as exc:
            logger.error("Kunde inte initiera Chroma PersistentClient: %s", exc)
            raise

        self.clients = {name: self.client for name in self.collection_names.values()}
        self._create_default_collections()

    def _build_embedding_function(self) -> Any | None:
        """Embeddings hanteras av Embedder-klassen — inte av ChromaDB."""
        return None

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

    def _get_client_for_collection(self, collection_name: str) -> Any:
        """Resolve which PersistentClient owns a given collection name."""
        resolved = self.collection_names.get(collection_name, collection_name)
        client = self.clients.get(resolved)
        if client is not None:
            return client
        # Fallback: try self.client (legacy single-instance)
        return self.client

    def _get_or_create_collection(self, collection_name: str):
        resolved_name = self.collection_names.get(collection_name, collection_name)
        client = self._get_client_for_collection(collection_name)
        return client.get_or_create_collection(name=resolved_name)

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
                client = self._get_client_for_collection(collection_name)
                collection = client.get_collection(collection_name)
                stats[collection_name] = collection.count()
            except Exception as exc:
                logger.error("Kunde inte läsa count för %s (%s): %s", key, collection_name, exc)
                stats[collection_name] = 0
        return stats
