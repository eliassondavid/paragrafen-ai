from __future__ import annotations

import logging
import os
from pathlib import Path
import threading
import time

import chromadb


INSTANCE_TO_COLLECTION = {
    "prop": "paragrafen_prop_v1",
    "sou": "paragrafen_sou_v1",
    "bet": "paragrafen_bet_v1",
    "ds": "paragrafen_ds_v1",
    "riksdag": "paragrafen_riksdag_v1",
    "praxis": "paragrafen_praxis_v1",
    "doktrin": "paragrafen_doktrin_v1",
    "sfs": "paragrafen_sfs_v1",
    "jo": "paragrafen_jo_v1",
    "jk": "paragrafen_jk_v1",
    "namnder": "paragrafen_namnder_v1",
    "foreskrift": "paragrafen_foreskrift_v1",
    "upphandling": "paragrafen_upphandling_v1",
    "fmakt": "paragrafen_fmakt_v1",
}

logger = logging.getLogger(__name__)


class CollectionNotFoundError(Exception):
    pass


class MockCollection:
    def __init__(self, instance_key: str, name: str) -> None:
        self.instance_key = instance_key
        self.name = name
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def query(self, *args: object, **kwargs: object) -> dict[str, list[list[object]]]:
        self.calls.append((args, kwargs))
        return {
            "ids": [[]],
            "documents": [[]],
            "metadatas": [[]],
            "distances": [[]],
        }


class ChromaClientPool:
    MAX_OPEN = 4
    TTL_SECONDS = 300

    def __init__(self, chroma_base_path: str = "data/index/chroma"):
        self.base_path = Path(chroma_base_path)
        self._clients: dict[str, tuple[chromadb.PersistentClient, float]] = {}
        self._lock = threading.Lock()

    def get_collection(self, instance_key: str) -> "chromadb.Collection":
        with self._lock:
            self._evict_expired_unlocked()

            collection_name = INSTANCE_TO_COLLECTION[instance_key]
            if os.environ.get("RAG_DRY_RUN") == "1":
                return MockCollection(instance_key=instance_key, name=collection_name)

            now = time.time()
            if instance_key in self._clients:
                client, _ = self._clients[instance_key]
                self._clients[instance_key] = (client, now)
                logger.debug("Cache-hit för Chroma-instans %s", instance_key)
                return client.get_collection(name=collection_name)

            instance_path = self.base_path / instance_key
            if not instance_path.exists():
                raise CollectionNotFoundError(f"Chroma-instans saknas: {instance_path}")

            if len(self._clients) >= self.MAX_OPEN:
                self._evict_oldest_unlocked()

            client = chromadb.PersistentClient(path=str(instance_path))
            self._clients[instance_key] = (client, now)
            logger.debug("Laddade Chroma-instans %s från %s", instance_key, instance_path)
            return client.get_collection(name=collection_name)

    def _evict_expired(self) -> None:
        with self._lock:
            self._evict_expired_unlocked()

    def _evict_oldest(self) -> None:
        with self._lock:
            self._evict_oldest_unlocked()

    def _evict_expired_unlocked(self) -> None:
        now = time.time()
        expired_keys = [
            key for key, (_, timestamp) in self._clients.items()
            if now - timestamp > self.TTL_SECONDS
        ]
        for key in expired_keys:
            del self._clients[key]
            logger.debug("Evictade utgången Chroma-instans %s", key)

    def _evict_oldest_unlocked(self) -> None:
        if not self._clients:
            return
        oldest_key = min(self._clients.items(), key=lambda item: item[1][1])[0]
        del self._clients[oldest_key]
        logger.debug("Evictade äldsta Chroma-instans %s", oldest_key)

