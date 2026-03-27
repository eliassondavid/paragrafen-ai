"""Embedding wrapper for forarbete pipelines."""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger("paragrafenai.noop")


class ForarbeteEmbedder:
    """Wrapper runt index.embedder.Embedder för förarbetespipelinen."""

    def __init__(self, config_path: str | Path = "config/embedding_config.yaml"):
        from index.embedder import Embedder

        self.embedder = Embedder(config_path=config_path)

    def embed(self, texts: list[str]) -> list[list[float]]:
        return self.embedder.embed(texts)


__all__ = [
    "ForarbeteEmbedder",
]
