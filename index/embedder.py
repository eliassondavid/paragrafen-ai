"""Embedding wrapper for the F4 indexing pipeline."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml
from sentence_transformers import SentenceTransformer

logger = logging.getLogger("paragrafenai.noop")


class Embedder:
    """Loads and runs the configured embedding model."""

    def __init__(self, config_path: str | Path = "config/embedding_config.yaml") -> None:
        self.repo_root = Path(__file__).resolve().parent.parent
        self.config_path = self._resolve_path(config_path)
        self.config = self._load_config(self.config_path)

        embedding_cfg = self.config.get("embedding", {})
        self.model_name = str(embedding_cfg.get("production_model", ""))
        self.max_tokens = int(embedding_cfg.get("max_tokens", 512))
        self.normalize_embeddings = bool(embedding_cfg.get("normalize_embeddings", True))

        logger.info("Laddar embedding-modell: %s", self.model_name)
        self.model = SentenceTransformer(self.model_name)

    def _resolve_path(self, path_value: str | Path) -> Path:
        candidate = Path(path_value)
        if candidate.is_absolute():
            return candidate
        return self.repo_root / candidate

    def _load_config(self, config_path: Path) -> dict[str, Any]:
        with config_path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        return data

    def _truncate_text(self, text: str) -> str:
        if not text:
            return ""

        tokenizer = getattr(self.model, "tokenizer", None)
        if tokenizer is None:
            # Fallback if tokenizer is not exposed by the model implementation.
            return " ".join(text.split()[: self.max_tokens])

        token_ids = tokenizer.encode(
            text,
            add_special_tokens=False,
            truncation=True,
            max_length=self.max_tokens,
        )
        return tokenizer.decode(
            token_ids,
            skip_special_tokens=True,
            clean_up_tokenization_spaces=True,
        )

    def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        processed = [self._truncate_text(text or "") for text in texts]
        try:
            vectors = self.model.encode(
                processed,
                normalize_embeddings=self.normalize_embeddings,
                convert_to_numpy=True,
                show_progress_bar=False,
            )
        except Exception as exc:
            logger.error("Embedding-fel: %s", exc)
            return []

        return [vector.tolist() for vector in vectors]

    def embed_single(self, text: str) -> list[float]:
        vectors = self.embed([text])
        return vectors[0] if vectors else []
