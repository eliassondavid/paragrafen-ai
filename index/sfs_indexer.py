"""Index normalized SFS chunks directly into ChromaDB."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger("paragrafenai.noop")

DEFAULT_COLLECTION_NAME = "paragrafen_sfs_v1"
DEFAULT_EMBEDDING_MODEL = "KBLab/sentence-bert-swedish-cased"
DEFAULT_NORM_DIR = "data/norm/sfs"
DEFAULT_CHROMA_PATH = "data/index/chroma/sfs"
DEFAULT_CONFIG_PATH = "config/embedding_config.yaml"


class SfsIndexer:
    """Indexes normalized SFS chunk lists into a dedicated Chroma collection."""

    def __init__(
        self,
        norm_dir: Path | str = DEFAULT_NORM_DIR,
        config_path: Path | str = DEFAULT_CONFIG_PATH,
        collection_name: str | None = None,
        chroma_path: Path | str | None = None,
        embedding_model: str | None = None,
    ) -> None:
        self.repo_root = Path(__file__).resolve().parent.parent
        self.norm_dir = self._resolve_path(norm_dir)
        self.config_path = self._resolve_path(config_path)

        config = self._load_config(self.config_path)
        chroma_cfg = self._extract_sfs_chroma_config(config)

        self.chroma_path = self._resolve_path(
            chroma_path or chroma_cfg.get("path") or DEFAULT_CHROMA_PATH
        )
        self.collection_name = (
            collection_name
            or self._as_non_empty_string(chroma_cfg.get("collection"))
            or DEFAULT_COLLECTION_NAME
        )
        self.embedding_model_name = (
            embedding_model
            or self._extract_embedding_model_name(config)
            or DEFAULT_EMBEDDING_MODEL
        )

        self._client: Any | None = None
        self._collection: Any | None = None
        self._embedding_model: Any | None = None

    def index_all(
        self,
        *,
        dry_run: bool = False,
        batch_size: int = 100,
    ) -> dict[str, int]:
        summary = {
            "documents_seen": 0,
            "documents_indexed": 0,
            "documents_skipped": 0,
            "chunks_indexed": 0,
            "chunks_skipped": 0,
            "errors": 0,
        }

        files = sorted(self.norm_dir.glob("*.json"))
        if batch_size <= 0:
            raise ValueError("batch_size måste vara större än 0.")

        for file_path in files:
            if file_path.name.startswith("_"):
                summary["documents_skipped"] += 1
                continue

            summary["documents_seen"] += 1

            try:
                with file_path.open("r", encoding="utf-8") as fh:
                    chunks = json.load(fh)
            except (OSError, json.JSONDecodeError) as exc:
                summary["errors"] += 1
                logger.warning("kunde_inte_lasa_sfs_fil", file=str(file_path), error=str(exc))
                continue

            if not isinstance(chunks, list) or not chunks:
                summary["documents_skipped"] += 1
                logger.warning("saknar_chunk_lista", file=str(file_path))
                continue

            file_rows: list[tuple[str, str, dict[str, Any]]] = []
            had_chunk_error = False

            for chunk in chunks:
                try:
                    prepared = self._prepare_chunk(chunk)
                except ValueError as exc:
                    had_chunk_error = True
                    summary["chunks_skipped"] += 1
                    logger.warning("skippad_chunk", file=str(file_path), error=str(exc))
                    continue

                if prepared is None:
                    summary["chunks_skipped"] += 1
                    continue

                file_rows.append(prepared)

            if not file_rows:
                summary["documents_skipped"] += 1
                if had_chunk_error:
                    logger.warning("inga_giltiga_chunks", file=str(file_path))
                continue

            if dry_run:
                summary["documents_indexed"] += 1
                summary["chunks_indexed"] += len(file_rows)
                continue

            try:
                for start in range(0, len(file_rows), batch_size):
                    batch = file_rows[start : start + batch_size]
                    ids = [row[0] for row in batch]
                    documents = [row[1] for row in batch]
                    metadatas = [row[2] for row in batch]
                    embeddings = self._encode_texts(documents)

                    self._get_collection().upsert(
                        ids=ids,
                        documents=documents,
                        metadatas=metadatas,
                        embeddings=embeddings,
                    )
                    summary["chunks_indexed"] += len(batch)
            except Exception as exc:
                summary["errors"] += 1
                logger.warning("kunde_inte_indexera_sfs_fil", file=str(file_path), error=str(exc))
                continue

            summary["documents_indexed"] += 1

        return summary

    def _prepare_chunk(self, chunk: Any) -> tuple[str, str, dict[str, Any]] | None:
        if not isinstance(chunk, dict):
            raise ValueError("Chunk är inte ett objekt.")

        namespace = self._as_non_empty_string(chunk.get("namespace"))
        if not namespace:
            raise ValueError("Chunk saknar namespace.")

        text = self._as_non_empty_string(chunk.get("text"))
        if not text:
            return None

        metadata = self._build_metadata(chunk)
        return namespace, text, metadata

    def _build_metadata(self, chunk: dict[str, Any]) -> dict[str, Any]:
        legal_area_raw = chunk.get("legal_area", "")
        if isinstance(legal_area_raw, str):
            legal_area = [area.strip() for area in legal_area_raw.split(",") if area.strip()]
        elif isinstance(legal_area_raw, list):
            legal_area = [str(area).strip() for area in legal_area_raw if str(area).strip()]
        else:
            legal_area = []

        metadata = {
            "namespace": str(chunk["namespace"]),
            "source_type": str(chunk["source_type"]),
            "source_id": self._string_or_default(chunk.get("source_id")),
            "sfs_nr": str(chunk["sfs_nr"]),
            "rubrik": self._string_or_default(chunk.get("rubrik")),
            "authority_level": str(chunk["authority_level"]),
            "norm_type": self._string_or_default(chunk.get("norm_type")),
            "legal_area": legal_area,
            "legal_area_confidence": self._string_or_default(chunk.get("legal_area_confidence")),
            "numbering_type": self._string_or_default(chunk.get("numbering_type")),
            "chunk_index": self._int_or_default(chunk.get("chunk_index"), default=0),
            "chunk_total": self._int_or_default(chunk.get("chunk_total"), default=0),
            "kapitel": self._string_or_default(chunk.get("kapitel")),
            "kapitelrubrik": self._string_or_default(chunk.get("kapitelrubrik")),
            "paragraf": self._string_or_default(chunk.get("paragraf")),
            "kortnamn": self._string_or_default(chunk.get("kortnamn")),
            "ikraftträdande": self._string_or_default(chunk.get("ikraftträdande")),
            "utfärdad": self._string_or_default(chunk.get("utfärdad")),
            "senaste_andring": self._string_or_default(chunk.get("senaste_andring")),
            "consolidation_source": self._string_or_default(chunk.get("consolidation_source")),
            "riksdagen_dok_id": self._string_or_default(chunk.get("riksdagen_dok_id")),
            "departement": self._string_or_default(chunk.get("departement")),
            "upphävd": bool(chunk.get("upphävd", False)),
            "has_table": bool(chunk.get("has_table", False)),
            "is_definition": bool(chunk.get("is_definition", False)),
            "is_overgangsbestammelse": bool(chunk.get("is_overgangsbestammelse", False)),
            "references_to": self._serialize_references(chunk.get("references_to")),
            "embedding_model": self.embedding_model_name,
        }
        return metadata

    def _encode_texts(self, texts: list[str]) -> list[list[float]]:
        model = self._get_embedding_model()
        vectors = model.encode(texts, normalize_embeddings=True)
        rows: list[list[float]] = []
        for vector in vectors:
            if hasattr(vector, "tolist"):
                rows.append(vector.tolist())
            else:
                rows.append(list(vector))
        return rows

    def _get_embedding_model(self) -> Any:
        if self._embedding_model is None:
            try:
                from sentence_transformers import SentenceTransformer
            except ImportError as exc:
                raise ImportError(
                    "sentence-transformers krävs för att indexera SFS till Chroma."
                ) from exc

            self._embedding_model = SentenceTransformer(self.embedding_model_name)
        return self._embedding_model

    def _get_collection(self) -> Any:
        if self._collection is None:
            if self._client is None:
                try:
                    import chromadb
                except ImportError as exc:
                    raise ImportError("chromadb krävs för att indexera SFS.") from exc

                self.chroma_path.mkdir(parents=True, exist_ok=True)
                self._client = chromadb.PersistentClient(path=str(self.chroma_path))

            self._collection = self._client.get_or_create_collection(
                name=self.collection_name,
                metadata={"hnsw:space": "cosine"},
            )

        return self._collection

    def _resolve_path(self, path_value: Path | str) -> Path:
        path = Path(path_value)
        if path.is_absolute():
            return path
        return self.repo_root / path

    def _load_config(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}

        try:
            import yaml
        except ImportError:
            logger.warning("yaml_saknas_anvander_default_config", file=str(path))
            return {}

        try:
            with path.open("r", encoding="utf-8") as fh:
                loaded = yaml.safe_load(fh) or {}
        except OSError as exc:
            logger.warning("kunde_inte_lasa_config", file=str(path), error=str(exc))
            return {}

        if not isinstance(loaded, dict):
            return {}
        return loaded

    def _extract_embedding_model_name(self, config: dict[str, Any]) -> str | None:
        embedding_cfg = config.get("embedding")
        if not isinstance(embedding_cfg, dict):
            return None

        return self._as_non_empty_string(
            embedding_cfg.get("production_model") or embedding_cfg.get("model")
        )

    def _extract_sfs_chroma_config(self, config: dict[str, Any]) -> dict[str, Any]:
        chroma_cfg = config.get("chroma")
        if not isinstance(chroma_cfg, dict):
            return {}

        instances = chroma_cfg.get("instances")
        if not isinstance(instances, dict):
            return {}

        sfs_cfg = instances.get("sfs")
        if not isinstance(sfs_cfg, dict):
            return {}

        return sfs_cfg

    def _serialize_references(self, value: Any) -> str:
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except (TypeError, ValueError, json.JSONDecodeError):
                parsed = [value] if value else []
            else:
                if not isinstance(parsed, list):
                    parsed = [parsed]
            return json.dumps(parsed, ensure_ascii=False)

        if isinstance(value, list):
            return json.dumps(value, ensure_ascii=False)
        if value is None:
            return json.dumps([], ensure_ascii=False)
        return json.dumps([value], ensure_ascii=False)

    def _string_or_default(self, value: Any, default: str = "") -> str:
        text = self._as_non_empty_string(value)
        return text if text is not None else default

    def _as_non_empty_string(self, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _int_or_default(self, value: Any, *, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Indexera normaliserade SFS-chunks till ChromaDB.")
    parser.add_argument("--norm-dir", default=DEFAULT_NORM_DIR, help="Katalog med normaliserade SFS JSON-filer.")
    parser.add_argument("--dry-run", action="store_true", help="Validera och räkna chunks utan att skriva till Chroma.")
    parser.add_argument("--batch-size", type=int, default=100, help="Antal chunks per upsert-anrop.")
    parser.add_argument(
        "--config-path",
        default=DEFAULT_CONFIG_PATH,
        help="Sökväg till embedding/chroma-konfiguration.",
    )
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    indexer = SfsIndexer(norm_dir=args.norm_dir, config_path=args.config_path)
    result = indexer.index_all(dry_run=args.dry_run, batch_size=args.batch_size)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
