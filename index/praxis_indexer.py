"""
Indexerare för normaliserad svensk rättspraxis -> ChromaDB.

Läser normaliserade JSON-filer (output från praxis_normalizer.py),
genererar embeddings explicit med SentenceTransformer,
och upserterar chunks till Chroma-instansen för praxis.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import structlog
import yaml

from index.vector_store import ChromaVectorStore

logger = structlog.get_logger()

PRAXIS_INSTANCE_KEY = "praxis"
PRAXIS_COLLECTION_NAME = "paragrafen_praxis_v1"
UPSERT_BATCH_SIZE = 50


@dataclass(frozen=True)
class EmbeddingSettings:
    model_name: str
    normalize_embeddings: bool = True
    batch_size: int = 32
    device: str = "cpu"


@dataclass
class IndexReport:
    """Rapport från indexering."""

    total_files: int = 0
    indexed_files: int = 0
    total_chunks: int = 0
    indexed_chunks: int = 0
    skipped_files: int = 0
    errors: int = 0
    error_details: list[dict[str, str]] = field(default_factory=list)
    collection_count_before: int = 0
    collection_count_after: int = 0

    @property
    def error_rate(self) -> float:
        if self.total_files == 0:
            return 0.0
        return self.errors / self.total_files

    def summary(self) -> str:
        return (
            f"Indexering klar: {self.indexed_files}/{self.total_files} filer, "
            f"{self.indexed_chunks}/{self.total_chunks} chunks indexerade, "
            f"{self.errors} fel (felkvot: {self.error_rate:.1%}). "
            f"Collection: {self.collection_count_before} -> {self.collection_count_after}"
        )


def _resolve_path(path_value: str | Path) -> Path:
    repo_root = Path(__file__).resolve().parent.parent
    candidate = Path(path_value)
    if candidate.is_absolute():
        return candidate
    return repo_root / candidate


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _load_embedding_settings(config_path: Path) -> EmbeddingSettings:
    config = _load_yaml(config_path)
    embedding_cfg = config.get("embedding", {})
    model_name = str(
        embedding_cfg.get("production_model") or embedding_cfg.get("model") or ""
    ).strip()
    if not model_name:
        raise ValueError(f"embedding.production_model saknas i {config_path}")

    return EmbeddingSettings(
        model_name=model_name,
        normalize_embeddings=bool(embedding_cfg.get("normalize_embeddings", True)),
        batch_size=int(embedding_cfg.get("batch_size", 32)),
        device=str(embedding_cfg.get("device", "cpu")),
    )


def _load_sentence_transformer(settings: EmbeddingSettings):
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:
        raise ImportError(
            "sentence-transformers krävs för live-indexering av praxis."
        ) from exc

    return SentenceTransformer(settings.model_name, device=settings.device)


def _to_embedding_rows(vectors: Any) -> list[list[float]]:
    rows: list[list[float]] = []
    for vector in vectors:
        if hasattr(vector, "tolist"):
            rows.append(vector.tolist())
        else:
            rows.append(list(vector))
    return rows


def _serialize_json_list(value: Any) -> str:
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed = [value] if value else []
        else:
            if isinstance(parsed, list):
                return json.dumps(parsed, ensure_ascii=False)
            parsed = [str(parsed)]
        return json.dumps(parsed, ensure_ascii=False)

    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False)
    if value is None:
        return json.dumps([], ensure_ascii=False)
    return json.dumps([value], ensure_ascii=False)


def _build_chunk_metadata(doc_meta: dict[str, Any], chunk: dict[str, Any]) -> dict[str, Any]:
    """Bygg chunk-metadata enligt praxis-schemat."""
    return {
        "chunk_id": str(chunk["chunk_id"]),
        "source_type": str(doc_meta["source_type"]),
        "namespace": str(chunk["namespace"]),
        "domstol": str(doc_meta["domstol"]),
        "year": int(doc_meta["year"]),
        "ref_no": int(doc_meta["ref_no"]),
        "ref_no_padded": str(doc_meta["ref_no_padded"]),
        "malnummer": str(doc_meta["malnummer"]),
        "avgorandedatum": str(doc_meta["avgorandedatum"]),
        "ar_vagledande": bool(doc_meta["ar_vagledande"]),
        "authority_level": str(doc_meta["authority_level"]),
        "citation": str(doc_meta["citation"]),
        "pinpoint": str(chunk["pinpoint"]),
        "legal_area": _serialize_json_list(doc_meta.get("legal_area")),
        "references_to": _serialize_json_list(doc_meta.get("references_to")),
        "chunk_index": int(chunk["chunk_index"]),
        "api_id": str(doc_meta["api_id"]),
        "harvest_source": str(doc_meta["harvest_source"]),
    }


def load_normalized_document(path: Path) -> dict[str, Any]:
    """Läser en normaliserad JSON-fil."""
    return json.loads(path.read_text(encoding="utf-8"))


def _encode_documents(
    model: Any,
    texts: list[str],
    *,
    normalize_embeddings: bool,
    batch_size: int,
) -> list[list[float]]:
    vectors = model.encode(
        texts,
        normalize_embeddings=normalize_embeddings,
        convert_to_numpy=True,
        show_progress_bar=False,
        batch_size=batch_size,
    )
    return _to_embedding_rows(vectors)


def index_directory(
    norm_dir: Path,
    vector_store: ChromaVectorStore,
    *,
    dry_run: bool = True,
    embedding_model: Any | None = None,
    normalize_embeddings: bool = True,
    embedding_batch_size: int = 32,
) -> IndexReport:
    """
    Indexerar alla normaliserade filer i en domstols katalog.

    Om dry_run=True: räknar chunks, validerar metadata, men skriver inget till Chroma.
    Om dry_run=False: upserterar chunks till Chroma.
    """
    report = IndexReport()

    json_files = sorted(norm_dir.glob("*.json"))
    report.total_files = len(json_files)

    if not json_files:
        logger.warning("no_normalized_files", norm_dir=str(norm_dir))
        return report

    if not dry_run:
        if embedding_model is None:
            raise ValueError("embedding_model måste anges när dry_run=False")
        report.collection_count_before = vector_store.get_collection_stats()[PRAXIS_COLLECTION_NAME]
        logger.info("chroma_count_before", count=report.collection_count_before)

    all_documents: list[str] = []
    all_metadatas: list[dict[str, Any]] = []

    for json_path in json_files:
        try:
            doc = load_normalized_document(json_path)
            chunks = doc.get("chunks", [])

            if not isinstance(chunks, list) or not chunks:
                report.skipped_files += 1
                continue

            file_chunk_count = 0
            for chunk in chunks:
                chunk_text = str(chunk.get("chunk_text", "")).strip()
                if not chunk_text:
                    logger.warning(
                        "empty_chunk_text",
                        file=json_path.name,
                        chunk_id=chunk.get("chunk_id"),
                    )
                    continue

                metadata = _build_chunk_metadata(doc, chunk)
                all_documents.append(chunk_text)
                all_metadatas.append(metadata)
                report.total_chunks += 1
                file_chunk_count += 1

            if file_chunk_count == 0:
                report.skipped_files += 1
                continue

            report.indexed_files += 1
        except Exception as exc:
            report.errors += 1
            report.error_details.append(
                {
                    "file": json_path.name,
                    "error": str(exc),
                    "type": type(exc).__name__,
                }
            )
            logger.error(
                "index_file_failed",
                file=json_path.name,
                error=str(exc),
            )

    if dry_run:
        report.indexed_chunks = report.total_chunks
        logger.info("dry_run_complete", total_chunks=report.total_chunks)
        return report

    for start in range(0, len(all_documents), UPSERT_BATCH_SIZE):
        end = start + UPSERT_BATCH_SIZE
        batch_docs = all_documents[start:end]
        batch_metadatas = all_metadatas[start:end]

        try:
            embeddings = _encode_documents(
                embedding_model,
                batch_docs,
                normalize_embeddings=normalize_embeddings,
                batch_size=embedding_batch_size,
            )
            added = vector_store.add_chunks(
                PRAXIS_INSTANCE_KEY,
                batch_docs,
                embeddings,
                batch_metadatas,
            )
            report.indexed_chunks += added

            if added != len(batch_docs):
                report.errors += 1
                report.error_details.append(
                    {
                        "file": f"batch_{start}",
                        "error": f"Endast {added}/{len(batch_docs)} chunks skrevs",
                        "type": "PartialBatchWrite",
                    }
                )
                logger.error(
                    "partial_batch_write",
                    batch_start=start,
                    added=added,
                    expected=len(batch_docs),
                )
        except Exception as exc:
            report.errors += 1
            report.error_details.append(
                {
                    "file": f"batch_{start}",
                    "error": str(exc),
                    "type": type(exc).__name__,
                }
            )
            logger.error("upsert_batch_failed", batch_start=start, error=str(exc))

    report.collection_count_after = vector_store.get_collection_stats()[PRAXIS_COLLECTION_NAME]
    logger.info("chroma_count_after", count=report.collection_count_after)
    return report


def index_all_courts(
    norm_base: Path,
    *,
    courts: list[str] | None = None,
    dry_run: bool = True,
    chroma_config: str | Path = "config/embedding_config.yaml",
) -> dict[str, IndexReport]:
    """
    Indexerar alla domstolar.

    Eskaleringsvillkor:
    - Om en domstolskod saknas i AUTHORITY_MAP -> ValueError i normalizer
    - Om felkvot > 1% per domstol -> logga varning
    """
    reports: dict[str, IndexReport] = {}
    config_path = _resolve_path(chroma_config)
    vector_store = ChromaVectorStore(config_path=config_path)

    settings: EmbeddingSettings | None = None
    embedding_model: Any | None = None
    if not dry_run:
        settings = _load_embedding_settings(config_path)
        logger.info("loading_embedding_model", model=settings.model_name, device=settings.device)
        embedding_model = _load_sentence_transformer(settings)

    if courts is None:
        court_dirs = sorted(
            d for d in norm_base.iterdir()
            if d.is_dir() and d.name.isupper()
        )
    else:
        court_dirs = [norm_base / court for court in courts if (norm_base / court).is_dir()]

    for court_dir in court_dirs:
        court_code = court_dir.name
        logger.info("indexing_court", court=court_code, dry_run=dry_run)

        report = index_directory(
            norm_dir=court_dir,
            vector_store=vector_store,
            dry_run=dry_run,
            embedding_model=embedding_model,
            normalize_embeddings=settings.normalize_embeddings if settings else True,
            embedding_batch_size=settings.batch_size if settings else 32,
        )
        reports[court_code] = report

        if report.error_rate > 0.01:
            logger.warning(
                "high_error_rate",
                court=court_code,
                rate=f"{report.error_rate:.1%}",
                errors=report.errors,
            )

        logger.info("court_indexed", court=court_code, summary=report.summary())

    return reports


def main(argv: list[str] | None = None) -> int:
    import argparse
    import logging

    parser = argparse.ArgumentParser(description="Indexera praxis till ChromaDB")
    parser.add_argument("--norm-base", default="data/norm/praxis")
    parser.add_argument("--courts", nargs="*", help="Domstolskoder, t.ex. HFD HDO")
    parser.add_argument("--chroma-config", default="config/embedding_config.yaml")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)

    reports = index_all_courts(
        norm_base=_resolve_path(args.norm_base),
        courts=args.courts,
        dry_run=args.dry_run,
        chroma_config=args.chroma_config,
    )

    for court_code, report in reports.items():
        print(f"{court_code}: {report.summary()}")

    return 1 if any(report.error_rate > 0.01 for report in reports.values()) else 0


if __name__ == "__main__":
    raise SystemExit(main())
