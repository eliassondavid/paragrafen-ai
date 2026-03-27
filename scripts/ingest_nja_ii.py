from __future__ import annotations

import argparse
import ast
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import yaml

logger = logging.getLogger("nja_ii_ingest")

DEFAULT_CONFIG_PATH = "config/nja_ii_config.yaml"
PAGE_HEADER_RE = re.compile(r"^## s\. (\d+)\s*$", re.MULTILINE)
META_RE = re.compile(r"<!--\s*([^:]+):\s*(.*?)\s*-->")
HEADER_LINE_RE = re.compile(r"^NJA II \d{4} s\. \d+")
NAMESPACE_RE = re.compile(r"^nja_ii::[a-z0-9_]+_\d{4}_s\d{3}_chunk_\d{3}$")


class TokenizerProtocol(Protocol):
    def encode(self, text: str, add_special_tokens: bool = False) -> list[int]:
        ...


@dataclass
class PageBlock:
    page_number: int | None
    text: str
    citation_precision: str


@dataclass
class VolumeDocument:
    file_path: Path
    volume_year: int
    metadata: dict[str, Any]
    pages: list[PageBlock]


@dataclass
class IngestSummary:
    blocks_read: int = 0
    blocks_skipped: int = 0
    chunks_produced: int = 0
    chunks_indexed: int = 0
    citation_exact: int = 0
    citation_block: int = 0
    volumes: set[int] = field(default_factory=set)


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def resolve_path(path_value: str | Path) -> Path:
    candidate = Path(path_value)
    return candidate if candidate.is_absolute() else repo_root() / candidate


def load_config(config_path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    with resolve_path(config_path).open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Ogiltig config: {config_path}")
    return data


def parse_markdown_metadata(text: str) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    for key, raw_value in META_RE.findall(text):
        value = raw_value.strip()
        parsed: Any = value
        if value.startswith("[") or value.startswith("{") or value.startswith(("'", '"')):
            try:
                parsed = ast.literal_eval(value)
            except (SyntaxError, ValueError):
                parsed = value
        elif value.isdigit():
            parsed = int(value)
        metadata[key.strip()] = parsed
    return metadata


def clean_page_text(raw_text: str) -> str:
    paragraphs: list[str] = []
    current_lines: list[str] = []

    for line in raw_text.splitlines():
        stripped = line.strip()
        if not stripped:
            if current_lines:
                paragraphs.append(" ".join(current_lines))
                current_lines = []
            continue
        if HEADER_LINE_RE.match(stripped):
            continue
        if stripped.startswith("(Nummer i NJA II "):
            continue
        if "Allmänna villkor för Norstedts Juridiks informationstjänster" in stripped:
            continue
        current_lines.append(stripped)

    if current_lines:
        paragraphs.append(" ".join(current_lines))

    return "\n\n".join(item.strip() for item in paragraphs if item.strip())


def extract_pages(text: str) -> list[PageBlock]:
    matches = list(PAGE_HEADER_RE.finditer(text))
    if not matches:
        return [PageBlock(page_number=None, text=clean_page_text(text), citation_precision="block")]

    pages: list[PageBlock] = []
    for idx, match in enumerate(matches):
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        pages.append(
            PageBlock(
                page_number=int(match.group(1)),
                text=clean_page_text(text[start:end]),
                citation_precision="exact",
            )
        )
    return pages


def parse_volume_file(file_path: str | Path) -> VolumeDocument:
    path = Path(file_path)
    text = path.read_text(encoding="utf-8")
    metadata = parse_markdown_metadata(text)

    volume_year = metadata.get("volym_år")
    if not isinstance(volume_year, int):
        match = re.search(r"(\d{4})", path.stem)
        if not match:
            raise ValueError(f"Kunde inte extrahera volymår från {path}")
        volume_year = int(match.group(1))

    return VolumeDocument(
        file_path=path,
        volume_year=volume_year,
        metadata=metadata,
        pages=extract_pages(text),
    )


def count_tokens(text: str, tokenizer: TokenizerProtocol | None = None) -> int:
    if tokenizer is not None:
        return len(tokenizer.encode(text, add_special_tokens=False))
    return len(text.split())


def split_long_paragraph(
    paragraph: str,
    max_tokens: int,
    tokenizer: TokenizerProtocol | None = None,
) -> list[str]:
    sentences = [item.strip() for item in re.split(r"(?<=[.!?])\s+", paragraph) if item.strip()]
    if not sentences:
        return []

    chunks: list[str] = []
    current = ""

    for sentence in sentences:
        candidate = sentence if not current else f"{current} {sentence}"
        if current and count_tokens(candidate, tokenizer) > max_tokens:
            chunks.append(current)
            current = sentence
        else:
            current = candidate

        if current and count_tokens(current, tokenizer) > max_tokens:
            words = current.split()
            current = ""
            while words:
                piece: list[str] = []
                while words and count_tokens(" ".join(piece + [words[0]]), tokenizer) <= max_tokens:
                    piece.append(words.pop(0))
                if not piece:
                    piece.append(words.pop(0))
                chunks.append(" ".join(piece))

    if current:
        chunks.append(current)

    return [item for item in chunks if item.strip()]


def chunk_text(
    text: str,
    max_tokens: int = 600,
    tokenizer: TokenizerProtocol | None = None,
) -> list[str]:
    paragraphs = [item.strip() for item in text.split("\n\n") if item.strip()]
    if not paragraphs:
        return []

    chunks: list[str] = []
    current = ""

    for paragraph in paragraphs:
        if count_tokens(paragraph, tokenizer) > max_tokens:
            if current:
                chunks.append(current)
                current = ""
            chunks.extend(split_long_paragraph(paragraph, max_tokens, tokenizer))
            continue

        candidate = paragraph if not current else f"{current}\n\n{paragraph}"
        if current and count_tokens(candidate, tokenizer) > max_tokens:
            chunks.append(current)
            current = paragraph
        else:
            current = candidate

    if current:
        chunks.append(current)

    return [item for item in chunks if item.strip()]


def load_legal_areas_config(path_value: str | Path) -> tuple[set[str], dict[str, str]]:
    with resolve_path(path_value).open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    raw_items = payload.get("legal_areas")
    if not isinstance(raw_items, list):
        raw_items = payload.get("areas", [])

    valid_ids: set[str] = set()
    alias_map: dict[str, str] = {}

    for item in raw_items:
        if not isinstance(item, dict):
            continue
        area_id = str(item.get("id", "")).strip()
        if not area_id:
            continue
        valid_ids.add(area_id)
        alias_map[area_id.lower()] = area_id
        alias_map[area_id.lower().replace("_", " ")] = area_id
        alias_map[area_id.lower().replace(" ", "_")] = area_id
        for alias in item.get("aliases", []) or []:
            alias_value = str(alias).strip().lower()
            if alias_value:
                alias_map[alias_value] = area_id
                alias_map[alias_value.replace("_", " ")] = area_id
                alias_map[alias_value.replace(" ", "_")] = area_id

    return valid_ids, alias_map


def normalize_legal_areas(raw_areas: list[str], legal_areas_path: str | Path) -> list[str]:
    valid_ids, alias_map = load_legal_areas_config(legal_areas_path)
    normalized: list[str] = []
    seen: set[str] = set()

    for raw_area in raw_areas:
        key = str(raw_area).strip().lower()
        if not key:
            continue
        canonical = (
            alias_map.get(key)
            or alias_map.get(key.replace("_", " "))
            or alias_map.get(key.replace(" ", "_"))
            or key.replace("_", " ")
        )
        if canonical not in seen:
            seen.add(canonical)
            normalized.append(canonical)

    if normalized:
        return normalized
    if "offentlig rätt" in valid_ids:
        return ["offentlig rätt"]
    return ["offentlig_rätt"]


def heuristic_legal_areas(text: str, lag: str, config: dict[str, Any]) -> list[str]:
    lowered = text.lower()
    patterns = [
        (["brott", "straff", "åtal", "brottsbalk"], ["straffrätt"]),
        (["rättegång", "tvistemål", "domstol", "rättegångsbalk"], ["processrätt"]),
        (["hyres", "arrende", "fast egendom", "jordabalk"], ["fastighetsrätt"]),
        (["upphovsrätt", "fotografisk bild", "konstnärliga"], ["immaterialrätt", "upphovsrätt"]),
        (["utmätning", "införsel", "utsökning", "kronofog"], ["processrätt"]),
        (["förälder", "vårdnad", "barn"], ["familjerätt"]),
    ]
    for words, areas in patterns:
        if any(word in lowered for word in words):
            return areas

    fallback = config.get("lag_legal_area_fallback", {}).get(lag)
    if isinstance(fallback, list):
        return [str(item) for item in fallback if str(item).strip()]
    return ["offentlig_rätt"]


class LegalAreaClassifier:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.legal_areas_path = config.get("legal_areas_path", "config/legal_areas.yaml")
        self.model_name = str(config.get("legal_area_model", ""))
        self.prompt_template = str(config.get("legal_area_prompt", "{text}"))
        self._client: Any | None = None
        self._disabled = False

    def _get_client(self) -> Any | None:
        if self._disabled:
            return None
        if self._client is not None:
            return self._client

        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key or not self.model_name:
            self._disabled = True
            return None

        try:
            import anthropic  # type: ignore
        except ImportError:
            logger.warning("anthropic saknas, använder fallback för legal_area.")
            self._disabled = True
            return None

        self._client = anthropic.Anthropic(api_key=api_key)
        return self._client

    def classify(self, text: str, lag: str) -> list[str]:
        fallback = normalize_legal_areas(
            heuristic_legal_areas(text, lag, self.config),
            self.legal_areas_path,
        )
        client = self._get_client()
        if client is None:
            return fallback

        try:
            response = client.messages.create(
                model=self.model_name,
                max_tokens=100,
                messages=[
                    {
                        "role": "user",
                        "content": self.prompt_template.format(text=text[:500]),
                    }
                ],
            )
            raw = "".join(getattr(item, "text", "") for item in getattr(response, "content", []))
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return normalize_legal_areas([str(item) for item in parsed], self.legal_areas_path)
        except Exception as exc:
            logger.warning("legal_area API-fel: %s", exc)

        return fallback


def determine_lag(volume_year: int, block_text: str, config: dict[str, Any]) -> str:
    defaults = config.get("volym_lag_default", {})
    if volume_year in defaults or str(volume_year) in defaults:
        return str(defaults.get(volume_year, defaults.get(str(volume_year)))).lower()

    rules = (
        config.get("mixed_volume_rules", {}).get(volume_year)
        or config.get("mixed_volume_rules", {}).get(str(volume_year), {})
    )
    fallback = (
        config.get("volym_lag_fallback", {}).get(volume_year)
        or config.get("volym_lag_fallback", {}).get(str(volume_year), {})
    )
    primary = str(fallback.get("primary", "diverse")).lower()
    secondary = str(fallback.get("secondary", primary)).lower()
    lowered = block_text.lower()

    if any(str(word).lower() in lowered for word in rules.get("secondary_keywords", [])):
        return secondary
    if any(str(word).lower() in lowered for word in rules.get("primary_keywords", [])):
        return primary

    logger.warning("Okänd lag-kod för volym %s, använder fallback %s.", volume_year, primary)
    return primary


def build_namespace(lag: str, volume_year: int, page_number: int | None, chunk_index: int) -> str:
    page = page_number if page_number is not None else 0
    namespace = f"nja_ii::{lag}_{volume_year}_s{page:03d}_chunk_{chunk_index:03d}"
    if not NAMESPACE_RE.fullmatch(namespace):
        raise ValueError(f"Ogiltigt namespace-format: {namespace}")
    return namespace


def build_citation_source(volume_year: int, page_number: int | None) -> str:
    page = page_number if page_number is not None else 0
    return f"njaii_{volume_year}_s{page:03d}"


def make_chunk_metadata(
    *,
    lag: str,
    volume_year: int,
    page_number: int | None,
    text: str,
    chunk_index: int,
    chunk_total: int,
    legal_area: list[str],
    embedding_model: str,
    citation_precision: str,
    fetched_at: str,
) -> dict[str, Any]:
    return {
        "namespace": build_namespace(lag, volume_year, page_number, chunk_index),
        "source_type": "nja_ii",
        "authority_level": "persuasive",
        "volym_år": volume_year,
        "lag": lag,
        "text": text,
        "citation_source": build_citation_source(volume_year, page_number),
        "citation_precision": citation_precision,
        "legal_area": legal_area,
        "embedding_model": embedding_model,
        "chunk_index": chunk_index,
        "chunk_total": chunk_total,
        "source_url": "",
        "fetched_at": fetched_at,
    }


def build_chunks_for_volume(
    volume: VolumeDocument,
    config: dict[str, Any],
    classifier: LegalAreaClassifier,
    tokenizer: TokenizerProtocol | None = None,
    limit: int | None = None,
) -> tuple[list[dict[str, Any]], IngestSummary]:
    summary = IngestSummary()
    chunks: list[dict[str, Any]] = []
    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    embedding_model = str(config.get("embedding_model", ""))

    for index, page in enumerate(volume.pages):
        if limit is not None and index >= limit:
            break

        summary.blocks_read += 1
        summary.volumes.add(volume.volume_year)

        if not page.text.strip():
            logger.warning("WARN: empty text in %s", volume.file_path.name)
            summary.blocks_skipped += 1
            continue

        lag = determine_lag(volume.volume_year, page.text, config)
        page_chunks = chunk_text(
            page.text,
            max_tokens=int(config.get("max_tokens", 600)),
            tokenizer=tokenizer,
        )
        if not page_chunks:
            logger.warning("WARN: empty text in %s", volume.file_path.name)
            summary.blocks_skipped += 1
            continue

        legal_area = classifier.classify(page.text, lag)
        for chunk_index, chunk_body in enumerate(page_chunks):
            chunks.append(
                make_chunk_metadata(
                    lag=lag,
                    volume_year=volume.volume_year,
                    page_number=page.page_number,
                    text=chunk_body,
                    chunk_index=chunk_index,
                    chunk_total=len(page_chunks),
                    legal_area=legal_area,
                    embedding_model=embedding_model,
                    citation_precision=page.citation_precision,
                    fetched_at=fetched_at,
                )
            )
            summary.chunks_produced += 1
            if page.citation_precision == "exact":
                summary.citation_exact += 1
            else:
                summary.citation_block += 1

    return chunks, summary


def ensure_unique_namespaces(chunks: list[dict[str, Any]]) -> None:
    seen: dict[str, str] = {}
    for chunk in chunks:
        namespace = str(chunk["namespace"])
        text = str(chunk["text"])
        previous = seen.get(namespace)
        if previous is None:
            seen[namespace] = text
            continue
        if previous != text:
            raise RuntimeError(f"Namespace-kollision detekterad: {namespace}")


def create_embedder(model_name: str) -> Any:
    from sentence_transformers import SentenceTransformer  # type: ignore
    return SentenceTransformer(model_name)


def get_tokenizer(embedder: Any) -> TokenizerProtocol | None:
    return (
        getattr(embedder, "tokenizer", None)
        or getattr(getattr(embedder, "_first_module", lambda: None)(), "tokenizer", None)
    )


def embed_texts(embedder: Any, texts: list[str]) -> list[list[float]]:
    vectors = embedder.encode(
        texts,
        normalize_embeddings=True,
        convert_to_numpy=True,
        show_progress_bar=False,
    )
    return [vector.tolist() for vector in vectors]


def create_collection(config: dict[str, Any]) -> Any:
    import chromadb  # type: ignore

    client = chromadb.PersistentClient(path=str(config["chroma_path"]))
    collection_name = str(config.get("collection_name", "paragrafen_forarbete_v1"))
    return client.get_or_create_collection(name=collection_name)


def upsert_chunks(
    collection: Any,
    chunks: list[dict[str, Any]],
    embeddings: list[list[float]],
    batch_size: int,
) -> int:
    indexed = 0
    for start in range(0, len(chunks), batch_size):
        batch = chunks[start : start + batch_size]
        collection.upsert(
            ids=[item["namespace"] for item in batch],
            embeddings=embeddings[start : start + batch_size],
            documents=[item["text"] for item in batch],
            metadatas=[{key: value for key, value in item.items() if key != "text"} for item in batch],
        )
        indexed += len(batch)
    return indexed


def print_summary(summary: IngestSummary, duration_seconds: float) -> None:
    print("=== NJA II Ingest Summary ===")
    print(f"Block lästa         : {summary.blocks_read}")
    print(f"Block skippade      : {summary.blocks_skipped} (tomma text-fält)")
    print(f"Chunks producerade  : {summary.chunks_produced}")
    print(f"Chunks indexerade   : {summary.chunks_indexed}")
    print(f"citation_precision  : exact {summary.citation_exact} / block {summary.citation_block}")
    print(f"Volymer             : {sorted(summary.volumes)}")
    print(f"Tid                 : {duration_seconds:.2f}s")


def _collect_chunks(
    files: list[Path],
    config: dict[str, Any],
    classifier: LegalAreaClassifier,
    tokenizer: TokenizerProtocol | None,
    limit: int | None,
) -> tuple[list[dict[str, Any]], IngestSummary]:
    """Gemensam insamlingslogik för dry-run och write-fasen.

    Anropas två gånger när --write används och en tokenizer är tillgänglig:
    första gången med tokenizer=None (ordräkning) för dry-run-kontrollen,
    andra gången med den faktiska tokenizern för write-fasen. De två körningarna
    kan producera olika chunk-antal om tokenizern delar annorlunda än ordräkning —
    det är avsiktligt beteende. Chunk-antalet i Summary-utskriften reflekterar
    alltid den fas som faktiskt skrevs till Chroma.
    """
    summary = IngestSummary()
    chunks: list[dict[str, Any]] = []
    remaining = limit

    for file_path in files:
        volume_limit = remaining
        volume_chunks, volume_summary = build_chunks_for_volume(
            parse_volume_file(file_path),
            config,
            classifier,
            tokenizer=tokenizer,
            limit=volume_limit,
        )
        chunks.extend(volume_chunks)
        summary.blocks_read += volume_summary.blocks_read
        summary.blocks_skipped += volume_summary.blocks_skipped
        summary.chunks_produced += volume_summary.chunks_produced
        summary.citation_exact += volume_summary.citation_exact
        summary.citation_block += volume_summary.citation_block
        summary.volumes.update(volume_summary.volumes)

        if remaining is not None:
            remaining -= volume_summary.blocks_read
            if remaining <= 0:
                break

    return chunks, summary


def _validate_summary(summary: IngestSummary) -> None:
    if summary.blocks_read and summary.blocks_skipped / summary.blocks_read > 0.05:
        raise RuntimeError(">5% tomma text-fält upptäcktes; avbryter körningen.")


def run_ingest(
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    dry_run: bool = True,
    volym: int | None = None,
    limit: int | None = None,
    verbose: bool = False,
) -> dict[str, Any]:
    config = load_config(config_path)
    classifier = LegalAreaClassifier(config)

    files = sorted(resolve_path(config.get("data_dir", "data/curated/nja_ii")).glob("*.md"))
    if volym is not None:
        files = [path for path in files if str(volym) in path.stem]
    if not files:
        raise FileNotFoundError("Inga NJA II-filer hittades.")

    start = time.perf_counter()

    # Fas 1: samla chunks med ordräkning (alltid, även vid --write).
    # Används för validering och dry-run-utskrift.
    chunks, summary = _collect_chunks(files, config, classifier, tokenizer=None, limit=limit)
    _validate_summary(summary)
    ensure_unique_namespaces(chunks)

    if verbose:
        for chunk in chunks:
            logger.info("Chunk %s", chunk["namespace"])

    if not dry_run and chunks:
        embedder = create_embedder(str(config["embedding_model"]))
        tokenizer = get_tokenizer(embedder)

        if tokenizer is not None:
            # Fas 2: samla om med tokenizer för exakt token-räkning.
            # Kan ge ett annat chunk-antal än fas 1 — se docstring för _collect_chunks.
            chunks, summary = _collect_chunks(
                files, config, classifier, tokenizer=tokenizer, limit=limit
            )
            _validate_summary(summary)
            ensure_unique_namespaces(chunks)

        summary.chunks_indexed = upsert_chunks(
            create_collection(config),
            chunks,
            embed_texts(embedder, [item["text"] for item in chunks]),
            batch_size=int(config.get("upsert_batch_size", 100)),
        )

    duration = time.perf_counter() - start
    print_summary(summary, duration)
    return {
        "summary": summary,
        "chunks": chunks,
        "duration_seconds": duration,
        "dry_run": dry_run,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Indexera NJA II-markdown till ChromaDB.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Kör utan att skriva till Chroma (default).",
    )
    parser.add_argument("--write", action="store_true", help="Skriv till ChromaDB.")
    parser.add_argument("--volym", type=int, help="Indexera endast specificerad volym.")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Sökväg till config-fil.")
    parser.add_argument("--limit", type=int, help="Max antal block att processa.")
    parser.add_argument("--verbose", action="store_true", help="Utförlig loggning per chunk.")
    return parser


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format="%(levelname)s:%(name)s:%(message)s",
        stream=sys.stdout,
    )


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    configure_logging(args.verbose)

    if args.limit is not None and args.limit <= 0:
        print("Fel: --limit måste vara > 0")
        return 1

    try:
        run_ingest(
            config_path=args.config,
            dry_run=not args.write,
            volym=args.volym,
            limit=args.limit,
            verbose=args.verbose,
        )
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        logger.error("%s", exc)
        print(str(exc))
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
