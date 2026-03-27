"""SOU adapter for the shared forarbete pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path
import re
from typing import Any, Iterable

from bs4 import BeautifulSoup

from pipelines.common.chunk_base import ForarbeteChunker
from pipelines.common.embed_base import ForarbeteEmbedder
from pipelines.common.fetch_base import FetchResult, ForarbeteFetcher, RawDocument
from pipelines.common.normalize_base import ForarbeteNormalizer, NormalizedDocument
from pipelines.common.parse_base import ForarbeteParser, Section
from pipelines.common.relations_base import RelationsExtractor
from pipelines.common.upsert_base import ForarbeteUpserter
from pipelines.common.validate_base import ForarbeteValidator

logger = logging.getLogger("paragrafenai.noop")

SOU_BETECKNING_RE = re.compile(r"(?i)\bsou\s+(\d{4})\s*:\s*(\d+)\b")
HEADING_MAX_WORDS = 14
HEADING_MAX_CHARS = 140
NOISE_PATTERNS = [
    re.compile(r"(?i)^sou\s+\d{4}:\d+$"),
    re.compile(r"(?i)^statens offentliga utredningar\s+\d{4}$"),
    re.compile(r"(?i)^(innehåll|innehållsförteckning)$"),
]


@dataclass
class AdapterResult:
    fetch: FetchResult
    validated: int
    upserted: int
    skipped: int
    errors: int


class SouFetcher(ForarbeteFetcher):
    def get_doktyp(self) -> str:
        return "sou"

    def get_output_dir(self) -> Path:
        return Path("data/raw/sou")

    def build_filename(self, document: dict[str, Any]) -> str | None:
        year, number = self._parse_beteckning(
            str(document.get("beteckning") or ""),
            datum=str(document.get("datum") or ""),
            riksmote=str(document.get("rm") or document.get("riksmote") or ""),
            nummer=str(document.get("nummer") or ""),
        )
        if year != "0000" or number != "0":
            return f"sou_{year}_{number}"

        dok_id = str(document.get("dok_id") or document.get("id") or "").strip().lower()
        if dok_id:
            return f"sou_{dok_id}"
        return None

    def should_skip(self, document: dict[str, Any]) -> tuple[bool, str]:
        beteckning = str(document.get("beteckning") or "").strip()
        if not beteckning:
            return True, "beteckning saknas"
        return False, ""

    def _parse_beteckning(
        self,
        beteckning: str,
        *,
        datum: str = "",
        riksmote: str = "",
        nummer: str = "",
    ) -> tuple[str, str]:
        match = SOU_BETECKNING_RE.search(beteckning or "")
        if match:
            return match.group(1), str(int(match.group(2)))

        year_match = re.match(r"(\d{4})", (datum or "").strip())
        if year_match and (nummer or beteckning.strip().isdigit()):
            candidate_number = nummer.strip() or beteckning.strip()
            if candidate_number.isdigit():
                return year_match.group(1), str(int(candidate_number))

        rm_year = re.match(r"(\d{4})", (riksmote or "").strip())
        if rm_year and (nummer or beteckning.strip().isdigit()):
            candidate_number = nummer.strip() or beteckning.strip()
            if candidate_number.isdigit():
                return rm_year.group(1), str(int(candidate_number))

        return "0000", "0"


class SouParser(ForarbeteParser):
    def get_section_patterns(self) -> list[tuple[str, str]]:
        return [
            ("sammanfattning", r"(?i)^sammanfattning"),
            ("forfattningsforslag", r"(?i)^författningsförslag|^lagförslag"),
            ("bakgrund", r"(?i)^bakgrund|^gällande rätt|^nuvarande ordning"),
            ("overvaganeden", r"(?i)^överväganden|^utredningens överväganden"),
            ("forslag", r"(?i)^förslag|^utredningens förslag"),
            ("konsekvenser", r"(?i)^konsekvens|^ekonomiska konsekvenser"),
            ("forfattningskommentar", r"(?i)^författningskommentar"),
            ("bilaga", r"(?i)^bilaga"),
        ]

    def parse(self, html: str, dok_id: str = "") -> list[Section]:
        if not html.strip():
            return []

        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:
            soup = BeautifulSoup(html, "html.parser")

        for tag_name in self.REMOVE_TAGS:
            for node in soup.find_all(tag_name):
                node.decompose()

        patterns = [
            (section_key, re.compile(pattern, re.IGNORECASE))
            for section_key, pattern in self.get_section_patterns()
        ]
        sections: list[Section] = []
        current_key = "main"
        current_title = "Huvudtext"
        current_parts: list[str] = []

        def flush_current() -> None:
            text = "\n\n".join(part for part in current_parts if part).strip()
            if not text:
                return
            sections.append(
                Section(
                    section_key=current_key,
                    section_title=current_title,
                    text=text,
                    level=2,
                )
            )

        paragraph_tags = ["h1", "h2", "h3", "p", "li"]
        for node in soup.find_all(paragraph_tags):
            text = self.clean_text(str(node))
            if not text or self._is_noise(text):
                continue

            if self._looks_like_heading(text, patterns):
                flush_current()
                current_parts = []
                current_title = text
                current_key = self._resolve_section_key(text, patterns)
                continue

            current_parts.append(text)

        flush_current()
        if sections:
            return sections
        return super().parse(html, dok_id=dok_id)

    def _looks_like_heading(
        self,
        text: str,
        patterns: list[tuple[str, re.Pattern[str]]],
    ) -> bool:
        if len(text) > HEADING_MAX_CHARS:
            return False
        if len(text.split()) > HEADING_MAX_WORDS:
            return False
        if text.endswith((".", ";", ":")) and len(text.split()) > 4:
            return False
        return any(pattern.search(text) for _, pattern in patterns)

    def _is_noise(self, text: str) -> bool:
        if text.isdigit():
            return True
        return any(pattern.search(text) for pattern in NOISE_PATTERNS)


class SouNormalizer(ForarbeteNormalizer):
    def __init__(self, *, relations: RelationsExtractor | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.relations = relations or RelationsExtractor()

    def build_chunk_id(self, raw: RawDocument, chunk_index: int) -> str:
        year, number = self._parse_beteckning(
            str(raw.metadata.get("beteckning") or ""),
            datum=str(raw.metadata.get("datum") or ""),
            riksmote=str(raw.metadata.get("rm") or raw.metadata.get("riksmote") or ""),
            nummer=str(raw.metadata.get("nummer") or ""),
        )
        return f"forarbete::sou_{year}_{number}_chunk_{chunk_index:03d}"

    def build_document_metadata(self, raw: RawDocument) -> dict[str, Any]:
        year, number = self._parse_beteckning(
            str(raw.metadata.get("beteckning") or ""),
            datum=str(raw.metadata.get("datum") or ""),
            riksmote=str(raw.metadata.get("rm") or raw.metadata.get("riksmote") or ""),
            nummer=str(raw.metadata.get("nummer") or ""),
        )
        citation = f"SOU {year}:{number}"
        html_url = str(
            raw.metadata.get("dokument_url_html")
            or raw.metadata.get("html_url")
            or raw.metadata.get("source_url")
            or ""
        )
        source_url = str(raw.metadata.get("source_url") or html_url)
        legal_area = self.serialize_list_field(raw.metadata.get("legal_area"))
        references_to = self.relations.extract(raw.status_json, "sou")
        ingest_method = "curated" if raw.metadata.get("_curated") else "api"
        return {
            "document_subtype": "sou",
            "canonical_citation": citation,
            "short_citation": citation,
            "title": str(raw.metadata.get("titel") or raw.metadata.get("title") or raw.filename),
            "department_or_committee": str(raw.metadata.get("organ") or ""),
            "session_or_year": str(raw.metadata.get("rm") or year),
            "issued_at": str(raw.metadata.get("datum") or ""),
            "source_url": source_url,
            "html_url": html_url,
            "status": "active",
            "is_active": True,
            "ingest_method": ingest_method,
            "curated_by": raw.metadata.get("curated_by"),
            "curated_note": raw.metadata.get("curated_note"),
            "legal_area": legal_area,
            "references_to": references_to,
            "beteckning": str(raw.metadata.get("beteckning") or citation),
            "titel": str(raw.metadata.get("titel") or raw.metadata.get("title") or raw.filename),
            "organ": str(raw.metadata.get("organ") or ""),
            "år": str(year),
            "nr": int(number),
            "datum": str(raw.metadata.get("datum") or ""),
            "riksmote": str(raw.metadata.get("rm") or raw.metadata.get("riksmote") or ""),
        }

    def build_chunk_metadata(
        self,
        raw: RawDocument,
        chunked_section: Any,
        *,
        chunk_index: int,
        chunk_total: int,
        document_metadata: dict[str, Any],
    ) -> dict[str, Any]:
        citation = str(document_metadata.get("canonical_citation") or raw.metadata.get("beteckning") or "")
        return {
            "citation": citation,
            "short_citation": str(document_metadata.get("short_citation") or citation),
            "legal_area": document_metadata.get("legal_area", "[]"),
            "references_to": document_metadata.get("references_to", "[]"),
            "source_url": document_metadata.get("source_url", ""),
            "document_subtype": "sou",
            "title": document_metadata.get("title", ""),
            "beteckning": document_metadata.get("beteckning", ""),
            "år": document_metadata.get("år", "0000"),
            "nr": document_metadata.get("nr", 0),
            "status": "active",
        }

    def _parse_beteckning(
        self,
        beteckning: str,
        *,
        datum: str = "",
        riksmote: str = "",
        nummer: str = "",
    ) -> tuple[str, str]:
        match = SOU_BETECKNING_RE.search(beteckning or "")
        if match:
            return match.group(1), str(int(match.group(2)))

        year_match = re.match(r"(\d{4})", (datum or "").strip())
        if year_match and (nummer or beteckning.strip().isdigit()):
            candidate_number = nummer.strip() or beteckning.strip()
            if candidate_number.isdigit():
                return year_match.group(1), str(int(candidate_number))

        rm_year = re.match(r"(\d{4})", (riksmote or "").strip())
        if rm_year and (nummer or beteckning.strip().isdigit()):
            candidate_number = nummer.strip() or beteckning.strip()
            if candidate_number.isdigit():
                return rm_year.group(1), str(int(candidate_number))

        return "0000", "0"


class SouAdapter:
    """Koordinerar fetch → parse → normalize → chunk → validate → upsert för SOU."""

    def __init__(
        self,
        config_path: str = "config/sources.yaml",
        embedding_config: str = "config/embedding_config.yaml",
    ) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.config_path = config_path
        self.embedding_config = embedding_config
        self.fetcher: SouFetcher | None = None
        self.parser = SouParser()
        self.chunker = ForarbeteChunker()
        self.relations = RelationsExtractor()
        self.normalizer = SouNormalizer(relations=self.relations, chunker=self.chunker)
        self.validator = ForarbeteValidator()
        self._embedder: ForarbeteEmbedder | None = None
        self._upserter: ForarbeteUpserter | None = None

    def run(
        self,
        *,
        dry_run: bool = False,
        fetch: bool = False,
        riksmote: str | None = None,
        incremental: bool = False,
        max_docs: int | None = None,
    ) -> AdapterResult:
        """
        Kör hela pipeline: fetch → parse → normalize → chunk → validate → upsert.
        Returnerar AdapterResult med statistik.
        """
        if dry_run:
            fetch_result = FetchResult(dry_run=True)
        elif fetch:
            fetcher = self._get_fetcher()
            if fetcher is None:
                fetch_result = FetchResult(dry_run=False, errors=1)
            else:
                fetch_result = fetcher.fetch_all(
                    dry_run=False,
                    riksmote=riksmote,
                    incremental=incremental,
                )
        else:
            fetch_result = FetchResult(dry_run=False)

        raw_documents = self._load_input_documents(riksmote=riksmote, max_docs=max_docs)
        if dry_run and not raw_documents:
            return AdapterResult(
                fetch=fetch_result,
                validated=0,
                upserted=0,
                skipped=0,
                errors=fetch_result.errors,
            )

        validated = 0
        upserted = 0
        skipped = 0
        errors = fetch_result.errors
        seen = 0
        embedder: ForarbeteEmbedder | None = None
        upserter: ForarbeteUpserter | None = None

        if not dry_run:
            embedder = self._get_embedder()
            upserter = self._get_upserter()

        for raw in raw_documents:
            seen += 1
            try:
                sections = self.parser.parse(raw.html_content, dok_id=raw.dok_id)
                if not sections:
                    skipped += 1
                    continue

                document = self.normalizer.normalize(raw, sections)
                validation_results = self.validator.validate_all(raw, document)
                if not all(result.passed for result in validation_results):
                    errors += 1
                    continue

                validated += 1
                if not document.chunks:
                    skipped += 1
                    continue

                if dry_run:
                    upserted += len(document.chunks)
                    continue

                if embedder is None or upserter is None:
                    errors += 1
                    continue

                embeddings = embedder.embed([chunk.chunk_text for chunk in document.chunks])
                if len(embeddings) != len(document.chunks):
                    logger.error("Fel antal embeddings för %s.", raw.dok_id)
                    errors += 1
                    continue

                upsert_result = upserter.upsert_chunks(document.chunks, embeddings, dry_run=False)
                upserted += upsert_result.upserted
                skipped += upsert_result.skipped
                errors += upsert_result.errors
            except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
                logger.error("SOU-adaptern kunde inte behandla %s: %s", raw.dok_id, exc)
                errors += 1

        self._log_error_ratio(errors=errors, total=max(seen, 1))
        return AdapterResult(
            fetch=fetch_result,
            validated=validated,
            upserted=upserted,
            skipped=skipped,
            errors=errors,
        )

    def _get_embedder(self) -> ForarbeteEmbedder | None:
        if self._embedder is None:
            try:
                self._embedder = ForarbeteEmbedder(self.embedding_config)
            except Exception as exc:
                logger.error("Kunde inte initiera embedder för SOU: %s", exc)
                return None
        return self._embedder

    def _get_upserter(self) -> ForarbeteUpserter | None:
        if self._upserter is None:
            try:
                chroma_path = self.repo_root / "data" / "index" / "chroma" / "sou"
                self._upserter = ForarbeteUpserter(
                    collection_name="paragrafen_sou_v1",
                    chroma_path=str(chroma_path),
                    config_path=self.embedding_config,
                )
            except Exception as exc:
                logger.error("Kunde inte initiera upserter för SOU: %s", exc)
                return None
        return self._upserter

    def _get_fetcher(self) -> SouFetcher | None:
        if self.fetcher is None:
            try:
                self.fetcher = SouFetcher(self.config_path)
            except Exception as exc:
                logger.error("Kunde inte initiera fetcher för SOU: %s", exc)
                return None
        return self.fetcher

    def _load_input_documents(
        self,
        *,
        riksmote: str | None = None,
        max_docs: int | None = None,
    ) -> list[RawDocument]:
        documents: list[RawDocument] = []
        for path in self._iter_input_paths():
            raw = self._load_raw_document(path)
            if raw is None:
                continue
            if riksmote and not self._raw_matches_riksmote(raw, riksmote):
                continue
            documents.append(raw)
            if max_docs is not None and len(documents) >= max_docs:
                break
        return documents

    def _iter_input_paths(self) -> Iterable[Path]:
        candidates = [
            self.repo_root / "data" / "raw" / "sou",
            self.repo_root / "data" / "raw" / "sou" / "curated",
        ]
        for directory in candidates:
            if not directory.exists():
                continue
            for path in sorted(directory.glob("*")):
                if path.is_dir():
                    continue
                if path.suffix.lower() not in {".json", ".html", ".htm", ".md", ".txt"}:
                    continue
                yield path

    def _load_raw_document(self, path: Path) -> RawDocument | None:
        try:
            if path.suffix.lower() == ".json":
                with path.open("r", encoding="utf-8") as fh:
                    payload = json.load(fh)
                return self._raw_from_json_payload(path, payload)
            text = path.read_text(encoding="utf-8")
        except (OSError, json.JSONDecodeError) as exc:
            logger.error("Kunde inte läsa råfil %s: %s", path, exc)
            return None

        metadata = self._metadata_from_filename(path)
        metadata["_curated"] = "curated" in path.parts
        html_content = text
        return RawDocument(
            dok_id=str(metadata.get("dok_id") or path.stem),
            filename=path.stem.lower(),
            metadata=metadata,
            status_json=self._minimal_status_json(str(metadata.get("dok_id") or path.stem), metadata),
            html_content=html_content,
            html_available=bool(html_content.strip()),
            content_hash=hashlib.sha256(html_content.encode("utf-8")).hexdigest(),
            fetched_at=datetime.now(timezone.utc).isoformat(),
        )

    def _raw_from_json_payload(self, path: Path, payload: Any) -> RawDocument | None:
        if not isinstance(payload, dict):
            return None

        if {"dok_id", "filename", "metadata", "status_json", "html_content"}.issubset(payload.keys()):
            metadata = payload.get("metadata")
            if not isinstance(metadata, dict):
                metadata = {}
            metadata = dict(metadata)
            metadata["_curated"] = bool(metadata.get("_curated")) or "curated" in path.parts
            status_json = payload.get("status_json") if isinstance(payload.get("status_json"), dict) else {}
            if not status_json:
                status_json = self._minimal_status_json(str(payload.get("dok_id") or path.stem), metadata)
            return RawDocument(
                dok_id=str(payload.get("dok_id") or path.stem),
                filename=str(payload.get("filename") or path.stem),
                metadata=metadata,
                status_json=status_json,
                html_content=str(payload.get("html_content") or ""),
                html_available=bool(payload.get("html_available")),
                content_hash=str(payload.get("content_hash") or hashlib.sha256(str(payload.get("html_content") or "").encode("utf-8")).hexdigest()),
                fetched_at=str(payload.get("fetched_at") or datetime.now(timezone.utc).isoformat()),
            )

        metadata = dict(payload)
        metadata["_curated"] = bool(metadata.get("_curated")) or "curated" in path.parts
        html_content = str(payload.get("html_content") or payload.get("html") or "")
        filename = self._normalize_stem(path.stem)
        status_json = payload.get("status_json") if isinstance(payload.get("status_json"), dict) else {}
        if not status_json:
            status_json = self._minimal_status_json(str(payload.get("dok_id") or path.stem), metadata)
        return RawDocument(
            dok_id=str(payload.get("dok_id") or path.stem),
            filename=filename,
            metadata=metadata,
            status_json=status_json,
            html_content=html_content,
            html_available=bool(payload.get("html_available", bool(html_content.strip()))),
            content_hash=str(payload.get("content_hash") or hashlib.sha256(html_content.encode("utf-8")).hexdigest()),
            fetched_at=str(payload.get("fetched_at") or datetime.now(timezone.utc).isoformat()),
        )

    def _metadata_from_filename(self, path: Path) -> dict[str, Any]:
        stem = self._normalize_stem(path.stem)
        match = re.search(r"sou[_-](\d{4})[_-](\d+)", stem, re.IGNORECASE)
        year = match.group(1) if match else "0000"
        number = str(int(match.group(2))) if match else "0"
        citation = f"SOU {year}:{number}"
        return {
            "beteckning": citation,
            "dok_id": stem,
            "titel": citation,
            "datum": f"{year}-01-01" if year != "0000" else "",
            "organ": "",
            "source_url": "",
            "dokument_url_html": "",
        }

    def _normalize_stem(self, stem: str) -> str:
        return re.sub(r"[^\w]+", "_", stem).strip("_").lower()

    def _minimal_status_json(self, dok_id: str, metadata: dict[str, Any]) -> dict[str, Any]:
        return {
            "dokumentstatus": {
                "dokument": {
                    "dok_id": dok_id,
                    "beteckning": metadata.get("beteckning", ""),
                    "titel": metadata.get("titel") or metadata.get("title") or "",
                    "datum": metadata.get("datum", ""),
                    "organ": metadata.get("organ", ""),
                }
            }
        }

    def _raw_matches_riksmote(self, raw: RawDocument, riksmote: str) -> bool:
        candidate = str(raw.metadata.get("rm") or raw.metadata.get("riksmote") or "").strip()
        if candidate:
            return candidate.replace("-", "/") == riksmote.replace("-", "/")
        year, _ = self.normalizer._parse_beteckning(
            str(raw.metadata.get("beteckning") or ""),
            datum=str(raw.metadata.get("datum") or ""),
        )
        return year == riksmote.replace("-", "/")[:4]

    def _log_error_ratio(self, *, errors: int, total: int) -> None:
        ratio = errors / total if total else 0.0
        if ratio > 0.01:
            logger.critical("SOU-pipelinen överskred tillåten felkvot: %.2f%%", ratio * 100)


__all__ = [
    "AdapterResult",
    "SouAdapter",
    "SouFetcher",
    "SouNormalizer",
    "SouParser",
]
