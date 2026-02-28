"""HTML parser and chunker for forarbete documents (F-5b)."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from bs4 import BeautifulSoup, Comment, Tag

logger = logging.getLogger("paragrafenai.noop")


@dataclass
class ParsedSection:
    """In-memory representation of one parsed section."""

    title: str
    paragraphs: list[str] = field(default_factory=list)
    page: int | None = None


class ForarbeteParser:
    """Parses raw SOU/prop JSON payloads into normalized chunk documents."""

    def __init__(
        self,
        config_path: str | Path = "config/sources.yaml",
        max_chunk_tokens: int = 800,
        min_chunk_tokens: int = 50,
    ) -> None:
        self.repo_root = Path(__file__).resolve().parent.parent
        self.config_path = self._resolve_path(config_path)
        self.max_chunk_tokens = max_chunk_tokens
        self.min_chunk_tokens = min_chunk_tokens

        payload = self._load_yaml(self.config_path)
        parsing_cfg = payload.get("html_parsing", {}) if isinstance(payload, dict) else {}

        self.header_tags = set(parsing_cfg.get("header_tags", ["h1", "h2", "h3"]))
        self.paragraph_tags = set(parsing_cfg.get("paragraph_tags", ["p", "li"]))
        self.remove_tags = set(parsing_cfg.get("remove_tags", ["script", "style"]))
        self.remove_classes = set(parsing_cfg.get("remove_classes", ["nav", "header", "footer", "breadcrumb", "sidebar"]))
        self.page_tag_names = set(parsing_cfg.get("page_tag_names", ["span"]))
        self.page_class_names = set(parsing_cfg.get("page_class_names", ["page"]))

        page_patterns = parsing_cfg.get(
            "page_number_patterns",
            [
                r"<!--\s*Page\s+(\d+)\s*-->",
                r"<span[^>]*class=['\"]?page['\"]?[^>]*>\s*(\d+)\s*</span>",
                r"\bPage\s+(\d+)\b",
            ],
        )
        self.page_regexes = [re.compile(pattern, re.IGNORECASE) for pattern in page_patterns]
        self.sentence_splitter = re.compile(r"(?<=[.!?])\s+|\.\n+")
        self.blankline_splitter = re.compile(r"\n\s*\n+")

    def parse(self, raw_doc: dict) -> dict | None:
        """
        Parse one raw document from F-5a and return normalized output.

        Returns None when parsing is not possible.
        """
        if not isinstance(raw_doc, dict):
            logger.error("Kan inte parsa dokument: ogiltigt payload-format")
            return None

        html_content = raw_doc.get("html_content")
        html_available = bool(raw_doc.get("html_available"))
        if not html_available or not html_content:
            reason = self._missing_html_reason(raw_doc)
            logger.warning("Hoppar över dokument utan HTML (%s): %s", reason, raw_doc.get("beteckning", "okänt"))
            return None

        html_text = self._coerce_html_text(html_content)
        if not html_text.strip():
            logger.warning("Hoppar över dokument med tom HTML: %s", raw_doc.get("beteckning", "okänt"))
            return None

        soup = BeautifulSoup(html_text, "lxml")
        self._clean_soup(soup)

        sections, strategy = self._extract_sections(soup, html_text)
        if not sections:
            logger.error("Kunde inte extrahera avsnitt: %s", raw_doc.get("beteckning", "okänt"))
            return None

        logger.info(
            "Parser-strategi=%s för dokument=%s",
            strategy,
            raw_doc.get("beteckning", raw_doc.get("dok_id", "okänt")),
        )

        chunks = self._build_chunks(sections)
        if not chunks:
            logger.warning("Inga chunks skapades för %s", raw_doc.get("beteckning", "okänt"))
            return None

        for index, chunk in enumerate(chunks):
            chunk["chunk_index"] = index
            chunk["chunk_total"] = len(chunks)

        beteckning = str(raw_doc.get("beteckning", "")).strip()
        title = str(raw_doc.get("titel") or raw_doc.get("title") or "").strip()
        if not title:
            title = beteckning
            logger.warning("titel saknas, använder beteckning som title: %s", beteckning)

        output: dict[str, Any] = {
            "beteckning": beteckning,
            "title": title,
            "year": self._extract_year(raw_doc),
            "department": str(raw_doc.get("organ", "") or "").strip(),
            "source_url": str(raw_doc.get("source_url", "") or "").strip(),
            "chunks": chunks,
        }
        return output

    def _resolve_path(self, path_value: str | Path) -> Path:
        path = Path(path_value)
        if path.is_absolute():
            return path
        return self.repo_root / path

    def _load_yaml(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            logger.warning("Config saknas, använder default parserregler: %s", path)
            return {}
        try:
            with path.open("r", encoding="utf-8") as fh:
                payload = yaml.safe_load(fh) or {}
                if isinstance(payload, dict):
                    return payload
        except Exception as exc:
            logger.warning("Kunde inte läsa parser-config (%s): %s", path, exc)
        return {}

    def _coerce_html_text(self, html_content: Any) -> str:
        if isinstance(html_content, bytes):
            for encoding in ("utf-8", "windows-1252", "latin-1"):
                try:
                    return html_content.decode(encoding)
                except UnicodeDecodeError:
                    continue
            return html_content.decode("utf-8", errors="replace")

        if isinstance(html_content, str):
            return html_content

        return str(html_content)

    def _clean_soup(self, soup: BeautifulSoup) -> None:
        for tag_name in self.remove_tags:
            for node in soup.find_all(tag_name):
                node.decompose()

        for class_name in self.remove_classes:
            for node in soup.find_all(class_=self._class_matcher(class_name)):
                node.decompose()

    def _class_matcher(self, class_name: str):
        target = class_name.lower()

        def _matcher(classes: Any) -> bool:
            if classes is None:
                return False
            if isinstance(classes, str):
                values = classes.split()
            else:
                values = [str(value) for value in classes]
            lowered = {value.lower() for value in values}
            return target in lowered

        return _matcher

    def _extract_sections(self, soup: BeautifulSoup, html_text: str) -> tuple[list[ParsedSection], str]:
        sections = self._extract_header_sections(soup)
        if sections:
            return sections, "headers"

        sections = self._extract_paragraph_sections(soup)
        if sections:
            return sections, "paragraphs"

        sections = self._extract_raw_sections(soup, html_text)
        if sections:
            return sections, "raw"

        return [], "none"

    def _extract_header_sections(self, soup: BeautifulSoup) -> list[ParsedSection]:
        sections: list[ParsedSection] = []
        current: ParsedSection | None = None
        current_page: int | None = None

        for node in soup.descendants:
            if isinstance(node, Comment):
                current_page = self._extract_page_from_text(str(node)) or current_page
                continue
            if not isinstance(node, Tag):
                continue

            if self._looks_like_page_tag(node):
                current_page = self._extract_page_from_tag(node) or current_page

            if node.name in self.header_tags:
                title = self._normalize_text(node.get_text(" ", strip=True))
                if not title:
                    continue
                if current and current.paragraphs:
                    sections.append(current)
                current = ParsedSection(title=title, page=current_page)
                continue

            if node.name in self.paragraph_tags and not self._is_nested_content_tag(node):
                if current is None:
                    continue
                paragraph = self._normalize_text(node.get_text(" ", strip=True))
                if not paragraph:
                    continue
                current.paragraphs.append(paragraph)
                if current.page is None and current_page is not None:
                    current.page = current_page

        if current and current.paragraphs:
            sections.append(current)
        return sections

    def _extract_paragraph_sections(self, soup: BeautifulSoup) -> list[ParsedSection]:
        sections: list[ParsedSection] = []
        current_page: int | None = None

        for node in soup.descendants:
            if isinstance(node, Comment):
                current_page = self._extract_page_from_text(str(node)) or current_page
                continue
            if not isinstance(node, Tag):
                continue

            if self._looks_like_page_tag(node):
                current_page = self._extract_page_from_tag(node) or current_page

            if node.name in self.paragraph_tags and not self._is_nested_content_tag(node):
                paragraph = self._normalize_text(node.get_text(" ", strip=True))
                if not paragraph:
                    continue
                title = paragraph[:60].rstrip() + "..."
                sections.append(ParsedSection(title=title, paragraphs=[paragraph], page=current_page))

        return sections

    def _extract_raw_sections(self, soup: BeautifulSoup, html_text: str) -> list[ParsedSection]:
        text = soup.get_text("\n", strip=True)
        normalized = re.sub(r"[ \t]+", " ", text)
        blocks = [self._normalize_text(chunk) for chunk in self.blankline_splitter.split(normalized) if chunk.strip()]
        if not blocks:
            return []

        first_page = self._extract_page_from_text(html_text)
        sections: list[ParsedSection] = []
        for idx, block in enumerate(blocks, start=1):
            sections.append(ParsedSection(title=f"Avsnitt {idx}", paragraphs=[block], page=first_page))
        return sections

    def _looks_like_page_tag(self, node: Tag) -> bool:
        if node.name not in self.page_tag_names:
            return False
        classes = {str(value).lower() for value in (node.get("class") or [])}
        return any(class_name in classes for class_name in self.page_class_names)

    def _extract_page_from_tag(self, node: Tag) -> int | None:
        text = node.get_text(" ", strip=True)
        return self._extract_page_from_text(text)

    def _extract_page_from_text(self, text: str) -> int | None:
        for regex in self.page_regexes:
            match = regex.search(text)
            if match:
                try:
                    return int(match.group(1))
                except (TypeError, ValueError, IndexError):
                    continue
        return None

    def _is_nested_content_tag(self, node: Tag) -> bool:
        for parent in node.parents:
            if isinstance(parent, Tag) and parent.name in self.paragraph_tags:
                return True
        return False

    def _normalize_text(self, text: str) -> str:
        return re.sub(r"\s+", " ", text).strip()

    def _estimate_tokens(self, text: str) -> int:
        if not text.strip():
            return 0
        return int(len(text.split()) * 1.3)

    def _split_overlong_paragraph(self, paragraph: str) -> list[str]:
        if self._estimate_tokens(paragraph) <= self.max_chunk_tokens:
            return [paragraph]

        sentences = [self._normalize_text(chunk) for chunk in self.sentence_splitter.split(paragraph) if chunk.strip()]
        if len(sentences) <= 1:
            return self._split_by_words(paragraph)

        parts: list[str] = []
        current: list[str] = []
        current_tokens = 0
        for sentence in sentences:
            sentence_tokens = self._estimate_tokens(sentence)
            if current and current_tokens + sentence_tokens > self.max_chunk_tokens:
                parts.append(" ".join(current).strip())
                current = [sentence]
                current_tokens = sentence_tokens
            else:
                current.append(sentence)
                current_tokens += sentence_tokens

        if current:
            parts.append(" ".join(current).strip())

        return [part for part in parts if part]

    def _split_by_words(self, text: str) -> list[str]:
        words = text.split()
        if not words:
            return []

        words_per_chunk = max(int(self.max_chunk_tokens / 1.3), 1)
        parts: list[str] = []
        for idx in range(0, len(words), words_per_chunk):
            parts.append(" ".join(words[idx : idx + words_per_chunk]))
        return parts

    def _chunk_section(self, section: ParsedSection) -> list[dict[str, Any]]:
        paragraphs: list[str] = []
        for paragraph in section.paragraphs:
            normalized = self._normalize_text(paragraph)
            if not normalized:
                continue
            paragraphs.extend(self._split_overlong_paragraph(normalized))

        if not paragraphs:
            return []

        chunks: list[dict[str, Any]] = []
        idx = 0
        total_paragraphs = len(paragraphs)

        while idx < total_paragraphs:
            selected: list[str] = []
            selected_tokens = 0
            cursor = idx

            while cursor < total_paragraphs:
                candidate = paragraphs[cursor]
                candidate_tokens = self._estimate_tokens(candidate)
                if selected and selected_tokens + candidate_tokens > self.max_chunk_tokens:
                    break
                selected.append(candidate)
                selected_tokens += candidate_tokens
                cursor += 1

                if selected_tokens >= self.max_chunk_tokens:
                    break

            if not selected:
                selected = [paragraphs[idx]]
                cursor = idx + 1

            chunks.append(
                {
                    "text": "\n\n".join(selected),
                    "section_title": section.title,
                    "pinpoint": self._pinpoint(section),
                    "legal_area": [],
                }
            )

            if cursor >= total_paragraphs:
                break

            if len(selected) > 1:
                next_idx = cursor - 1
            else:
                next_idx = cursor
            idx = next_idx if next_idx > idx else cursor

        filtered = [chunk for chunk in chunks if self._estimate_tokens(chunk["text"]) >= self.min_chunk_tokens]
        if filtered:
            return filtered

        logger.warning("Alla chunks under minimi-gräns i avsnitt '%s', slår ihop till en chunk", section.title)
        return [
            {
                "text": "\n\n".join(paragraphs),
                "section_title": section.title,
                "pinpoint": self._pinpoint(section),
                "legal_area": [],
            }
        ]

    def _pinpoint(self, section: ParsedSection) -> str:
        if section.page is not None:
            return f"s. {section.page}"
        fallback = self._normalize_text(section.title)[:30]
        return fallback or "Avsnitt"

    def _build_chunks(self, sections: list[ParsedSection]) -> list[dict[str, Any]]:
        chunks: list[dict[str, Any]] = []
        for section in sections:
            chunks.extend(self._chunk_section(section))
        return chunks

    def _extract_year(self, raw_doc: dict[str, Any]) -> int:
        year = raw_doc.get("year")
        if isinstance(year, int):
            return year
        if isinstance(year, str) and year.isdigit():
            return int(year)

        beteckning = str(raw_doc.get("beteckning", ""))
        match = re.search(r"(\d{4})\s*:", beteckning)
        if match:
            return int(match.group(1))
        return 0

    def _missing_html_reason(self, raw_doc: dict[str, Any]) -> str:
        year = self._extract_year(raw_doc)
        if year and year < 1980:
            return "pdf_only"
        return "no_html"
