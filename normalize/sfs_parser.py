"""Parse SFS HTML documents into paragraph-level chunks."""

from __future__ import annotations

import html as html_lib
import re
from dataclasses import dataclass
from typing import Any

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - fallback is covered instead
    BeautifulSoup = None  # type: ignore[assignment]


@dataclass(frozen=True, slots=True)
class _Heading:
    """Represents the latest heading metadata seen before a paragraph."""

    start: int
    kapitel_nr: str | None
    kapitel_titel: str | None


class SfsParser:
    """Parser that chunks SFS documents with paragraph-first strategy."""

    KAPITEL_PATTERNS = [
        re.compile(r"^(\d+)\s*[Kk]ap\.?\s*(.*)$"),
        re.compile(r"^[Kk]apitel\s+(\d+)\.?\s*(.*)$"),
        re.compile(r"^[Kk][Aa][Pp]\.?\s*(\d+)\.?\s*(.*)$"),
    ]

    _PARAGRAF_START_RE = re.compile(
        r"(?m)^\s*(?:(?P<num_a>\d+[A-Za-z]?)\s*§|§\s*(?P<num_b>\d+[A-Za-z]?))(?=\s|$)"
    )
    _NUMBERED_START_RE = re.compile(r"(?m)^\s*(?P<num>\d+[A-Za-z]?)\.\s+")
    _SPECIAL_HEADING_RE = re.compile(
        r"(?i)^(?P<heading>Övergångsbestämmelser|Bilag(?:a|or)|Ikraftträdande(?:bestämmelser)?)\b.*$"
    )

    def __init__(self, max_fallback_tokens: int = 1200) -> None:
        self.max_fallback_tokens = max_fallback_tokens

    def parse(self, raw_doc: dict[str, Any]) -> dict[str, Any] | None:
        """Parse one raw SFS document into normalized chunk schema."""
        if not raw_doc.get("html_available", True):
            return None

        html_content = str(raw_doc.get("html_content", "")).strip()
        if not html_content:
            return None

        text = self._normalize_text(self._clean_html_to_text(html_content))
        if not text:
            return None

        headings = self._extract_headings(text)
        chunks = self._parse_paragraf_chunks(text, headings)

        if not chunks:
            chunks = self._parse_numbered_chunks(text)
        if not chunks:
            chunks = self._parse_paragraph_fallback(text)
        if not chunks:
            return None

        total = len(chunks)
        ikraft = self._as_optional_str(raw_doc.get("ikraftträdandedatum"))
        for index, chunk in enumerate(chunks):
            chunk["chunk_index"] = index
            chunk["chunk_total"] = total
            chunk["ikraftträdandedatum"] = ikraft

        return {
            "sfs_nr": str(raw_doc.get("sfs_nr", "")),
            "titel": str(raw_doc.get("titel", "")),
            "ikraftträdandedatum": ikraft,
            "consolidation_source": str(raw_doc.get("consolidation_source", "")),
            "source_url": str(raw_doc.get("source_url", "")),
            "chunks": chunks,
        }

    def _clean_html_to_text(self, html_content: str) -> str:
        """Remove non-content HTML and return readable text."""
        if BeautifulSoup is None:
            return self._clean_html_fallback(html_content)

        try:
            soup = BeautifulSoup(html_content, "lxml")
        except Exception:
            soup = BeautifulSoup(html_content, "html.parser")

        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        for selector in (
            "nav",
            "header",
            "footer",
            "aside",
            ".breadcrumb",
            ".breadcrumbs",
            ".pagination",
            ".cookie",
            ".cookies",
            "#menu",
            "#nav",
        ):
            for el in soup.select(selector):
                el.decompose()

        return soup.get_text(separator="\n")

    def _clean_html_fallback(self, html_content: str) -> str:
        cleaned = re.sub(r"(?is)<(script|style|noscript)\b.*?>.*?</\1>", " ", html_content)
        cleaned = re.sub(r"(?i)<br\s*/?>", "\n", cleaned)
        cleaned = re.sub(r"(?i)</(p|div|li|h1|h2|h3|h4|h5|h6|tr|section|article)>", "\n", cleaned)
        cleaned = re.sub(r"(?s)<[^>]+>", " ", cleaned)
        return html_lib.unescape(cleaned)

    def _normalize_text(self, text: str) -> str:
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        lines = [line.strip() for line in text.split("\n")]
        text = "\n".join(lines)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]{2,}", " ", text)
        return text.strip()

    def _extract_headings(self, text: str) -> list[_Heading]:
        headings: list[_Heading] = []
        offset = 0
        for line in text.splitlines(keepends=True):
            stripped = line.strip()
            if stripped:
                chapter_heading = self._parse_chapter_line(stripped)
                if chapter_heading is not None:
                    headings.append(_Heading(offset, chapter_heading[0], chapter_heading[1]))
                else:
                    special_heading = self._parse_special_heading(stripped)
                    if special_heading is not None:
                        headings.append(_Heading(offset, None, special_heading))
            offset += len(line)
        return headings

    def _parse_chapter_line(self, line: str) -> tuple[str, str | None] | None:
        for pattern in self.KAPITEL_PATTERNS:
            match = pattern.match(line)
            if match:
                kapitel_nr = match.group(1).strip()
                kapitel_titel = self._as_optional_str(match.group(2))
                return kapitel_nr, kapitel_titel
        return None

    def _parse_special_heading(self, line: str) -> str | None:
        match = self._SPECIAL_HEADING_RE.match(line)
        if not match:
            return None
        heading = match.group("heading")
        lowered = heading.lower()
        if lowered.startswith("övergång"):
            return "Övergångsbestämmelser"
        if lowered.startswith("bilag"):
            return "Bilaga"
        return "Ikraftträdande"

    def _parse_paragraf_chunks(
        self, text: str, headings: list[_Heading]
    ) -> list[dict[str, Any]]:
        matches = list(self._PARAGRAF_START_RE.finditer(text))
        if not matches:
            return []

        chunks: list[dict[str, Any]] = []
        for index, match in enumerate(matches):
            start = match.start()
            end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
            chunk_text = text[start:end].strip()
            if not chunk_text:
                continue

            paragraf_nr = (match.group("num_a") or match.group("num_b") or "").strip().lower() or None
            kapitel_nr, kapitel_titel = self._heading_for_position(start, headings)
            chunks.append(
                {
                    "paragraf_nr": paragraf_nr,
                    "kapitel_nr": kapitel_nr,
                    "kapitel_titel": kapitel_titel,
                    "text": chunk_text,
                    "legal_area": [],
                }
            )
        return chunks

    def _parse_numbered_chunks(self, text: str) -> list[dict[str, Any]]:
        matches = list(self._NUMBERED_START_RE.finditer(text))
        if not matches:
            return []

        chunks: list[dict[str, Any]] = []
        for index, match in enumerate(matches):
            start = match.start()
            end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
            chunk_text = text[start:end].strip()
            if not chunk_text:
                continue

            chunks.append(
                {
                    "paragraf_nr": match.group("num").strip().lower(),
                    "kapitel_nr": None,
                    "kapitel_titel": None,
                    "text": chunk_text,
                    "legal_area": [],
                }
            )
        return chunks

    def _parse_paragraph_fallback(self, text: str) -> list[dict[str, Any]]:
        blocks = [block.strip() for block in re.split(r"\n{2,}", text) if block.strip()]
        if not blocks:
            return []

        chunks: list[dict[str, Any]] = []
        current_blocks: list[str] = []
        current_tokens = 0

        for block in blocks:
            block_tokens = self._token_estimate(block)
            if current_blocks and current_tokens + block_tokens > self.max_fallback_tokens:
                chunk_text = "\n\n".join(current_blocks).strip()
                if chunk_text:
                    chunks.append(
                        {
                            "paragraf_nr": None,
                            "kapitel_nr": None,
                            "kapitel_titel": None,
                            "text": chunk_text,
                            "legal_area": [],
                        }
                    )
                current_blocks = [block]
                current_tokens = block_tokens
                continue

            current_blocks.append(block)
            current_tokens += block_tokens

        if current_blocks:
            chunk_text = "\n\n".join(current_blocks).strip()
            if chunk_text:
                chunks.append(
                    {
                        "paragraf_nr": None,
                        "kapitel_nr": None,
                        "kapitel_titel": None,
                        "text": chunk_text,
                        "legal_area": [],
                    }
                )
        return chunks

    def _heading_for_position(
        self, position: int, headings: list[_Heading]
    ) -> tuple[str | None, str | None]:
        chosen: _Heading | None = None
        for heading in headings:
            if heading.start > position:
                break
            chosen = heading
        if chosen is None:
            return None, None
        return chosen.kapitel_nr, chosen.kapitel_titel

    def _token_estimate(self, text: str) -> int:
        # Light-weight approximation that keeps fallback chunks reasonably bounded.
        return len(re.findall(r"\S+", text))

    def _as_optional_str(self, value: Any) -> str | None:
        if value is None:
            return None
        converted = str(value).strip()
        return converted or None
