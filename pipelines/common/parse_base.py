"""Shared HTML parsing primitives for forarbete pipelines."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import re
from typing import Iterable

from bs4 import BeautifulSoup, NavigableString, Tag

logger = logging.getLogger("paragrafenai.noop")


@dataclass
class Section:
    section_key: str
    section_title: str
    text: str
    level: int


class ForarbeteParser:
    """Bas-parser för riksdagens HTML-dokument."""

    HEADER_LEVELS = {"h1": 1, "h2": 2, "h3": 3}
    TEXT_TAGS = {"p", "li", "blockquote", "td", "pre"}
    REMOVE_TAGS = {"script", "style", "noscript"}

    def parse(self, html: str, dok_id: str = "") -> list[Section]:
        """
        Extrahera sektioner från HTML.
        Anropar self.get_section_patterns() för dokumenttypsspecifika mönster.
        """
        if not html.strip():
            return []

        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:
            soup = BeautifulSoup(html, "html.parser")

        for tag_name in self.REMOVE_TAGS:
            for node in soup.find_all(tag_name):
                node.decompose()

        body = soup.body or soup
        patterns = [
            (section_key, re.compile(pattern, re.IGNORECASE))
            for section_key, pattern in self.get_section_patterns()
        ]

        sections: list[Section] = []
        current_title = "Huvudtext"
        current_key = "main"
        current_level = 1
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
                    level=current_level,
                )
            )

        for node in self._iter_relevant_nodes(body):
            if node.name in self.HEADER_LEVELS:
                title = self.clean_text(str(node))
                if not title:
                    continue
                flush_current()
                current_parts = []
                current_title = title
                current_key = self._resolve_section_key(title, patterns)
                current_level = self.HEADER_LEVELS.get(node.name, 1)
                continue

            text = self.clean_text(str(node))
            if text:
                current_parts.append(text)

        flush_current()
        if sections:
            return sections

        fallback_text = self.clean_text(str(body))
        if not fallback_text:
            logger.warning("Parsern extraherade ingen text för dokument %s.", dok_id or "okänt")
            return []

        return [
            Section(
                section_key="main",
                section_title="Huvudtext",
                text=fallback_text,
                level=1,
            )
        ]

    def get_section_patterns(self) -> list[tuple[str, str]]:
        """
        Returnera lista av (section_key, regex_pattern) för sektionsigenkänning.
        Implementeras av adapterns parser.
        """
        return []

    def clean_text(self, html_fragment: str) -> str:
        """Ta bort HTML-taggar, normalisera whitespace."""
        try:
            fragment = BeautifulSoup(html_fragment or "", "html.parser")
            text = fragment.get_text(" ", strip=True)
        except Exception:
            text = html_fragment or ""
        return re.sub(r"\s+", " ", text).strip()

    def _iter_relevant_nodes(self, root: Tag) -> Iterable[Tag]:
        for node in root.descendants:
            if not isinstance(node, Tag):
                continue
            if node.name in self.HEADER_LEVELS:
                yield node
                continue
            if node.name in self.TEXT_TAGS and not self._has_relevant_parent(node):
                yield node
                continue
            if node.name == "div" and not self._contains_structural_children(node):
                text = self.clean_text(str(node))
                if text:
                    yield node

    def _has_relevant_parent(self, node: Tag) -> bool:
        for parent in node.parents:
            if not isinstance(parent, Tag):
                continue
            if parent.name in self.TEXT_TAGS:
                return True
        return False

    def _contains_structural_children(self, node: Tag) -> bool:
        for child in node.children:
            if isinstance(child, Tag) and child.name in self.HEADER_LEVELS | self.TEXT_TAGS:
                return True
            if isinstance(child, NavigableString) and child.strip():
                return False
        return False

    def _resolve_section_key(
        self,
        title: str,
        patterns: list[tuple[str, re.Pattern[str]]],
    ) -> str:
        for section_key, pattern in patterns:
            if pattern.search(title):
                return section_key
        slug = re.sub(r"[^\w]+", "", title.casefold())
        return slug or "main"


__all__ = [
    "ForarbeteParser",
    "Section",
]
