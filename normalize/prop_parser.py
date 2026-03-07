"""Parse proposition HTML into section-aware chunks with page mapping."""

from __future__ import annotations

import logging
import re
from typing import Any

from bs4 import BeautifulSoup, Tag

logger = logging.getLogger("paragrafenai.noop")

SECTION_PATTERNS = {
    "summary": r"(?i)propositionens\s+huvudsakliga\s+innehåll",
    "toc": r"(?i)innehållsförteckning",
    "background": r"(?i)(ärendet\s+och\s+dess\s+beredning|bakgrund|gällande\s+rätt)",
    "consultation": r"(?i)remissinstanserna",
    "rationale": r"(?i)skälen\s+för\s+(regeringens\s+)?förslag",
    "transitional": r"(?i)(ikraftträdande|övergångsbestämmelser)",
    "consequences": r"(?i)(konsekvenser|konsekvensanalys)",
    "commentary": r"(?i)författningskommentar",
    "legislation": r"(?i)författningsförslag",
    "appendix": r"(?i)bilaga",
}

PAGE_ID_RE = re.compile(r"page_(\d+)$", re.IGNORECASE)
COMMENTARY_SPLIT_RE = re.compile(r"(?m)^\s*((?:\d+\s*kap\.\s*)?\d+\s*§)")


def parse_prop_html(html_content: str, dok_id: str = "") -> list[dict[str, Any]]:
    """Parse proposition HTML and return section records."""
    soup = BeautifulSoup(html_content or "", "html.parser")
    page_sections = _parse_page_sections(soup)
    if page_sections is not None:
        return _split_commentary_sections(_filter_toc_sections(page_sections))

    logger.warning(
        "Dokument %s saknar #page_N-struktur — sidnummer ej tillgängliga",
        dok_id or "okänt",
    )
    fallback_sections = _parse_fallback_sections(soup)
    return _split_commentary_sections(_filter_toc_sections(fallback_sections))


def _parse_page_sections(soup: BeautifulSoup) -> list[dict[str, Any]] | None:
    pages: list[dict[str, Any]] = []
    for page_node in soup.find_all(id=PAGE_ID_RE):
        if not isinstance(page_node, Tag):
            continue
        match = PAGE_ID_RE.fullmatch(str(page_node.get("id", "")).strip())
        if not match:
            continue
        page_number = int(match.group(1))
        raw_text = _normalize_page_text(page_node.get_text("\n", strip=True), preserve_breaks=True)
        search_text = _normalize_page_text(raw_text)
        pages.append(
            {
                "page": page_number,
                "raw_text": raw_text,
                "search_text": search_text,
            }
        )

    if not pages:
        return None

    pages.sort(key=lambda item: item["page"])
    boundaries: list[dict[str, Any]] = []
    for page in pages:
        match = _find_section_match(page["search_text"])
        if match is None:
            continue
        section_name, section_title = match
        boundaries.append(
            {
                "section": section_name,
                "section_title": section_title,
                "page_start": page["page"],
            }
        )

    if not boundaries:
        return [
            {
                "section": "other",
                "section_title": "other",
                "text": _join_page_texts(pages, pages[0]["page"], pages[-1]["page"]),
                "page_start": pages[0]["page"],
                "page_end": pages[-1]["page"],
            }
        ]

    sections: list[dict[str, Any]] = []
    first_page = pages[0]["page"]
    last_page = pages[-1]["page"]
    if boundaries[0]["page_start"] > first_page:
        sections.append(
            {
                "section": "other",
                "section_title": "other",
                "text": _join_page_texts(pages, first_page, boundaries[0]["page_start"] - 1),
                "page_start": first_page,
                "page_end": boundaries[0]["page_start"] - 1,
            }
        )

    for index, boundary in enumerate(boundaries):
        next_page = boundaries[index + 1]["page_start"] if index + 1 < len(boundaries) else last_page + 1
        page_end = next_page - 1
        sections.append(
            {
                "section": boundary["section"],
                "section_title": boundary["section_title"],
                "text": _join_page_texts(pages, boundary["page_start"], page_end),
                "page_start": boundary["page_start"],
                "page_end": page_end,
            }
        )

    return sections


def _parse_fallback_sections(soup: BeautifulSoup) -> list[dict[str, Any]]:
    full_text = _normalize_page_text(soup.get_text("\n", strip=True), preserve_breaks=True)
    search_text = _normalize_page_text(full_text)
    matches = _find_all_section_matches(search_text)
    if not matches:
        return [
            {
                "section": "other",
                "section_title": "other",
                "text": full_text,
                "page_start": 0,
                "page_end": 0,
            }
        ]

    sections: list[dict[str, Any]] = []
    if matches[0]["start"] > 0:
        prefix = search_text[: matches[0]["start"]].strip()
        if prefix:
            sections.append(
                {
                    "section": "other",
                    "section_title": "other",
                    "text": prefix,
                    "page_start": 0,
                    "page_end": 0,
                }
            )

    for index, match in enumerate(matches):
        next_start = matches[index + 1]["start"] if index + 1 < len(matches) else len(search_text)
        sections.append(
            {
                "section": match["section"],
                "section_title": match["title"],
                "text": search_text[match["start"] : next_start].strip(),
                "page_start": 0,
                "page_end": 0,
            }
        )
    return sections


def _join_page_texts(pages: list[dict[str, Any]], start_page: int, end_page: int) -> str:
    texts = [
        page["raw_text"]
        for page in pages
        if start_page <= int(page["page"]) <= end_page and str(page["raw_text"]).strip()
    ]
    return "\n\n".join(texts).strip()


def _normalize_page_text(text: str, preserve_breaks: bool = False) -> str:
    if preserve_breaks:
        lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines()]
        return "\n".join(line for line in lines if line).strip()
    return re.sub(r"\s+", " ", text or "").strip()


def _find_section_match(text: str) -> tuple[str, str] | None:
    best_match: tuple[str, str, int] | None = None
    for section_name, pattern in SECTION_PATTERNS.items():
        match = re.search(pattern, text)
        if not match:
            continue
        if match.start() > 80:
            continue
        candidate = (section_name, _normalize_page_text(match.group(0)), match.start())
        if best_match is None or candidate[2] < best_match[2]:
            best_match = candidate

    if best_match is None:
        return None
    return best_match[0], best_match[1]


def _find_all_section_matches(text: str) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for section_name, pattern in SECTION_PATTERNS.items():
        for match in re.finditer(pattern, text):
            candidates.append(
                {
                    "section": section_name,
                    "title": _normalize_page_text(match.group(0)),
                    "start": match.start(),
                    "end": match.end(),
                }
            )

    candidates.sort(key=lambda item: (item["start"], item["end"]))
    resolved: list[dict[str, Any]] = []
    current_end = -1
    for candidate in candidates:
        if candidate["start"] < current_end:
            continue
        resolved.append(candidate)
        current_end = candidate["end"]
    return resolved


def _filter_toc_sections(sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [section for section in sections if section.get("section") != "toc"]


def _split_commentary_sections(sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    split_sections: list[dict[str, Any]] = []
    for section in sections:
        if section.get("section") != "commentary":
            split_sections.append(section)
            continue

        matches = list(COMMENTARY_SPLIT_RE.finditer(section.get("text", "")))
        if not matches:
            split_sections.append(section)
            continue

        for index, match in enumerate(matches):
            next_start = matches[index + 1].start() if index + 1 < len(matches) else len(section["text"])
            text = section["text"][match.start() : next_start].strip()
            paragraf = _normalize_page_text(match.group(1))
            split_sections.append(
                {
                    **section,
                    "section_title": f"Författningskommentar — {paragraf}",
                    "text": text,
                }
            )

    return split_sections
