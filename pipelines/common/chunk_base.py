"""Shared chunking primitives for forarbete pipelines."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import re

from pipelines.common.parse_base import Section
from transformers import AutoTokenizer

logger = logging.getLogger("paragrafenai.noop")


@dataclass
class ChunkConfig:
    min_tokens: int = 150
    max_tokens: int = 350
    overlap_tokens: int = 35
    min_chunk_chars: int = 50


@dataclass
class ChunkedSection:
    section_path: str
    section_title: str
    chunk_text: str
    token_count: int
    chunk_index: int


class ForarbeteChunker:
    """
    Tvåstegsmodell:
    Steg 1: Strukturell segmentering (sektioner från parse_base)
    Steg 2: Tokenchunkning inom sektion

    Specialregel: om hela dokumentet < min_tokens → one-document-one-chunk
    """

    def __init__(
        self,
        config: ChunkConfig | None = None,
        tokenizer_name: str = "KBLab/sentence-bert-swedish-cased",
    ):
        self.config = config or ChunkConfig()
        self._tokenizer = AutoTokenizer.from_pretrained(
            tokenizer_name, use_fast=True
        )

    def chunk_sections(self, sections: list[Section]) -> list[ChunkedSection]:
        """
        Chunka sektioner till rätt storlek.
        Bryt aldrig över sektionsgränser.
        Lägg till överlapp från föregående chunk.
        """
        if not sections:
            return []

        usable_sections = [
            section
            for section in sections
            if len((section.text or "").strip()) >= self.config.min_chunk_chars
        ]
        if not usable_sections:
            return []

        total_tokens = sum(self.count_tokens(section.text) for section in usable_sections)
        if total_tokens < self.config.min_tokens:
            combined_text = self._combine_sections(usable_sections)
            if len(combined_text) < self.config.min_chunk_chars:
                return []
            return [
                ChunkedSection(
                    section_path="document",
                    section_title="Hela dokumentet",
                    chunk_text=combined_text,
                    token_count=self.count_tokens(combined_text),
                    chunk_index=0,
                )
            ]

        chunks: list[ChunkedSection] = []
        for section in usable_sections:
            section_chunks = self._chunk_single_section(section)
            chunks.extend(section_chunks)

        for index, chunk in enumerate(chunks):
            chunk.chunk_index = index
        return chunks

    def count_tokens(self, text: str) -> int:
        return len(
            self._tokenizer.encode(text or "", add_special_tokens=False)
        )

    def _chunk_single_section(self, section: Section) -> list[ChunkedSection]:
        text = re.sub(r"\s+", " ", section.text or "").strip()
        if len(text) < self.config.min_chunk_chars:
            return []

        token_count = self.count_tokens(text)
        if token_count <= self.config.max_tokens:
            return [
                ChunkedSection(
                    section_path=section.section_key or "main",
                    section_title=section.section_title or "Huvudtext",
                    chunk_text=text,
                    token_count=token_count,
                    chunk_index=0,
                )
            ]

        words = text.split()
        chunks: list[ChunkedSection] = []
        start = 0
        while start < len(words):
            end = min(start + self.config.max_tokens, len(words))
            chunk_words = words[start:end]
            chunk_text = " ".join(chunk_words).strip()
            if len(chunk_text) >= self.config.min_chunk_chars:
                chunks.append(
                    ChunkedSection(
                        section_path=section.section_key or "main",
                        section_title=section.section_title or "Huvudtext",
                        chunk_text=chunk_text,
                        token_count=len(chunk_words),
                        chunk_index=0,
                    )
                )
            if end >= len(words):
                break
            next_start = max(end - self.config.overlap_tokens, start + 1)
            start = next_start

        return chunks

    def _combine_sections(self, sections: list[Section]) -> str:
        parts: list[str] = []
        for section in sections:
            title = (section.section_title or "").strip()
            text = re.sub(r"\s+", " ", section.text or "").strip()
            if not text:
                continue
            if title:
                parts.append(f"{title}\n{text}")
            else:
                parts.append(text)
        return "\n\n".join(parts).strip()


__all__ = [
    "ChunkConfig",
    "ChunkedSection",
    "ForarbeteChunker",
]
