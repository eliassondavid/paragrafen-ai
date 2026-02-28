"""Klarspråkslager för post-processing av LLM-svar i §AI."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger("paragrafenai.noop")


class KlarsprakLayer:
    """Apply simple language improvements to legal LLM responses."""

    def __init__(self, config_dir: str = "config") -> None:
        self._repo_root = Path(__file__).resolve().parent.parent
        self._config_dir = self._resolve_config_dir(config_dir)

        terms_payload = self._load_yaml(self._config_dir / "legal_terms.yaml")
        passive_payload = self._load_yaml(self._config_dir / "passive_patterns.yaml")

        raw_terms = terms_payload.get("legal_terms", {})
        self._legal_terms: dict[str, str] = {}
        if isinstance(raw_terms, dict):
            for term, explanation in raw_terms.items():
                normalized_term = str(term).strip()
                normalized_explanation = str(explanation).strip()
                if normalized_term and normalized_explanation:
                    self._legal_terms[normalized_term] = normalized_explanation

        self._passive_patterns: list[dict[str, str]] = []
        raw_patterns = passive_payload.get("passive_patterns", [])
        if isinstance(raw_patterns, list):
            for item in raw_patterns:
                if not isinstance(item, dict):
                    continue
                pattern = str(item.get("pattern", "")).strip()
                replacement = str(item.get("replacement", "")).strip()
                if pattern and replacement:
                    self._passive_patterns.append({"pattern": pattern, "replacement": replacement})

    def process(self, answer: str, query: str, legal_area: str | None = None) -> str:
        """Run all F-10 transformations in the defined order."""
        _ = query
        processed = str(answer)

        processed = self._inject_term_explanations(processed)
        processed = self._split_long_sentences(processed)
        processed = self._rewrite_passive_patterns(processed)
        processed = self._inject_heading_if_needed(processed, legal_area)

        return processed

    def _resolve_config_dir(self, config_dir: str) -> Path:
        path = Path(config_dir)
        if path.is_absolute():
            return path
        return self._repo_root / path

    def _load_yaml(self, path: Path) -> dict[str, Any]:
        try:
            with path.open("r", encoding="utf-8") as fh:
                payload = yaml.safe_load(fh) or {}
                if isinstance(payload, dict):
                    return payload
                return {}
        except FileNotFoundError:
            logger.warning("Konfigurationsfil saknas: %s", path)
            return {}
        except yaml.YAMLError as exc:
            logger.error("Ogiltig YAML i %s: %s", path, exc)
            return {}

    def _inject_term_explanations(self, text: str) -> str:
        updated_text = text
        for term, explanation in self._legal_terms.items():
            replacement = f"{term} ({explanation})"
            updated_text = re.sub(
                rf"\b{re.escape(term)}\b",
                replacement,
                updated_text,
                count=1,
                flags=re.IGNORECASE,
            )
        return updated_text

    def _split_long_sentences(self, text: str) -> str:
        current = text
        for _ in range(3):
            next_text = self._split_pass_once(current)
            if next_text == current:
                break
            current = next_text
        return current

    def _split_pass_once(self, text: str) -> str:
        chunks = re.findall(r"[^.!?]+[.!?]*", text)
        if not chunks:
            return text

        processed_chunks: list[str] = []
        for chunk in chunks:
            processed_chunks.append(self._split_chunk_if_needed(chunk))
        return "".join(processed_chunks)

    def _split_chunk_if_needed(self, chunk: str) -> str:
        match = re.match(r"^(\s*)(.*?)([.!?]*)$", chunk, flags=re.DOTALL)
        if not match:
            return chunk

        leading_ws = match.group(1)
        body = match.group(2)
        trailing_punct = match.group(3)

        body_stripped = body.strip()
        if len(body_stripped.split()) <= 40:
            return chunk

        split_idx, delimiter = self._find_split_point(body_stripped)
        if split_idx < 0:
            return chunk

        left = body_stripped[:split_idx].rstrip(" ,;")
        right = body_stripped[split_idx + len(delimiter):].lstrip()

        if not left or not right:
            return chunk

        return f"{leading_ws}{left}. {right}{trailing_punct}"

    def _find_split_point(self, text: str) -> tuple[int, str]:
        for delimiter in [", och ", ", men ", "; "]:
            idx = self._find_outside_parentheses_and_quotes(text, delimiter)
            if idx >= 0:
                return idx, delimiter
        return -1, ""

    def _find_outside_parentheses_and_quotes(self, text: str, delimiter: str) -> int:
        paren_depth = 0
        in_quotes = False
        max_start = len(text) - len(delimiter)

        for idx, char in enumerate(text):
            if char == '"':
                in_quotes = not in_quotes
            elif not in_quotes:
                if char == "(":
                    paren_depth += 1
                elif char == ")" and paren_depth > 0:
                    paren_depth -= 1

            if idx > max_start:
                continue

            if paren_depth == 0 and not in_quotes and text.startswith(delimiter, idx):
                return idx

        return -1

    def _rewrite_passive_patterns(self, text: str) -> str:
        updated_text = text
        for item in self._passive_patterns:
            pattern = item["pattern"]
            replacement = item["replacement"]
            regex = re.compile(re.escape(pattern), flags=re.IGNORECASE)

            def replace_match(match: re.Match[str]) -> str:
                return self._preserve_capitalization(match.group(0), replacement)

            updated_text = regex.sub(replace_match, updated_text)

        return updated_text

    def _preserve_capitalization(self, original: str, replacement: str) -> str:
        if original.isupper():
            return replacement.upper()
        if original[:1].isupper():
            return replacement[:1].upper() + replacement[1:]
        return replacement

    def _inject_heading_if_needed(self, text: str, legal_area: str | None) -> str:
        has_heading = any(
            line.startswith("#") or line.strip().startswith("##")
            for line in text.splitlines()
        )
        word_count = len(text.split())

        if has_heading or word_count <= 200:
            return text

        if legal_area:
            heading = f"## Vad lagen säger om {legal_area}"
        else:
            heading = "## Vad lagen säger"

        return f"{heading}\n\n{text}"
