"""Typed relation extraction for forarbete documents."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

logger = logging.getLogger("paragrafenai.noop")

RELATION_TYPES = {
    "bet": {"prop": "treats", "sfs": "produces", "rskr": "confirms"},
    "rskr": {"bet": "confirms", "prop": "cites", "sfs": "produces"},
    "dir": {"sou": "mandates", "ds": "mandates"},
    "ds": {"dir": "implements", "prop": "cites"},
    "prop": {"sou": "cites", "sfs": "cites", "dir": "cites"},
    "sou": {"dir": "implements", "prop": "cites"},
}


class RelationsExtractor:
    """
    Extrahera typade relationer från dokumentstatus-JSON.

    Källa: dokreferens-blocket i /dokumentstatus/{dok_id}.json
    Output: JSON-serialiserad lista av {"target": "...", "relation": "..."}
    """

    def extract(self, status_json: dict[str, Any], from_subtype: str) -> str:
        """
        Returnerar JSON-sträng: '[{"target": "forarbete::prop_...", "relation": "treats"}]'
        """
        relations: list[dict[str, str]] = []
        for reference in self._iter_references(status_json):
            dok_id = self._first_non_empty(reference, "dok_id", "id")
            doktyp = self._normalize_subtype(
                self._first_non_empty(reference, "doktyp", "typ", "subtyp")
            )
            beteckning = self._first_non_empty(reference, "beteckning", "refdok")

            target = self._map_dok_id_to_namespace(dok_id, doktyp)
            if target is None and beteckning:
                target = self._map_beteckning_to_namespace(beteckning)
            if target is None:
                unknown_key = dok_id or beteckning or "unknown"
                target = f"unknown::{unknown_key}"

            to_subtype = self._infer_target_subtype(target, doktyp, beteckning)
            relation = self._infer_relation(from_subtype, to_subtype)
            relations.append({"target": target, "relation": relation})

        deduped: list[dict[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for relation in relations:
            key = (relation["target"], relation["relation"])
            if key in seen:
                continue
            seen.add(key)
            deduped.append(relation)
        return json.dumps(deduped, ensure_ascii=False)

    def _map_dok_id_to_namespace(self, dok_id: str, doktyp: str) -> str | None:
        """
        Försök mappa riksdagens dok_id till §AI namespace.
        Returnera None om mapping ej möjlig (sparas som unknown::).
        """
        normalized_doktyp = self._normalize_subtype(doktyp)
        value = (dok_id or "").strip()
        if not value:
            return None
        if value.startswith("forarbete::") or value.startswith("sfs::"):
            return value

        sfs_match = re.search(r"(\d{4})[-:](\d+)$", value)
        if normalized_doktyp == "sfs" and sfs_match:
            return f"sfs::{sfs_match.group(1)}:{int(sfs_match.group(2))}"

        if normalized_doktyp in {"prop", "sou", "ds", "dir", "bet", "rskr", "nja_ii"}:
            textual_match = re.search(
                r"(prop|sou|ds|dir|bet|rskr|nja_ii)[_:](\d{4}(?:[-/]\d{2,4})?)[:_ -]?([a-z0-9]+)",
                value,
                re.IGNORECASE,
            )
            if textual_match:
                subtype = textual_match.group(1).lower()
                first = textual_match.group(2).replace("/", "-")
                second = re.sub(r"[^\w]+", "_", textual_match.group(3)).strip("_").lower()
                return f"forarbete::{subtype}_{first}_{second}"

        return None

    def _infer_relation(self, from_subtype: str, to_subtype: str) -> str:
        """Slå upp relationstyp i RELATION_TYPES. Fallback: 'cites'."""
        source = self._normalize_subtype(from_subtype)
        target = self._normalize_subtype(to_subtype)
        return RELATION_TYPES.get(source, {}).get(target, "cites")

    def _iter_references(self, payload: Any) -> list[dict[str, Any]]:
        blocks = self._find_dokreferens_blocks(payload)
        references: list[dict[str, Any]] = []
        for block in blocks:
            references.extend(self._flatten_reference_block(block))
        return references

    def _find_dokreferens_blocks(self, payload: Any) -> list[Any]:
        blocks: list[Any] = []
        if isinstance(payload, dict):
            for key, value in payload.items():
                if key.lower() == "dokreferens":
                    blocks.append(value)
                else:
                    blocks.extend(self._find_dokreferens_blocks(value))
        elif isinstance(payload, list):
            for item in payload:
                blocks.extend(self._find_dokreferens_blocks(item))
        return blocks

    def _flatten_reference_block(self, block: Any) -> list[dict[str, Any]]:
        if isinstance(block, dict):
            if any(key in block for key in ("dok_id", "id", "beteckning", "doktyp", "typ")):
                return [block]
            flattened: list[dict[str, Any]] = []
            for value in block.values():
                flattened.extend(self._flatten_reference_block(value))
            return flattened
        if isinstance(block, list):
            flattened: list[dict[str, Any]] = []
            for item in block:
                flattened.extend(self._flatten_reference_block(item))
            return flattened
        return []

    def _map_beteckning_to_namespace(self, beteckning: str) -> str | None:
        value = (beteckning or "").strip()
        if not value:
            return None

        patterns = [
            ("prop", r"(?i)\bprop\.?\s*(\d{4}(?:/\d{2,4})?)\s*:\s*(\d+)\b"),
            ("sou", r"(?i)\bsou\s+(\d{4})\s*:\s*(\d+)\b"),
            ("ds", r"(?i)\bds\s+(\d{4})\s*:\s*(\d+)\b"),
            ("dir", r"(?i)\bdir\.?\s*(\d{4})\s*:\s*(\d+)\b"),
            ("rskr", r"(?i)\brskr\.?\s*(\d{4}(?:/\d{2,4})?)\s*:\s*(\d+)\b"),
            ("nja_ii", r"(?i)\bnja\s*ii\s*(\d{4})\s*s\.\s*(\d+)\b"),
        ]
        for subtype, pattern in patterns:
            match = re.search(pattern, value)
            if not match:
                continue
            first = match.group(1).replace("/", "-")
            second = match.group(2)
            return f"forarbete::{subtype}_{first}_{second}"

        bet_match = re.search(
            r"(?i)\bbet\.?\s*(\d{4}(?:/\d{2,4})?)\s*:\s*([a-zåäö]{1,6}\d+[a-zåäö]?)\b",
            value,
        )
        if bet_match:
            first = bet_match.group(1).replace("/", "-")
            second = re.sub(r"[^\w]+", "_", bet_match.group(2), flags=re.IGNORECASE).strip("_").lower()
            return f"forarbete::bet_{first}_{second}"

        sfs_match = re.search(r"(?i)\bsfs\s+(\d{4})\s*:\s*(\d+)\b", value)
        if sfs_match:
            return f"sfs::{sfs_match.group(1)}:{int(sfs_match.group(2))}"

        return None

    def _infer_target_subtype(self, target: str, doktyp: str, beteckning: str) -> str:
        normalized = self._normalize_subtype(doktyp)
        if normalized:
            return normalized
        if target.startswith("sfs::"):
            return "sfs"
        match = re.match(r"forarbete::([a-z0-9_]+)_", target)
        if match:
            return match.group(1)
        mapped = self._map_beteckning_to_namespace(beteckning)
        if mapped and mapped.startswith("forarbete::"):
            return mapped.split("::", 1)[1].split("_", 1)[0]
        return "unknown"

    def _normalize_subtype(self, value: str) -> str:
        normalized = (value or "").strip().casefold().replace("proposition", "prop")
        alias_map = {
            "betankande": "bet",
            "betänkande": "bet",
            "utskottsbetankande": "bet",
            "utskottsbetänkande": "bet",
            "riksskrivelse": "rskr",
            "skr": "rskr",
            "kommittedirektiv": "dir",
            "kommittédirektiv": "dir",
        }
        return alias_map.get(normalized, normalized)

    def _first_non_empty(self, payload: dict[str, Any], *keys: str) -> str:
        for key in keys:
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""


__all__ = [
    "RELATION_TYPES",
    "RelationsExtractor",
]
