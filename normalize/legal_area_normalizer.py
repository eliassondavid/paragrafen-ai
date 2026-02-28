"""Legal area normalization helpers for F-5b."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger("paragrafenai.noop")


class LegalAreaNormalizer:
    """Normalizes legal areas against config/legal_areas.yaml."""

    def __init__(self, config_path: str | Path = "config/legal_areas.yaml") -> None:
        self.repo_root = Path(__file__).resolve().parent.parent
        self.config_path = self._resolve_path(config_path)

        payload = self._load_yaml(self.config_path)
        self.valid_ids: set[str] = set()
        self.alias_to_id: dict[str, str] = {}
        self.excluded_ids: set[str] = set()

        for item in payload.get("legal_areas", []) or []:
            area_id = str(item.get("id", "")).strip()
            if not area_id:
                continue

            self.valid_ids.add(area_id)
            self.alias_to_id[area_id.lower()] = area_id
            if bool(item.get("excluded", False)):
                self.excluded_ids.add(area_id)

            for alias in item.get("aliases", []) or []:
                alias_value = str(alias).strip().lower()
                if alias_value:
                    self.alias_to_id[alias_value] = area_id

    def normalize(self, raw_areas: list[str]) -> list[str]:
        """
        Normalize legal areas against controlled vocabulary.

        Unknown values are kept and logged.
        """
        if not raw_areas:
            return raw_areas

        normalized: list[str] = []
        seen: set[str] = set()

        for raw_value in raw_areas:
            value = str(raw_value).strip()
            if not value:
                continue

            canonical = self.alias_to_id.get(value.lower(), value)
            if canonical not in self.valid_ids:
                logger.warning("Okänt legal_area-värde, behålls: %s", canonical)

            if canonical not in seen:
                seen.add(canonical)
                normalized.append(canonical)

        return normalized

    def is_excluded(self, area_id: str) -> bool:
        """Return True if the legal area is excluded in §AI."""
        value = str(area_id).strip()
        if not value:
            return False
        canonical = self.alias_to_id.get(value.lower(), value)
        return canonical in self.excluded_ids

    def _resolve_path(self, path_value: str | Path) -> Path:
        path = Path(path_value)
        if path.is_absolute():
            return path
        return self.repo_root / path

    def _load_yaml(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            logger.warning("legal_areas-config saknas: %s", path)
            return {}
        try:
            with path.open("r", encoding="utf-8") as fh:
                payload = yaml.safe_load(fh) or {}
                if isinstance(payload, dict):
                    return payload
        except Exception as exc:
            logger.error("Kunde inte läsa legal_areas-config (%s): %s", path, exc)
        return {}
