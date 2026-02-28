from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import yaml

LOGGER = logging.getLogger("paragrafenai.noop")

BUILT_IN_KEYWORDS: dict[str, list[str]] = {
    "straffratt": [
        "brott",
        "brottslig",
        "straff",
        "fängelse",
        "åtal",
        "åklagare",
        "häkte",
        "häktad",
        "dom",
        "dömdes",
        "stöld",
        "rån",
        "mord",
        "misshandel",
        "bedrägeri",
        "narkotika",
        "rattfylleri",
        "brottsbalken",
    ],
    "asyl": [
        "asyl",
        "asylansökan",
        "flyktingstatus",
        "uppehållstillstånd",
        "ut",
        "utvisning",
        "avvisning",
        "migrationsverket",
        "migrationsdomstol",
        "flykting",
        "skyddsstatus",
    ],
    "skatteratt": [
        "skatt",
        "inkomstskatt",
        "moms",
        "mervärdesskatt",
        "skattedeklaration",
        "skatteverket",
        "skattebrott",
        "f-skatt",
        "deklaration",
    ],
    "vbu": [
        "vårdnad",
        "umgänge",
        "boende",
        "ensam vårdnad",
        "gemensam vårdnad",
        "umgängesrätt",
        "barnets bästa",
        "socialnämnden",
    ],
}

BUILT_IN_AREAS: list[dict[str, Any]] = [
    {
        "id": "straffrätt",
        "label": "Straffrätt",
        "sfs_patterns": ["1962:700", "2010:1408"],
        "message": "Denna tjänst täcker inte straffrättsliga frågor. Kontakta en advokat eller rättshjälpen.",
    },
    {
        "id": "asyl",
        "label": "Asylrätt och migration",
        "sfs_patterns": ["2005:716", "2016:752"],
        "message": "Asylrättsliga frågor kräver juridiskt ombud. Kontakta Advokatjouren eller Rådgivningsbyrån för asylsökande.",
    },
    {
        "id": "skatterätt",
        "label": "Skatterätt",
        "sfs_patterns": ["1999:1229"],
        "message": "För skattefrågor, kontakta Skatteverket eller en skatterådgivare.",
    },
    {
        "id": "vbu",
        "label": "Vårdnad, boende och umgänge",
        "sfs_patterns": ["1949:381_kap6"],
        "message": "Tvister om vårdnad, boende och umgänge kräver juridiskt ombud. Kontakta familjerätten i din kommun.",
    },
]


def _normalize_area_id(area_id: str) -> str:
    return (
        area_id.lower()
        .replace("å", "a")
        .replace("ä", "a")
        .replace("ö", "o")
        .replace(" ", "")
    )


class AreaBlocker:
    def __init__(self, config_path: str | Path | None = None) -> None:
        self.config_path = Path(config_path) if config_path else self._resolve_default_config_path()
        self._areas = self._load_areas()
        self._keyword_patterns = self._build_keyword_patterns(self._areas)

    def is_blocked(self, query: str) -> tuple[bool, str | None]:
        """
        Kontrollera om en användarfråga berör ett exkluderat rättsområde.

        Returnerar (True, hänvisningsmeddelande) om blockerad.
        Returnerar (False, None) om ej blockerad.
        """
        if not query:
            return (False, None)

        for area in self._areas:
            area_id = _normalize_area_id(str(area.get("id", "")))
            pattern = self._keyword_patterns.get(area_id)
            if not pattern:
                continue
            if pattern.search(query):
                return (True, self._get_message(area))
        return (False, None)

    def is_sfs_blocked(self, sfs_nr: str) -> tuple[bool, str | None]:
        """
        Kontrollera om ett SFS-nummer tillhör ett exkluderat rättsområde.
        Används vid retrieval för att filtrera bort chunks.

        Returnerar (True, hänvisningsmeddelande) om blockerad.
        Returnerar (False, None) om ej blockerad.
        """
        if not sfs_nr:
            return (False, None)

        normalized_sfs = sfs_nr.strip().lower()

        for area in self._areas:
            for pattern in self._iter_sfs_patterns(area):
                normalized_pattern = pattern.lower()
                if "_kap" in normalized_pattern:
                    base_pattern = normalized_pattern.split("_kap", 1)[0]
                    if normalized_sfs == normalized_pattern:
                        return (True, self._get_message(area))
                    if "_kap" in normalized_sfs and normalized_sfs.startswith(f"{base_pattern}_kap"):
                        return (True, self._get_message(area))
                    continue

                if normalized_sfs == normalized_pattern:
                    return (True, self._get_message(area))

        return (False, None)

    @staticmethod
    def _resolve_default_config_path() -> Path:
        here = Path(__file__).resolve()
        for candidate in [here.parent, *here.parents]:
            config_path = candidate / "config" / "excluded_areas.yaml"
            if config_path.exists():
                return config_path
        return here.parents[1] / "config" / "excluded_areas.yaml"

    def _load_areas(self) -> list[dict[str, Any]]:
        if not self.config_path.exists():
            LOGGER.warning("excluded_areas.yaml not found at %s. Falling back to built-in areas.", self.config_path)
            return list(BUILT_IN_AREAS)

        try:
            raw_data = yaml.safe_load(self.config_path.read_text(encoding="utf-8"))
        except OSError:
            LOGGER.exception("Failed to read excluded areas config at %s. Falling back to built-ins.", self.config_path)
            return list(BUILT_IN_AREAS)
        except yaml.YAMLError:
            LOGGER.exception("Failed to parse excluded areas config at %s. Falling back to built-ins.", self.config_path)
            return list(BUILT_IN_AREAS)

        if not isinstance(raw_data, dict):
            LOGGER.error("Invalid excluded_areas.yaml format: top level is not a mapping. Falling back to built-ins.")
            return list(BUILT_IN_AREAS)

        excluded_areas = raw_data.get("excluded_areas")
        if not isinstance(excluded_areas, list):
            LOGGER.error("Invalid excluded_areas.yaml format: 'excluded_areas' must be a list. Falling back to built-ins.")
            return list(BUILT_IN_AREAS)

        normalized_areas: list[dict[str, Any]] = []
        for area in excluded_areas:
            if not isinstance(area, dict):
                continue
            area_id = str(area.get("id", "")).strip()
            if not area_id:
                continue
            normalized_areas.append(
                {
                    "id": area_id,
                    "label": str(area.get("label", area_id)),
                    "message": str(area.get("message", "Denna fråga täcks inte av tjänsten.")),
                    "keywords": area.get("keywords"),
                    "sfs_patterns": area.get("sfs_patterns", []),
                }
            )

        if not normalized_areas:
            LOGGER.error("No valid area entries found in excluded_areas.yaml. Falling back to built-ins.")
            return list(BUILT_IN_AREAS)

        return normalized_areas

    def _build_keyword_patterns(self, areas: list[dict[str, Any]]) -> dict[str, re.Pattern[str]]:
        patterns: dict[str, re.Pattern[str]] = {}

        for area in areas:
            area_id_raw = str(area.get("id", ""))
            area_id = _normalize_area_id(area_id_raw)
            if not area_id:
                continue

            terms = self._build_terms_for_area(area)
            escaped_terms = [re.escape(term) for term in terms if term]
            if not escaped_terms:
                continue

            pattern = re.compile(r"\b(?:" + "|".join(escaped_terms) + r")\b", flags=re.IGNORECASE)
            patterns[area_id] = pattern

        return patterns

    def _build_terms_for_area(self, area: dict[str, Any]) -> list[str]:
        keywords = area.get("keywords")
        terms: list[str] = []

        if isinstance(keywords, list):
            terms.extend(str(term).strip() for term in keywords if str(term).strip())

        normalized_id = _normalize_area_id(str(area.get("id", "")))
        if not terms:
            terms.extend(BUILT_IN_KEYWORDS.get(normalized_id, []))

        if not terms:
            label = str(area.get("label", "")).strip()
            if label:
                terms.append(label)
            area_id = str(area.get("id", "")).strip()
            if area_id:
                terms.append(area_id)

        # Preserve insertion order while de-duplicating.
        deduped_terms = list(
            dict.fromkeys(term.lower() for term in terms if term and len(term.strip()) >= 3)
        )
        return deduped_terms

    @staticmethod
    def _iter_sfs_patterns(area: dict[str, Any]) -> list[str]:
        raw_patterns = area.get("sfs_patterns")
        if not isinstance(raw_patterns, list):
            return []
        return [str(pattern).strip() for pattern in raw_patterns if str(pattern).strip()]

    @staticmethod
    def _get_message(area: dict[str, Any]) -> str:
        message = area.get("message")
        if isinstance(message, str) and message.strip():
            return message
        return "Denna fråga täcks inte av tjänsten."
