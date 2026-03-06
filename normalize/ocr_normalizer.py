"""OCR-normalisering för pre-1997 SOU-material.

Hanterar vanliga OCR-artefakter där enskilda tecken eller siffror
separerats med mellanslag under skanningsprocessen.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Läser YAML-konfiguration."""
    if config_path is None:
        config_path = Path("config/sou_api_config.yaml")
    config_path = Path(config_path)
    with config_path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def normalize_ocr_spacing(text: str) -> str:
    """Normaliserar OCR-artefakter där enstaka tecken/siffror separerats av mellanslag.

    Regler:
    - Sekvens av ≥2 tokens med exakt 1 bokstav vardera → sammanfogas
      ('r ä t t e g å n g' → 'rättegång')
    - Sekvens av ≥2 tokens med 1–2 siffror vardera → sammanfogas
      ('s t o c k h o l m 19 3 8' → 'stockholm 1938')
    - Normal text lämnas orörd.

    Algoritm: Token-baserad vänster-till-höger genomgång med girig matchning.
    """
    tokens = text.split(" ")
    result = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]

        # Bokstavssekvens: exakt 1 Unicode-bokstav per token
        if len(tok) == 1 and tok.isalpha():
            group = [tok]
            j = i + 1
            while j < len(tokens) and len(tokens[j]) == 1 and tokens[j].isalpha():
                group.append(tokens[j])
                j += 1
            if len(group) >= 2:
                result.append("".join(group))
                i = j
                continue

        # Siffersekvens: 1–2 siffror per token (hanterar '19 3 8', '1 9 3 8')
        elif 1 <= len(tok) <= 2 and tok.isdigit():
            group = [tok]
            j = i + 1
            while j < len(tokens) and 1 <= len(tokens[j]) <= 2 and tokens[j].isdigit():
                group.append(tokens[j])
                j += 1
            if len(group) >= 2:
                result.append("".join(group))
                i = j
                continue

        result.append(tok)
        i += 1

    return " ".join(result)


def _try_hunspell_normalize(text: str, lang: str = "sv_SE") -> tuple[str, int, str]:
    """Försöker hunspell-baserad normalisering. Returnerar (text, corrections, method)."""
    try:
        import hunspell  # type: ignore
        hobj = hunspell.HunSpell(f"/usr/share/hunspell/{lang}.dic", f"/usr/share/hunspell/{lang}.aff")
        words = text.split()
        corrected = []
        count = 0
        for word in words:
            if not hobj.spell(word) and len(word) > 2:
                suggestions = hobj.suggest(word)
                if suggestions:
                    corrected.append(suggestions[0])
                    count += 1
                    continue
            corrected.append(word)
        return " ".join(corrected), count, "hunspell"
    except (ImportError, OSError):
        return text, 0, "unavailable"


def normalize_document(
    doc_name: str,
    text: str,
    quality: str = "medium",
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Normaliserar ett dokuments text med OCR-korrigering.

    Steg 1: spacing-normalisering (alltid).
    Steg 2: hunspell-korrigering om tillgängligt, annars heuristisk fallback.
    """
    cfg = config or load_config()
    ocr_cfg = cfg.get("ocr", {})
    lang = str(ocr_cfg.get("hunspell_dict", "sv_SE"))
    min_seq = int(ocr_cfg.get("min_sequence_length", 3))
    fallback_min_seq = int(ocr_cfg.get("fallback_min_sequence_length", 4))

    # Steg 1: spacing
    spaced_normalized = normalize_ocr_spacing(text)
    spacing_corrections = 0
    if spaced_normalized != text:
        # Räkna antal sammanfogade sekvenser som grov uppskattning
        spacing_corrections = sum(
            1 for a, b in zip(text.split(), spaced_normalized.split()) if a != b
        )

    # Steg 2: hunspell eller heuristisk
    hunspell_text, hunspell_count, method = _try_hunspell_normalize(spaced_normalized, lang=lang)

    if method == "hunspell":
        final_text = hunspell_text
        total_corrections = spacing_corrections + hunspell_count
        validation = "hunspell"
        logger.info("Hunspell-normalisering av %s: %s korrektioner", doc_name, total_corrections)
    else:
        # Heuristisk fallback: logga, använd spacing-normaliserat
        final_text = spaced_normalized
        total_corrections = spacing_corrections
        validation = "heuristic"
        logger.info(
            "Heuristisk fallback för %s (hunspell ej tillgängligt): %s korrektioner",
            doc_name,
            total_corrections,
        )

    return {
        "normalized_text": final_text,
        "corrections_count": total_corrections,
        "validation": validation,
        "quality": quality,
    }
