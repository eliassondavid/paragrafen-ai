"""Extraktion av innehållsförteckning (TOC) för SOU-dokument."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

TOC_PATTERN = re.compile(r"([A-ZÅÄÖ][^\n\.]{5,80}?)\s*\.{2,}\s*(\d{1,4})\s")


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Läser YAML-konfiguration."""
    if config_path is None:
        config_path = Path("config/sou_api_config.yaml")
    config_path = Path(config_path)
    with config_path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def sanitize_doc_name(name: str) -> str:
    """Skapar stabilt katalognamn för dokument."""
    return re.sub(r"[\\/:*?\"<>|]", "_", name.strip())


def _is_monotonic_enough(entries: list[dict[str, Any]], max_non_monotonic: int) -> bool:
    """Validerar monotonitet med tillåten felkvot (2 per 10 poster)."""
    if len(entries) < 2:
        return False

    non_monotonic = 0
    previous = entries[0]["page"]
    for current in entries[1:]:
        if current["page"] < previous:
            non_monotonic += 1
        previous = current["page"]

    windows = max(1, len(entries) // 10)
    allowed_non_monotonic = max_non_monotonic * windows
    return non_monotonic <= allowed_non_monotonic


def extract_toc(text: str, config: dict[str, Any] | None = None) -> list[dict[str, Any]] | None:
    """Extraherar TOC-rader ur text och returnerar lista eller None."""
    cfg = config or load_config()
    toc_cfg = cfg.get("toc", {})

    min_entries = int(toc_cfg.get("min_entries", 3))
    max_page_number = int(toc_cfg.get("max_page_number", 2000))
    max_non_monotonic = int(toc_cfg.get("max_non_monotonic", 2))

    matches = TOC_PATTERN.findall(text)
    if not matches:
        return None

    entries: list[dict[str, Any]] = []
    for section, page_raw in matches:
        page = int(page_raw)
        if page <= 0 or page > max_page_number:
            continue
        entries.append({"section": section.strip(), "page": page})

    if len(entries) < min_entries:
        logger.info("TOC under min_entries (%s < %s)", len(entries), min_entries)
        return None

    if not _is_monotonic_enough(entries, max_non_monotonic=max_non_monotonic):
        logger.warning("TOC kasserad p.g.a. för många icke-monotona sidnummer.")
        return None

    return entries


def save_toc(
    toc: list[dict[str, Any]] | None,
    doc_name: str,
    output_dir: Path | None = None,
    config: dict[str, Any] | None = None,
) -> Path:
    """Sparar TOC till `data/norm/sou/{namn}/toc.json`."""
    cfg = config or load_config()
    extraction_cfg = cfg.get("extraction", {})
    output_dir = output_dir or Path(str(extraction_cfg.get("output_dir", "data/norm/sou/")))

    doc_dir = output_dir / sanitize_doc_name(doc_name)
    doc_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "document": doc_name,
        "toc_found": bool(toc),
        "toc": toc or [],
    }

    target = doc_dir / "toc.json"
    with target.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)

    logger.info("TOC sparad till %s", target)
    return target


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
