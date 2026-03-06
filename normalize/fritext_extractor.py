"""Extraktion och normalisering av API-fritext för pre-1997 SOU."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

import yaml

from ocr_normalizer import normalize_document
from toc_extractor import extract_toc, save_toc

logger = logging.getLogger(__name__)


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Läser YAML-konfiguration."""
    if config_path is None:
        config_path = Path("config/sou_api_config.yaml")
    config_path = Path(config_path)
    with config_path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def sanitize_doc_name(name: str) -> str:
    """Skapar stabilt katalognamn för dokument."""
    cleaned = re.sub(r"[\\/:*?\"<>|]", "_", name.strip())
    return re.sub(r"\s+", " ", cleaned)


def _detect_part_number(doc: dict[str, Any]) -> int | None:
    for key in ("part_number", "del", "delnummer", "part"):
        value = doc.get(key)
        if value is None:
            continue
        try:
            parsed = int(value)
            if parsed > 0:
                return parsed
        except (TypeError, ValueError):
            continue

    for text in (str(doc.get("namn", "")), str(doc.get("titel", ""))):
        match = re.search(r"\bdel\s*(\d{1,2})\b", text, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def storage_doc_name(doc: dict[str, Any]) -> str:
    """Ger dokumentnamn för lagring, inklusive delsuffix vid behov."""
    name = str(doc.get("namn", "okänt_dokument"))
    part = _detect_part_number(doc)
    if part is None:
        return name
    if re.search(r"\bdel\s*\d+\b", name, flags=re.IGNORECASE):
        return name
    return f"{name} del {part:02d}"


def determine_ocr_quality(year: int, config: dict[str, Any] | None = None) -> str:
    """Bestämmer OCR-kvalitet från konfigurerade epoker."""
    cfg = config or load_config()
    epochs = cfg.get("quality", {}).get("epochs", [])

    for epoch in epochs:
        year_range = epoch.get("range", [])
        if len(year_range) != 2:
            continue
        start, end = int(year_range[0]), int(year_range[1])
        if start <= year <= end:
            return str(epoch.get("ocr_quality", "medium"))

    if year < 1970:
        return "low"
    if year <= 1996:
        return "medium"
    return "high"


def _extract_fritext_value(doc: dict[str, Any]) -> str:
    """Extraherar fritext från API-postens `fritext[0]`."""
    fritext = doc.get("fritext")
    if isinstance(fritext, list) and fritext:
        return str(fritext[0] or "")
    if isinstance(fritext, str):
        return fritext
    return ""


def _append_skipped_document(
    doc: dict[str, Any],
    reason: str,
    text_length: int,
    config: dict[str, Any],
) -> Path:
    """Lägger till dokument i skipped-listan."""
    skipped_file = Path(str(config.get("empty_document", {}).get("skipped_file", "data/cache/skipped_documents.json")))
    skipped_file.parent.mkdir(parents=True, exist_ok=True)

    payload: list[dict[str, Any]] = []
    if skipped_file.exists():
        try:
            with skipped_file.open("r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except (json.JSONDecodeError, OSError):
            payload = []

    payload.append(
        {
            "document": storage_doc_name(doc),
            "year": int(doc.get("ar", 0)),
            "reason": reason,
            "text_length": text_length,
        }
    )

    with skipped_file.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)

    return skipped_file


def extract_and_normalize_fritext(
    doc: dict[str, Any], config: dict[str, Any] | None = None
) -> dict[str, Any] | None:
    """Extraherar API-fritext, normaliserar OCR och sparar text.json."""
    cfg = config or load_config()
    empty_cfg = cfg.get("empty_document", {})
    ocr_cfg = cfg.get("ocr", {})
    extraction_cfg = cfg.get("extraction", {})

    doc_name = storage_doc_name(doc)
    year = int(doc.get("ar", 0))
    raw_text = _extract_fritext_value(doc)

    min_fritext_length = int(empty_cfg.get("min_fritext_length", 500))
    if len(raw_text) < min_fritext_length:
        logger.warning("Tom/kort fritext för %s (%s tecken)", doc_name, len(raw_text))
        skipped_file = _append_skipped_document(
            doc=doc,
            reason="fritext_too_short",
            text_length=len(raw_text),
            config=cfg,
        )
        logger.warning("Dokument tillagd i skip-lista: %s", skipped_file)
        return None

    quality = determine_ocr_quality(year, config=cfg)
    normalize_before_year = int(ocr_cfg.get("normalize_before_year", 1970))
    normalize_optional_max_year = int(ocr_cfg.get("normalize_optional_max_year", 1996))

    normalized_text = raw_text
    corrections_count = 0

    if year < normalize_before_year:
        norm_report = normalize_document(doc_name=doc_name, text=raw_text, quality=quality, config=cfg)
        normalized_text = norm_report["normalized_text"]
        corrections_count = int(norm_report["corrections_count"])
    elif year <= normalize_optional_max_year:
        norm_report = normalize_document(doc_name=doc_name, text=raw_text, quality=quality, config=cfg)
        normalized_text = norm_report["normalized_text"]
        corrections_count = int(norm_report["corrections_count"])
        logger.info("OCR-normalisering körd för 1970-1996 (%s), korrektioner=%s", doc_name, corrections_count)

    toc = extract_toc(normalized_text, config=cfg)
    save_toc(toc=toc, doc_name=doc_name, config=cfg)

    payload = {
        "document": doc_name,
        "text_source": "api_fritext",
        "ocr_quality": quality,
        "text_length": len(normalized_text),
        "normalized_text": normalized_text,
        "ocr_corrections": corrections_count,
        "toc_found": bool(toc),
        "toc": toc or [],
    }

    output_dir = Path(str(extraction_cfg.get("output_dir", "data/norm/sou/"))) / sanitize_doc_name(doc_name)
    output_dir.mkdir(parents=True, exist_ok=True)
    text_path = output_dir / "text.json"

    with text_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)

    logger.info("Sparad normaliserad fritext: %s", text_path)
    return payload


def process_fritext_batch(docs: list[dict[str, Any]], config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Bearbetar batch med API-fritextdokument."""
    cfg = config or load_config()

    report: dict[str, Any] = {
        "total": len(docs),
        "success": 0,
        "failed": 0,
        "results": [],
    }

    for doc in docs:
        try:
            result = extract_and_normalize_fritext(doc, config=cfg)
            if result is None:
                report["failed"] += 1
                report["results"].append(
                    {
                        "document": storage_doc_name(doc),
                        "status": "skipped",
                        "reason": "fritext_too_short",
                    }
                )
            else:
                report["success"] += 1
                report["results"].append(
                    {
                        "document": result["document"],
                        "status": "success",
                        "text_length": result["text_length"],
                    }
                )
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Misslyckad fritext-bearbetning för %s: %s", storage_doc_name(doc), exc)
            report["failed"] += 1
            report["results"].append(
                {
                    "document": storage_doc_name(doc),
                    "status": "failed",
                    "reason": str(exc),
                }
            )

    return report


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    logger.info("fritext_extractor.py är redo. Anropa extract_and_normalize_fritext().")
