"""PDF-extraktion för SOU-dokument (1997+).

Modulen laddar ner PDF:er, extraherar text per sida med PyMuPDF och sparar
intermediärdata för vidare chunking.
"""

from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from typing import Any

import fitz
import requests
import yaml

logger = logging.getLogger(__name__)


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Läser YAML-konfiguration.

    Config-path kan sättas via env `SOU_CONFIG_PATH`; annars används
    `config/sou_api_config.yaml`.
    """
    if config_path is None:
        config_path = Path("config/sou_api_config.yaml")
    config_path = Path(config_path)
    with config_path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def sanitize_doc_name(name: str) -> str:
    """Skapar stabilt katalognamn för dokument."""
    cleaned = re.sub(r"[\\/:*?\"<>|]", "_", name.strip())
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def detect_part_number(doc: dict[str, Any]) -> int | None:
    """Identifierar delnummer för flerbandsverk om det finns."""
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

    text_candidates = [str(doc.get("namn", "")), str(doc.get("titel", ""))]
    for text in text_candidates:
        match = re.search(r"\bdel\s*(\d{1,2})\b", text, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def build_pdf_filename(doc: dict[str, Any]) -> str:
    """Bygger PDF-filnamn enligt namespace-konvention."""
    year = int(doc["ar"])
    number = int(doc["nummer"])
    part = detect_part_number(doc)
    if part is not None:
        return f"sou_{year}_{number}_del{part:02d}.pdf"
    return f"sou_{year}_{number}.pdf"


def _write_response_to_file(response: requests.Response, target_path: Path) -> None:
    """Skriver HTTP-body till temporär fil och flyttar atomiskt."""
    tmp_path = target_path.with_suffix(target_path.suffix + ".part")
    with tmp_path.open("wb") as fh:
        for chunk in response.iter_content(chunk_size=1024 * 64):
            if chunk:
                fh.write(chunk)
    tmp_path.replace(target_path)


def download_pdf(
    doc: dict[str, Any],
    output_dir: Path | None = None,
    config: dict[str, Any] | None = None,
    session: requests.Session | None = None,
) -> Path | None:
    """Laddar ner PDF för ett SOU-dokument.

    Returnerar sökväg till PDF vid lyckad nedladdning, annars `None`.
    """
    cfg = config or load_config()
    pdf_cfg = cfg.get("pdf", {})
    api_cfg = cfg.get("api", {})

    output_dir = output_dir or Path(str(pdf_cfg.get("download_dir", "data/raw/sou/pdf/")))
    output_dir.mkdir(parents=True, exist_ok=True)

    url = str(doc.get("url", "")).strip()
    if not url:
        logger.warning("Saknar PDF-URL för %s", doc.get("namn", "okänt"))
        return None

    filename = build_pdf_filename(doc)
    target_path = output_dir / filename

    min_valid_size = int(pdf_cfg.get("min_valid_size_bytes", 10240))
    if target_path.exists() and target_path.stat().st_size >= min_valid_size:
        logger.info("Hoppar över befintlig PDF: %s", target_path)
        return target_path

    timeout = int(pdf_cfg.get("http_timeout", api_cfg.get("timeout", 60)))
    max_retries = int(pdf_cfg.get("max_retries", api_cfg.get("max_retries", 3)))
    backoff_base = int(api_cfg.get("backoff_base", 2))

    http_client = session or requests.Session()

    for attempt in range(1, max_retries + 1):
        try:
            response = http_client.get(url, timeout=timeout, stream=True)
            status = response.status_code
            if status == 404:
                logger.warning("PDF saknas (404) för %s", doc.get("namn", "okänt"))
                return None
            if status >= 500:
                raise requests.HTTPError(f"Serverfel {status}")
            response.raise_for_status()

            _write_response_to_file(response, target_path)
            size = target_path.stat().st_size
            if size < min_valid_size:
                logger.warning("Korrupt PDF (<10KB) för %s: %s", doc.get("namn", "okänt"), target_path)
                target_path.unlink(missing_ok=True)
                return None

            max_size_warn = int(pdf_cfg.get("max_size_warn_bytes", 52428800))
            if size > max_size_warn:
                logger.warning("Stor PDF (>%s byte): %s", max_size_warn, target_path)

            logger.info("Nedladdad PDF: %s", target_path)
            return target_path
        except (requests.RequestException, OSError) as exc:
            if attempt >= max_retries:
                logger.error(
                    "Misslyckad nedladdning efter %s försök för %s (%s)",
                    max_retries,
                    doc.get("namn", "okänt"),
                    exc,
                )
                return None
            delay_seconds = backoff_base**attempt
            logger.warning(
                "Nedladdningsfel (%s/%s) för %s: %s. Väntar %ss.",
                attempt,
                max_retries,
                doc.get("namn", "okänt"),
                exc,
                delay_seconds,
            )
            time.sleep(delay_seconds)
    return None


def download_all_pdfs(
    docs: list[dict[str, Any]], output_dir: Path | None = None, config: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Laddar ner alla PDF:er för SOU:er med ar >= 1997."""
    cfg = config or load_config()
    pdf_cfg = cfg.get("pdf", {})
    api_cfg = cfg.get("api", {})

    output_dir = output_dir or Path(str(pdf_cfg.get("download_dir", "data/raw/sou/pdf/")))
    output_dir.mkdir(parents=True, exist_ok=True)

    rate_limit_delay = float(api_cfg.get("rate_limit_delay", 0.5))

    eligible_docs = [doc for doc in docs if int(doc.get("ar", 0)) >= 1997]
    result: dict[str, Any] = {
        "total": len(eligible_docs),
        "success": 0,
        "failed": 0,
        "skipped_existing": 0,
        "failed_urls": [],
    }

    for idx, doc in enumerate(eligible_docs):
        target_path = output_dir / build_pdf_filename(doc)
        min_valid_size = int(pdf_cfg.get("min_valid_size_bytes", 10240))

        if target_path.exists() and target_path.stat().st_size >= min_valid_size:
            result["skipped_existing"] += 1
            logger.info("Befintlig PDF, skippar: %s", target_path)
        else:
            downloaded_path = download_pdf(doc, output_dir=output_dir, config=cfg)
            if downloaded_path is None:
                result["failed"] += 1
                result["failed_urls"].append(
                    {
                        "namn": str(doc.get("namn", "okänt")),
                        "url": str(doc.get("url", "")),
                        "error": "download_failed",
                    }
                )
            else:
                result["success"] += 1

        if idx < len(eligible_docs) - 1:
            time.sleep(rate_limit_delay)

    return result


def extract_pages(pdf_path: str | Path, config: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Extraherar text per sida med 1-baserad sidnumrering."""
    cfg = config or load_config()
    extraction_cfg = cfg.get("extraction", {})
    page_base = int(extraction_cfg.get("page_number_base", 1))

    pages: list[dict[str, Any]] = []

    try:
        document = fitz.open(str(pdf_path))
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Kunde inte öppna PDF %s: %s", pdf_path, exc)
        return pages

    try:
        if document.needs_pass:
            logger.error("Lösenordsskyddad PDF, avbryter: %s", pdf_path)
            return pages

        for idx in range(document.page_count):
            page_number = idx + page_base
            text = ""
            try:
                page = document.load_page(idx)
                text = page.get_text("text") or ""
            except Exception as exc:  # pylint: disable=broad-except
                logger.error("Fel vid textextraktion sida %s i %s: %s", page_number, pdf_path, exc)

            pages.append(
                {
                    "page_number": page_number,
                    "text": text,
                    "char_count": len(text),
                }
            )
    finally:
        document.close()

    return pages


def save_pages(
    pages: list[dict[str, Any]],
    doc_name: str,
    output_dir: Path | None = None,
    config: dict[str, Any] | None = None,
    document_label: str | None = None,
) -> Path:
    """Sparar extraherade sidor i `pages.json`."""
    cfg = config or load_config()
    extraction_cfg = cfg.get("extraction", {})
    output_dir = output_dir or Path(str(extraction_cfg.get("output_dir", "data/norm/sou/")))

    doc_dir = output_dir / sanitize_doc_name(doc_name)
    doc_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "document": document_label or doc_name,
        "total_pages": len(pages),
        "extraction_tool": str(extraction_cfg.get("tool", "pymupdf")),
        "pages": pages,
    }

    target_path = doc_dir / "pages.json"
    with target_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)

    logger.info("Sparade sidor till %s", target_path)
    return target_path


def process_pdf_document(doc: dict[str, Any], config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Bearbetar ett dokument: nedladdning + extraktion + lagring."""
    cfg = config or load_config()
    extraction_cfg = cfg.get("extraction", {})
    min_text_threshold = int(extraction_cfg.get("min_text_threshold", 100))

    result: dict[str, Any] = {
        "document": str(doc.get("namn", "okänt")),
        "status": "failed",
        "pdf_path": None,
        "pages_path": None,
        "reason": None,
    }

    pdf_path = download_pdf(doc, config=cfg)
    if pdf_path is None:
        result["reason"] = "download_failed"
        return result

    pages = extract_pages(pdf_path, config=cfg)
    if not pages:
        result["reason"] = "extraction_failed_or_protected"
        return result

    total_chars = sum(page["char_count"] for page in pages)
    if total_chars < min_text_threshold:
        logger.warning("Bildbaserad eller tom PDF för %s (<%s tecken)", doc.get("namn", "okänt"), min_text_threshold)
        result["reason"] = "image_based_pdf"
        result["pdf_path"] = str(pdf_path)
        return result

    pages_path = save_pages(
        pages,
        doc_name=str(doc.get("namn", "okänt")),
        config=cfg,
        document_label=str(doc.get("namn", "okänt")),
    )

    result.update(
        {
            "status": "success",
            "pdf_path": str(pdf_path),
            "pages_path": str(pages_path),
            "reason": None,
        }
    )
    return result


def process_pdf_batch(docs: list[dict[str, Any]], config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Kör PDF-bearbetning för en batch dokument."""
    cfg = config or load_config()
    rate_limit_delay = float(cfg.get("api", {}).get("rate_limit_delay", 0.5))

    eligible_docs = [doc for doc in docs if int(doc.get("ar", 0)) >= 1997]
    report: dict[str, Any] = {
        "total": len(eligible_docs),
        "processed": 0,
        "success": 0,
        "failed": 0,
        "results": [],
    }

    for idx, doc in enumerate(eligible_docs):
        result = process_pdf_document(doc, config=cfg)
        report["processed"] += 1
        report["results"].append(result)

        if result["status"] == "success":
            report["success"] += 1
        else:
            report["failed"] += 1

        if idx < len(eligible_docs) - 1:
            time.sleep(rate_limit_delay)

    return report


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    logger.info(
        "pdf_extractor.py är redo. Kör via import och anropa process_pdf_batch() "
        "eller download_all_pdfs()."
    )
