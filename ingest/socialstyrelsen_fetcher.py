#!/usr/bin/env python3
"""socialstyrelsen_fetcher.py — Discovery + PDF-nedladdning."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import sys
import time
from typing import Any

import requests
from bs4 import BeautifulSoup

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.getLogger("chromadb").setLevel(logging.ERROR)
logging.getLogger("sentence_transformers").setLevel(logging.ERROR)

BASE_URL = "https://www.socialstyrelsen.se"
CATALOG_URL = f"{BASE_URL}/publikationer/"
RAW_DIR = Path("data/raw/socialstyrelsen")
PDF_DIR = RAW_DIR / "pdf"
CATALOG_PATH = RAW_DIR / "catalog.json"
ERRORS_PATH = RAW_DIR / "fetch_errors.jsonl"
SCHEMA_VERSION = "v0.15"


def log(message: str) -> None:
    print(message, flush=True)


def classify_doc_type(name: str) -> str:
    import re

    if re.match(r"SOSFS\s+\d{4}:\d+", name):
        return "sosfs"
    if re.match(r"HSLF-FS\s+\d{4}:\d+", name):
        return "hslf_fs"
    return "other"


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def extract_balanced_json(html: str, start_index: int) -> dict[str, Any]:
    start = html.find("{", start_index)
    if start < 0:
        raise RuntimeError("Kunde inte hitta start på hydration-JSON. ESKALERA.")

    depth = 0
    in_string = False
    escaped = False
    end = start
    for index, char in enumerate(html[start:], start):
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
            continue
        if char == "{":
            depth += 1
            continue
        if char == "}":
            depth -= 1
            if depth == 0:
                end = index
                break
    return json.loads(html[start : end + 1])


def fetch_catalog(session: requests.Session) -> list[dict[str, Any]]:
    """Extrahera alla publikationer från SSR React-hydration i index-sidan."""
    response = session.get(CATALOG_URL, timeout=30)
    response.raise_for_status()
    html = response.text

    marker_index = html.find("SOS.Components.PublicationListPage,")
    if marker_index < 0:
        raise RuntimeError("PublicationListPage inte hittad i HTML — sidstruktur kan ha ändrats. ESKALERA.")

    data = extract_balanced_json(html, marker_index)
    publications = data.get("children", [])
    if len(publications) < 100:
        raise RuntimeError(f"For få publikationer ({len(publications)}) — trolig strukturändring. ESKALERA.")
    if not isinstance(publications, list):
        raise RuntimeError("Hydration-data saknar lista i children. ESKALERA.")
    return [item for item in publications if isinstance(item, dict)]


def fetch_pdf_url(session: requests.Session, pub_url: str) -> str | None:
    """Hämta PDF-URL från publikationssidan."""
    full_url = f"{BASE_URL}{pub_url}"
    try:
        response = session.get(full_url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        for anchor in soup.find_all("a", href=True):
            href = str(anchor["href"]).strip()
            if ".pdf" in href.lower() and "contentassets" in href.lower():
                if not href.startswith("http"):
                    href = f"{BASE_URL}{href}"
                return href
        return None
    except Exception as exc:
        log(f"  Fel vid hämtning av {full_url}: {exc}")
        return None


def download_pdf(session: requests.Session, pdf_url: str, dest_path: Path) -> bool:
    """Ladda ner PDF. Returnera True vid framgång."""
    if dest_path.exists() and dest_path.stat().st_size > 1000:
        return True

    try:
        response = session.get(pdf_url, timeout=60, stream=True)
        response.raise_for_status()
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with dest_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    handle.write(chunk)
        return dest_path.exists() and dest_path.stat().st_size > 1000
    except Exception as exc:
        log(f"  Nedladdningsfel {pdf_url}: {exc}")
        return False


def select_publications(
    publications: list[dict[str, Any]],
    *,
    only_foreskrifter: bool,
    max_docs: int | None,
) -> list[dict[str, Any]]:
    enriched_publications: list[dict[str, Any]] = []
    for publication in publications:
        name = str(publication.get("name") or "").strip()
        doc_type = classify_doc_type(name)
        if only_foreskrifter and doc_type == "other":
            continue
        enriched = dict(publication)
        enriched["doc_type"] = doc_type
        enriched_publications.append(enriched)

    if max_docs is not None and not only_foreskrifter:
        # Testkörningar behöver träffa konverterbara dokument tidigt i flödet.
        type_order = {"sosfs": 0, "hslf_fs": 1, "other": 2}
        enriched_publications.sort(
            key=lambda item: (
                type_order.get(str(item.get("doc_type") or "other"), 9),
                str(item.get("publishOnWebFrom") or ""),
                str(item.get("articleNumber") or ""),
            )
        )

    selected: list[dict[str, Any]] = []
    for enriched in enriched_publications:
        selected.append(enriched)
        if max_docs is not None and len(selected) >= max_docs:
            break
    return selected


def process_publication(
    session: requests.Session,
    publication: dict[str, Any],
) -> dict[str, Any]:
    article_number = str(publication.get("articleNumber") or "").strip()
    result = dict(publication)
    result.setdefault("schema_version", SCHEMA_VERSION)

    if not article_number:
        result["pdf_url"] = None
        result["pdf_path"] = None
        result["pdf_downloaded"] = False
        return result

    pdf_url = fetch_pdf_url(session, str(publication.get("url") or "").strip())
    result["pdf_url"] = pdf_url

    if not pdf_url:
        result["pdf_path"] = None
        result["pdf_downloaded"] = False
        return result

    dest_path = PDF_DIR / f"{article_number}.pdf"
    downloaded = download_pdf(session, pdf_url, dest_path)
    result["pdf_path"] = str(dest_path).replace("\\", "/")
    result["pdf_downloaded"] = downloaded
    return result


def run(
    *,
    max_docs: int | None,
    verbose: bool,
    only_foreskrifter: bool,
) -> dict[str, int]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    ERRORS_PATH.write_text("", encoding="utf-8")

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0 Safari/537.36"
            )
        }
    )

    publications = fetch_catalog(session)
    selected = select_publications(
        publications,
        only_foreskrifter=only_foreskrifter,
        max_docs=max_docs,
    )

    processed: list[dict[str, Any]] = []
    foreskrift_total = 0
    foreskrift_missing_pdf = 0
    pdf_download_failures = 0

    for index, publication in enumerate(selected, start=1):
        result = process_publication(session, publication)
        processed.append(result)

        if result["doc_type"] in {"sosfs", "hslf_fs"}:
            foreskrift_total += 1
            if not result.get("pdf_url"):
                foreskrift_missing_pdf += 1

        if result.get("pdf_url") and not result.get("pdf_downloaded"):
            pdf_download_failures += 1

        if not result.get("pdf_url"):
            append_jsonl(
                ERRORS_PATH,
                {
                    "articleNumber": result.get("articleNumber"),
                    "name": result.get("name"),
                    "url": result.get("url"),
                    "error": "pdf_url_not_found",
                },
            )
        elif not result.get("pdf_downloaded"):
            append_jsonl(
                ERRORS_PATH,
                {
                    "articleNumber": result.get("articleNumber"),
                    "name": result.get("name"),
                    "url": result.get("url"),
                    "pdf_url": result.get("pdf_url"),
                    "error": "pdf_download_failed",
                },
            )

        if verbose or index % 100 == 0:
            status = "OK" if result.get("pdf_downloaded") else "FEL"
            short_name = str(result.get("name") or "")[:80]
            log(f"[{index}/{len(selected)}] {short_name} — {status}")

        if index % 100 == 0:
            save_json(CATALOG_PATH, processed)

        if index < len(selected):
            time.sleep(1.0)

    save_json(CATALOG_PATH, processed)

    if foreskrift_total >= 25 and foreskrift_total > 0:
        missing_ratio = foreskrift_missing_pdf / foreskrift_total
        if missing_ratio > 0.20:
            raise RuntimeError("ESKALERA — PDF-URL kan ej extraheras för > 20% av föreskrifter")

    return {
        "catalog_total": len(publications),
        "selected_total": len(selected),
        "pdf_download_failures": pdf_download_failures,
        "foreskrift_total": foreskrift_total,
        "foreskrift_missing_pdf": foreskrift_missing_pdf,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Hämta Socialstyrelsens publikationer och PDF:er.")
    parser.add_argument("--max-docs", type=int, default=None, help="Begränsa antal dokument.")
    parser.add_argument("--verbose", action="store_true", help="Visa logg per dokument.")
    parser.add_argument(
        "--only-foreskrifter",
        action="store_true",
        help="Hoppa över dokument av typen 'other'.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    stats = run(
        max_docs=args.max_docs,
        verbose=args.verbose,
        only_foreskrifter=args.only_foreskrifter,
    )
    log(
        "Klart: "
        f"katalog={stats['catalog_total']} "
        f"valda={stats['selected_total']} "
        f"foreskrifter={stats['foreskrift_total']} "
        f"saknar_pdf={stats['foreskrift_missing_pdf']} "
        f"download_failures={stats['pdf_download_failures']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
