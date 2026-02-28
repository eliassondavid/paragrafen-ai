"""
sfs_fetcher.py — Hämtar SFS-dokument från Riksdagens öppna API.

Steg S-1 i SFS-pipelinen (paragrafen.ai).

Funktioner:
  - full_crawl()          — Initial hämtning av alla ~11 400 SFS-dokument
  - incremental_update()  — Daglig diff via systemdatum-polling
  - fetch_single(dok_id)  — Hämta ett enskilt dokument (för test/debug)

API-bas: https://data.riksdagen.se/
Licens: Fri att använda med källhänvisning.
Rate limiting: 0.5 sek/request (god sed).

Beslut S4: Riksdagens API är primär och enda källa.
"""

import json
import time
import logging
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Konfiguration
# ---------------------------------------------------------------------------

BASE_URL = "https://data.riksdagen.se"
RAW_DIR = Path("data/raw/sfs")
STATE_DIR = Path("data/state")
STATE_FILE = STATE_DIR / "sfs_last_check.json"

REQUEST_DELAY = 0.5
MAX_RETRIES = 5
RETRY_BACKOFF_FACTOR = 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("sfs_fetcher")

# ---------------------------------------------------------------------------
# HTTP-session
# ---------------------------------------------------------------------------

def _create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "paragrafen-ai/1.0 (https://paragrafen.ai)",
        "Accept": "application/json",
    })
    return session

SESSION = _create_session()

# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

def fetch_document_list(page: int = 1) -> dict:
    url = f"{BASE_URL}/dokumentlista/"
    params = {
        "doktyp": "sfs", "sort": "datum", "sortorder": "desc",
        "utformat": "json", "a": "s", "p": page,
    }
    resp = SESSION.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("dokumentlista", data)


def fetch_document_status(dok_id: str) -> dict:
    url = f"{BASE_URL}/dokumentstatus/{dok_id}.json"
    resp = SESSION.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("dokumentstatus", data)

# ---------------------------------------------------------------------------
# Lagring
# ---------------------------------------------------------------------------

def _safe_name(dok_id: str) -> str:
    return dok_id.replace("/", "_").replace("\\", "_")


def save_raw(dok_id: str, data: dict) -> Path:
    path = RAW_DIR / f"{_safe_name(dok_id)}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path


def load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_systemdatum": "2020-01-01 00:00:00", "last_run": None, "total_documents": 0}


def save_state(state: dict):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

# ---------------------------------------------------------------------------
# Full crawl
# ---------------------------------------------------------------------------

def full_crawl(start_page=1, max_pages=None, skip_existing=True, dry_run=False) -> dict:
    stats = {
        "fetched": 0, "skipped": 0, "errors": 0, "error_details": [],
        "total_pages": 0, "total_documents_api": 0,
        "start_time": datetime.now(timezone.utc).isoformat(),
    }

    log.info("Hämtar dokumentlista, sida %d...", start_page)
    first_result = fetch_document_list(start_page)
    total_pages = int(first_result.get("@sidor", 1))
    total_docs = int(first_result.get("@traffar", 0))
    stats["total_pages"] = total_pages
    stats["total_documents_api"] = total_docs

    end_page = total_pages if max_pages is None else min(start_page + max_pages - 1, total_pages)
    log.info("Totalt %d dokument på %d sidor. Hämtar sida %d–%d.", total_docs, total_pages, start_page, end_page)

    if dry_run:
        log.info("DRY RUN — avslutar.")
        return stats

    page = start_page
    while page <= end_page:
        if page == start_page:
            result = first_result
        else:
            time.sleep(REQUEST_DELAY)
            try:
                result = fetch_document_list(page)
            except requests.RequestException as e:
                log.error("Sida %d: %s", page, e)
                stats["errors"] += 1
                stats["error_details"].append({"page": page, "error": str(e)})
                page += 1
                continue

        documents = result.get("dokument", [])
        if not documents:
            log.warning("Sida %d tom — avslutar.", page)
            break

        for doc in documents:
            dok_id = doc.get("dok_id", "")
            if not dok_id:
                continue

            raw_path = RAW_DIR / f"{_safe_name(dok_id)}.json"
            if skip_existing and raw_path.exists():
                stats["skipped"] += 1
                continue

            time.sleep(REQUEST_DELAY)
            try:
                full_doc = fetch_document_status(dok_id)
                save_raw(dok_id, full_doc)
                stats["fetched"] += 1
                if stats["fetched"] % 100 == 0:
                    log.info("Framsteg: %d hämtade, %d hoppade, %d fel (sida %d/%d)",
                             stats["fetched"], stats["skipped"], stats["errors"], page, end_page)
            except (requests.RequestException, json.JSONDecodeError, KeyError) as e:
                log.error("%s: %s", dok_id, e)
                stats["errors"] += 1
                stats["error_details"].append({"dok_id": dok_id, "error": str(e)})

        page += 1

    stats["end_time"] = datetime.now(timezone.utc).isoformat()

    state = load_state()
    state["total_documents"] = stats["fetched"] + stats["skipped"]
    save_state(state)

    report_path = STATE_DIR / "crawl_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    log.info("Crawl klar: %d hämtade, %d hoppade, %d fel.", stats["fetched"], stats["skipped"], stats["errors"])
    return stats

# ---------------------------------------------------------------------------
# Inkrementell uppdatering
# ---------------------------------------------------------------------------

def incremental_update() -> dict:
    state = load_state()
    last_known = state["last_systemdatum"]
    stats = {"new": 0, "updated": 0, "errors": 0, "new_dok_ids": [], "updated_dok_ids": [],
             "start_time": datetime.now(timezone.utc).isoformat()}

    log.info("Inkrementell uppdatering. Senast: %s", last_known)
    newest = last_known

    for page in range(1, 11):
        time.sleep(REQUEST_DELAY)
        try:
            result = fetch_document_list(page)
        except requests.RequestException as e:
            log.error("Sida %d: %s", page, e)
            stats["errors"] += 1
            break

        documents = result.get("dokument", [])
        if not documents:
            break

        found_old = False
        for doc in documents:
            dok_id = doc.get("dok_id", "")
            systemdatum = doc.get("systemdatum", doc.get("publicerad", ""))
            if not dok_id or not systemdatum:
                continue
            if systemdatum <= last_known:
                found_old = True
                break
            if systemdatum > newest:
                newest = systemdatum

            raw_path = RAW_DIR / f"{_safe_name(dok_id)}.json"
            is_update = raw_path.exists()

            time.sleep(REQUEST_DELAY)
            try:
                full_doc = fetch_document_status(dok_id)
                save_raw(dok_id, full_doc)
                if is_update:
                    stats["updated"] += 1
                    stats["updated_dok_ids"].append(dok_id)
                else:
                    stats["new"] += 1
                    stats["new_dok_ids"].append(dok_id)
                log.info("%s: %s", "Uppdaterad" if is_update else "Ny", dok_id)
            except requests.RequestException as e:
                log.error("%s: %s", dok_id, e)
                stats["errors"] += 1

        if found_old:
            break

    state["last_systemdatum"] = newest
    save_state(state)
    stats["end_time"] = datetime.now(timezone.utc).isoformat()
    log.info("Klar: %d nya, %d uppdaterade, %d fel.", stats["new"], stats["updated"], stats["errors"])
    return stats

# ---------------------------------------------------------------------------
# Enskilt dokument
# ---------------------------------------------------------------------------

def fetch_single(dok_id: str) -> dict:
    log.info("Hämtar %s...", dok_id)
    full_doc = fetch_document_status(dok_id)
    path = save_raw(dok_id, full_doc)
    log.info("Sparat: %s", path)
    return full_doc

# ---------------------------------------------------------------------------
# Verifiering
# ---------------------------------------------------------------------------

def verify_crawl() -> dict:
    local_count = len(list(RAW_DIR.glob("*.json")))
    result = fetch_document_list(1)
    api_count = int(result.get("@traffar", 0))
    report = {
        "local_files": local_count, "api_total": api_count,
        "coverage_pct": round(local_count / api_count * 100, 2) if api_count else 0,
        "missing": api_count - local_count,
        "verified_at": datetime.now(timezone.utc).isoformat(),
    }
    log.info("Verifiering: %d/%d (%.1f%%)", report["local_files"], report["api_total"], report["coverage_pct"])
    return report

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="SFS-fetcher — paragrafen.ai")
    sub = parser.add_subparsers(dest="command")

    cp = sub.add_parser("crawl", help="Full initial crawl")
    cp.add_argument("--start-page", type=int, default=1)
    cp.add_argument("--max-pages", type=int, default=None)
    cp.add_argument("--no-skip", action="store_true")
    cp.add_argument("--dry-run", action="store_true")

    sub.add_parser("update", help="Inkrementell uppdatering")

    sp = sub.add_parser("single", help="Hämta enskilt dokument")
    sp.add_argument("dok_id", help="t.ex. sfs-2017-900")

    sub.add_parser("verify", help="Verifiera crawl")

    args = parser.parse_args()

    if args.command == "crawl":
        s = full_crawl(args.start_page, args.max_pages, not args.no_skip, args.dry_run)
        print(json.dumps(s, indent=2, ensure_ascii=False))
    elif args.command == "update":
        s = incremental_update()
        print(json.dumps(s, indent=2, ensure_ascii=False))
    elif args.command == "single":
        doc = fetch_single(args.dok_id)
        print(f"Hämtat: {args.dok_id} ({len(json.dumps(doc)):,} bytes)")
    elif args.command == "verify":
        r = verify_crawl()
        print(json.dumps(r, indent=2, ensure_ascii=False))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
