#!/usr/bin/env python3
"""
sfs_fetch_missing.py — Hämta saknade SFS-dokument
Kör: python3 sfs_fetch_missing.py

Läser _missing_ids.json och hämtar varje dok_id från Riksdagens API.
Resumable — hoppar över redan hämtade.

Uppskattad tid: ~15 min (1415 dok × 0.5s delay)
"""

import json
import time
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Konfig
# ---------------------------------------------------------------------------
RAW_DIR = Path("data/raw/sfs")
MISSING_FILE = RAW_DIR / "_missing_ids.json"
API_BASE = "https://data.riksdagen.se"
REQUEST_DELAY = 0.5
MAX_RETRIES = 3
TIMEOUT = 30

# ---------------------------------------------------------------------------
# Hjälpfunktioner
# ---------------------------------------------------------------------------

def safe_filename(dok_id: str) -> str:
    """Konvertera dok_id till filnamn. Hanterar mellanslag och specialtecken."""
    return dok_id.replace("/", "_").replace("\\", "_").replace(" ", "_") + ".json"


def fetch_with_retry(url: str) -> requests.Response | None:
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=TIMEOUT)
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 404:
                return None
            else:
                print(f"  HTTP {resp.status_code}, retry {attempt+1}")
        except requests.RequestException as e:
            print(f"  Error: {e}, retry {attempt+1}")
        time.sleep(2 ** attempt)
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not MISSING_FILE.exists():
        print(f"Hittar inte {MISSING_FILE}")
        print("Kör sfs_crawl_diagnostic.py först.")
        sys.exit(1)

    with open(MISSING_FILE, "r", encoding="utf-8") as f:
        missing_ids = json.load(f)

    print(f"Saknade dok_id att hämta: {len(missing_ids)}")
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    stats = {"fetched": 0, "skipped": 0, "failed": 0, "failed_ids": []}
    start = time.time()

    for i, dok_id in enumerate(missing_ids):
        filepath = RAW_DIR / safe_filename(dok_id)

        # Resume: hoppa över redan hämtade
        if filepath.exists() and filepath.stat().st_size > 100:
            stats["skipped"] += 1
            continue

        # Progress
        if (i + 1) % 100 == 0 or i < 3:
            elapsed = time.time() - start
            done = stats["fetched"] + stats["failed"]
            rate = done / max(elapsed, 1)
            remaining = (len(missing_ids) - i) / max(rate, 0.01)
            print(f"[{i+1}/{len(missing_ids)}] {dok_id} "
                  f"({stats['fetched']} ok, {stats['failed']} fel, "
                  f"~{remaining/60:.0f} min kvar)")

        url = f"{API_BASE}/dokumentstatus/{dok_id}.json"
        resp = fetch_with_retry(url)

        if resp is None:
            stats["failed"] += 1
            stats["failed_ids"].append(dok_id)
        else:
            try:
                data = resp.json()
                data["_crawl_meta"] = {
                    "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "fetch_source": "missing_backfill",
                }
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                stats["fetched"] += 1
            except Exception as e:
                print(f"  Parse error {dok_id}: {e}")
                stats["failed"] += 1
                stats["failed_ids"].append(dok_id)

        time.sleep(REQUEST_DELAY)

    elapsed = time.time() - start

    print()
    print("=" * 50)
    print(f"KLAR ({elapsed/60:.1f} min)")
    print(f"  Hämtade:  {stats['fetched']}")
    print(f"  Hoppade:  {stats['skipped']}")
    print(f"  Misslyck: {stats['failed']}")
    print("=" * 50)

    if stats["failed_ids"]:
        fail_file = RAW_DIR / "_still_missing.json"
        with open(fail_file, "w", encoding="utf-8") as f:
            json.dump(stats["failed_ids"], f, ensure_ascii=False, indent=2)
        print(f"Kvarvarande misslyckade: {fail_file}")

    # Slutverifiering
    total_local = len(list(RAW_DIR.glob("*.json"))) - len(list(RAW_DIR.glob("_*.json")))
    print(f"\nTotalt lokala SFS-filer nu: {total_local}")


if __name__ == "__main__":
    main()
