#!/usr/bin/env python3
"""
sfs_crawl_diagnostic.py — Diagnostik för SFS-crawl
Kör: python sfs_crawl_diagnostic.py

Undersöker:
1. Hur många filer finns lokalt vs. API:ets totala antal?
2. Vilka dok_id:n saknas?
3. Är saknade dok_id:n hämtningsbara (404 vs. timeout vs. annat)?
4. Finns det mönster i saknade (specifika årsintervall, subtyper)?
"""

import json
import os
import time
import requests
from pathlib import Path
from collections import Counter

# ---------------------------------------------------------------------------
# Konfig — justera om din mappstruktur skiljer sig
# ---------------------------------------------------------------------------
RAW_DIR = Path("data/raw/sfs")
API_BASE = "https://data.riksdagen.se"
REQUEST_DELAY = 0.5

# ---------------------------------------------------------------------------
# 1. Lokal inventering
# ---------------------------------------------------------------------------
print("=" * 60)
print("1. LOKAL INVENTERING")
print("=" * 60)

local_files = list(RAW_DIR.glob("sfs-*.json"))
print(f"Lokala JSON-filer: {len(local_files)}")

# Extrahera dok_id:n från filnamn
local_ids = set()
for f in local_files:
    # sfs-2017-900.json → sfs-2017-900
    dok_id = f.stem
    local_ids.add(dok_id)

# Kolla filstorlekar — tomma/korrupta filer?
sizes = [f.stat().st_size for f in local_files]
empty_files = [f.name for f in local_files if f.stat().st_size < 100]
if empty_files:
    print(f"⚠️  Tomma/korrupta filer (<100 bytes): {len(empty_files)}")
    for ef in empty_files[:10]:
        print(f"   {ef}")
else:
    print("Inga tomma filer.")

print(f"Filstorlekar: min={min(sizes)//1024}KB, max={max(sizes)//1024}KB, snitt={sum(sizes)//len(sizes)//1024}KB")

# ---------------------------------------------------------------------------
# 2. Hämta komplett dok_id-lista från API
# ---------------------------------------------------------------------------
print()
print("=" * 60)
print("2. HÄMTAR KOMPLETT DOK_ID-LISTA FRÅN API")
print("=" * 60)

all_api_ids = []
page = 1
total_pages = None

while True:
    params = {
        "doktyp": "sfs",
        "utformat": "json",
        "sort": "datum",
        "sortorder": "asc",
        "p": page,
    }
    
    try:
        resp = requests.get(f"{API_BASE}/dokumentlista/", params=params, timeout=30)
        data = resp.json()
    except Exception as e:
        print(f"Fel på sida {page}: {e}")
        break
    
    wrapper = data.get("dokumentlista", {})
    docs = wrapper.get("dokument", [])
    
    if total_pages is None:
        total_pages = int(wrapper.get("@sidor", 1))
        total_docs = int(wrapper.get("@traffar", 0))
        print(f"API rapporterar: {total_docs} dokument, {total_pages} sidor")
    
    if not docs:
        break
    
    for doc in docs:
        did = doc.get("dok_id", "")
        subtyp = doc.get("subtyp", "")
        all_api_ids.append({"dok_id": did, "subtyp": subtyp, "datum": doc.get("datum", ""), "titel": doc.get("titel", "")})
    
    if page % 50 == 0:
        print(f"  Sida {page}/{total_pages} ({len(all_api_ids)} dok hittills)")
    
    if page >= total_pages:
        break
    
    page += 1
    time.sleep(REQUEST_DELAY)

print(f"Totalt i API: {len(all_api_ids)} dok_id:n")

# ---------------------------------------------------------------------------
# 3. Jämför — vilka saknas?
# ---------------------------------------------------------------------------
print()
print("=" * 60)
print("3. JÄMFÖRELSE: LOKALT VS. API")
print("=" * 60)

api_id_set = {d["dok_id"] for d in all_api_ids}
missing_ids = api_id_set - local_ids
extra_local = local_ids - api_id_set

print(f"I API men EJ lokalt:  {len(missing_ids)}")
print(f"Lokalt men EJ i API:  {len(extra_local)}")
print(f"Täckning:             {len(local_ids & api_id_set)}/{len(api_id_set)} ({100*len(local_ids & api_id_set)/max(len(api_id_set),1):.1f}%)")

# ---------------------------------------------------------------------------
# 4. Mönsteranalys av saknade
# ---------------------------------------------------------------------------
if missing_ids:
    print()
    print("=" * 60)
    print("4. MÖNSTERANALYS AV SAKNADE DOK_ID")
    print("=" * 60)
    
    # Vilka subtyper saknas?
    missing_docs = [d for d in all_api_ids if d["dok_id"] in missing_ids]
    subtyp_counter = Counter(d["subtyp"] for d in missing_docs)
    print(f"\nSubtyper bland saknade:")
    for st, count in subtyp_counter.most_common():
        print(f"  {st or '(tom)'}: {count}")
    
    # Vilka årtionden?
    year_counter = Counter()
    for d in missing_docs:
        try:
            year = d["datum"][:4] if d["datum"] else "okänt"
            decade = year[:3] + "0-tal" if year != "okänt" else "okänt"
            year_counter[decade] += 1
        except:
            year_counter["okänt"] += 1
    
    print(f"\nÅrtionden bland saknade:")
    for dec, count in sorted(year_counter.items()):
        print(f"  {dec}: {count}")
    
    # Visa 20 exempel
    print(f"\nExempel på saknade (först 20):")
    for d in sorted(missing_docs, key=lambda x: x["dok_id"])[:20]:
        print(f"  {d['dok_id']}: {d['titel'][:60]}... (subtyp={d['subtyp']})")

# ---------------------------------------------------------------------------
# 5. Testa hämtning av 5 saknade
# ---------------------------------------------------------------------------
if missing_ids:
    print()
    print("=" * 60)
    print("5. TESTAR HÄMTNING AV 5 SAKNADE DOKUMENT")
    print("=" * 60)
    
    test_ids = sorted(missing_ids)[:5]
    for dok_id in test_ids:
        url = f"{API_BASE}/dokumentstatus/{dok_id}.json"
        try:
            resp = requests.get(url, timeout=15)
            status = resp.status_code
            size = len(resp.content) if resp.ok else 0
            
            if resp.ok:
                data = resp.json()
                dok = data.get("dokumentstatus", {}).get("dokument", {})
                has_html = bool(dok.get("html", ""))
                print(f"  ✅ {dok_id}: HTTP {status}, {size//1024}KB, html={'ja' if has_html else 'NEJ'}")
            else:
                print(f"  ❌ {dok_id}: HTTP {status}")
        except Exception as e:
            print(f"  ❌ {dok_id}: {e}")
        
        time.sleep(REQUEST_DELAY)

# ---------------------------------------------------------------------------
# 6. Sammanfattning + rekommendation
# ---------------------------------------------------------------------------
print()
print("=" * 60)
print("6. SAMMANFATTNING")
print("=" * 60)

if len(missing_ids) == 0:
    print("✅ Komplett crawl — alla dokument hämtade!")
elif len(missing_ids) < 100:
    print(f"⚠️  {len(missing_ids)} saknade dokument — troligen transient fel.")
    print("   Rekommendation: Kör crawlern igen (resume hoppar över redan hämtade)")
else:
    print(f"❌ {len(missing_ids)} saknade dokument — kan bero på:")
    print("   - Paginerings-gap (API-sidor som timeout:ade)")
    print("   - subtyp-filtrering (sfst vs sfsr — se ovan)")
    print("   - Rate limiting under crawl")
    print("   Rekommendation: Kör crawlern igen med --no-skip för missade sidor")

# Spara saknade ids för re-crawl
if missing_ids:
    missing_file = RAW_DIR / "_missing_ids.json"
    with open(missing_file, "w", encoding="utf-8") as f:
        json.dump(sorted(missing_ids), f, ensure_ascii=False, indent=2)
    print(f"\nSaknade dok_id sparade i: {missing_file}")
