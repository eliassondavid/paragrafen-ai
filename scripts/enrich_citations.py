#!/usr/bin/env python3
"""
enrich_citations.py — Extrahera och berika citations_to/cites_praxis i normaliserade praxis-filer

Steg A: Bygg namnuppslagstabell från HD:s officiella PDF
Steg B: Kör citation-extraktion mot alla normaliserade JSON-filer
Steg C: Skriv tillbaka in-place med backup

Kör:
    python3 enrich_citations.py --dry-run
    python3 enrich_citations.py --build-lookup-only   # bara namnlista
    python3 enrich_citations.py                       # fullkörning
    python3 enrich_citations.py --restore-backup      # återställ

Filstruktur:
    data/norm/praxis/{domstol}/*.json        ← uppdateras in-place
    data/norm/praxis/.enrichment_backup/     ← backup före första körning
    data/norm/praxis/enrichment_log.jsonl    ← logg per fil
    data/curated/nja_names.json              ← namnuppslagstabell (cache)
"""

import re
import json
import shutil
import argparse
import logging
from pathlib import Path
from datetime import datetime
from collections import defaultdict

try:
    import pdfplumber
except ImportError:
    print("Saknar pdfplumber. Kör: pip3 install pdfplumber")
    raise

# ---------------------------------------------------------------------------
# Konfiguration
# ---------------------------------------------------------------------------

PRAXIS_DIR    = Path("/Users/davideliasson/Projects/paragrafen-ai/data/norm/praxis")
BACKUP_DIR    = PRAXIS_DIR / ".enrichment_backup"
LOG_FILE      = PRAXIS_DIR / "enrichment_log.jsonl"
LOOKUP_FILE   = Path("/Users/davideliasson/Projects/paragrafen-ai/data/curated/nja_names.json")
PDF_URL       = "https://www.domstol.se/globalassets/filer/domstol/hogstadomstolen/namngivna-rattsfall/officiell-lista-over-namngivna-rattsfall.pdf"
PDF_LOCAL     = Path("/tmp/namngivna_rattsfall.pdf")

# Alla kända målkoder (Domstolsverkets föreskrifter + ÖF)
MALTYP_CODES  = r"(?:T|Ö|B|Ä|K|FT|F|FFT|FÄ|M|PMT|PMFT|PMÄ|P|ÖÄ|ÖH|PMÖ|PMÖÄ|ÖF)"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Steg A — Namnuppslagstabell
# ---------------------------------------------------------------------------

def build_name_lookup(pdf_path: Path) -> dict[str, str]:
    """
    Parsa HD:s officiella PDF och bygg:
        namn (lowercase) → "NJA YYYY s. N"

    Tabellstruktur (extraheras via extract_table):
        rad[0] = radnummer
        rad[1] = "YYYY s. N"   (kolumn A = NJA-referens)
        rad[2] = namn           (kolumn B)
        rad[3] = målnummer      (kolumn C, ofta tom)
        rad[4] = datum          (kolumn D, ofta tom)
    """
    lookup: dict[str, str] = {}
    NJA_REF_RE = re.compile(r"(\d{4})\s+s\.\s+(\d+)")

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue
            for row in table:
                if not row or len(row) < 3:
                    continue
                nja_cell = (row[1] or "").strip()
                namn_cell = (row[2] or "").strip()

                # Hoppa över header-rader
                if nja_cell in ("NJA", "A", "") or namn_cell in ("NAMN", "B", ""):
                    continue

                m = NJA_REF_RE.search(nja_cell)
                if not m:
                    continue

                år = m.group(1)
                sida = m.group(2)
                namn = namn_cell

                if namn and len(namn) > 2:
                    citation = f"NJA {år} s. {sida}"
                    lookup[namn.lower()] = citation
                    log.debug(f"  Namn: '{namn}' → {citation}")

    log.info(f"Namnuppslagstabell: {len(lookup)} poster")
    return lookup


def load_or_build_lookup(force_rebuild: bool = False) -> dict[str, str]:
    """Ladda från cache eller bygg från PDF."""
    if LOOKUP_FILE.exists() and not force_rebuild:
        with open(LOOKUP_FILE, encoding="utf-8") as f:
            data = json.load(f)
        log.info(f"Namnlista laddad från cache: {len(data)} poster")
        return data

    # Ladda ner PDF om den inte finns lokalt
    if not PDF_LOCAL.exists():
        log.info(f"Laddar ner namnlista från {PDF_URL}")
        import urllib.request
        urllib.request.urlretrieve(PDF_URL, PDF_LOCAL)

    lookup = build_name_lookup(PDF_LOCAL)

    # Spara cache
    LOOKUP_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOOKUP_FILE, "w", encoding="utf-8") as f:
        json.dump(lookup, f, ensure_ascii=False, indent=2)
    log.info(f"Namnlista sparad: {LOOKUP_FILE}")
    return lookup


# ---------------------------------------------------------------------------
# Steg B — Citation-extraktion
# ---------------------------------------------------------------------------

class CitationExtractor:
    """Extraherar alla juridiska hänvisningar från löptext."""

    def __init__(self, name_lookup: dict[str, str]):
        self.name_lookup = name_lookup  # namn.lower() → "NJA YYYY s. N"

        # NJA-ref med valfritt namn: "Multitotal" NJA 2017 s. 9
        self.NJA_WITH_NAME = re.compile(
            r'"([^"]+)"\s+NJA\s+(\d{4})\s+s\.?\s+(\d+)',
            re.IGNORECASE
        )
        # NJA-ref utan namn: NJA 2017 s. 9 eller NJA 1969 s 326
        self.NJA_BARE = re.compile(
            r'(?<!")\bNJA\s+(\d{4})\s+s\.?\s+(\d+)',
            re.IGNORECASE
        )
        # NJA notisfall: NJA 2016 N 9
        self.NJA_NOT = re.compile(
            r'\bNJA\s+(\d{4})\s+N\s+(\d+)',
            re.IGNORECASE
        )
        # HFD ref: HFD 2023 ref. 33
        self.HFD_REF = re.compile(
            r'\bHFD\s+(\d{4})\s+ref\.?\s+(\d+)',
            re.IGNORECASE
        )
        # HFD notisfall: HFD 2025 not. 28
        self.HFD_NOT = re.compile(
            r'\bHFD\s+(\d{4})\s+not\.?\s+(\d+)',
            re.IGNORECASE
        )
        # HD-avgörande utan NJA-ref: HD:s dom/beslut ... i mål T 6154-24
        self.HD_MAL = re.compile(
            r'(?:Högsta domstolens|HD:s)\s+(?:dom|beslut|utslag)\s+(?:den\s+)?[\w\s]+\s+i\s+mål\s+'
            r'(' + MALTYP_CODES + r'\s+\d+[-–]\d+)',
            re.IGNORECASE
        )
        # HFD-avgörande utan ref: HFD:s dom ... i mål
        self.HFD_MAL = re.compile(
            r'(?:Högsta förvaltningsdomstolens|HFD:s)\s+(?:dom|beslut)\s+(?:den\s+)?[\w\s]+\s+i\s+mål\s+'
            r'(' + MALTYP_CODES + r'\s+\d+[-–]\d+)',
            re.IGNORECASE
        )
        # Prop-ref: prop. 1971:30 del 2 s. 529 eller prop. 2012/13:45 s. 113
        self.PROP = re.compile(
            r'\bprop\.\s+(\d{4}(?:/\d{2,4})?:\d+)(?:\s+del\s+\d+)?(?:\s+s\.?\s+\d+(?:\s*f{1,2}\.?)?)?',
            re.IGNORECASE
        )
        # SOU: SOU 1938:44
        self.SOU = re.compile(
            r'\bSOU\s+(\d{4}:\d+)',
            re.IGNORECASE
        )
        # Ds: Ds 2020:12
        self.DS = re.compile(
            r'\bDs\s+(\d{4}:\d+)',
            re.IGNORECASE
        )
        # Kommittédirektiv: dir. 2019:31
        self.DIR = re.compile(
            r'\bdir\.\s+(\d{4}:\d+)',
            re.IGNORECASE
        )
        # Utskottsbetänkande: bet. 2020/21:CU2
        self.BET = re.compile(
            r'\bbet\.\s+(\d{4}(?:/\d{2,4})?:[A-ZÅÄÖa-zåäö]+\d+)',
            re.IGNORECASE
        )
        # NJA II: NJA II 1943 s. 449 (med eller utan punkt)
        self.NJA_II = re.compile(
            r'\bNJA\s+II\s+(\d{4})\s+s\.?\s+(\d+)',
            re.IGNORECASE
        )
        # RÅ (äldre HFD): RÅ 2004 ref. 1, RÅ 2004 not. 12
        self.RA_REF = re.compile(
            r'\bRÅ\s+(\d{4})\s+ref\.?\s+(\d+)',
            re.IGNORECASE
        )
        self.RA_NOT = re.compile(
            r'\bRÅ\s+(\d{4})\s+not\.?\s+(\d+)',
            re.IGNORECASE
        )
        # EU-domstolen: C-123/45, T-123/45, mål C-123/45
        self.EU_DOM = re.compile(
            r'\b(?:mål\s+)?([CT]-\d+/\d{2,4})',
            re.IGNORECASE
        )
        # EU ECLI: EU:C:2019:123
        self.EU_ECLI = re.compile(
            r'\bEU:[CT]:\d{4}:\d+',
            re.IGNORECASE
        )
        # Europadomstolen via målnummer: nr 12345/67 eller nr. 12345/67
        self.ECHR_NR = re.compile(
            r'\bnr\.?\s+(\d{4,6}/\d{2,4})',
            re.IGNORECASE
        )
        # Juridiska tidskrifter: SvJT 2019 s. 45, JT 2020-21 s. 123, FT 2018 s. 67
        self.TIDSKRIFT = re.compile(
            r'\b(SvJT|JT|FT|SvSkT|ERT|IR)\s+(\d{4}(?:[–\-]\d{2,4})?)\s+s\.?\s+(\d+)',
            re.IGNORECASE
        )
        # Namnref utan NJA-nummer i samma dom: "Multitotal" p. 49
        self.NAME_POINT = re.compile(
            r'"([^"]+)"\s+p\.\s+\d+',
            re.IGNORECASE
        )

    def extract(self, text: str) -> dict:
        """
        Returnerar:
        {
            "cites_praxis": ["NJA 2017 s. 9", "HFD 2023 ref. 33", "C-123/45" ...],
            "references_to": ["forarbete::prop_1971_30", "nja_ii::1943_s449", ...],
            "cites_doktrin": [],   # stub — fylls i fas 2
            "cites_myndighet": []  # stub — fylls i fas 2
        }
        """
        cites_praxis: set[str] = set()
        references_to: set[str] = set()

        # NJA med namn
        for m in self.NJA_WITH_NAME.finditer(text):
            citation = f"NJA {m.group(2)} s. {m.group(3)}"
            cites_praxis.add(citation)

        # NJA utan namn
        for m in self.NJA_BARE.finditer(text):
            citation = f"NJA {m.group(1)} s. {m.group(2)}"
            cites_praxis.add(citation)

        # NJA notisfall
        for m in self.NJA_NOT.finditer(text):
            citation = f"NJA {m.group(1)} N {m.group(2)}"
            cites_praxis.add(citation)

        # HFD ref + not
        for m in self.HFD_REF.finditer(text):
            cites_praxis.add(f"HFD {m.group(1)} ref. {m.group(2)}")
        for m in self.HFD_NOT.finditer(text):
            cites_praxis.add(f"HFD {m.group(1)} not. {m.group(2)}")

        # RÅ (äldre HFD)
        for m in self.RA_REF.finditer(text):
            cites_praxis.add(f"RÅ {m.group(1)} ref. {m.group(2)}")
        for m in self.RA_NOT.finditer(text):
            cites_praxis.add(f"RÅ {m.group(1)} not. {m.group(2)}")

        # HD/HFD via målnummer
        for m in self.HD_MAL.finditer(text):
            cites_praxis.add(f"HD mål {m.group(1)}")
        for m in self.HFD_MAL.finditer(text):
            cites_praxis.add(f"HFD mål {m.group(1)}")

        # EU-domstolen
        for m in self.EU_DOM.finditer(text):
            cites_praxis.add(f"CJEU {m.group(1)}")
        for m in self.EU_ECLI.finditer(text):
            cites_praxis.add(m.group(0))

        # Europadomstolen
        for m in self.ECHR_NR.finditer(text):
            cites_praxis.add(f"ECHR nr {m.group(1)}")

        # Namnreferens utan NJA-nummer — slå upp i tabell
        for m in self.NAME_POINT.finditer(text):
            namn = m.group(1).strip().lower()
            if namn in self.name_lookup:
                cites_praxis.add(self.name_lookup[namn])

        # Prop-referenser
        for m in self.PROP.finditer(text):
            prop_id = m.group(1).replace("/", "_").replace(":", "_")
            references_to.add(f"forarbete::prop_{prop_id}")

        # SOU
        for m in self.SOU.finditer(text):
            sou_id = m.group(1).replace(":", "_")
            references_to.add(f"forarbete::sou_{sou_id}")

        # Ds
        for m in self.DS.finditer(text):
            ds_id = m.group(1).replace(":", "_")
            references_to.add(f"forarbete::ds_{ds_id}")

        # Dir
        for m in self.DIR.finditer(text):
            dir_id = m.group(1).replace(":", "_")
            references_to.add(f"forarbete::dir_{dir_id}")

        # Utskottsbetänkanden
        for m in self.BET.finditer(text):
            bet_id = m.group(1).replace("/", "_").replace(":", "_")
            references_to.add(f"forarbete::bet_{bet_id}")

        # NJA II
        for m in self.NJA_II.finditer(text):
            references_to.add(f"nja_ii::{m.group(1)}_s{m.group(2)}")

        # Juridiska tidskrifter
        for m in self.TIDSKRIFT.finditer(text):
            tidskrift = m.group(1).upper()
            år = m.group(2)
            sida = m.group(3)
            references_to.add(f"tidskrift::{tidskrift}_{år}_s{sida}")

        return {
            "cites_praxis": sorted(cites_praxis),
            "references_to": sorted(references_to),
            "cites_doktrin": [],    # stub — fylls i fas 2 (doktrin-pipeline)
            "cites_myndighet": [],  # stub — fylls i fas 2 (myndighetsföreskrifter)
        }


# ---------------------------------------------------------------------------
# Steg B — Berika en JSON-fil
# ---------------------------------------------------------------------------

def enrich_file(
    json_path: Path,
    extractor: CitationExtractor,
    dry_run: bool = False,
) -> dict:
    """
    Läs normaliserad JSON, extrahera citat från alla chunks,
    slå ihop och skriv tillbaka. Returnerar logg-post.
    """
    with open(json_path, encoding="utf-8") as f:
        doc = json.load(f)

    # Samla all text från chunks
    full_text = " ".join(
        chunk.get("chunk_text", "")
        for chunk in doc.get("chunks", [])
    )

    extracted = extractor.extract(full_text)

    # Befintliga värden (kan vara lista eller JSON-sträng)
    def parse_list_field(val) -> list:
        if isinstance(val, list):
            return val
        if isinstance(val, str):
            try:
                return json.loads(val)
            except Exception:
                return []
        return []

    existing_praxis = set(parse_list_field(doc.get("cites_praxis", "[]")))
    existing_refs   = set(parse_list_field(doc.get("references_to", "[]")))
    existing_doktrin   = parse_list_field(doc.get("cites_doktrin", "[]"))
    existing_myndighet = parse_list_field(doc.get("cites_myndighet", "[]"))

    new_praxis = set(extracted["cites_praxis"]) - existing_praxis
    new_refs   = set(extracted["references_to"]) - existing_refs

    merged_praxis = sorted(existing_praxis | new_praxis)
    merged_refs   = sorted(existing_refs | new_refs)

    log_entry = {
        "file": str(json_path),
        "timestamp": datetime.now().isoformat(),
        "cites_praxis_added": sorted(new_praxis),
        "references_to_added": sorted(new_refs),
        "dry_run": dry_run,
    }

    if not dry_run and (new_praxis or new_refs):
        doc["cites_praxis"]    = json.dumps(merged_praxis, ensure_ascii=False)
        doc["references_to"]   = json.dumps(merged_refs, ensure_ascii=False)
        # Skriv stub-fält om de saknas (fylls i fas 2)
        if "cites_doktrin" not in doc:
            doc["cites_doktrin"]   = json.dumps(existing_doktrin, ensure_ascii=False)
        if "cites_myndighet" not in doc:
            doc["cites_myndighet"] = json.dumps(existing_myndighet, ensure_ascii=False)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=4)

    return log_entry


# ---------------------------------------------------------------------------
# Backup
# ---------------------------------------------------------------------------

def take_backup():
    """Kopiera hela praxis-katalogen till .enrichment_backup (en gång)."""
    if BACKUP_DIR.exists():
        log.info(f"Backup finns redan: {BACKUP_DIR} — hoppar över")
        return
    log.info(f"Tar backup → {BACKUP_DIR}")
    shutil.copytree(PRAXIS_DIR, BACKUP_DIR, ignore=shutil.ignore_patterns(".enrichment_backup"))
    log.info("Backup klar")


def restore_backup():
    """Återställ från backup."""
    if not BACKUP_DIR.exists():
        log.error("Ingen backup hittad")
        return
    log.info("Återställer från backup...")
    for src in BACKUP_DIR.rglob("*.json"):
        rel = src.relative_to(BACKUP_DIR)
        dst = PRAXIS_DIR / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
    log.info("Återställning klar")


# ---------------------------------------------------------------------------
# Huvudfunktion
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="§AI — Berika citations_to/cites_praxis")
    parser.add_argument("--dry-run", action="store_true",
                        help="Extrahera men skriv inte tillbaka")
    parser.add_argument("--build-lookup-only", action="store_true",
                        help="Bygg bara namnuppslagstabell och avsluta")
    parser.add_argument("--rebuild-lookup", action="store_true",
                        help="Tvinga ombyggnad av namnuppslagstabell")
    parser.add_argument("--restore-backup", action="store_true",
                        help="Återställ filer från backup")
    parser.add_argument("--domstol", type=str,
                        help="Begränsa till en domstol (t.ex. HDO)")
    parser.add_argument("--limit", type=int,
                        help="Max antal filer att bearbeta (för test)")
    args = parser.parse_args()

    if args.restore_backup:
        restore_backup()
        return

    # Bygg namnuppslagstabell
    lookup = load_or_build_lookup(force_rebuild=args.rebuild_lookup)

    if args.build_lookup_only:
        print(f"\nNamnuppslagstabell: {len(lookup)} poster")
        print("Exempel:")
        for k, v in list(lookup.items())[:5]:
            print(f"  '{k}' → {v}")
        return

    # Backup
    if not args.dry_run:
        take_backup()

    # Hitta alla JSON-filer
    if args.domstol:
        search_dir = PRAXIS_DIR / args.domstol
    else:
        search_dir = PRAXIS_DIR

    all_files = sorted(search_dir.rglob("*.json"))
    # Exkludera backup och loggfiler
    all_files = [f for f in all_files if ".enrichment_backup" not in str(f)
                 and f.name != "enrichment_log.jsonl"]

    if args.limit:
        all_files = all_files[:args.limit]

    log.info(f"Filer att bearbeta: {len(all_files)}")
    if args.dry_run:
        log.info("DRY RUN — skriver inte tillbaka")

    extractor = CitationExtractor(name_lookup=lookup)

    processed = 0
    enriched = 0
    log_entries = []

    for json_path in all_files:
        try:
            entry = enrich_file(json_path, extractor, dry_run=args.dry_run)
            if entry["cites_praxis_added"] or entry["references_to_added"]:
                enriched += 1
                if processed < 20 or enriched <= 5:  # visa första exemplen
                    log.info(f"  {json_path.name}: "
                             f"+{len(entry['cites_praxis_added'])} praxis, "
                             f"+{len(entry['references_to_added'])} refs")
            log_entries.append(entry)
            processed += 1
            if processed % 500 == 0:
                log.info(f"  ... {processed}/{len(all_files)} filer bearbetade")
        except Exception as e:
            log.error(f"FEL: {json_path}: {e}")

    # Skriv logg
    if not args.dry_run:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            for entry in log_entries:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    print()
    print("=== SAMMANFATTNING ===")
    print(f"Filer bearbetade : {processed}")
    print(f"Filer berikade   : {enriched}")
    if args.dry_run:
        print("[DRY RUN — inga filer skrivna]")
    else:
        print(f"Logg             : {LOG_FILE}")


if __name__ == "__main__":
    main()
