#!/usr/bin/env python3
"""
sfs_pipeline.py — Orkestrerare för SFS-pipelinen (paragrafen.ai)

Kommandon:
  normalize --all            Normaliserar alla rådata i data/raw/sfs/
  normalize --sfs 2017:900   Normaliserar ett enskilt SFS-nummer
  normalize --limit N        Normaliserar max N dokument (test)
  verify                     Verifierar normaliserad data
  stats                      Visar statistik över normaliserade filer

Användning:
  python3 sfs_pipeline.py normalize --all
  python3 sfs_pipeline.py normalize --sfs 2017:900
  python3 sfs_pipeline.py normalize --limit 100
  python3 sfs_pipeline.py stats
  python3 sfs_pipeline.py verify
"""

import sys
import os
import json
import time
import argparse
import traceback
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("sfs_pipeline")

from normalize.sfs_parser import parse_html
from normalize.sfs_chunker import chunk_paragraphs
from normalize.sfs_normalizer import normalize_chunks

RAW_DIR  = ROOT / "data" / "raw"  / "sfs"
NORM_DIR = ROOT / "data" / "norm" / "sfs"


def list_raw_files() -> list:
    if not RAW_DIR.exists():
        logger.error(f"RAW_DIR saknas: {RAW_DIR}")
        return []
    return sorted(RAW_DIR.glob("sfs-*.json"))


def sfs_nr_from_dok_id(dok_id: str) -> str:
    """'sfs-2017-900' -> '2017:900', 'sfs-1736-0123_1' -> '1736:0123_1'
    Mellanslag i gamla dok_id ('sfs-1723-1016 1') ersätts med underscore."""
    s = dok_id.replace("sfs-", "").replace(" ", "_")
    parts = s.split("-", 1)
    if len(parts) == 2:
        return f"{parts[0]}:{parts[1]}"
    return dok_id


def load_raw(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Kunde ej läsa {path.name}: {e}")
        return None


def get_dok(raw: dict) -> dict:
    """Hanterar båda JSON-strukturerna: d['dokument'] och d['dokumentstatus']['dokument']."""
    dok = raw.get("dokument") or raw.get("dokumentstatus", {}).get("dokument") or {}
    return dok


def extract_meta(raw: dict, sfs_nr: str) -> dict:
    dok = get_dok(raw)
    return {
        "sfs_nr":           sfs_nr,
        "rubrik":           dok.get("titel", ""),
        "departement":      dok.get("organ", ""),
        "ikraftträdande":   dok.get("datum", ""),
        "utfärdad":         dok.get("datum", ""),
        "riksdagen_dok_id": dok.get("dok_id", ""),
        "senaste_andring":  dok.get("publicerad", ""),
        "upphävd":          False,
        "source_id":        dok.get("dok_id", sfs_nr.replace(":", "-")),
        "kortnamn":         "",
    }


def process_one(path) -> dict:
    t0 = time.time()
    result = {
        "path":       str(Path(path).name),
        "sfs_nr":     "",
        "status":     "FAIL",
        "chunks":     0,
        "paragraphs": 0,
        "errors":     [],
        "elapsed_s":  0,
    }

    try:
        raw = load_raw(path)
        if raw is None:
            result["errors"].append("Kunde ej läsa fil")
            return result

        dok = get_dok(raw)
        dok_id = dok.get("dok_id")
        if not dok_id:
            result["status"] = "SKIP"
            result["errors"].append("Tomt dok_id — trolig bihangsförfattning")
            return result
        sfs_nr = sfs_nr_from_dok_id(dok_id)
        result["sfs_nr"] = sfs_nr

        # Skippa historiska kantfall: _s.1-suffix och RFS-prefix
        if "_s." in sfs_nr or sfs_nr.upper().startswith("RFS"):
            result["status"] = "SKIP"
            result["errors"].append(f"Historisk kantfall-beteckning: {sfs_nr}")
            return result

        html = dok.get("html", "")
        if not html or len(html) < 50:
            result["status"] = "SKIP"
            result["errors"].append(f"Ingen HTML (len={len(html)})")
            return result

        meta = extract_meta(raw, sfs_nr)

        paragraphs = parse_html(html, sfs_nr, meta)
        result["paragraphs"] = len(paragraphs)

        if not paragraphs:
            result["status"] = "SKIP"
            result["errors"].append("Inga paragrafer parsade")
            return result

        chunks = chunk_paragraphs(paragraphs, sfs_nr, meta)

        norm_meta = {
            "sfs_nr":      sfs_nr,
            "rubrik":      meta["rubrik"],
            "departement": meta["departement"],
        }
        norm_chunks, errors = normalize_chunks(chunks, norm_meta)
        result["chunks"] = len(norm_chunks)
        result["errors"] = errors[:10]

        if not norm_chunks:
            result["status"] = "FAIL" if errors else "SKIP"
            return result

        _save_norm(sfs_nr, norm_chunks)
        result["status"] = "OK" if not errors else "WARN"

    except Exception as e:
        result["errors"].append(str(e))
        result["traceback"] = traceback.format_exc()

    result["elapsed_s"] = round(time.time() - t0, 2)
    return result


def _save_norm(sfs_nr: str, chunks: list):
    NORM_DIR.mkdir(parents=True, exist_ok=True)
    safe = sfs_nr.replace(":", "-")
    out_path = NORM_DIR / f"sfs-{safe}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False)


def cmd_normalize(args):
    if args.sfs:
        safe = args.sfs.replace(":", "-")
        candidates = list(RAW_DIR.glob(f"sfs-{safe}*.json"))
        if not candidates:
            logger.error(f"Hittade ingen råfil för {args.sfs} i {RAW_DIR}")
            sys.exit(1)
        files = candidates[:1]
        logger.info(f"Normaliserar {args.sfs} ({files[0].name})")
    else:
        files = list_raw_files()
        if not files:
            logger.error("Inga råfiler att normalisera.")
            sys.exit(1)
        if args.limit:
            files = files[:args.limit]

    total = len(files)
    ok = warn = skip = fail = 0
    total_chunks = 0
    errors_log = []

    logger.info(f"Startar normalisering av {total} filer ...")
    t_start = time.time()

    for i, path in enumerate(files, 1):
        result = process_one(path)
        s = result["status"]

        if s == "OK":     ok += 1
        elif s == "WARN": warn += 1; errors_log.append(result)
        elif s == "SKIP": skip += 1
        else:             fail += 1; errors_log.append(result)

        total_chunks += result["chunks"]

        if i % 100 == 0 or i == total:
            elapsed = time.time() - t_start
            rate = i / elapsed if elapsed > 0 else 0
            eta_s = (total - i) / rate if rate > 0 else 0
            logger.info(
                f"  {i:5d}/{total}  OK={ok} WARN={warn} SKIP={skip} FAIL={fail} "
                f"chunks={total_chunks:,}  {rate:.0f} dok/s  ETA {eta_s/60:.1f} min"
            )

    elapsed_total = time.time() - t_start

    print()
    print("=" * 60)
    print("NORMALISERING KLAR")
    print("=" * 60)
    print(f"  Filer:          {total}")
    print(f"  OK:             {ok}")
    print(f"  WARN:           {warn}")
    print(f"  SKIP:           {skip}")
    print(f"  FAIL:           {fail}")
    print(f"  Chunks totalt:  {total_chunks:,}")
    print(f"  Tid:            {elapsed_total/60:.1f} min")
    print(f"  Output:         {NORM_DIR}")

    if errors_log:
        err_path = NORM_DIR / "_normalize_errors.json"
        NORM_DIR.mkdir(parents=True, exist_ok=True)
        with open(err_path, "w", encoding="utf-8") as f:
            json.dump(errors_log, f, ensure_ascii=False, indent=2)
        print(f"  Felrapport:     {err_path}")

    print("=" * 60)


def cmd_stats(args):
    files = sorted(NORM_DIR.glob("sfs-*.json"))
    if not files:
        print(f"Inga normaliserade filer i {NORM_DIR}")
        return

    total_chunks = 0
    total_files = 0
    type_counts = {}
    norm_type_counts = {}
    area_counts = {}

    for path in files:
        try:
            with open(path) as f:
                chunks = json.load(f)
            if not chunks:
                continue
            total_files += 1
            total_chunks += len(chunks)
            first = chunks[0]
            nt = first.get("numbering_type", "?")
            type_counts[nt] = type_counts.get(nt, 0) + 1
            nm = first.get("norm_type", "?")
            norm_type_counts[nm] = norm_type_counts.get(nm, 0) + 1
            for area in first.get("legal_area", "").split(","):
                area = area.strip()
                if area:
                    area_counts[area] = area_counts.get(area, 0) + 1
        except Exception:
            pass

    print(f"\nNormaliserad data — {NORM_DIR}")
    print(f"  Lagar:          {total_files:,}")
    print(f"  Chunks totalt:  {total_chunks:,}")
    print(f"  Snitt chunks:   {total_chunks/total_files:.1f}" if total_files else "")
    print(f"\nNumbering type:")
    for k, v in sorted(type_counts.items()):
        print(f"  {k:12s}: {v:5d}")
    print(f"\nNorm type:")
    for k, v in sorted(norm_type_counts.items(), key=lambda x: -x[1]):
        print(f"  {k:15s}: {v:5d}")
    print(f"\nTop 15 legal_area:")
    for k, v in sorted(area_counts.items(), key=lambda x: -x[1])[:15]:
        print(f"  {k:30s}: {v:5d}")


def cmd_verify(args):
    files = sorted(NORM_DIR.glob("sfs-*.json"))
    if not files:
        print(f"Inga normaliserade filer i {NORM_DIR}")
        return

    ok = fail = empty = 0
    for path in files:
        try:
            with open(path) as f:
                chunks = json.load(f)
            if not chunks:
                empty += 1
                continue
            required = ["namespace", "sfs_nr", "text", "numbering_type", "norm_type"]
            missing = [r for r in required if not chunks[0].get(r)]
            if missing:
                fail += 1
                print(f"  FAIL {path.name}: saknar {missing}")
            else:
                ok += 1
        except Exception as e:
            fail += 1
            print(f"  FAIL {path.name}: {e}")

    print(f"\nVerifiering: {ok} OK  {empty} tomma  {fail} FAIL  av {len(files)} filer")


def main():
    parser = argparse.ArgumentParser(description="SFS-pipeline — normalize, verify, stats")
    sub = parser.add_subparsers(dest="command")

    p_norm = sub.add_parser("normalize", help="Normalisera rådata")
    grp = p_norm.add_mutually_exclusive_group()
    grp.add_argument("--all",  action="store_true", help="Normalisera alla filer")
    grp.add_argument("--sfs",  metavar="SFS_NR",    help="Enskilt SFS-nummer, t.ex. 2017:900")
    p_norm.add_argument("--limit", metavar="N", type=int, help="Max antal filer (test)")

    sub.add_parser("stats",  help="Visa statistik")
    sub.add_parser("verify", help="Verifiera normaliserade filer")

    args = parser.parse_args()

    if args.command == "normalize":
        if not args.all and not args.sfs and not args.limit:
            parser.error("Ange --all, --sfs SFS_NR eller --limit N")
        cmd_normalize(args)
    elif args.command == "stats":
        cmd_stats(args)
    elif args.command == "verify":
        cmd_verify(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
# patch applied below via sed
