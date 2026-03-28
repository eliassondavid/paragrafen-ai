#!/usr/bin/env python3
"""
Hämta ECHR/HUDOC-dokument för Danelius rättsfallslista.

Skriptet använder en checkpointfil för resume, loggar saknade träffar och
försöker hitta bästa HUDOC-post med flera sökfall och enkel fuzzy-rankning.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CASES_PATH = PROJECT_ROOT / "data" / "emrk" / "danelius_cases.json"
OUTPUT_DIR = PROJECT_ROOT / "data" / "emrk" / "hudoc"
HTML_DIR = OUTPUT_DIR / "html"
PDF_DIR = OUTPUT_DIR / "pdf"
METADATA_DIR = OUTPUT_DIR / "metadata"
CHECKPOINT_PATH = OUTPUT_DIR / "checkpoint.json"
MISSING_LOG_PATH = OUTPUT_DIR / "missing.jsonl"
LOG_PATH = OUTPUT_DIR / "fetch.log"
RETRY_LOG_PATH = OUTPUT_DIR / "retry_fetch.log"
RETRY_MISSING_LOG_PATH = OUTPUT_DIR / "retry_still_missing.jsonl"
RATE_LIMIT_SECONDS = 2.0
HUDOC_SEARCH_URL = "https://hudoc.echr.coe.int/app/query/results"
HUDOC_HTML_URL = "https://hudoc.echr.coe.int/app/conversion/docx/html/body"
HUDOC_PDF_URL = "https://hudoc.echr.coe.int/app/conversion/pdf/"
NAME_QUERY_LENGTH = 25
DATE_QUERY_LENGTH = 100
DATE_TOLERANCE_DAYS = 45
NEARBY_DATE_OFFSETS = (
    1,
    -1,
    2,
    -2,
    3,
    -3,
    5,
    -5,
    7,
    -7,
    10,
    -10,
    14,
    -14,
    21,
    -21,
    28,
    -28,
    30,
    -30,
    35,
    -35,
    42,
    -42,
    45,
    -45,
)

USER_AGENT = "paragrafen-ai-emrk-fetcher/1.0"

STATE_INFO = {
    "Albanien": {"code": "ALB", "english": "ALBANIA"},
    "Andorra": {"code": "AND", "english": "ANDORRA"},
    "Armenien": {"code": "ARM", "english": "ARMENIA"},
    "Azerbajdzjan": {"code": "AZE", "english": "AZERBAIJAN"},
    "Belgien": {"code": "BEL", "english": "BELGIUM"},
    "Bosnien och Hercegovina": {"code": "BIH", "english": "BOSNIA AND HERZEGOVINA"},
    "Bulgarien": {"code": "BGR", "english": "BULGARIA"},
    "Cypern": {"code": "CYP", "english": "CYPRUS"},
    "Danmark": {"code": "DNK", "english": "DENMARK"},
    "Estland": {"code": "EST", "english": "ESTONIA"},
    "Finland": {"code": "FIN", "english": "FINLAND"},
    "Frankrike": {"code": "FRA", "english": "FRANCE"},
    "Förenade kungariket": {"code": "GBR", "english": "UNITED KINGDOM"},
    "Georgien": {"code": "GEO", "english": "GEORGIA"},
    "Grekland": {"code": "GRC", "english": "GREECE"},
    "Irland": {"code": "IRL", "english": "IRELAND"},
    "Island": {"code": "ISL", "english": "ICELAND"},
    "Italien": {"code": "ITA", "english": "ITALY"},
    "Kroatien": {"code": "HRV", "english": "CROATIA"},
    "Lettland": {"code": "LVA", "english": "LATVIA"},
    "Liechtenstein": {"code": "LIE", "english": "LIECHTENSTEIN"},
    "Litauen": {"code": "LTU", "english": "LITHUANIA"},
    "Luxemburg": {"code": "LUX", "english": "LUXEMBOURG"},
    "Malta": {"code": "MLT", "english": "MALTA"},
    "Moldavien": {"code": "MDA", "english": "MOLDOVA"},
    "Monaco": {"code": "MCO", "english": "MONACO"},
    "Montenegro": {"code": "MNE", "english": "MONTENEGRO"},
    "Nederländerna": {"code": "NLD", "english": "NETHERLANDS"},
    "Nordmakedonien": {"code": "MKD", "english": "NORTH MACEDONIA"},
    "Norge": {"code": "NOR", "english": "NORWAY"},
    "Polen": {"code": "POL", "english": "POLAND"},
    "Portugal": {"code": "PRT", "english": "PORTUGAL"},
    "Rumänien": {"code": "ROU", "english": "ROMANIA"},
    "Ryssland": {"code": "RUS", "english": "RUSSIA"},
    "San Marino": {"code": "SMR", "english": "SAN MARINO"},
    "Schweiz": {"code": "CHE", "english": "SWITZERLAND"},
    "Serbien": {"code": "SRB", "english": "SERBIA"},
    "Slovakien": {"code": "SVK", "english": "SLOVAKIA"},
    "Slovenien": {"code": "SVN", "english": "SLOVENIA"},
    "Spanien": {"code": "ESP", "english": "SPAIN"},
    "Sverige": {"code": "SWE", "english": "SWEDEN"},
    "Tjeckien": {"code": "CZE", "english": "CZECH REPUBLIC"},
    "Turkiet": {"code": "TUR", "english": "TURKEY"},
    "Tyskland": {"code": "DEU", "english": "GERMANY"},
    "Ukraina": {"code": "UKR", "english": "UKRAINE"},
    "Ungern": {"code": "HUN", "english": "HUNGARY"},
    "Österrike": {"code": "AUT", "english": "AUSTRIA"},
}

NOISE_MARKERS = (
    "[translation]",
    "translation by",
    "legal summary",
    "summary by",
    "press release",
    "communicated case",
    "announcement",
)


@dataclass
class SearchHit:
    itemid: str
    docname: str
    appno: str
    respondent: str
    kpdate: str
    doctypebranch: str
    importance: str
    ecli: str
    separateopinion: Any
    raw: dict[str, Any]


@dataclass
class RankedHit:
    hit: SearchHit
    query: str | None
    query_kind: str
    query_result_count: int
    score: float
    name_score: float
    respondent_score: float
    date_distance_days: int | None


@dataclass(frozen=True)
class QueryPlan:
    query: str
    kind: str
    length: int


BASE_TEXT_REPLACEMENTS = {
    "111ot": "mot",
    "111.fl.": "m.fl.",
    "111.fl": "m.fl.",
    " och ": " and ",
    " m.fl.": " and others",
    " m.fl": " and others",
    " nr ": " no ",
    "(nr ": "(no ",
}

OCR_VARIANT_TRANSLATIONS = (
    str.maketrans(
        {
            "$": "s",
            "§": "s",
            "5": "s",
            "0": "o",
            "1": "l",
            "|": "l",
            "!": "i",
            "]": "j",
            "[": "i",
            "{": "c",
            "}": "c",
            "<": "c",
            ">": "c",
            "~": "n",
            "?": "",
            "·": "",
            "/": "l",
        }
    ),
    str.maketrans(
        {
            "$": "s",
            "§": "s",
            "5": "s",
            "0": "o",
            "1": "i",
            "|": "l",
            "!": "i",
            "]": "j",
            "[": "i",
            "{": "c",
            "}": "c",
            "<": "c",
            ">": "c",
            "~": "n",
            "?": "",
            "·": "",
            "/": "i",
        }
    ),
)

OCR_SIGNATURE_TRANSLATION = str.maketrans(
    {
        "$": "s",
        "§": "s",
        "5": "s",
        "0": "o",
        "1": "i",
        "|": "i",
        "!": "i",
        "]": "j",
        "[": "i",
        "{": "c",
        "}": "c",
        "<": "c",
        ">": "c",
        "~": "n",
        "?": "",
        "·": "",
        "/": "i",
        ":": "s",
        ";": "s",
    }
)


def apply_base_replacements(text: str) -> str:
    normalized = text.lower()
    for old, new in BASE_TEXT_REPLACEMENTS.items():
        normalized = normalized.replace(old, new)
    normalized = normalized.replace(" c. ", " v. ").replace(" contre ", " v. ").replace(" against ", " v. ")
    return normalized


def normalize_text(text: str) -> str:
    text = apply_base_replacements(text)
    text = strip_accents(text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def aggressive_normalize_text(text: str) -> str:
    normalized = apply_base_replacements(text)
    normalized = strip_accents(normalized)
    normalized = re.sub(r"[:;]+", "s", normalized)
    normalized = normalized.translate(OCR_SIGNATURE_TRANSLATION)
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def consonant_signature(text: str) -> str:
    normalized = aggressive_normalize_text(text)
    tokens = []
    for token in normalized.split():
        signature = re.sub(r"[aeiouy]+", "", token)
        tokens.append(signature or token[:1])
    return " ".join(tokens)


def normalized_forms(text: str) -> list[str]:
    forms: list[str] = []
    for candidate in (
        normalize_text(text),
        aggressive_normalize_text(text),
        consonant_signature(text),
    ):
        if candidate and candidate not in forms:
            forms.append(candidate)
    return forms


def dedupe_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def strip_parser_noise(text: str) -> str:
    cleaned = re.sub(r"^Rättsfall\s+\d+[A-Za-z\s.,-]*", "", text, flags=re.IGNORECASE).strip(" ,")
    if " mot " in cleaned:
        tail = re.split(r"\)\s*[0-9][0-9,\s~]*", cleaned)[-1].strip(" ,")
        if tail and tail != cleaned:
            cleaned = tail
    cleaned = re.sub(r"\b\d{2,4}\b", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,")
    return cleaned or text


def ocr_name_variants(text: str) -> list[str]:
    base = strip_parser_noise(text)
    variants = [base]
    for translation in OCR_VARIANT_TRANSLATIONS:
        translated = strip_accents(base).translate(translation)
        translated = re.sub(r"[:;]+", "s", translated)
        translated = re.sub(r"\s+", " ", translated).strip(" ,")
        variants.append(translated)
        variants.append(translated.replace("/", ""))
        variants.append(translated.replace("/", "l"))
        variants.append(translated.replace("/", "i"))
    if base.startswith("]"):
        variants.append("J" + base[1:])
    if base.startswith("0"):
        variants.append("O" + base[1:])
    variants.append(re.sub(r"^[^A-Za-z]+", "", base))
    return dedupe_preserve_order([re.sub(r"\s+", " ", variant).strip(" ,") for variant in variants if variant])


def query_name_variants(part1: str) -> list[str]:
    variants: list[str] = []

    for cleaned in ocr_name_variants(part1):
        variants.append(cleaned)
        variants.append(cleaned.replace(" och ", " and ").replace(" m.fl.", " and others").replace(" m.fl", " and others"))
        variants.append(re.sub(r"\([^)]*\)", "", cleaned).strip(" ,"))
        variants.append(re.sub(r"\b\d+\b", " ", cleaned).strip(" ,"))

        if "," in cleaned:
            first, rest = [piece.strip() for piece in cleaned.split(",", 1)]
            if first and rest:
                variants.append(f"{rest} {first}")
                variants.append(f"{rest} {first.split()[0]}")

        if " mot " in cleaned:
            variants.append(cleaned.split(" mot ", 1)[0].strip(" ,"))
            variants.append(cleaned.rsplit(" mot ", 1)[-1].strip(" ,"))

        words = cleaned.split()
        if len(words) >= 2:
            variants.append(" ".join(words[-2:]))
        if len(words) >= 4:
            variants.append(" ".join(words[-4:]))

    return dedupe_preserve_order([variant for variant in variants if variant])


def strip_accents(text: str) -> str:
    return "".join(
        char for char in unicodedata.normalize("NFKD", text) if not unicodedata.combining(char)
    )


def sanitize_filename(name: str) -> str:
    lowered = strip_accents(name.lower())
    lowered = re.sub(r"[^a-z0-9_]+", "_", lowered)
    lowered = re.sub(r"_+", "_", lowered).strip("_")
    return lowered[:120]


def split_respondent_states(respondent_state: str) -> list[str]:
    return [state.strip() for state in respondent_state.split(",") if state.strip()]


def respondent_codes(respondent_state: str) -> list[str]:
    codes: list[str] = []
    for state in split_respondent_states(respondent_state):
        info = STATE_INFO.get(state)
        if info and info["code"] not in codes:
            codes.append(info["code"])
    return codes


def english_respondent_name(respondent_state: str) -> str | None:
    names: list[str] = []
    for state in split_respondent_states(respondent_state):
        info = STATE_INFO.get(state)
        if not info:
            return None
        names.append(info["english"])
    if not names:
        return None
    if len(names) == 1:
        return names[0]
    return " AND ".join(names)


def parse_case_date(value: str) -> date | None:
    if not value:
        return None
    candidate = value[:10]
    try:
        return datetime.strptime(candidate, "%Y-%m-%d").date()
    except ValueError:
        return None


def nearby_expected_dates(expected_date: str) -> list[str]:
    parsed = parse_case_date(expected_date)
    if parsed is None:
        return []
    return [
        (parsed + timedelta(days=offset)).isoformat()
        for offset in NEARBY_DATE_OFFSETS
    ]


def respondent_match_score(respondent_state: str, hit_respondent: str) -> float:
    expected_codes = set(respondent_codes(respondent_state))
    if not expected_codes:
        return 0.0
    actual_codes = {code.strip() for code in hit_respondent.split(";") if code.strip()}
    if not actual_codes:
        return 0.0
    if actual_codes == expected_codes:
        return 0.24 if len(expected_codes) > 1 else 0.20
    if expected_codes.issubset(actual_codes):
        return 0.22
    overlap = len(expected_codes & actual_codes)
    if overlap == len(expected_codes):
        return 0.18
    if overlap:
        return 0.10 * (overlap / len(expected_codes))
    return 0.0


def date_match_score(expected_date: str, hit_date: str) -> tuple[float, int | None]:
    expected = parse_case_date(expected_date)
    actual = parse_case_date(hit_date)
    if expected is None or actual is None:
        return 0.0, None
    distance = abs((actual - expected).days)
    if distance == 0:
        return 0.35, 0
    if distance <= DATE_TOLERANCE_DAYS:
        return 0.22 * (1 - (distance / (DATE_TOLERANCE_DAYS + 1))), distance
    return 0.0, distance


def load_checkpoint() -> dict[str, int]:
    if CHECKPOINT_PATH.exists():
        with CHECKPOINT_PATH.open(encoding="utf-8") as handle:
            return json.load(handle)
    return {"processed": 0, "downloaded": 0, "missing": 0, "errors": 0}


def save_checkpoint(checkpoint: dict[str, int]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with CHECKPOINT_PATH.open("w", encoding="utf-8") as handle:
        json.dump(checkpoint, handle, ensure_ascii=False, indent=2)


def parse_hit(result: dict[str, Any]) -> SearchHit:
    columns = result.get("columns", result)
    return SearchHit(
        itemid=str(columns.get("itemid", "")),
        docname=str(columns.get("docname", "")),
        appno=str(columns.get("appno", "")),
        respondent=str(columns.get("respondent", "")),
        kpdate=str(columns.get("kpdate", "")),
        doctypebranch=str(columns.get("doctypebranch", "")),
        importance=str(columns.get("importance", "")),
        ecli=str(columns.get("ecli", "")),
        separateopinion=columns.get("separateopinion", False),
        raw=columns,
    )


def build_name_queries(part1: str, respondent_state: str) -> list[QueryPlan]:
    queries: list[QueryPlan] = []
    seen: set[str] = set()
    respondent_name = english_respondent_name(respondent_state)
    expected_codes = respondent_codes(respondent_state)

    for part_name in query_name_variants(part1):
        candidates = [
            f'docname:"CASE OF {part_name}"',
            f'docname:"{part_name}"',
        ]

        if respondent_name:
            candidates.extend(
                [
                    f'docname:"CASE OF {part_name} v. {respondent_name}"',
                    f'docname:"CASE OF {part_name} AGAINST {respondent_name}"',
                ]
            )

        for code in expected_codes:
            candidates.append(f'docname:"{part_name}" AND respondent:"{code}"')

        for candidate in candidates:
            if candidate and candidate not in seen:
                seen.add(candidate)
                queries.append(QueryPlan(query=candidate, kind="name", length=NAME_QUERY_LENGTH))

    return queries


def build_exact_date_queries(respondent_state: str, expected_date: str) -> list[QueryPlan]:
    if not expected_date:
        return []

    expected_codes = respondent_codes(respondent_state)
    queries: list[QueryPlan] = []

    if len(expected_codes) <= 1:
        if expected_codes:
            queries.append(
                QueryPlan(
                    query=f'respondent:"{expected_codes[0]}" AND kpdate:"{expected_date}"',
                    kind="date_exact",
                    length=DATE_QUERY_LENGTH,
                )
            )
    else:
        queries.append(
            QueryPlan(
                query=f'kpdate:"{expected_date}"',
                kind="date_exact_composite",
                length=DATE_QUERY_LENGTH,
            )
        )
        for code in expected_codes:
            queries.append(
                QueryPlan(
                    query=f'respondent:"{code}" AND kpdate:"{expected_date}"',
                    kind="date_exact_component",
                    length=DATE_QUERY_LENGTH,
                )
            )

    return queries


def build_nearby_date_queries(respondent_state: str, expected_date: str) -> list[QueryPlan]:
    expected_codes = respondent_codes(respondent_state)
    queries: list[QueryPlan] = []
    seen: set[str] = set()

    for nearby_date in nearby_expected_dates(expected_date):
        if len(expected_codes) <= 1:
            if not expected_codes:
                continue
            candidate = f'respondent:"{expected_codes[0]}" AND kpdate:"{nearby_date}"'
            if candidate not in seen:
                seen.add(candidate)
                queries.append(QueryPlan(query=candidate, kind="date_nearby", length=DATE_QUERY_LENGTH))
            continue

        candidate = f'kpdate:"{nearby_date}"'
        if candidate not in seen:
            seen.add(candidate)
            queries.append(QueryPlan(query=candidate, kind="date_nearby_composite", length=DATE_QUERY_LENGTH))

    return queries


def search_hudoc(session: requests.Session, query: str, length: int = 25) -> list[SearchHit]:
    response = session.get(
        HUDOC_SEARCH_URL,
        params={
            "query": query,
            "select": "itemid,docname,appno,respondent,conclusion,kpdate,doctypebranch,importance,separateopinion,ecli",
            "sort": "kpdate Descending",
            "start": 0,
            "length": length,
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    return [parse_hit(result) for result in payload.get("results", [])]


def score_hit(
    hit: SearchHit,
    part1: str,
    expected_date: str,
    respondent_state: str,
) -> tuple[float, float, float, int | None]:
    name_variants = query_name_variants(part1)
    best_name_score = 0.0
    for variant in name_variants:
        best_name_score = max(best_name_score, similarity(hit.docname, variant))

    score = best_name_score
    date_score, date_distance_days = date_match_score(expected_date, hit.kpdate)
    respondent_score = respondent_match_score(respondent_state, hit.respondent)
    score += date_score
    score += respondent_score
    lowered_docname = hit.docname.lower()
    if any(marker in lowered_docname for marker in NOISE_MARKERS):
        score -= 0.30
    if hit.doctypebranch and "advisory" in hit.doctypebranch.lower():
        score -= 0.20
    if hit.doctypebranch and "execution" in hit.doctypebranch.lower():
        score -= 0.08
    if hit.itemid.startswith("001-") and not any(marker in lowered_docname for marker in NOISE_MARKERS):
        score += 0.05
    return score, best_name_score, respondent_score, date_distance_days


def similarity(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    best = 0.0
    for left_form in normalized_forms(left):
        for right_form in normalized_forms(right):
            if not left_form or not right_form:
                continue
            left_tokens = set(left_form.split())
            right_tokens = set(right_form.split())
            overlap = len(left_tokens & right_tokens) / max(len(right_tokens), 1)
            best = max(best, 0.55 * overlap + 0.45 * sequence_ratio(left_form, right_form))
    return best


def sequence_ratio(left: str, right: str) -> float:
    from difflib import SequenceMatcher

    return SequenceMatcher(None, left, right).ratio()


INLINE_DATA_IMAGE_RE = re.compile(
    r"<img\b[^>]*\bsrc=(['\"])data:image/[^'\"]*\1[^>]*>",
    re.IGNORECASE,
)


def sanitize_hudoc_html(html: str) -> tuple[str, int]:
    """Ta bort inline base64-bilder som inte behövs för textutvinning."""
    removed = 0

    def _replace(match: re.Match[str]) -> str:
        nonlocal removed
        removed += 1
        return "<!-- inline HUDOC image removed -->"

    sanitized = INLINE_DATA_IMAGE_RE.sub(_replace, html)
    return sanitized, removed


def absorb_query_hits(
    session: requests.Session,
    query_plans: list[QueryPlan],
    part1: str,
    respondent_state: str,
    expected_date: str,
) -> dict[str, RankedHit]:
    ranked: dict[str, RankedHit] = {}

    for query_plan in query_plans:
        try:
            hits = search_hudoc(session, query_plan.query, length=query_plan.length)
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response else "?"
            if status_code in (403, 429):
                raise
            continue
        except requests.RequestException:
            continue

        for hit in hits:
            score, name_score, respondent_score, date_distance_days = score_hit(
                hit, part1, expected_date, respondent_state
            )
            current = ranked.get(hit.itemid)
            if current is None or score > current.score:
                ranked[hit.itemid] = RankedHit(
                    hit=hit,
                    query=query_plan.query,
                    query_kind=query_plan.kind,
                    query_result_count=len(hits),
                    score=score,
                    name_score=name_score,
                    respondent_score=respondent_score,
                    date_distance_days=date_distance_days,
                )
    return ranked


def is_acceptable_hit(ranked_hit: RankedHit) -> bool:
    if ranked_hit.score >= 0.72 and ranked_hit.name_score >= 0.20:
        return True
    if (
        ranked_hit.date_distance_days == 0
        and ranked_hit.respondent_score >= 0.18
        and ranked_hit.score >= 0.74
        and ranked_hit.name_score >= 0.12
    ):
        return True
    if (
        ranked_hit.date_distance_days == 0
        and ranked_hit.respondent_score >= 0.18
        and ranked_hit.query_result_count <= 20
        and ranked_hit.score >= 0.73
        and ranked_hit.name_score >= 0.15
    ):
        return True
    if (
        ranked_hit.date_distance_days == 0
        and ranked_hit.respondent_score >= 0.18
        and ranked_hit.query_result_count <= 8
        and ranked_hit.score >= 0.42
    ):
        return True
    if (
        ranked_hit.date_distance_days is not None
        and ranked_hit.date_distance_days <= DATE_TOLERANCE_DAYS
        and ranked_hit.respondent_score >= 0.18
        and ranked_hit.query_result_count <= 5
        and ranked_hit.score >= 0.48
        and ranked_hit.name_score >= 0.12
    ):
        return True
    if (
        ranked_hit.query_kind.startswith("date")
        and ranked_hit.date_distance_days == 0
        and ranked_hit.query_result_count == 1
        and ranked_hit.respondent_score >= 0.18
        and ranked_hit.score >= 0.35
    ):
        return True
    return False


def collect_ranked_hits(
    session: requests.Session,
    part1: str,
    respondent_state: str,
    expected_date: str,
) -> list[RankedHit]:
    ranked: dict[str, RankedHit] = {}

    for query_plans in (
        build_name_queries(part1, respondent_state),
        build_exact_date_queries(respondent_state, expected_date),
        build_nearby_date_queries(respondent_state, expected_date),
    ):
        for itemid, ranked_hit in absorb_query_hits(
            session=session,
            query_plans=query_plans,
            part1=part1,
            respondent_state=respondent_state,
            expected_date=expected_date,
        ).items():
            current = ranked.get(itemid)
            if current is None or ranked_hit.score > current.score:
                ranked[itemid] = ranked_hit

        ordered = sorted(ranked.values(), key=lambda item: item.score, reverse=True)
        if ordered and is_acceptable_hit(ordered[0]):
            return ordered

    return sorted(ranked.values(), key=lambda item: item.score, reverse=True)


def pick_best_hit(
    session: requests.Session,
    part1: str,
    respondent_state: str,
    expected_date: str,
) -> list[RankedHit]:
    ordered = collect_ranked_hits(
        session=session,
        part1=part1,
        respondent_state=respondent_state,
        expected_date=expected_date,
    )
    if not ordered:
        return []
    if not is_acceptable_hit(ordered[0]):
        return []
    return ordered


def download_document(
    session: requests.Session,
    itemid: str,
    output_path: Path,
) -> tuple[bool, str]:
    response = session.get(
        HUDOC_HTML_URL,
        params={"id": itemid, "library": "ECHR"},
        timeout=60,
    )
    if response.status_code == 204:
        return False, "no_html_body"
    if not response.ok:
        return False, f"http_{response.status_code}"
    if len(response.text) < 500:
        return False, "short_body"
    sanitized_html, removed_images = sanitize_hudoc_html(response.text)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(sanitized_html, encoding="utf-8")
    if removed_images:
        append_log(
            f"SANITIZED_INLINE_IMAGES itemid={itemid} removed={removed_images} output={output_path.name}"
        )
    return True, "ok"


def download_pdf(
    session: requests.Session,
    hit: SearchHit,
    output_path: Path,
) -> tuple[bool, str]:
    response = session.get(
        HUDOC_PDF_URL,
        params={"filename": f"{hit.docname}.pdf", "id": hit.itemid, "library": "ECHR"},
        timeout=60,
    )
    if response.status_code == 204:
        return False, "no_pdf"
    if not response.ok:
        return False, f"pdf_http_{response.status_code}"
    if len(response.content) < 500:
        return False, "short_pdf"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(response.content)
    return True, "pdf_ok"


def render_pdf_as_html(pdf_path: Path, html_path: Path) -> tuple[bool, str]:
    try:
        result = subprocess.run(
            ["pdftotext", "-layout", str(pdf_path), "-"],
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except FileNotFoundError:
        return False, "pdftotext_missing"
    except subprocess.SubprocessError:
        return False, "pdftotext_failed"

    extracted = result.stdout.strip()
    if not extracted:
        return False, "empty_pdf_text"

    escaped = (
        extracted.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
    html = "<html><body><pre>\n" + escaped + "\n</pre></body></html>\n"
    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(html, encoding="utf-8")
    return True, "pdf_to_html"


def try_download_ranked_hit(
    session: requests.Session,
    ranked_hit: RankedHit,
    safe_name: str,
) -> tuple[bool, str, str | None]:
    html_path = HTML_DIR / f"{safe_name}.html"
    pdf_path = PDF_DIR / f"{safe_name}.pdf"

    downloaded, reason = download_document(session, ranked_hit.hit.itemid, html_path)
    if downloaded:
        return True, "html", None

    pdf_downloaded, pdf_reason = download_pdf(session, ranked_hit.hit, pdf_path)
    if not pdf_downloaded:
        return False, reason if reason != "no_html_body" else pdf_reason, None

    rendered, render_reason = render_pdf_as_html(pdf_path, html_path)
    if rendered:
        return True, "pdf_to_html", None
    return True, "pdf", render_reason


def guess_formation(hit: SearchHit) -> str:
    doctype = hit.doctypebranch.lower()
    docname = hit.docname.lower()
    if "grandchamber" in doctype or "grand chamber" in docname:
        return "grand_chamber"
    if "committee" in doctype:
        return "committee"
    if "advisory" in doctype:
        return "advisory_opinion"
    return "chamber"


def load_cases(cases_json_path: Path) -> list[dict[str, Any]]:
    with cases_json_path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload["cases"]


def append_missing(case: dict[str, Any], attempted_query: str | None, reason: str) -> None:
    append_missing_to_path(MISSING_LOG_PATH, case, attempted_query, reason)


def append_missing_to_path(
    path: Path,
    case: dict[str, Any],
    attempted_query: str | None,
    reason: str,
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "timestamp": date.today().isoformat(),
                    "reason": reason,
                    "attempted_query": attempted_query,
                    **case,
                },
                ensure_ascii=False,
            )
            + "\n"
        )


def append_log(message: str) -> None:
    append_log_to_path(LOG_PATH, message)


def append_log_to_path(path: Path, message: str) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(message + "\n")


def process_case(
    session: requests.Session,
    case: dict[str, Any],
    *,
    index_label: str,
    log_path: Path,
    missing_log_path: Path,
    skip_if_downloaded: bool = True,
) -> str:
    part1 = case["part1"]
    search_part1 = case.get("search_part1", part1)
    respondent_state = case["respondent_state"]
    expected_date = case["date"]
    safe_name = sanitize_filename(f"{part1}_v_{respondent_state}_{expected_date}")
    html_path = HTML_DIR / f"{safe_name}.html"
    pdf_path = PDF_DIR / f"{safe_name}.pdf"
    metadata_path = METADATA_DIR / f"{safe_name}.json"

    if skip_if_downloaded and metadata_path.exists():
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            if metadata.get("downloaded") and (html_path.exists() or pdf_path.exists()):
                return "skipped"
        except json.JSONDecodeError:
            pass

    try:
        ranked_hits = collect_ranked_hits(
            session=session,
            part1=search_part1,
            respondent_state=respondent_state,
            expected_date=expected_date,
        )
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response else "?"
        append_log_to_path(
            log_path,
            f"HTTP_ERROR index={index_label} status={status_code} case={part1} respondent={respondent_state}",
        )
        if status_code in (403, 429):
            raise
        return "error"
    except requests.RequestException as exc:
        append_log_to_path(
            log_path,
            f"REQUEST_ERROR index={index_label} case={part1} respondent={respondent_state} error={exc}",
        )
        return "error"

    if not ranked_hits:
        append_missing_to_path(missing_log_path, case, None, "no_match")
        return "missing"

    if not is_acceptable_hit(ranked_hits[0]):
        best_ranked_hit = ranked_hits[0]
        metadata = {
            "case_name": f"{part1} mot {respondent_state}",
            "search_part1": search_part1,
            "application_no": best_ranked_hit.hit.appno,
            "hudoc_id": best_ranked_hit.hit.itemid,
            "respondent_state": respondent_state,
            "date": expected_date,
            "danelius_pages": case.get("danelius_pages", ""),
            "formation": guess_formation(best_ranked_hit.hit),
            "ecli": best_ranked_hit.hit.ecli,
            "importance": best_ranked_hit.hit.importance,
            "doctypebranch": best_ranked_hit.hit.doctypebranch,
            "separate_opinion": best_ranked_hit.hit.separateopinion,
            "hudoc_docname": best_ranked_hit.hit.docname,
            "hudoc_respondent": best_ranked_hit.hit.respondent,
            "downloaded": False,
            "download_reason": "no_match",
            "attempted_query": best_ranked_hit.query,
            "attempted_query_kind": best_ranked_hit.query_kind,
            "attempted_query_result_count": best_ranked_hit.query_result_count,
            "match_score": best_ranked_hit.score,
            "match_name_score": best_ranked_hit.name_score,
            "match_respondent_score": best_ranked_hit.respondent_score,
            "match_date_distance_days": best_ranked_hit.date_distance_days,
            "candidate_count": len(ranked_hits),
            "primary_candidate_itemid": best_ranked_hit.hit.itemid,
            "primary_candidate_docname": best_ranked_hit.hit.docname,
            "primary_candidate_query_kind": best_ranked_hit.query_kind,
        }
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        append_missing_to_path(missing_log_path, case, best_ranked_hit.query, "no_match")
        append_log_to_path(
            log_path,
            f"LOW_CONFIDENCE index={index_label} itemid={best_ranked_hit.hit.itemid} "
            f"query={best_ranked_hit.query!r} case={part1} respondent={respondent_state}",
        )
        return "missing"

    best_ranked_hit = ranked_hits[0]
    final_ranked_hit = best_ranked_hit
    final_download_reason = "no_match"
    final_format = ""
    render_note: str | None = None
    downloaded = False

    for ranked_hit in ranked_hits[:8]:
        downloaded, final_format, render_note = try_download_ranked_hit(session, ranked_hit, safe_name)
        final_ranked_hit = ranked_hit
        if downloaded:
            final_download_reason = final_format
            break
        final_download_reason = final_format

    metadata = {
        "case_name": f"{part1} mot {respondent_state}",
        "search_part1": search_part1,
        "application_no": final_ranked_hit.hit.appno,
        "hudoc_id": final_ranked_hit.hit.itemid,
        "respondent_state": respondent_state,
        "date": expected_date,
        "danelius_pages": case.get("danelius_pages", ""),
        "formation": guess_formation(final_ranked_hit.hit),
        "ecli": final_ranked_hit.hit.ecli,
        "importance": final_ranked_hit.hit.importance,
        "doctypebranch": final_ranked_hit.hit.doctypebranch,
        "separate_opinion": final_ranked_hit.hit.separateopinion,
        "hudoc_docname": final_ranked_hit.hit.docname,
        "hudoc_respondent": final_ranked_hit.hit.respondent,
        "downloaded": downloaded,
        "download_reason": final_download_reason,
        "download_render_note": render_note,
        "attempted_query": final_ranked_hit.query,
        "attempted_query_kind": final_ranked_hit.query_kind,
        "attempted_query_result_count": final_ranked_hit.query_result_count,
        "match_score": final_ranked_hit.score,
        "match_name_score": final_ranked_hit.name_score,
        "match_respondent_score": final_ranked_hit.respondent_score,
        "match_date_distance_days": final_ranked_hit.date_distance_days,
        "candidate_count": len(ranked_hits),
        "primary_candidate_itemid": best_ranked_hit.hit.itemid,
        "primary_candidate_docname": best_ranked_hit.hit.docname,
        "primary_candidate_query_kind": best_ranked_hit.query_kind,
    }
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    if downloaded:
        append_log_to_path(
            log_path,
            f"DOWNLOADED index={index_label} itemid={final_ranked_hit.hit.itemid} format={final_format} "
            f"query={final_ranked_hit.query!r} case={part1} respondent={respondent_state}",
        )
        return "downloaded"

    append_missing_to_path(missing_log_path, case, final_ranked_hit.query, final_download_reason)
    append_log_to_path(
        log_path,
        f"UNRESOLVED index={index_label} itemid={final_ranked_hit.hit.itemid} reason={final_download_reason} "
        f"query={final_ranked_hit.query!r} case={part1} respondent={respondent_state}",
    )
    return "missing"


def load_missing_cases(
    missing_log_path: Path,
    reason_filter: str | None = None,
) -> list[dict[str, Any]]:
    if not missing_log_path.exists():
        return []

    cases: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()

    for line in missing_log_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if reason_filter and row.get("reason") != reason_filter:
            continue
        key = (
            row["part1"],
            row["respondent_state"],
            row["date"],
            row.get("danelius_pages", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        cases.append(
            {
                "part1": row["part1"],
                "respondent_state": row["respondent_state"],
                "date": row["date"],
                "danelius_pages": row.get("danelius_pages", ""),
            }
        )

    return cases


def run(cases_json_path: Path, limit: int | None = None) -> dict[str, int]:
    HTML_DIR.mkdir(parents=True, exist_ok=True)
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_DIR.mkdir(parents=True, exist_ok=True)

    cases = load_cases(cases_json_path)
    checkpoint = load_checkpoint()
    start_index = checkpoint["processed"]
    if limit is not None:
        cases = cases[:limit]
        start_index = min(start_index, len(cases))

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    append_log(f"START total={len(cases)} start_index={start_index}")

    for index, case in enumerate(cases[start_index:], start=start_index):
        outcome = process_case(
            session=session,
            case=case,
            index_label=str(index),
            log_path=LOG_PATH,
            missing_log_path=MISSING_LOG_PATH,
        )
        if outcome == "downloaded":
            checkpoint["downloaded"] += 1
        elif outcome == "missing":
            checkpoint["missing"] += 1
        elif outcome == "error":
            checkpoint["errors"] += 1

        checkpoint["processed"] = index + 1

        if checkpoint["processed"] % 50 == 0:
            save_checkpoint(checkpoint)
            append_log(
                "PROGRESS "
                f"processed={checkpoint['processed']} downloaded={checkpoint['downloaded']} "
                f"missing={checkpoint['missing']} errors={checkpoint['errors']}"
            )

        if outcome != "skipped":
            time.sleep(RATE_LIMIT_SECONDS)

    save_checkpoint(checkpoint)
    append_log(
        "DONE "
        f"processed={checkpoint['processed']} downloaded={checkpoint['downloaded']} "
        f"missing={checkpoint['missing']} errors={checkpoint['errors']}"
    )
    return checkpoint


def retry_missing(reason: str | None = None, limit: int | None = None) -> dict[str, int]:
    HTML_DIR.mkdir(parents=True, exist_ok=True)
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_DIR.mkdir(parents=True, exist_ok=True)

    cases = load_missing_cases(MISSING_LOG_PATH, reason_filter=reason)
    if limit is not None:
        cases = cases[:limit]

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    RETRY_LOG_PATH.write_text("", encoding="utf-8")
    RETRY_MISSING_LOG_PATH.write_text("", encoding="utf-8")
    append_log_to_path(RETRY_LOG_PATH, f"RETRY_START total={len(cases)} reason={reason or 'any'}")

    stats = {"processed": 0, "downloaded": 0, "missing": 0, "errors": 0}

    for index, case in enumerate(cases):
        outcome = process_case(
            session=session,
            case=case,
            index_label=f"retry:{index}",
            log_path=RETRY_LOG_PATH,
            missing_log_path=RETRY_MISSING_LOG_PATH,
            skip_if_downloaded=True,
        )
        stats["processed"] += 1
        if outcome == "downloaded":
            stats["downloaded"] += 1
        elif outcome == "missing":
            stats["missing"] += 1
        elif outcome == "error":
            stats["errors"] += 1

        if stats["processed"] % 50 == 0:
            append_log_to_path(
                RETRY_LOG_PATH,
                "RETRY_PROGRESS "
                f"processed={stats['processed']} downloaded={stats['downloaded']} "
                f"missing={stats['missing']} errors={stats['errors']}",
            )

        if outcome != "skipped":
            time.sleep(RATE_LIMIT_SECONDS)

    append_log_to_path(
        RETRY_LOG_PATH,
        "RETRY_DONE "
        f"processed={stats['processed']} downloaded={stats['downloaded']} "
        f"missing={stats['missing']} errors={stats['errors']}",
    )
    return stats


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("cases_json_path", nargs="?", type=Path, default=DEFAULT_CASES_PATH)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--retry-missing", action="store_true")
    parser.add_argument("--reason", choices=["no_match", "no_html_body"], default=None)
    args = parser.parse_args()

    if args.retry_missing:
        checkpoint = retry_missing(reason=args.reason, limit=args.limit)
    else:
        checkpoint = run(args.cases_json_path, args.limit)
    print(json.dumps(checkpoint, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
