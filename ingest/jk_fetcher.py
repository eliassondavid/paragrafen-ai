"""Fetch JK decisions from jk.se and persist them as raw JSON."""

from __future__ import annotations

import argparse
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import re
import sys
import time
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag
import requests


logger = logging.getLogger("jk_fetcher")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BASE_URL = "https://www.jk.se"
SEARCH_URL = f"{BASE_URL}/beslut-och-yttranden/"
RAW_DIR = PROJECT_ROOT / "data/raw/jk"
DECISIONS_DIR = RAW_DIR / "decisions"
CATALOG_PATH = RAW_DIR / "catalog.json"
SCHEMA_VERSION = "v0.15"
LICENSE = "public_domain"
REQUEST_TIMEOUT = 30
REQUEST_DELAY_SECONDS = 2.0
PROGRESS_EVERY = 50
MIN_TEXT_LENGTH = 500

KATEGORI_AUTHORITY = {
    "Skadeståndsärenden": "binding",
    "Ersättning vid frihetsinskränkning": "binding",
    "Tillsynsärenden": "guiding",
    "Tryck- och yttrandefrihetsärenden": "binding",
    "Remissyttranden": "persuasive",
}

FALLBACK_CATEGORY_TYPES = {
    "Skadeståndsärenden": "39",
    "Ersättning vid frihetsinskränkning": "40",
    "Tillsynsärenden": "41",
    "Tryck- och yttrandefrihetsärenden": "42",
    "Remissyttranden": "43",
}

PRIMARY_DNR_RE = re.compile(
    r"Diarienr:\s*([\d/]+)(?=\s*(?:/|Beslutsdatum:|\n|$))",
    re.IGNORECASE,
)
FALLBACK_DNR_RE = re.compile(
    r"Diarienr:\s*(.+?)(?=\s*/\s*Beslutsdatum:|\s*Beslutsdatum:|\n|$)",
    re.IGNORECASE,
)
BESLUTSDATUM_RE = re.compile(
    r"Beslutsdatum:\s*(.+?)(?=\n|$)",
    re.IGNORECASE,
)

SWEDISH_MONTHS = {
    "jan": 1,
    "januari": 1,
    "feb": 2,
    "februari": 2,
    "mar": 3,
    "mars": 3,
    "apr": 4,
    "april": 4,
    "maj": 5,
    "jun": 6,
    "juni": 6,
    "jul": 7,
    "juli": 7,
    "aug": 8,
    "augusti": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "okt": 10,
    "oktober": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

REQUEST_HEADERS = {
    "User-Agent": "paragrafen-ai-jk-fetcher/0.1 (+https://paragrafen.ai)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


@dataclass(frozen=True)
class CategorySpec:
    """A JK search category and its backend form value."""

    name: str
    type_value: str


@dataclass(frozen=True)
class DecisionListing:
    """One decision discovered in the JK search results."""

    dnr: str
    titel: str
    beslutsdatum: str
    kategori: str
    source_url: str

    @property
    def authority_level(self) -> str:
        return KATEGORI_AUTHORITY.get(self.kategori, "guiding")

    @property
    def dnr_norm(self) -> str:
        return normalize_dnr(self.dnr)

    @property
    def filename(self) -> str:
        return f"jk_{self.dnr_norm}.json"

    @property
    def file_path(self) -> Path:
        return DECISIONS_DIR / self.filename

    @property
    def dok_id(self) -> str:
        return f"jk_{self.dnr_norm}"


class PauseExecution(RuntimeError):
    """Raised when discovery assumptions no longer hold."""


class RateLimitedSession:
    """requests.Session wrapper with a minimum delay between calls."""

    def __init__(self, delay_seconds: float = REQUEST_DELAY_SECONDS) -> None:
        self.delay_seconds = delay_seconds
        self.session = requests.Session()
        self.session.headers.update(REQUEST_HEADERS)
        self._last_request_at = 0.0

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        return self._request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> requests.Response:
        return self._request("POST", url, **kwargs)

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        elapsed = time.monotonic() - self._last_request_at
        if self._last_request_at and elapsed < self.delay_seconds:
            time.sleep(self.delay_seconds - elapsed)
        response = self.session.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
        self._last_request_at = time.monotonic()
        return response


class JKCategoryCursor:
    """Lazy paginator that yields one discovered listing at a time."""

    def __init__(self, session: RateLimitedSession, category: CategorySpec) -> None:
        self.session = session
        self.category = category
        self.buffer: list[DecisionListing] = []
        self.seen_urls: set[str] = set()
        self.initial_loaded = False
        self.next_page = 1
        self.exhausted = False

    def next_listing(self) -> DecisionListing | None:
        while not self.buffer and not self.exhausted:
            self._load_more()
        if not self.buffer:
            return None
        return self.buffer.pop(0)

    def _load_more(self) -> None:
        if self.exhausted:
            return

        payload: list[tuple[str, str]] = [("typ", self.category.type_value)]
        if not self.initial_loaded:
            payload.extend([("do-search", "Sök"), ("page", "1")])
        else:
            payload.extend([("show-more", "Visa fler"), ("page", str(self.next_page))])

        try:
            response = self.session.post(SEARCH_URL, data=payload)
            response.raise_for_status()
        except requests.Timeout as exc:
            raise PauseExecution(
                f"Timeout vid discovery för kategorin {self.category.name}."
            ) from exc
        except requests.RequestException as exc:
            raise PauseExecution(
                f"Discovery-anropet för kategorin {self.category.name} misslyckades: {exc}"
            ) from exc

        soup = BeautifulSoup(response.text, "html.parser")
        listings = parse_search_results(soup, self.category.name)
        new_items = [listing for listing in listings if listing.source_url not in self.seen_urls]
        for listing in new_items:
            self.seen_urls.add(listing.source_url)
        self.buffer.extend(new_items)

        self.initial_loaded = True
        next_page = extract_next_page_value(soup)
        has_show_more = bool(soup.select_one("button[name='show-more']"))
        if has_show_more and next_page:
            self.next_page = next_page
        else:
            self.exhausted = True

        if has_show_more and not new_items:
            self.exhausted = True


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def ensure_output_dirs() -> None:
    DECISIONS_DIR.mkdir(parents=True, exist_ok=True)


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def normalize_dnr(value: str) -> str:
    cleaned = normalize_whitespace(value)
    cleaned = cleaned.replace("/", "_")
    cleaned = re.sub(r"[^0-9A-Za-z_]+", "_", cleaned)
    return re.sub(r"_+", "_", cleaned).strip("_")


def extract_dnr(text: str) -> str:
    primary = PRIMARY_DNR_RE.search(text or "")
    if primary:
        return normalize_whitespace(primary.group(1))
    fallback = FALLBACK_DNR_RE.search(text or "")
    if fallback:
        return normalize_whitespace(fallback.group(1))
    return ""


def extract_beslutsdatum(text: str) -> str:
    match = BESLUTSDATUM_RE.search(text or "")
    if not match:
        return ""
    return parse_swedish_date(match.group(1)) or normalize_whitespace(match.group(1))


def parse_swedish_date(value: str) -> str:
    candidate = normalize_whitespace(value).rstrip(".")
    if not candidate:
        return ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", candidate):
        return candidate

    match = re.fullmatch(r"(\d{1,2})\s+([A-Za-zÅÄÖåäö]+)\s+(\d{4})", candidate)
    if not match:
        return ""

    day = int(match.group(1))
    month_key = match.group(2).casefold()
    year = int(match.group(3))
    month = SWEDISH_MONTHS.get(month_key)
    if month is None:
        return ""
    return f"{year:04d}-{month:02d}-{day:02d}"


def load_catalog() -> dict[str, dict[str, Any]]:
    if not CATALOG_PATH.exists():
        return {}

    try:
        payload = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise PauseExecution(f"Kunde inte läsa {CATALOG_PATH}: {exc}") from exc

    if not isinstance(payload, list):
        raise PauseExecution("catalog.json måste vara en lista av poster.")

    catalog: dict[str, dict[str, Any]] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        source_url = str(item.get("source_url") or "").strip()
        if source_url:
            catalog[source_url] = item
    return catalog


def save_catalog(catalog: dict[str, dict[str, Any]]) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    ordered = sorted(
        catalog.values(),
        key=lambda item: (
            str(item.get("beslutsdatum") or ""),
            str(item.get("dnr") or ""),
        ),
        reverse=True,
    )
    CATALOG_PATH.write_text(
        json.dumps(ordered, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def extract_category_specs(session: RateLimitedSession) -> list[CategorySpec]:
    try:
        response = session.get(SEARCH_URL)
        response.raise_for_status()
    except requests.Timeout as exc:
        raise PauseExecution("Timeout när JK:s söksida skulle hämtas.") from exc
    except requests.RequestException as exc:
        raise PauseExecution(f"Kunde inte hämta JK:s söksida: {exc}") from exc

    soup = BeautifulSoup(response.text, "html.parser")
    labels = soup.select("div.checkbox-container label.checkbox-label")
    discovered: dict[str, str] = {}
    for label in labels:
        input_node = label.find("input", attrs={"name": "typ"})
        if not isinstance(input_node, Tag):
            continue
        name = normalize_whitespace(label.get_text(" ", strip=True))
        type_value = normalize_whitespace(str(input_node.get("value") or ""))
        if name and type_value:
            discovered[name] = type_value

    specs: list[CategorySpec] = []
    for category_name in KATEGORI_AUTHORITY:
        type_value = discovered.get(category_name) or FALLBACK_CATEGORY_TYPES.get(category_name)
        if not type_value:
            raise PauseExecution(
                f"Kunde inte hitta backend-värdet för kategorin {category_name}."
            )
        specs.append(CategorySpec(name=category_name, type_value=type_value))
    return specs


def parse_search_results(soup: BeautifulSoup, category_name: str) -> list[DecisionListing]:
    results_container = soup.select_one("div.ruling-results div.results")
    if not isinstance(results_container, Tag):
        raise PauseExecution(
            f"Kategorin {category_name} gav ingen resultatlista. Discovery kräver annan metod."
        )

    listings: list[DecisionListing] = []
    for date_div in results_container.find_all("div", class_="date"):
        heading = date_div.find_next_sibling("h2")
        if not isinstance(heading, Tag):
            continue
        link = heading.find("a", href=True)
        if not isinstance(link, Tag):
            continue

        meta_text = date_div.get_text(" ", strip=True)
        dnr = extract_dnr(meta_text)
        if not dnr:
            logger.warning("Kunde inte läsa diarienummer från sökresultat: %s", meta_text)
            continue

        beslutsdatum = extract_beslutsdatum(meta_text)
        listing = DecisionListing(
            dnr=dnr,
            titel=normalize_whitespace(link.get_text(" ", strip=True)),
            beslutsdatum=beslutsdatum,
            kategori=category_name,
            source_url=urljoin(BASE_URL, str(link.get("href"))),
        )
        listings.append(listing)

    if not listings:
        raise PauseExecution(
            f"Kategorin {category_name} gav ingen beslutshitt. Discovery kräver annan metod."
        )
    return listings


def extract_next_page_value(soup: BeautifulSoup) -> int | None:
    node = soup.find("input", attrs={"name": "page"})
    if not isinstance(node, Tag):
        return None
    raw_value = normalize_whitespace(str(node.get("value") or ""))
    try:
        return int(raw_value)
    except ValueError:
        return None


def parse_title(soup: BeautifulSoup, main: Tag | None, fallback: str) -> str:
    for selector in ("h1", "main h1", "article h1", "main h2", "article h2", "h2"):
        node = soup.select_one(selector)
        if isinstance(node, Tag):
            title = normalize_whitespace(node.get_text(" ", strip=True))
            if title:
                return title
    if isinstance(main, Tag):
        title_candidate = main.find(["h1", "h2"])
        if isinstance(title_candidate, Tag):
            title = normalize_whitespace(title_candidate.get_text(" ", strip=True))
            if title:
                return title
    return normalize_whitespace(fallback)


def build_catalog_entry(payload: dict[str, Any], file_path: Path) -> dict[str, Any]:
    try:
        rendered_path = str(file_path.relative_to(PROJECT_ROOT))
    except ValueError:
        rendered_path = str(file_path)
    return {
        "dok_id": payload["dok_id"],
        "dnr": payload["dnr"],
        "titel": payload["titel"],
        "beslutsdatum": payload["beslutsdatum"],
        "kategori": payload["kategori"],
        "authority_level": payload["authority_level"],
        "source_url": payload["source_url"],
        "file_path": rendered_path,
        "fetched_at": payload["fetched_at"],
    }


def fetch_decision_payload(
    session: RateLimitedSession,
    listing: DecisionListing,
) -> dict[str, Any] | None:
    try:
        response = session.get(listing.source_url)
    except requests.Timeout:
        logger.error("Timeout när beslutet hämtades: %s", listing.source_url)
        return None
    except requests.RequestException as exc:
        logger.error("HTTP-fel när beslutet hämtades %s: %s", listing.source_url, exc)
        return None

    if response.status_code == 404:
        logger.error("404 för beslut %s", listing.source_url)
        return None

    try:
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Kunde inte hämta beslut %s: %s", listing.source_url, exc)
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    main = soup.find("main") or soup.find("article")
    if not isinstance(main, Tag):
        logger.error("Saknar <main>/<article> i beslut %s", listing.source_url)
        return None

    text_content = main.get_text("\n", strip=True)
    dnr = extract_dnr(text_content) or listing.dnr
    beslutsdatum = extract_beslutsdatum(text_content) or listing.beslutsdatum
    titel = parse_title(soup, main, listing.titel)

    if len(text_content) < MIN_TEXT_LENGTH:
        logger.warning(
            "Beslutet %s ser ut att sakna fulltext (%d tecken). Skippas.",
            listing.source_url,
            len(text_content),
        )
        return None

    fetched_at = datetime.now(timezone.utc).isoformat()
    return {
        "dok_id": f"jk_{normalize_dnr(dnr)}",
        "dnr": dnr,
        "titel": titel,
        "beslutsdatum": beslutsdatum,
        "kategori": listing.kategori,
        "authority_level": listing.authority_level,
        "source_url": listing.source_url,
        "html_content": response.text,
        "text_content": text_content,
        "myndighet": "JK",
        "source_type": "myndighetsbeslut",
        "document_subtype": "jk",
        "fetched_at": fetched_at,
        "schema_version": SCHEMA_VERSION,
        "license": LICENSE,
    }


def load_existing_catalog_entry(file_path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.error("Kunde inte läsa befintlig JK-JSON %s: %s", file_path.name, exc)
        return None
    if not isinstance(payload, dict):
        logger.error("Befintlig JK-JSON %s har ogiltig form.", file_path.name)
        return None
    return build_catalog_entry(payload, file_path)


def run(
    *,
    max_docs: int | None = None,
    kategori: str | None = None,
    verbose: bool = False,
) -> dict[str, int]:
    setup_logging(verbose)
    ensure_output_dirs()

    session = RateLimitedSession()
    available_categories = extract_category_specs(session)

    if kategori:
        selected = [spec for spec in available_categories if spec.name == kategori]
        if not selected:
            raise PauseExecution(f"Okänd kategori: {kategori}")
    else:
        selected = available_categories

    cursors = deque(JKCategoryCursor(session, spec) for spec in selected)
    catalog = load_catalog()

    stats = {
        "processed": 0,
        "saved": 0,
        "skipped_existing": 0,
        "failed": 0,
    }

    while cursors and (max_docs is None or stats["processed"] < max_docs):
        cursor = cursors.popleft()
        listing = cursor.next_listing()
        if listing is None:
            continue

        stats["processed"] += 1
        file_path = listing.file_path

        if file_path.exists():
            stats["skipped_existing"] += 1
            existing_entry = load_existing_catalog_entry(file_path)
            if existing_entry is not None:
                catalog[listing.source_url] = existing_entry
            logger.info("[SKIP] %s finns redan", file_path.name)
        else:
            payload = fetch_decision_payload(session, listing)
            if payload is None:
                stats["failed"] += 1
            else:
                file_path.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                catalog[listing.source_url] = build_catalog_entry(payload, file_path)
                stats["saved"] += 1
                logger.info("[OK] %s", file_path.name)

        if stats["processed"] % PROGRESS_EVERY == 0:
            logger.info(
                "Framsteg: %d behandlade, %d sparade, %d skip, %d fel",
                stats["processed"],
                stats["saved"],
                stats["skipped_existing"],
                stats["failed"],
            )

        if not cursor.exhausted:
            cursors.append(cursor)

    save_catalog(catalog)
    return stats


def print_summary(stats: dict[str, int]) -> None:
    print("JK-fetch klar.")
    print(f"  Behandlade:     {stats['processed']}")
    print(f"  Sparade:        {stats['saved']}")
    print(f"  Skip befintlig: {stats['skipped_existing']}")
    print(f"  Failed:         {stats['failed']}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Hämta JK-beslut till rå-JSON.")
    parser.add_argument("--max-docs", type=int, default=None)
    parser.add_argument("--kategori", default=None, choices=list(KATEGORI_AUTHORITY))
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    try:
        stats = run(max_docs=args.max_docs, kategori=args.kategori, verbose=args.verbose)
    except PauseExecution as exc:
        logger.error(str(exc))
        return 1

    print_summary(stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
