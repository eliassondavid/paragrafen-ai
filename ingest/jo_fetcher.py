"""Discovery and PDF download for JO decisions."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import re
import time
from typing import Any

import requests
from playwright.sync_api import Browser, Error as PlaywrightError, Page, sync_playwright

try:
    import certifi
except ImportError:  # pragma: no cover - optional dependency
    certifi = None


logger = logging.getLogger("jo_fetcher")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data/raw/jo"
PDF_DIR = RAW_DIR / "pdf"
METADATA_DIR = RAW_DIR / "metadata"
CATALOG_PATH = RAW_DIR / "catalog.json"

SEARCH_URL_TEMPLATE = (
    "https://www.jo.se/jo-beslut/sokresultat/"
    "?query=&datefrom=2000-01-01&dateto=2025-12-31&page={page}"
)
SEARCH_SOURCE_URL = "https://www.jo.se/jo-beslut/sokresultat/"
DATE_FROM = "2000-01-01"
DATE_TO = "2025-12-31"

KNOWN_TOTAL_DECISIONS = 3689
HITS_PER_PAGE = 10
PAGE_DELAY_SECONDS = 2.0
PDF_DELAY_SECONDS = 1.0
PAGE_TIMEOUT_MS = 60_000
REQUEST_TIMEOUT_SECONDS = 60
MAX_DOWNLOAD_RETRIES = 3
MIN_PDF_BYTES = 100

DNR_RE = re.compile(r"\b\d{2,5}[-/]\d{4}\b")
DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
PDF_URL_RE = re.compile(
    r"^https://www\.jo\.se/app/uploads/resolve_pdfs/\d+_[^/]+\.pdf$"
)
TOTAL_HITS_RE = re.compile(r"Visar:\s*([\d\s]+)\s*träffar", re.IGNORECASE)

HEADERS = {
    "User-Agent": "paragrafen-ai-jo-fetcher/0.1 (+https://paragrafen.ai)",
    "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
}


class PauseExecution(RuntimeError):
    """Raised when the fetcher should stop and report back."""


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def ensure_output_dirs() -> None:
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_DIR.mkdir(parents=True, exist_ok=True)


def normalize_dnr(value: str) -> str:
    return value.strip().replace("/", "-")


def extract_dnr_from_filename(filename: str) -> tuple[str | None, list[str]]:
    matches = re.findall(r"(\d{2,5}-\d{4})(?!\d)", filename)
    if not matches:
        return None, []
    return matches[0], matches[1:]


def parse_total_hits(text: str) -> int | None:
    match = TOTAL_HITS_RE.search(text or "")
    if not match:
        return None
    try:
        return int(match.group(1).replace(" ", ""))
    except ValueError:
        return None


def catalog_key_for_decision(decision: dict[str, Any]) -> str:
    dnr = str(decision.get("dnr") or "").strip()
    if dnr:
        return dnr
    pdf_filename = str(decision.get("pdf_filename") or "").strip()
    if pdf_filename:
        return pdf_filename
    return str(decision.get("pdf_url") or "").strip()


def load_catalog() -> dict[str, dict[str, Any]]:
    if not CATALOG_PATH.exists():
        return {}

    try:
        payload = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise PauseExecution(f"Kunde inte läsa katalogfilen {CATALOG_PATH}: {exc}") from exc

    if not isinstance(payload, list):
        raise PauseExecution("catalog.json måste innehålla en lista med beslut.")

    catalog: dict[str, dict[str, Any]] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        dnr = str(item.get("dnr") or "").strip()
        if not dnr:
            continue
        catalog[dnr] = dict(item)
    return catalog


def save_catalog(catalog: dict[str, dict[str, Any]]) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    payload = list(catalog.values())
    CATALOG_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def save_metadata(decision: dict[str, Any]) -> None:
    path = METADATA_DIR / f"jo_{decision['dnr']}.json"
    path.write_text(
        json.dumps(decision, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def validate_decision_payload(decision: dict[str, str]) -> dict[str, Any]:
    dnr = normalize_dnr(decision.get("dnr", ""))
    if not DNR_RE.fullmatch(dnr):
        logger.warning("DOM-metadata innehåller oväntat dnr-format: %r", decision)

    beslutdatum = decision.get("beslutsdatum", "").strip()
    if not DATE_RE.fullmatch(beslutdatum):
        raise PauseExecution(f"Ogiltigt beslutsdatum för {dnr}: {beslutdatum!r}")

    titel = decision.get("titel", "").strip()
    if not titel:
        raise PauseExecution(f"Saknar titel för {dnr}.")

    pdf_url = decision.get("pdf_url", "").strip()
    payload: dict[str, Any] = {
        "dnr": dnr,
        "titel": titel,
        "beslutsdatum": beslutdatum,
        "pdf_url": pdf_url,
        "pdf_filename": f"jo_{dnr}.pdf",
        "source_url": SEARCH_SOURCE_URL,
        "myndighet": "JO",
        "document_type": "beslut",
    }
    if not pdf_url:
        logger.warning("Beslut %s saknar PDF-URL, skippas", dnr)
        payload["pdf_missing"] = True
        return payload

    if not PDF_URL_RE.fullmatch(pdf_url):
        raise PauseExecution(
            "PDF-URL-mönstret hittades inte i renderad HTML "
            f"eller var oväntat för {dnr}: {pdf_url!r}"
        )

    pdf_filename = pdf_url.rsplit("/", 1)[-1]
    primary_dnr, secondary_dnrs = extract_dnr_from_filename(pdf_filename)
    if primary_dnr is None:
        logger.warning(
            "PDF-filnamnet saknar läsbart dnr; använder DOM-metadata för %r (%r).",
            dnr,
            pdf_filename,
        )
    elif primary_dnr != dnr:
        logger.warning(
            "PDF-filnamnets primära dnr skiljer sig från DOM-metadata (%r vs %r i %r). "
            "Använder DOM-metadata.",
            primary_dnr,
            dnr,
            pdf_filename,
        )

    if secondary_dnrs:
        payload["dnr_secondary"] = secondary_dnrs
        logger.info("Sekundära dnr i %s: %s", pdf_filename, ", ".join(secondary_dnrs))
    return payload


class JOSearchSession:
    """Owns one Playwright page and paginates through the rendered search UI."""

    def __init__(self, browser: Browser) -> None:
        self.page: Page = browser.new_page()
        self.current_page = 0
        self.total_hits = 0
        self.total_pages = 0

    def close(self) -> None:
        self.page.close()

    def initialize(self) -> None:
        try:
            # JO renders the result list client-side. The query parameters in the URL are
            # currently not enough to apply the date filter, so we set the inputs via UI.
            self.page.goto(
                SEARCH_URL_TEMPLATE.format(page=1),
                wait_until="networkidle",
                timeout=PAGE_TIMEOUT_MS,
            )
            with self._expect_search_result(page_number=1) as response_info:
                self.page.locator('input[name="filter_search_date_from"]').fill(DATE_FROM)
                self.page.locator('input[name="filter_search_date_to"]').fill(DATE_TO)
                self.page.locator("button.button--blue").click(force=True)
            expected_total_hits, expected_first_article_id = self._parse_search_response(
                response_info.value
            )
            self._wait_for_page_dom(
                target_page=1,
                expected_total_hits=expected_total_hits,
                expected_first_article_id=expected_first_article_id,
            )
        except PlaywrightError as exc:
            raise PauseExecution(f"Playwright kraschade vid initiering av JO-sökningen: {exc}") from exc

        status_text = self.page.locator('span.label-span[role="status"]').inner_text().strip()
        total_hits = parse_total_hits(status_text)
        if total_hits is None:
            raise PauseExecution(f"Kunde inte läsa ut totalt antal träffar från {status_text!r}.")

        self.total_hits = total_hits
        self.total_pages = max((total_hits + HITS_PER_PAGE - 1) // HITS_PER_PAGE, 1)
        self.current_page = 1

    def fetch_page(self, page_number: int) -> list[dict[str, Any]]:
        if self.current_page == 0:
            self.initialize()

        if page_number < 1:
            raise ValueError("Page number must be >= 1.")

        if page_number < self.current_page:
            raise PauseExecution(
                f"JO-fetcher stöder bara framåtpaginering i denna körning "
                f"(begärd sida {page_number}, aktuell sida {self.current_page})."
            )

        while self.current_page < page_number:
            self._go_to_next_page()

        decisions = self._extract_current_page()
        if not decisions:
            raise PauseExecution(f"Inga beslut extraherades från JO-sida {page_number}.")
        return decisions

    def _go_to_next_page(self) -> None:
        target_page = self.current_page + 1
        if target_page > self.total_pages:
            return

        try:
            with self._expect_search_result(page_number=target_page) as response_info:
                self.page.locator("#go-to-pagination button.pagination__next").click(force=True)
            expected_total_hits, expected_first_article_id = self._parse_search_response(
                response_info.value
            )
            self._wait_for_page_dom(
                target_page=target_page,
                expected_total_hits=expected_total_hits,
                expected_first_article_id=expected_first_article_id,
            )
            self.current_page = target_page
        except PlaywrightError as exc:
            raise PauseExecution(
                f"Playwright kraschade vid paginering till JO-sida {target_page}: {exc}"
            ) from exc

    def _expect_search_result(self, page_number: int):
        def matcher(response: Any) -> bool:
            if "admin-ajax.php" not in response.url or response.request.method != "POST":
                return False
            post_data = response.request.post_data or ""
            return (
                "action=get_jo_search_result" in post_data
                and f"page={page_number}" in post_data
                and f"date_from={DATE_FROM}" in post_data
                and f"date_to={DATE_TO}" in post_data
            )

        return self.page.expect_response(matcher, timeout=PAGE_TIMEOUT_MS)

    def _parse_search_response(self, response: Any) -> tuple[int, str]:
        try:
            payload = response.json()
        except Exception as exc:  # pragma: no cover - defensive against Playwright internals
            raise PauseExecution(f"Kunde inte läsa JO:s sökrespons som JSON: {exc}") from exc

        if not isinstance(payload, dict):
            raise PauseExecution("JO:s sökrespons var inte ett JSON-objekt.")

        raw_total_hits = payload.get("total_hits")
        try:
            total_hits = int(raw_total_hits)
        except (TypeError, ValueError) as exc:
            raise PauseExecution(f"Kunde inte tolka total_hits från JO-responsen: {raw_total_hits!r}") from exc

        search_hits = payload.get("search_hits")
        if not isinstance(search_hits, list) or not search_hits:
            raise PauseExecution("JO-responsen saknade search_hits.")

        first_hit = search_hits[0] if isinstance(search_hits[0], dict) else {}
        first_article_id = str(first_hit.get("id") or "").strip()
        if not first_article_id:
            raise PauseExecution("JO-responsen saknade första träffens id.")

        return total_hits, first_article_id

    def _wait_for_page_dom(
        self,
        *,
        target_page: int,
        expected_total_hits: int,
        expected_first_article_id: str,
    ) -> None:
        self.page.wait_for_function(
            """({ targetPage, expectedTotalHits, expectedFirstArticleId }) => {
                const active = document.querySelector(
                    "#go-to-pagination button.pagination__page--active"
                );
                const status = document.querySelector('span.label-span[role="status"]');
                const firstArticle = document.querySelector("article");
                const statusMatch = status && status.textContent
                    ? status.textContent.match(/Visar:\\s*(\\d+)\\s*träffar/i)
                    : null;
                return Boolean(
                    active &&
                    active.textContent &&
                    active.textContent.trim() === String(targetPage) &&
                    statusMatch &&
                    Number(statusMatch[1]) === Number(expectedTotalHits) &&
                    firstArticle &&
                    firstArticle.id === String(expectedFirstArticleId)
                );
            }""",
            arg={
                "targetPage": target_page,
                "expectedTotalHits": expected_total_hits,
                "expectedFirstArticleId": expected_first_article_id,
            },
            timeout=PAGE_TIMEOUT_MS,
        )

    def _extract_current_page(self) -> list[dict[str, Any]]:
        articles = self.page.locator("article")
        article_count = articles.count()
        if article_count == 0:
            raise PauseExecution(
                f"Inga artiklar hittades i renderad HTML på JO-sida {self.current_page}."
            )

        pdf_link_count = self.page.locator('article a[href*="resolve_pdfs"]').count()
        if pdf_link_count == 0:
            raise PauseExecution(
                f"PDF-URL-mönstret hittades inte i renderad HTML på JO-sida {self.current_page}."
            )

        raw_results = articles.evaluate_all(
            """(nodes, sourceUrl) => nodes.map((article) => {
                const dateNode = article.querySelector(".entry-meta span:nth-child(1) span");
                const dnrNode = article.querySelector(".entry-meta span:nth-child(2) span");
                const titleNode = article.querySelector("h2");
                const pdfLink = Array.from(article.querySelectorAll("a")).find(
                    (link) => /\\/resolve_pdfs\\//.test(link.href)
                );
                return {
                    dnr: dnrNode ? dnrNode.textContent.trim() : "",
                    titel: titleNode ? titleNode.textContent.trim() : "",
                    beslutsdatum: dateNode ? dateNode.textContent.trim() : "",
                    pdf_url: pdfLink ? pdfLink.href.trim() : "",
                    source_url: sourceUrl,
                };
            })""",
            SEARCH_SOURCE_URL,
        )

        decisions = [
            validate_decision_payload(item)
            for item in raw_results
        ]
        if len(decisions) != article_count:
            raise PauseExecution(
                f"Mismatch i extraktion på JO-sida {self.current_page}: "
                f"{len(decisions)} beslut av {article_count} artiklar."
            )
        return decisions


def build_requests_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    if certifi is not None:
        session.verify = certifi.where()
    return session


def download_pdf(
    session: requests.Session,
    *,
    decision: dict[str, Any],
    destination: Path,
) -> tuple[str, int]:
    """Return (status, bytes_written). status in {downloaded, failed, empty}."""

    temp_path = destination.with_suffix(".pdf.part")

    for attempt in range(1, MAX_DOWNLOAD_RETRIES + 1):
        try:
            with session.get(
                decision["pdf_url"],
                timeout=REQUEST_TIMEOUT_SECONDS,
                stream=True,
            ) as response:
                response.raise_for_status()
                destination.parent.mkdir(parents=True, exist_ok=True)
                bytes_written = 0
                with temp_path.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=64 * 1024):
                        if not chunk:
                            continue
                        handle.write(chunk)
                        bytes_written += len(chunk)

            if bytes_written == 0:
                temp_path.unlink(missing_ok=True)
                return "empty", 0

            if bytes_written < MIN_PDF_BYTES:
                temp_path.unlink(missing_ok=True)
                logger.warning(
                    "PDF för %s blev ovanligt liten (%d bytes) och räknas som tom.",
                    decision["dnr"],
                    bytes_written,
                )
                return "empty", bytes_written

            with temp_path.open("rb") as handle:
                header = handle.read(4)
            if header != b"%PDF":
                temp_path.unlink(missing_ok=True)
                logger.warning(
                    "Nedladdningen för %s såg inte ut som en PDF (header=%r).",
                    decision["dnr"],
                    header,
                )
                return "failed", bytes_written

            temp_path.replace(destination)
            return "downloaded", bytes_written
        except requests.RequestException as exc:
            temp_path.unlink(missing_ok=True)
            logger.warning(
                "PDF-nedladdning misslyckades för %s (%s), försök %d/%d.",
                decision["dnr"],
                exc,
                attempt,
                MAX_DOWNLOAD_RETRIES,
            )
            if attempt < MAX_DOWNLOAD_RETRIES:
                time.sleep(attempt)

    return "failed", 0


def maybe_raise_on_empty_pdf_ratio(empty_pdfs: int, attempted_downloads: int) -> None:
    if attempted_downloads < 20:
        return
    if attempted_downloads == 0:
        return
    ratio = empty_pdfs / attempted_downloads
    if ratio > 0.20:
        raise PauseExecution(
            "Mer än 20% av PDF-nedladdningarna gav tom fil "
            f"({empty_pdfs}/{attempted_downloads}, {ratio:.1%})."
        )


def discover_decisions(
    *,
    max_pages: int | None,
    max_docs: int | None,
    catalog: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], int, int]:
    selection: list[dict[str, Any]] = []
    selected_dnrs: set[str] = set()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        search_session = JOSearchSession(browser)
        try:
            search_session.initialize()
            live_total_pages = search_session.total_pages
            live_total_hits = search_session.total_hits
            target_pages = live_total_pages if max_pages is None else min(max_pages, live_total_pages)

            logger.info(
                "JO-search initierad: %d träffar över %d sidor för intervallet %s till %s.",
                live_total_hits,
                live_total_pages,
                DATE_FROM,
                DATE_TO,
            )

            for page_number in range(1, target_pages + 1):
                if page_number == 1 or page_number % 10 == 0:
                    logger.info(
                        "Discovery sida %d/%d | katalog=%d | urval=%d",
                        page_number,
                        target_pages,
                        len(catalog),
                        len(selection),
                    )

                decisions = search_session.fetch_page(page_number)
                for decision in decisions:
                    catalog[catalog_key_for_decision(decision)] = decision
                    save_metadata(decision)

                    if decision["dnr"] not in selected_dnrs:
                        selected_dnrs.add(decision["dnr"])
                        selection.append(decision)
                        if max_docs is not None and len(selection) >= max_docs:
                            break

                save_catalog(catalog)

                if max_docs is not None and len(selection) >= max_docs:
                    logger.info("max_docs=%d nådd efter discovery.", max_docs)
                    break

                if page_number < target_pages:
                    time.sleep(PAGE_DELAY_SECONDS)
        finally:
            search_session.close()
            browser.close()

    return selection, search_session.total_hits, search_session.total_pages


def download_decisions(decisions: list[dict[str, Any]]) -> dict[str, int]:
    stats = {
        "downloaded": 0,
        "skipped": 0,
        "failed": 0,
        "empty": 0,
        "attempted": 0,
    }

    if not decisions:
        return stats

    with build_requests_session() as session:
        for index, decision in enumerate(decisions, start=1):
            destination = PDF_DIR / decision["pdf_filename"]
            save_metadata(decision)

            if decision.get("pdf_missing"):
                stats["skipped"] += 1
                continue

            if destination.exists() and destination.stat().st_size >= MIN_PDF_BYTES:
                stats["skipped"] += 1
                continue

            result, _ = download_pdf(session, decision=decision, destination=destination)
            stats["attempted"] += 1

            if result == "downloaded":
                stats["downloaded"] += 1
            elif result == "empty":
                stats["empty"] += 1
                stats["failed"] += 1
            else:
                stats["failed"] += 1

            maybe_raise_on_empty_pdf_ratio(stats["empty"], stats["attempted"])

            if index < len(decisions):
                time.sleep(PDF_DELAY_SECONDS)

    return stats


def run(
    max_pages: int | None,
    max_docs: int | None,
    verbose: bool,
) -> int:
    setup_logging(verbose)
    ensure_output_dirs()

    catalog = load_catalog()
    logger.info("Befintlig JO-katalog: %d poster.", len(catalog))

    try:
        decisions, live_total_hits, live_total_pages = discover_decisions(
            max_pages=max_pages,
            max_docs=max_docs,
            catalog=catalog,
        )
        download_stats = download_decisions(decisions)
    except PauseExecution as exc:
        logger.error("PAUSAR JO-fetcher: %s", exc)
        return 1

    coverage_known_ratio = len(catalog) / KNOWN_TOTAL_DECISIONS if KNOWN_TOTAL_DECISIONS else 0.0
    coverage_live_ratio = len(catalog) / live_total_hits if live_total_hits else 0.0

    logger.info(
        "PDF-fas klar: nedladdade=%d, skippade=%d, fel=%d, tomma=%d, nätförsök=%d.",
        download_stats["downloaded"],
        download_stats["skipped"],
        download_stats["failed"],
        download_stats["empty"],
        download_stats["attempted"],
    )
    logger.info(
        "Coverage: katalog=%d | live_total=%d (%d sidor) | live_ratio=%.1f%% | "
        "spec_known≈%d | spec_ratio=%.1f%%",
        len(catalog),
        live_total_hits,
        live_total_pages,
        coverage_live_ratio * 100,
        KNOWN_TOTAL_DECISIONS,
        coverage_known_ratio * 100,
    )

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Skrapa JO-beslut via Playwright och ladda ned PDF:er via requests.",
    )
    parser.add_argument("--max-pages", type=int, default=None, help="Begränsa antal söksidor.")
    parser.add_argument("--max-docs", type=int, default=None, help="Begränsa antal beslut.")
    parser.add_argument("--verbose", action="store_true", help="Visa debug-loggning.")
    args = parser.parse_args()
    raise SystemExit(run(args.max_pages, args.max_docs, args.verbose))


if __name__ == "__main__":
    main()
