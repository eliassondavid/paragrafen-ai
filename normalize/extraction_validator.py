"""Validering av PDF-extraktion mot API-fritext."""

from __future__ import annotations

import logging
import random
import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)
WORD_PATTERN = re.compile(r"\w+", flags=re.UNICODE)


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Läser YAML-konfiguration."""
    if config_path is None:
        config_path = Path("config/sou_api_config.yaml")
    config_path = Path(config_path)
    with config_path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _join_pdf_text(pages: list[dict[str, Any]]) -> str:
    return "\n".join(str(page.get("text", "")) for page in pages)


def _normalize_words(text: str) -> list[str]:
    return WORD_PATTERN.findall(text.lower())


def _sample_phrases(
    api_words: list[str],
    sample_count: int,
    phrase_word_count: int,
    rng: random.Random,
) -> list[str]:
    """Tar slumpmässiga 20-ordsfraser från API-fritext."""
    if len(api_words) < phrase_word_count:
        return []

    max_start = len(api_words) - phrase_word_count
    available_starts = list(range(max_start + 1))
    if not available_starts:
        return []

    chosen_starts = rng.sample(available_starts, k=min(sample_count, len(available_starts)))
    return [" ".join(api_words[start : start + phrase_word_count]) for start in chosen_starts]


def _best_phrase_similarity(
    phrase_words: list[str],
    pdf_words: list[str],
    phrase_word_count: int,
) -> float:
    """Beräknar högsta fuzzy-likhet mellan fras och PDF-fönster."""
    if not pdf_words or len(pdf_words) < phrase_word_count:
        return 0.0

    phrase_text = " ".join(phrase_words)
    best = 0.0

    # Stega i intervall för att hålla kostnaden rimlig för stora dokument.
    step = max(1, phrase_word_count // 2)
    for idx in range(0, len(pdf_words) - phrase_word_count + 1, step):
        candidate = " ".join(pdf_words[idx : idx + phrase_word_count])
        score = SequenceMatcher(None, phrase_text, candidate).ratio()
        if score > best:
            best = score
            if best >= 0.99:
                break
    return best


def validate_extraction(
    pages: list[dict[str, Any]],
    api_fritext: str,
    config: dict[str, Any] | None = None,
    random_seed: int = 42,
) -> dict[str, Any]:
    """Validerar extraherad PDF-text mot API-fritext för ett dokument."""
    cfg = config or load_config()
    validation_cfg = cfg.get("validation", {})

    tolerance = float(validation_cfg.get("length_tolerance", 0.5))
    sample_count = int(validation_cfg.get("random_samples", 10))
    phrase_word_count = int(validation_cfg.get("phrase_word_count", 20))
    fuzzy_threshold = float(validation_cfg.get("fuzzy_threshold", 0.8))
    min_pages_for_large_docs = int(validation_cfg.get("min_pages_for_large_docs", 10))
    large_doc_char_threshold = int(validation_cfg.get("large_doc_char_threshold", 100000))

    pdf_text = _join_pdf_text(pages)
    pdf_length = len(pdf_text)
    api_length = len(api_fritext)

    # Längd inom +/- 50%.
    min_allowed = int(api_length * (1 - tolerance))
    max_allowed = int(api_length * (1 + tolerance))
    length_ok = min_allowed <= pdf_length <= max_allowed if api_length > 0 else pdf_length == 0

    api_words = _normalize_words(api_fritext)
    pdf_words = _normalize_words(pdf_text)
    rng = random.Random(random_seed)
    sampled_phrases = _sample_phrases(api_words, sample_count=sample_count, phrase_word_count=phrase_word_count, rng=rng)

    phrase_checks: list[dict[str, Any]] = []
    phrase_passed = 0

    pdf_text_lower = pdf_text.lower()
    for phrase in sampled_phrases:
        if phrase in pdf_text_lower:
            similarity = 1.0
        else:
            similarity = _best_phrase_similarity(
                phrase_words=phrase.split(),
                pdf_words=pdf_words,
                phrase_word_count=phrase_word_count,
            )

        passed = similarity >= fuzzy_threshold
        if passed:
            phrase_passed += 1
        phrase_checks.append(
            {
                "phrase": phrase,
                "similarity": round(similarity, 4),
                "passed": passed,
            }
        )

    phrase_check_ok = phrase_passed == len(sampled_phrases)

    page_count = len(pages)
    page_count_ok = True
    if pdf_length > large_doc_char_threshold:
        page_count_ok = page_count > min_pages_for_large_docs

    validation_passed = length_ok and phrase_check_ok and page_count_ok

    report = {
        "validation_passed": validation_passed,
        "length_check": {
            "pdf_length": pdf_length,
            "api_length": api_length,
            "min_allowed": min_allowed,
            "max_allowed": max_allowed,
            "passed": length_ok,
        },
        "phrase_check": {
            "samples_requested": sample_count,
            "samples_tested": len(sampled_phrases),
            "threshold": fuzzy_threshold,
            "passed_samples": phrase_passed,
            "passed": phrase_check_ok,
            "details": phrase_checks,
        },
        "page_count_check": {
            "page_count": page_count,
            "large_doc_char_threshold": large_doc_char_threshold,
            "min_pages_for_large_docs": min_pages_for_large_docs,
            "passed": page_count_ok,
        },
    }

    logger.info("Validering klar: passed=%s", validation_passed)
    return report


def validate_batch(validation_reports: list[dict[str, Any]], config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Batch-validering: kräver minst 95% pass-rate."""
    cfg = config or load_config()
    validation_cfg = cfg.get("validation", {})
    required_pass_rate = float(validation_cfg.get("batch_pass_rate", 0.95))

    total = len(validation_reports)
    passed = sum(1 for report in validation_reports if report.get("validation_passed"))
    pass_rate = (passed / total) if total > 0 else 0.0

    result = {
        "total": total,
        "passed": passed,
        "pass_rate": round(pass_rate, 4),
        "required_pass_rate": required_pass_rate,
        "batch_passed": pass_rate >= required_pass_rate if total > 0 else False,
    }
    logger.info("Batch-validering: %s", result)
    return result


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
