"""Convert raw ARN PDF/DOC/WPD files into JSON documents."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import re
import subprocess
import sys
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logger = logging.getLogger("paragrafenai.noop")

PDFTOTEXT_BIN = "/opt/homebrew/bin/pdftotext"
SOFFICE_BIN = "/opt/homebrew/bin/soffice"
INPUT_DIR = "data/raw/arn"
OUTPUT_DIR = "data/raw/arn/json"
ERROR_LOG = "data/raw/arn/json/_conversion_errors.jsonl"
SCHEMA_VERSION = "v0.15"
MIN_TEXT_LENGTH = 50
SUPPORTED_FORMATS = {".pdf", ".doc", ".wpd"}
SKIP_REASONS = (
    "already_exists",
    "no_dnr",
    "empty_text",
    "libreoffice_timeout",
    "conversion_error",
    "read_error",
)


@dataclass
class ConversionResult:
    """Outcome for one source file."""

    status: str
    reason: str | None = None
    dok_id: str | None = None


def utc_now_iso() -> str:
    """Return a UTC timestamp without fractional seconds."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def extract_dnr(filename: str) -> str | None:
    """Extract ARN case number from a filename."""
    match = re.search(r"(\d{4})-(\d{4,6})", filename)
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    return None


def dnr_to_dok_id(dnr: str) -> str:
    """Build the ARN document id from dnr."""
    return f"arn_{dnr.replace('-', '_')}"


def build_document_payload(
    *,
    dnr: str,
    source_file: str,
    source_format: str,
    text_content: str,
    extraction_method: str,
    fetched_at: str | None = None,
) -> dict[str, Any]:
    """Build the required ARN JSON document."""
    cleaned_text = text_content.strip()
    dok_id = dnr_to_dok_id(dnr)
    return {
        "dok_id": dok_id,
        "dnr": dnr,
        "source_type": "namnder",
        "document_subtype": "arn",
        "authority_level": "persuasive",
        "title": f"Ärendereferat {dnr}",
        "source_file": source_file,
        "source_format": source_format,
        "text_content": cleaned_text,
        "text_length": len(cleaned_text),
        "extraction_method": extraction_method,
        "extraction_quality": 1.0,
        "fetched_at": fetched_at or utc_now_iso(),
        "schema_version": SCHEMA_VERSION,
        "license": "public_domain",
    }


def collect_source_files(input_dir: Path) -> list[Path]:
    """Collect supported ARN source files."""
    return sorted(
        path
        for path in input_dir.iterdir()
        if path.is_file() and path.suffix.lower() in SUPPORTED_FORMATS
    )


def read_text_file(path: Path) -> str:
    """Read text from disk with a forgiving UTF-8 decoder."""
    return path.read_bytes().decode("utf-8", errors="replace")


class ArnConverter:
    """Convert one ARN source corpus into JSON files."""

    def __init__(
        self,
        *,
        input_dir: str | Path = INPUT_DIR,
        output_dir: str | Path = OUTPUT_DIR,
        error_log_path: str | Path = ERROR_LOG,
    ) -> None:
        self.repo_root = Path(__file__).resolve().parent.parent
        self.input_dir = self._resolve_path(input_dir)
        self.output_dir = self._resolve_path(output_dir)
        self.error_log_path = self._resolve_path(error_log_path)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.error_log_path.parent.mkdir(parents=True, exist_ok=True)

    def run(
        self,
        *,
        max_docs: int | None = None,
        verbose: bool = False,
    ) -> dict[str, Any]:
        """Process the configured corpus sequentially."""
        source_files = collect_source_files(self.input_dir)
        if max_docs is not None:
            source_files = source_files[:max_docs]

        stats: dict[str, Any] = {
            "processed": len(source_files),
            "ok": 0,
            "skipped": 0,
            "reasons": {reason: 0 for reason in SKIP_REASONS},
        }

        for source_path in source_files:
            result = self.process_file(source_path)
            if result.status == "ok":
                stats["ok"] += 1
                if verbose:
                    print(f"[OK]   {source_path.name} -> {result.dok_id}")
                continue

            stats["skipped"] += 1
            if result.reason in stats["reasons"]:
                stats["reasons"][result.reason] += 1
            if verbose:
                print(f"[SKIP] {source_path.name} -> {result.reason}")

        return stats

    def process_file(self, source_path: Path) -> ConversionResult:
        """Convert one source file into a JSON record."""
        dnr = extract_dnr(source_path.name)
        if not dnr:
            self.log_error(source_file=source_path.name, reason="no_dnr")
            return ConversionResult(status="skipped", reason="no_dnr")

        dok_id = dnr_to_dok_id(dnr)
        output_path = self.output_dir / f"{dok_id}.json"
        if output_path.exists():
            self.log_error(source_file=source_path.name, reason="already_exists")
            return ConversionResult(status="skipped", reason="already_exists", dok_id=dok_id)

        source_format = source_path.suffix.lower().lstrip(".")
        extraction_method = "pdftotext" if source_format == "pdf" else "libreoffice"

        try:
            raw_text = self.extract_text(source_path)
        except ConversionSkip as exc:
            self.log_error(source_file=source_path.name, reason=exc.reason)
            return ConversionResult(status="skipped", reason=exc.reason, dok_id=dok_id)

        cleaned_text = raw_text.strip()
        if len(cleaned_text) < MIN_TEXT_LENGTH:
            self.log_error(source_file=source_path.name, reason="empty_text")
            return ConversionResult(status="skipped", reason="empty_text", dok_id=dok_id)

        payload = build_document_payload(
            dnr=dnr,
            source_file=source_path.name,
            source_format=source_format,
            text_content=cleaned_text,
            extraction_method=extraction_method,
        )
        with output_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        return ConversionResult(status="ok", dok_id=dok_id)

    def extract_text(self, source_path: Path) -> str:
        """Extract text based on source format."""
        suffix = source_path.suffix.lower()
        if suffix == ".pdf":
            return self.extract_pdf_text(source_path)
        if suffix in {".doc", ".wpd"}:
            return self.extract_libreoffice_text(source_path)
        raise ConversionSkip("conversion_error")

    def extract_pdf_text(self, source_path: Path) -> str:
        """Extract text from PDF using pdftotext."""
        completed = subprocess.run(
            [PDFTOTEXT_BIN, "-layout", str(source_path), "-"],
            check=False,
            capture_output=True,
        )
        if completed.returncode != 0:
            raise ConversionSkip("conversion_error")
        return completed.stdout.decode("utf-8", errors="replace")

    def extract_libreoffice_text(self, source_path: Path) -> str:
        """Extract DOC/WPD text through a sequential soffice invocation."""
        tmp_txt = Path("/tmp") / f"{source_path.stem}.txt"
        try:
            if tmp_txt.exists():
                tmp_txt.unlink()

            process = subprocess.Popen(
                [
                    SOFFICE_BIN,
                    "--headless",
                    "--convert-to",
                    "txt",
                    "--outdir",
                    "/tmp",
                    str(source_path),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            try:
                process.communicate(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()
                raise ConversionSkip("libreoffice_timeout") from None

            if process.returncode != 0:
                raise ConversionSkip("conversion_error")

            try:
                return read_text_file(tmp_txt)
            except OSError as exc:
                raise ConversionSkip("read_error") from exc
        finally:
            try:
                if tmp_txt.exists():
                    tmp_txt.unlink()
            except OSError:
                logger.warning("Kunde inte radera temporär txt-fil: %s", tmp_txt)

    def log_error(self, *, source_file: str, reason: str) -> None:
        """Append one JSONL error record."""
        payload = {
            "source_file": source_file,
            "reason": reason,
            "timestamp": utc_now_iso(),
        }
        with self.error_log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False))
            fh.write("\n")

    def _resolve_path(self, path_value: str | Path) -> Path:
        path = Path(path_value)
        if path.is_absolute():
            return path
        return self.repo_root / path


class ConversionSkip(Exception):
    """Raised when one source file should be skipped."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def print_summary(stats: dict[str, Any], error_log_path: Path) -> None:
    """Print the required end-of-run summary."""
    print("ARN-konvertering klar.")
    print(f"  Processerade: {stats['processed']}")
    print(f"  OK:           {stats['ok']}")
    print(f"  Skippade:     {stats['skipped']}")
    print(f"    - already_exists: {stats['reasons']['already_exists']}")
    print(f"    - no_dnr:         {stats['reasons']['no_dnr']}")
    print(f"    - empty_text:     {stats['reasons']['empty_text']}")
    print(f"    - libreoffice_timeout: {stats['reasons']['libreoffice_timeout']}")
    print(f"    - conversion_error: {stats['reasons']['conversion_error']}")
    print(f"    - read_error:     {stats['reasons']['read_error']}")
    print(f"  Fel loggade till: {ERROR_LOG}")


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Konvertera ARN-beslut till JSON.")
    parser.add_argument("--max-docs", type=int, default=None)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s:%(name)s:%(message)s",
    )

    converter = ArnConverter()
    stats = converter.run(max_docs=args.max_docs, verbose=args.verbose)
    print_summary(stats, converter.error_log_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
