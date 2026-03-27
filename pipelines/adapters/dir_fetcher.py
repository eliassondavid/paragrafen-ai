"""Fetch-only adapter for kommittedirektiv."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any

from pipelines.common.fetch_base import ForarbeteFetcher

YEAR_RE = re.compile(r"^(?P<year>\d{4})")


class DirFetcher(ForarbeteFetcher):
    def get_doktyp(self) -> str:
        return "dir"

    def get_output_dir(self) -> Path:
        return Path("data/raw/dir")

    def build_filename(self, document: dict[str, Any]) -> str | None:
        datum = str(document.get("datum") or "").strip()
        nummer = str(document.get("nummer") or "").strip()
        year_match = YEAR_RE.match(datum)
        if year_match and nummer:
            return f"dir_{year_match.group('year')}_{nummer}"

        dok_id = str(document.get("dok_id") or document.get("id") or "").strip().lower()
        if dok_id:
            return f"dir_{dok_id}"
        return None

    def should_skip(self, document: dict[str, Any]) -> tuple[bool, str]:
        return False, ""
