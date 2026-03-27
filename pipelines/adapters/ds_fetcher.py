"""Fetch-only adapter for departementsserien."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any

from pipelines.common.fetch_base import ForarbeteFetcher

DS_BETECKNING_RE = re.compile(r"(?i)\bds\s+(\d{4})\s*:\s*(\d+)\b")


class DsFetcher(ForarbeteFetcher):
    def get_doktyp(self) -> str:
        return "ds"

    def get_output_dir(self) -> Path:
        return Path("data/raw/ds")

    def build_filename(self, document: dict[str, Any]) -> str | None:
        beteckning = str(document.get("beteckning") or "").strip()
        match = DS_BETECKNING_RE.search(beteckning)
        if match:
            return f"ds_{match.group(1)}_{int(match.group(2))}"

        dok_id = str(document.get("dok_id") or document.get("id") or "").strip().lower()
        if dok_id:
            return f"ds_{dok_id}"
        return None

    def should_skip(self, document: dict[str, Any]) -> tuple[bool, str]:
        return False, ""
