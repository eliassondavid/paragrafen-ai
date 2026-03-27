"""Fetch-only adapter for riksdagens betankanden."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any

from pipelines.common.fetch_base import ForarbeteFetcher

BETECKNING_RE = re.compile(r"^(?P<rm>\d{4}/\d{2})\s*:\s*(?P<label>.+)$")


class BetFetcher(ForarbeteFetcher):
    def get_doktyp(self) -> str:
        return "bet"

    def get_output_dir(self) -> Path:
        return Path("data/raw/bet")

    def build_filename(self, document: dict[str, Any]) -> str | None:
        beteckning = str(document.get("beteckning") or "").strip()
        match = BETECKNING_RE.match(beteckning)
        if match:
            riksmote = match.group("rm").replace("/", "-")
            beteckning_norm = re.sub(r"[^0-9a-z]+", "", match.group("label").lower())
            if riksmote and beteckning_norm:
                return f"bet_{riksmote}_{beteckning_norm}"

        dok_id = str(document.get("dok_id") or document.get("id") or "").strip().lower()
        if dok_id:
            return f"bet_{dok_id}"
        return None

    def should_skip(self, document: dict[str, Any]) -> tuple[bool, str]:
        return False, ""
