"""Fetch-only adapter for riksdagsskrivelser."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pipelines.common.fetch_base import ForarbeteFetcher


class RskrFetcher(ForarbeteFetcher):
    def get_doktyp(self) -> str:
        return "rskr"

    def get_output_dir(self) -> Path:
        return Path("data/raw/rskr")

    def build_filename(self, document: dict[str, Any]) -> str | None:
        riksmote = str(document.get("rm") or document.get("riksmote") or "").strip()
        nummer = str(document.get("nummer") or "").strip()
        if riksmote and nummer:
            return f"rskr_{riksmote.replace('/', '-')}_{nummer}"

        dok_id = str(document.get("dok_id") or document.get("id") or "").strip().lower()
        if dok_id:
            return f"rskr_{dok_id}"
        return None

    def should_skip(self, document: dict[str, Any]) -> tuple[bool, str]:
        return False, ""
