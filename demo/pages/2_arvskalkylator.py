"""Inbäddad multipage-vy för arvskalkylatorn."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from demo.arvskalkylator.app import render_app  # noqa: E402

render_app(standalone=False)
