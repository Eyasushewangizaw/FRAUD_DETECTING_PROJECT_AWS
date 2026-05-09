"""Ensure ``src/producer`` is importable when running pytest from the repo root."""

from __future__ import annotations

import sys
from pathlib import Path

_PRODROOT = Path(__file__).resolve().parents[1] / "src" / "producer"
if str(_PRODROOT) not in sys.path:
    sys.path.insert(0, str(_PRODROOT))
