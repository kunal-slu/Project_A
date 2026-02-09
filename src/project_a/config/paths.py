"""Canonical path helpers for the local data lake."""

from __future__ import annotations

import os
from pathlib import Path


def _data_root() -> Path:
    root = os.environ.get("DATA_ROOT", "data")
    return Path(root).expanduser()


BRONZE = _data_root() / "bronze"
SILVER = _data_root() / "silver"
GOLD = _data_root() / "gold"


def get_paths() -> dict[str, Path]:
    """Return Bronze/Silver/Gold paths based on current environment."""
    root = _data_root()
    return {"bronze": root / "bronze", "silver": root / "silver", "gold": root / "gold"}
