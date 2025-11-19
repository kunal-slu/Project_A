"""
Simple watermark persistence for incremental loads.
Stores/retrieves ISO8601 timestamps in a lightweight JSON file under checkpoints.
"""
import os
import json
from datetime import datetime
from typing import Optional


def _path(root: str, table: str) -> str:
    os.makedirs(root, exist_ok=True)
    return os.path.join(root, f"{table}_watermark.json")


def load_watermark(root: str, table: str) -> Optional[str]:
    fp = _path(root, table)
    if not os.path.exists(fp):
        return None
    try:
        with open(fp, "r") as f:
            data = json.load(f)
        return data.get("last_modified_at")
    except Exception:
        return None


def save_watermark(root: str, table: str, iso_ts: str) -> None:
    fp = _path(root, table)
    with open(fp, "w") as f:
        json.dump({"last_modified_at": iso_ts, "saved_at": datetime.utcnow().isoformat()}, f)


