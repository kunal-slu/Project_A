#!/usr/bin/env python3
"""
Retention cleanup for local partitions.

Deletes partition folders older than N days for bronze/silver/gold.
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import shutil
from pathlib import Path

from project_a.utils.config import load_config_resolved


PARTITION_RE = re.compile(r"(date|dt|day)=([0-9]{4}-[0-9]{2}-[0-9]{2})")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Retention cleanup")
    parser.add_argument("--config", default="local/config/local.yaml")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--paths", default="")
    return parser.parse_args()


def iter_partition_dirs(root: Path):
    for path in root.rglob("*"):
        if path.is_dir() and PARTITION_RE.search(str(path)):
            yield path


def main() -> None:
    args = parse_args()
    cfg = load_config_resolved(args.config)
    cutoff = dt.date.today() - dt.timedelta(days=args.days)

    paths = []
    if args.paths:
        paths = [Path(p.strip()) for p in args.paths.split(",") if p.strip()]
    else:
        paths_cfg = cfg.get("paths", {})
        for key in ("bronze_root", "silver_root", "gold_root", "bronze", "silver", "gold"):
            val = paths_cfg.get(key)
            if val and not val.startswith("s3"):
                paths.append(Path(val.replace("file://", "")))

    for root in paths:
        if not root.exists():
            continue
        for part in iter_partition_dirs(root):
            match = PARTITION_RE.search(str(part))
            if not match:
                continue
            date_str = match.group(2)
            try:
                part_date = dt.date.fromisoformat(date_str)
            except ValueError:
                continue
            if part_date < cutoff:
                shutil.rmtree(part, ignore_errors=True)
                print(f"Deleted {part}")


if __name__ == "__main__":
    main()
