#!/usr/bin/env python3
"""
Backfill helper for running jobs over a date range.

This uses run-date overrides and writes a success sentinel file so reruns are idempotent.
"""

from __future__ import annotations

import argparse
import datetime as dt
import subprocess
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill jobs over a date range")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--jobs", required=True, help="Comma-separated job names")
    parser.add_argument("--config", default="local/config/local.yaml")
    parser.add_argument("--env", default="local")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def daterange(start: dt.date, end: dt.date):
    current = start
    while current <= end:
        yield current
        current += dt.timedelta(days=1)


def main() -> None:
    args = parse_args()
    start = dt.date.fromisoformat(args.start_date)
    end = dt.date.fromisoformat(args.end_date)
    jobs = [job.strip() for job in args.jobs.split(",") if job.strip()]

    for day in daterange(start, end):
        run_date = day.isoformat()
        for job in jobs:
            sentinel = Path("artifacts/backfill") / job / f"{run_date}.done"
            sentinel.parent.mkdir(parents=True, exist_ok=True)

            if sentinel.exists() and not args.force:
                print(f"[SKIP] {job} {run_date} (already done)")
                continue

            cmd = [
                "python3",
                "-m",
                "project_a.pipeline.run_pipeline",
                "--job",
                job,
                "--env",
                args.env,
                "--config",
                args.config,
                "--run-date",
                run_date,
            ]
            print(f"[RUN] {' '.join(cmd)}")
            subprocess.check_call(cmd)
            sentinel.write_text("ok\n", encoding="utf-8")


if __name__ == "__main__":
    main()
