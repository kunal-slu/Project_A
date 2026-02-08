#!/usr/bin/env python3
"""
Run the full Project A ETL pipeline locally.

Usage:
    python3 run_complete_etl.py --config local/config/local.yaml --env local
    python3 run_complete_etl.py --jobs fx_json_to_bronze,bronze_to_silver,silver_to_gold
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

DEFAULT_JOBS = [
    "fx_json_to_bronze",
    "snowflake_to_bronze",
    "crm_to_bronze",
    "redshift_to_bronze",
    "kafka_csv_to_bronze",
    "bronze_to_silver",
    "silver_to_gold",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Project A end-to-end ETL locally.")
    parser.add_argument(
        "--config",
        default="local/config/local.yaml",
        help="Path to config yaml (default: local/config/local.yaml)",
    )
    parser.add_argument(
        "--env",
        default="local",
        help="Environment name (default: local)",
    )
    parser.add_argument(
        "--jobs",
        default=",".join(DEFAULT_JOBS),
        help="Comma-separated job list to run",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue running remaining jobs if one fails",
    )
    parser.add_argument(
        "--with-validation",
        action="store_true",
        help="Run bronze sanity check + comprehensive DQ after pipeline",
    )
    return parser.parse_args()


def run_job(job: str, config: str, env: str) -> int:
    cmd = [
        sys.executable,
        "-m",
        "project_a.pipeline.run_pipeline",
        "--job",
        job,
        "--env",
        env,
        "--config",
        config,
    ]
    print(f"\n==> Running job: {job}")
    return subprocess.call(cmd)


def main() -> int:
    args = parse_args()
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config not found: {config_path}")
        return 2

    jobs = [j.strip() for j in args.jobs.split(",") if j.strip()]
    if not jobs:
        print("No jobs specified.")
        return 2

    exit_code = 0
    for job in jobs:
        code = run_job(job, str(config_path), args.env)
        if code != 0:
            print(f"Job failed: {job} (exit={code})")
            exit_code = code
            if not args.continue_on_error:
                return exit_code

    if args.with_validation:
        print("\n==> Running bronze sanity check")
        subprocess.call(
            [sys.executable, "scripts/check_bronze_data.py", "--config", str(config_path)]
        )
        print("\n==> Running comprehensive DQ")
        subprocess.call(
            [
                sys.executable,
                "jobs/dq/run_comprehensive_dq.py",
                "--layer",
                "all",
                "--env",
                args.env,
                "--config",
                str(config_path),
                "--output",
                "artifacts/dq/reports/comprehensive_dq.txt",
            ]
        )

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
