#!/usr/bin/env python3
"""
Run the full Project A ETL pipeline locally.

Usage:
    python3 run_complete_etl.py --config local/config/local.yaml --env local
    python3 run_complete_etl.py --jobs fx_json_to_bronze,bronze_to_silver,silver_to_gold
"""

from __future__ import annotations

import argparse
import time
import subprocess
import sys
from pathlib import Path

from project_a.monitoring.alerts import emit_alert
from project_a.utils.config import load_config_resolved
from project_a.utils.spark_session import build_spark

DEFAULT_JOBS = [
    "fx_json_to_bronze",
    "snowflake_to_bronze",
    "crm_to_bronze",
    "redshift_to_bronze",
    "kafka_events_csv_snapshot_to_bronze",
    "bronze_to_silver",
    "silver_to_gold",
    "gold_truth_tests",
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
    parser.add_argument(
        "--with-slo",
        action="store_true",
        help="Evaluate pipeline SLOs (runtime + data freshness)",
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
    start_ts = time.perf_counter()
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
    if args.with_slo:
        total_seconds = round(time.perf_counter() - start_ts, 2)
        cfg = load_config_resolved(str(config_path))
        slo_cfg = (cfg.get("monitoring", {}) or {}).get("slo", {}) or cfg.get("slo", {})
        runtime_slo = float(slo_cfg.get("pipeline_runtime_seconds", 0) or 0)
        if runtime_slo and total_seconds > runtime_slo:
            msg = f"Pipeline runtime {total_seconds:.2f}s exceeded SLO {runtime_slo:.2f}s"
            print(f"SLO BREACH: {msg}")
            emit_alert("Pipeline SLO breach", msg, level="WARN", config=cfg)
        else:
            print(f"SLO runtime check OK: {total_seconds:.2f}s")

        freshness_hours = float(slo_cfg.get("freshness_hours", 0) or 0)
        if freshness_hours:
            spark = build_spark(app_name="slo_freshness", config=cfg)
            try:
                paths = cfg.get("paths", {})
                silver_root = paths.get("silver_root") or paths.get("silver") or "data/silver"
                gold_root = paths.get("gold_root") or paths.get("gold") or "data/gold"
                # Prefer gold fact orders if present
                df_path = f"{gold_root}/fact_orders"
                if not Path(df_path.replace("file://", "")).exists():
                    df_path = f"{silver_root}/orders_silver"
                df = spark.read.parquet(df_path)
                if "order_date" in df.columns:
                    max_date = df.selectExpr("max(order_date) as max_date").collect()[0]["max_date"]
                    if max_date:
                        from datetime import date, datetime, time as dt_time, timezone

                        if hasattr(max_date, "to_pydatetime"):
                            # pandas Timestamp
                            max_dt = max_date.to_pydatetime()
                        elif isinstance(max_date, str):
                            max_dt = datetime.fromisoformat(max_date)
                        elif isinstance(max_date, date) and not isinstance(max_date, datetime):
                            max_dt = datetime.combine(max_date, dt_time.min)
                        else:
                            max_dt = max_date

                        if isinstance(max_dt, datetime):
                            if getattr(max_dt, "tzinfo", None) is None:
                                max_dt = max_dt.replace(tzinfo=timezone.utc)
                        else:
                            # Fallback for numpy datetime64 or other types
                            try:
                                import numpy as np

                                if isinstance(max_dt, np.datetime64):
                                    seconds = max_dt.astype("datetime64[s]").astype(int)
                                    max_dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
                                else:
                                    max_dt = datetime.fromtimestamp(
                                        getattr(max_dt, "timestamp", lambda: 0)(), tz=timezone.utc
                                    )
                            except Exception:
                                max_dt = datetime.fromtimestamp(0, tz=timezone.utc)

                        age_hours = (
                            datetime.now(timezone.utc) - max_dt
                        ).total_seconds() / 3600
                        if age_hours < 0:
                            msg = (
                                "Data freshness check found future-dated records: "
                                f"max order_date is {abs(age_hours):.2f}h ahead of current time"
                            )
                            print(f"SLO WARNING: {msg}")
                            emit_alert("Data freshness anomaly", msg, level="WARN", config=cfg)
                        elif age_hours > freshness_hours:
                            msg = f"Data freshness {age_hours:.2f}h exceeds SLO {freshness_hours:.2f}h"
                            print(f"SLO BREACH: {msg}")
                            emit_alert("Data freshness SLO breach", msg, level="WARN", config=cfg)
                        else:
                            print(f"SLO freshness OK: {age_hours:.2f}h")
            finally:
                spark.stop()

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
