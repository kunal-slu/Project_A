#!/usr/bin/env python3
"""
DQ Gate: Enforces Great Expectations suites as hard gates
Fails pipeline on critical violations
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from project_a.config_loader import load_config_resolved
from project_a.dq.gate import DQGate
from project_a.utils.spark_session import build_spark

logger = logging.getLogger(__name__)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="DQ Gate")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--layer", required=True, help="Layer (bronze/silver/gold)")
    parser.add_argument("--env", default="dev", help="Environment (dev/staging/prod/local)")
    parser.add_argument(
        "--config",
        help="Config file path (local or s3://...). Defaults to config/{env}.yaml or local/config/local.yaml",
    )
    args = parser.parse_args()

    # Load config
    if args.config:
        config_path = args.config
    else:
        candidate = Path(f"config/{args.env}.yaml")
        if candidate.exists():
            config_path = str(candidate)
        else:
            local_candidate = Path("local/config/local.yaml")
            if local_candidate.exists():
                config_path = str(local_candidate)
            else:
                config_path = str(Path("config/dev.yaml"))

    config = load_config_resolved(config_path, env=args.env)

    # Build Spark session
    spark = build_spark(app_name="dq_gate", config=config)

    try:
        storage_fmt = (
            (config.get("storage") or {}).get("format")
            or ("iceberg" if config.get("iceberg", {}).get("enabled") else "delta")
        ).lower()

        buckets = config.get("buckets", {}) if isinstance(config, dict) else {}
        lake_bucket = buckets.get("lake")
        if lake_bucket:
            table_path = f"s3a://{lake_bucket}/{args.layer}/{args.table}/"
        else:
            paths_cfg = config.get("paths", {}) if isinstance(config, dict) else {}
            root = paths_cfg.get(f"{args.layer}_root") or paths_cfg.get(args.layer)
            if not root:
                raise ValueError(f"Missing {args.layer} path in config for local DQ gate")
            table_path = f"{root.rstrip('/')}/{args.table}"

        if storage_fmt == "iceberg":
            catalog = config.get("iceberg", {}).get("catalog_name", "local")
            try:
                df = spark.read.format("iceberg").load(f"{catalog}.{args.table}")
            except Exception:
                # Fallback to path-based read
                try:
                    df = spark.read.format("delta").load(table_path)
                except Exception:
                    df = spark.read.parquet(table_path)
        elif storage_fmt == "delta":
            df = spark.read.format("delta").load(table_path)
        else:
            df = spark.read.parquet(table_path)

        # Run DQ gate (pass config)
        gate = DQGate(config)
        result = gate.check_and_block(spark, df, args.table, layer=args.layer)

        logger.info("DQ Gate passed for %s.%s", args.layer, args.table)
        logger.info("Critical failures: %s", result.get("critical_failures", 0))
        logger.info("Warnings: %s", result.get("warnings", 0))
        sys.exit(0)

    except Exception as e:
        logger.error("DQ Gate error: %s", e)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
