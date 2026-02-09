#!/usr/bin/env python3
"""
Row-level reconciliation between layers.

Compares counts and key overlaps for critical tables.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from project_a.utils.config import load_config_resolved
from project_a.utils.spark_session import build_spark
from project_a.dq.comprehensive_validator import ComprehensiveValidator


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Layer reconciliation checks")
    parser.add_argument("--config", default="local/config/local.yaml")
    parser.add_argument("--output", default="artifacts/dq/reconciliation.json")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config_resolved(args.config)
    spark = build_spark(app_name="reconcile_counts", config=cfg)

    try:
        paths = cfg.get("paths", {})
        bronze_root = paths.get("bronze_root") or paths.get("bronze") or "data/bronze"
        silver_root = paths.get("silver_root") or paths.get("silver") or "data/silver"
        gold_root = paths.get("gold_root") or paths.get("gold") or "data/gold"

        bronze = {
            "customers": spark.read.parquet(f"{bronze_root}/snowflake/customers"),
            "orders": spark.read.parquet(f"{bronze_root}/snowflake/orders"),
            "products": spark.read.parquet(f"{bronze_root}/snowflake/products"),
        }
        silver = {
            "customers": spark.read.parquet(f"{silver_root}/customers_silver"),
            "orders": spark.read.parquet(f"{silver_root}/orders_silver"),
            "products": spark.read.parquet(f"{silver_root}/products_silver"),
        }
        gold = {
            "fact_orders": spark.read.parquet(f"{gold_root}/fact_orders"),
            "dim_customer": spark.read.parquet(f"{gold_root}/dim_customer"),
            "dim_product": spark.read.parquet(f"{gold_root}/dim_product"),
        }

        validator = ComprehensiveValidator(spark, dq_config=cfg.get("dq", {}))
        results = {
            "bronze_to_silver": validator._reconcile_layer_pairs(
                "bronze", "silver", bronze, silver
            ),
            "silver_to_gold": validator._reconcile_layer_pairs(
                "silver", "gold", silver, gold
            ),
        }

        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(results, indent=2, default=str))
        print(f"Reconciliation written to {out_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
