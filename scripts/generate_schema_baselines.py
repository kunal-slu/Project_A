#!/usr/bin/env python3
"""Generate or update schema baselines from existing Silver/Gold tables."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from project_a.config_loader import load_config_resolved
from project_a.utils.schema_evolution import enforce_schema_evolution
from project_a.utils.spark_session import build_spark


def _layer_format(config: dict[str, Any], layer: str) -> str:
    if layer == "bronze":
        return "parquet"
    storage_fmt = (config.get("storage", {}) or {}).get("format")
    if storage_fmt:
        return str(storage_fmt).lower()
    iceberg_cfg = config.get("iceberg", {})
    if iceberg_cfg.get("enabled") and layer in ("silver", "gold"):
        return "iceberg"
    return "parquet"


def _read_table(spark, fmt: str, path: str, table_name: str, catalog: str) -> Any:
    if fmt == "iceberg":
        return spark.read.format("iceberg").load(f"{catalog}.{table_name}")
    if fmt == "delta":
        return spark.read.format("delta").load(path)
    return spark.read.parquet(path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="local/config/local.yaml")
    parser.add_argument("--layer", default="silver,gold")
    parser.add_argument("--update", action="store_true", help="Update baseline even if already exists")
    args = parser.parse_args()

    config = load_config_resolved(args.config)
    spark = build_spark(config)

    try:
        layers = [l.strip() for l in args.layer.split(",") if l.strip()]
        schema_cfg = config.get("schema_evolution", {})
        if not schema_cfg.get("enabled", False):
            raise SystemExit("schema_evolution.enabled is false in config")

        baseline_cfg = {
            "enabled": True,
            "baseline_path": schema_cfg.get("baseline_path", "artifacts/schema_baselines"),
            "update_baseline": bool(args.update),
            "policies": schema_cfg.get("policies", {}),
        }

        paths_cfg = config.get("paths", {})
        tables_cfg = config.get("tables", {})
        catalog = config.get("iceberg", {}).get("catalog_name", "local")

        for layer in layers:
            fmt = _layer_format(config, layer)
            if layer == "silver":
                root = paths_cfg.get("silver_root") or paths_cfg.get("silver") or "data/silver"
                tables = tables_cfg.get("silver", {})
            elif layer == "gold":
                root = paths_cfg.get("gold_root") or paths_cfg.get("gold") or "data/gold"
                tables = tables_cfg.get("gold", {})
            else:
                raise SystemExit(f"Unsupported layer: {layer}")

            for _, table_name in tables.items():
                path = f"{root}/{table_name}"
                df = _read_table(spark, fmt, path, table_name, catalog)
                enforce_schema_evolution(
                    df,
                    table_name=table_name,
                    layer=layer,
                    config=baseline_cfg,
                )
                print(f"Baseline checked for {layer}.{table_name}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
