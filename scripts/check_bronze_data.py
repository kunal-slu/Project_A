#!/usr/bin/env python3
"""Lightweight bronze data sanity check.

Checks:
- ID columns are not null and mostly unique
- Date columns are not in the far future / too old
- Numeric columns (amount, price, rate, quantity) are non-negative

Outputs a JSON report to artifacts/dq/bronze_sanity_report.json.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


def _strip_file_prefix(path: str) -> str:
    if path.startswith("file://"):
        return path[len("file://"):]
    return path


def _load_config(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _date_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if "date" in c.lower() or "timestamp" in c.lower()]


def _id_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c.endswith("_id")]


PRIMARY_KEYS = {
    "crm_accounts": ["Id"],
    "crm_contacts": ["Id"],
    "crm_opportunities": ["Id"],
    "snowflake_customers": ["customer_id"],
    "snowflake_orders": ["order_id"],
    "snowflake_products": ["product_id"],
    "redshift_behavior": ["behavior_id"],
    "kafka_events": ["event_id"],
    "fx_rates_json": [],
}


def _numeric_columns(df: pd.DataFrame) -> list[str]:
    candidates = [c for c in df.columns if any(x in c.lower() for x in ["amount", "price", "rate", "quantity"])]
    return candidates


def _check_df(df: pd.DataFrame, name: str, max_future_days: int, max_past_years: int) -> dict[str, Any]:
    results: dict[str, Any] = {
        "table": name,
        "row_count": int(len(df)),
        "issues": [],
        "details": {},
    }

    now = datetime.utcnow().date()
    max_future = now + timedelta(days=max_future_days)
    min_past = now - timedelta(days=max_past_years * 365)

    # ID checks
    pk_cols = PRIMARY_KEYS.get(name, _id_columns(df))
    for col in pk_cols:
        nulls = df[col].isna().sum()
        dupes = df[col].duplicated().sum()
        results["details"][col] = {
            "nulls": int(nulls),
            "duplicates": int(dupes),
        }
        if nulls > 0:
            results["issues"].append(f"{col}: {nulls} null IDs")
        if dupes > 0:
            results["issues"].append(f"{col}: {dupes} duplicate IDs")

    # Date checks
    for col in _date_columns(df):
        series = df[col].replace("", pd.NA)
        parsed = pd.to_datetime(series, errors="coerce", utc=True)
        invalid = (series.notna() & parsed.isna()).sum()
        future = (parsed.dt.date > max_future).sum()
        old = (parsed.dt.date < min_past).sum()
        results["details"][col] = {
            "invalid_format": int(invalid),
            "future_dates": int(future),
            "too_old_dates": int(old),
        }
        if invalid > 0:
            results["issues"].append(f"{col}: {invalid} invalid dates")
        if future > 0:
            results["issues"].append(f"{col}: {future} future dates")
        if old > 0:
            results["issues"].append(f"{col}: {old} too-old dates")

    # Numeric sanity
    for col in _numeric_columns(df):
        if pd.api.types.is_numeric_dtype(df[col]):
            neg = (df[col] < 0).sum()
            results["details"][col] = {"negative": int(neg)}
            if neg > 0:
                results["issues"].append(f"{col}: {neg} negative values")

    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="local/config/local.yaml")
    parser.add_argument("--sample-rows", type=int, default=50000)
    parser.add_argument("--output", default="artifacts/dq/bronze_sanity_report.json")
    args = parser.parse_args()

    config = _load_config(args.config)
    dq_cfg = config.get("dq", {})
    realism = dq_cfg.get("realism", {})
    max_future_days = int(realism.get("max_future_days", 3))
    max_past_years = int(realism.get("max_past_years", 20))

    sources = config.get("sources", {})
    tables: dict[str, str] = {}

    # CRM
    crm = sources.get("crm", {})
    crm_base = _strip_file_prefix(crm.get("base_path", "data/bronze/crm"))
    tables["crm_accounts"] = str(Path(crm_base) / crm.get("files", {}).get("accounts", "accounts.csv"))
    tables["crm_contacts"] = str(Path(crm_base) / crm.get("files", {}).get("contacts", "contacts.csv"))
    tables["crm_opportunities"] = str(Path(crm_base) / crm.get("files", {}).get("opportunities", "opportunities.csv"))

    # Snowflake
    snowflake = sources.get("snowflake", {})
    snow_base = _strip_file_prefix(snowflake.get("base_path", "data/bronze/snowflake"))
    tables["snowflake_customers"] = str(Path(snow_base) / snowflake.get("files", {}).get("customers", "customers.csv"))
    tables["snowflake_orders"] = str(Path(snow_base) / snowflake.get("files", {}).get("orders", "orders.csv"))
    tables["snowflake_products"] = str(Path(snow_base) / snowflake.get("files", {}).get("products", "products.csv"))

    # Redshift
    redshift = sources.get("redshift", {})
    red_base = _strip_file_prefix(redshift.get("base_path", "data/bronze/redshift"))
    tables["redshift_behavior"] = str(Path(red_base) / redshift.get("files", {}).get("behavior", "behavior.csv"))

    # Kafka
    kafka = sources.get("kafka_sim", {})
    kafka_base = _strip_file_prefix(kafka.get("base_path", "data/bronze/kafka"))
    tables["kafka_events"] = str(Path(kafka_base) / kafka.get("files", {}).get("orders_seed", "events.csv"))

    # FX JSONL
    fx = sources.get("fx", {})
    fx_base = _strip_file_prefix(fx.get("raw_path", "data/bronze/fx/json"))
    tables["fx_rates_json"] = str(Path(fx_base) / fx.get("files", {}).get("daily_rates_json", "fx_rates_historical.json"))

    report = {
        "generated_at": datetime.utcnow().isoformat(),
        "tables": {},
        "summary": {},
    }

    for name, path in tables.items():
        p = Path(path)
        if not p.exists():
            report["tables"][name] = {"missing": True, "path": path}
            continue

        if name == "fx_rates_json":
            df = pd.read_json(p, lines=True, nrows=args.sample_rows)
        else:
            df = pd.read_csv(p, nrows=args.sample_rows)

        result = _check_df(df, name, max_future_days, max_past_years)
        result["path"] = path
        report["tables"][name] = result

    total_issues = sum(len(t.get("issues", [])) for t in report["tables"].values() if not t.get("missing"))
    report["summary"] = {
        "tables_checked": len(report["tables"]),
        "total_issues": total_issues,
    }

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    main()
