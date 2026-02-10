#!/usr/bin/env python3
# ruff: noqa: E402
"""
Gold Truth Tests

Run critical invariants and reconciliation checks for Gold outputs.
Generates a JSON report at artifacts/run_audit/<timestamp>.json
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from project_a.utils.config import load_config_resolved
from project_a.utils.path_resolver import resolve_data_path
from project_a.utils.spark_session import build_spark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Gold Truth Tests.")
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
        "--output",
        default=None,
        help="Optional output path (default: artifacts/run_audit/gold_truth_<ts>.json)",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=None,
        help="Optional rolling window in days for reconciliation (default: full)",
    )
    return parser.parse_args()


def _to_local(path: str) -> str:
    if path.startswith("file://"):
        parsed = urlparse(path)
        return unquote(parsed.path)
    return path


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


def _read_table(
    spark: SparkSession,
    fmt: str,
    path: str,
    table_name: str | None = None,
    catalog_name: str | None = None,
) -> Any:
    if fmt == "iceberg":
        if not table_name or not catalog_name:
            raise ValueError("Iceberg reads require catalog_name and table_name")
        return spark.read.format("iceberg").load(f"{catalog_name}.{table_name}")
    if fmt == "delta":
        return spark.read.format("delta").load(path)
    return spark.read.parquet(path)


def _try_load_table(
    spark: SparkSession,
    fmt: str,
    path: str,
    table_name: str | None = None,
    catalog_name: str | None = None,
):
    try:
        df = _read_table(spark, fmt, path, table_name=table_name, catalog_name=catalog_name)
        df.limit(1).count()
        return df, None
    except Exception as exc:
        return None, str(exc)


def _prepare_fx_rates(fx_rates_df):
    fx_base = fx_rates_df.select(
        F.col("trade_date").alias("fx_date"),
        F.col("base_ccy").alias("fx_base_ccy"),
        F.col("counter_ccy").alias("fx_counter_ccy"),
        F.col("rate").alias("fx_rate"),
    )

    direct = fx_base.filter(F.col("fx_counter_ccy") == F.lit("USD")).select(
        "fx_date",
        F.col("fx_base_ccy").alias("fx_currency"),
        F.col("fx_rate").alias("fx_rate"),
    )
    inverted = (
        fx_base.filter(F.col("fx_base_ccy") == F.lit("USD"))
        .filter(F.col("fx_counter_ccy") != F.lit("USD"))
        .select(
            "fx_date",
            F.col("fx_counter_ccy").alias("fx_currency"),
            (F.lit(1.0) / F.col("fx_rate")).alias("fx_rate"),
        )
    )

    fx_rates_norm = direct.unionByName(inverted, allowMissingColumns=True).dropDuplicates(
        ["fx_date", "fx_currency"]
    )
    return fx_rates_norm


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def run_gold_truth_tests(
    config_path: str, env: str, output_path: str | None, window_days: int | None
) -> dict[str, Any]:
    config = load_config_resolved(config_path, env)
    spark = build_spark(app_name="gold-truth-tests", config=config)

    timestamp = datetime.now(timezone.utc)
    run_id = timestamp.strftime("%Y%m%dT%H%M%SZ")

    gold_root = resolve_data_path(config, "gold")
    silver_root = resolve_data_path(config, "silver")

    fmt_gold = _layer_format(config, "gold")
    fmt_silver = _layer_format(config, "silver")
    iceberg_cfg = config.get("iceberg", {})
    catalog = iceberg_cfg.get("catalog_name", "local")

    gold_root_local = _to_local(gold_root)

    report: dict[str, Any] = {
        "run_id": run_id,
        "timestamp_utc": timestamp.isoformat(),
        "env": env,
        "gold_truth_version": 1,
        "paths": {
            "gold_root": gold_root,
            "silver_root": silver_root,
        },
        "gold_table_rows": {},
        "null_duplicate_checks": {},
        "join_checks": [],
        "contract_checks": {},
        "reconciliation": {},
        "fx_coverage": {},
        "warnings": [],
    }

    gold_tables_cfg = (config.get("tables", {}) or {}).get("gold", {})
    gold_table_names = set(gold_tables_cfg.values())
    # Extra gold tables seen in local runs / legacy flows
    gold_table_names.update({"dim_date", "product_performance"})

    if Path(gold_root_local).exists():
        for entry in Path(gold_root_local).iterdir():
            if entry.is_dir():
                gold_table_names.add(entry.name)

    gold_table_names = sorted(gold_table_names)

    pk_map = {
        "behavior_analytics": ["customer_id", "event_type", "device_type", "browser"],
        "customer_360": ["customer_id"],
        "dim_customer": ["customer_sk"],
        "dim_account": ["account_sk"],
        "dim_contact": ["contact_sk"],
        "dim_product": ["product_sk"],
        "dim_date": ["date_sk"],
        "fact_orders": ["order_id"],
        "fact_opportunity": ["opportunity_id"],
        "fact_order_events": ["event_id"],
        "product_performance": ["product_id"],
    }

    loaded_gold: dict[str, Any] = {}

    for table in gold_table_names:
        path = f"{gold_root}/{table}"
        df, error = _try_load_table(spark, fmt_gold, path, table_name=table, catalog_name=catalog)
        if error:
            report["null_duplicate_checks"][table] = {
                "status": "missing",
                "error": error,
            }
            report["gold_table_rows"][table] = None
            continue

        loaded_gold[table] = df
        row_count = df.count()
        report["gold_table_rows"][table] = row_count

        keys = pk_map.get(table)
        if not keys:
            # best-effort heuristic
            candidates = [c for c in df.columns if c.endswith("_sk")]
            if not candidates:
                candidates = [c for c in df.columns if c.endswith("_id")]
            keys = candidates[:1] if candidates else []

        if not keys:
            report["null_duplicate_checks"][table] = {
                "status": "skipped",
                "reason": "no primary key heuristic",
            }
            continue

        null_condition = None
        for key in keys:
            expr = F.col(key).isNull()
            null_condition = expr if null_condition is None else (null_condition | expr)

        nulls = df.filter(null_condition).count() if null_condition is not None else 0
        dupes = df.groupBy(*keys).count().filter(F.col("count") > 1).count()

        report["null_duplicate_checks"][table] = {
            "status": "pass" if nulls == 0 and dupes == 0 else "fail",
            "pk": keys,
            "nulls": nulls,
            "duplicates": dupes,
        }

    # Join cardinality checks
    def join_check(left_df, right_df, key_cols: list[str], label: str) -> None:
        right_dupes = right_df.groupBy(*key_cols).count().filter(F.col("count") > 1).count()
        before = left_df.count()
        after = left_df.join(right_df, on=key_cols, how="left").count()
        unmatched = left_df.join(right_df, on=key_cols, how="left_anti").count()
        report["join_checks"].append(
            {
                "label": label,
                "right_key_dupes": right_dupes,
                "join_explosion": after - before,
                "unmatched_left": unmatched,
                "status": "pass" if right_dupes == 0 and (after - before) == 0 else "fail",
            }
        )

    # Load silver tables needed
    orders_silver, err_orders = _try_load_table(
        spark,
        fmt_silver,
        f"{silver_root}/orders_silver",
        table_name="orders_silver",
        catalog_name=catalog,
    )
    customers_silver, err_customers = _try_load_table(
        spark,
        fmt_silver,
        f"{silver_root}/customers_silver",
        table_name="customers_silver",
        catalog_name=catalog,
    )
    products_silver, err_products = _try_load_table(
        spark,
        fmt_silver,
        f"{silver_root}/products_silver",
        table_name="products_silver",
        catalog_name=catalog,
    )
    behavior_silver, err_behavior = _try_load_table(
        spark,
        fmt_silver,
        f"{silver_root}/customer_behavior_silver",
        table_name="customer_behavior_silver",
        catalog_name=catalog,
    )
    opportunities_silver, err_opps = _try_load_table(
        spark,
        fmt_silver,
        f"{silver_root}/opportunities_silver",
        table_name="opportunities_silver",
        catalog_name=catalog,
    )
    order_events_silver, err_events = _try_load_table(
        spark,
        fmt_silver,
        f"{silver_root}/order_events_silver",
        table_name="order_events_silver",
        catalog_name=catalog,
    )
    fx_rates_silver, err_fx = _try_load_table(
        spark,
        fmt_silver,
        f"{silver_root}/fx_rates_silver",
        table_name="fx_rates_silver",
        catalog_name=catalog,
    )

    dim_customer = loaded_gold.get("dim_customer")
    dim_product = loaded_gold.get("dim_product")
    dim_account = loaded_gold.get("dim_account")
    dim_contact = loaded_gold.get("dim_contact")
    fact_orders = loaded_gold.get("fact_orders")
    fact_order_events = loaded_gold.get("fact_order_events")
    fact_opportunity = loaded_gold.get("fact_opportunity")

    # Orders -> dims
    if orders_silver is not None and dim_customer is not None:
        current_customers = dim_customer.filter(F.col("is_current") == F.lit(True)).select(
            "customer_id", "customer_sk"
        )
        join_check(
            orders_silver, current_customers, ["customer_id"], "orders_silver -> dim_customer"
        )
    else:
        report["join_checks"].append(
            {
                "label": "orders_silver -> dim_customer",
                "status": "skipped",
                "reason": "missing data",
            }
        )

    if orders_silver is not None and dim_product is not None:
        current_products = dim_product.filter(F.col("is_current") == F.lit(True)).select(
            "product_id", "product_sk"
        )
        join_check(orders_silver, current_products, ["product_id"], "orders_silver -> dim_product")
    else:
        report["join_checks"].append(
            {"label": "orders_silver -> dim_product", "status": "skipped", "reason": "missing data"}
        )

    # Customer 360 join checks
    if orders_silver is not None and dim_customer is not None:
        current_customers = dim_customer.filter(F.col("is_current") == F.lit(True)).select(
            "customer_id", "customer_sk"
        )
        order_metrics = orders_silver.groupBy("customer_id").agg(
            F.sum("total_amount").alias("sum_total_amount"),
            F.countDistinct("order_id").alias("order_count"),
        )
        join_check(
            current_customers,
            order_metrics,
            ["customer_id"],
            "dim_customer (current) -> order_metrics",
        )
    else:
        report["join_checks"].append(
            {
                "label": "dim_customer (current) -> order_metrics",
                "status": "skipped",
                "reason": "missing data",
            }
        )

    if behavior_silver is not None and dim_customer is not None:
        current_customers = dim_customer.filter(F.col("is_current") == F.lit(True)).select(
            "customer_id", "customer_sk"
        )
        behavior_metrics = behavior_silver.groupBy("customer_id").agg(
            F.sum(F.col("time_spent_seconds").cast("double")).alias("total_time_spent"),
            F.countDistinct("session_id").alias("session_count"),
        )
        join_check(
            current_customers,
            behavior_metrics,
            ["customer_id"],
            "dim_customer (current) -> behavior_metrics",
        )
    else:
        report["join_checks"].append(
            {
                "label": "dim_customer (current) -> behavior_metrics",
                "status": "skipped",
                "reason": "missing data",
            }
        )

    # Product performance join check
    if orders_silver is not None and dim_product is not None:
        current_products = dim_product.filter(F.col("is_current") == F.lit(True)).select(
            "product_id", "product_sk"
        )
        product_agg = orders_silver.groupBy("product_id").agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("total_amount").alias("revenue"),
        )
        join_check(
            current_products,
            product_agg,
            ["product_id"],
            "dim_product (current) -> product_agg",
        )
    else:
        report["join_checks"].append(
            {
                "label": "dim_product (current) -> product_agg",
                "status": "skipped",
                "reason": "missing data",
            }
        )

    # Opportunity join checks (only if gold fact exists)
    if (
        fact_opportunity is not None
        and opportunities_silver is not None
        and dim_account is not None
    ):
        current_accounts = dim_account.filter(F.col("is_current") == F.lit(True)).select(
            "account_id", "account_sk"
        )
        join_check(
            opportunities_silver,
            current_accounts,
            ["account_id"],
            "opportunities_silver -> dim_account",
        )
    else:
        report["join_checks"].append(
            {
                "label": "opportunities_silver -> dim_account",
                "status": "skipped",
                "reason": "missing data",
            }
        )

    if (
        fact_opportunity is not None
        and opportunities_silver is not None
        and dim_contact is not None
    ):
        current_contacts = dim_contact.filter(F.col("is_current") == F.lit(True)).select(
            "contact_id", "contact_sk"
        )
        join_check(
            opportunities_silver,
            current_contacts,
            ["contact_id"],
            "opportunities_silver -> dim_contact",
        )
    else:
        report["join_checks"].append(
            {
                "label": "opportunities_silver -> dim_contact",
                "status": "skipped",
                "reason": "missing data",
            }
        )

    # Kafka order events join checks (only if gold fact exists)
    if (
        fact_order_events is not None
        and order_events_silver is not None
        and dim_customer is not None
    ):
        current_customers = dim_customer.filter(F.col("is_current") == F.lit(True)).select(
            "customer_id", "customer_sk"
        )
        join_check(
            order_events_silver,
            current_customers,
            ["customer_id"],
            "order_events_silver -> dim_customer",
        )
    else:
        report["join_checks"].append(
            {
                "label": "order_events_silver -> dim_customer",
                "status": "skipped",
                "reason": "missing data",
            }
        )

    if (
        fact_order_events is not None
        and order_events_silver is not None
        and fact_orders is not None
    ):
        join_check(
            order_events_silver,
            fact_orders.select("order_id").dropDuplicates(["order_id"]),
            ["order_id"],
            "order_events_silver -> fact_orders",
        )
    else:
        report["join_checks"].append(
            {
                "label": "order_events_silver -> fact_orders",
                "status": "skipped",
                "reason": "missing data",
            }
        )

    # Contract checks for interview-grade correctness guarantees
    if dim_customer is not None:
        current_dim_customer = (
            dim_customer.filter(F.col("is_current") == F.lit(True))
            if "is_current" in dim_customer.columns
            else dim_customer
        )
        current_customer_dupes = (
            current_dim_customer.groupBy("customer_id").count().filter(F.col("count") > 1).count()
        )
        report["contract_checks"]["dim_customer_current_unique_customer_id"] = {
            "status": "pass" if current_customer_dupes == 0 else "fail",
            "duplicates": current_customer_dupes,
        }
    else:
        report["contract_checks"]["dim_customer_current_unique_customer_id"] = {
            "status": "skipped",
            "reason": "missing dim_customer",
        }

    if fact_orders is not None and dim_customer is not None:
        current_customer_ids = (
            dim_customer.filter(F.col("is_current") == F.lit(True))
            .select("customer_id")
            .dropDuplicates(["customer_id"])
        )
        fact_orders_customer_orphans = (
            fact_orders.select("customer_id")
            .dropDuplicates(["customer_id"])
            .join(current_customer_ids, "customer_id", "left_anti")
            .count()
        )
        report["contract_checks"]["fact_orders_customer_fk_coverage"] = {
            "status": "pass" if fact_orders_customer_orphans == 0 else "fail",
            "orphans": fact_orders_customer_orphans,
        }
    else:
        report["contract_checks"]["fact_orders_customer_fk_coverage"] = {
            "status": "skipped",
            "reason": "missing fact_orders/dim_customer",
        }

    product_performance = loaded_gold.get("product_performance")
    if fact_orders is not None and product_performance is not None:
        fact_revenue = (
            fact_orders.agg(F.sum(F.col("order_amount_usd")).alias("sum_rev")).collect()[0][
                "sum_rev"
            ]
            or 0.0
        )
        perf_revenue = (
            product_performance.agg(F.sum(F.col("total_revenue_usd")).alias("sum_rev")).collect()[
                0
            ]["sum_rev"]
            or 0.0
        )
        diff = abs(float(fact_revenue) - float(perf_revenue))
        tolerance_pct = float(
            ((config.get("dq", {}) or {}).get("reconciliation", {}) or {}).get("tolerance_pct", 0.1)
        )
        allowed = max(0.01, abs(float(fact_revenue)) * (tolerance_pct / 100.0))
        report["contract_checks"]["revenue_fact_vs_product_performance"] = {
            "status": "pass" if diff <= allowed else "fail",
            "fact_orders_revenue_usd": float(fact_revenue),
            "product_performance_revenue_usd": float(perf_revenue),
            "abs_diff": diff,
            "allowed_diff": allowed,
            "tolerance_pct": tolerance_pct,
        }
    else:
        report["contract_checks"]["revenue_fact_vs_product_performance"] = {
            "status": "skipped",
            "reason": "missing fact_orders/product_performance",
        }

    if fact_order_events is not None and fact_orders is not None:
        event_order_orphans = (
            fact_order_events.filter(F.col("order_id").isNotNull())
            .select("order_id")
            .dropDuplicates(["order_id"])
            .join(
                fact_orders.select("order_id").dropDuplicates(["order_id"]), "order_id", "left_anti"
            )
            .count()
        )
        report["contract_checks"]["fact_order_events_order_fk_coverage"] = {
            "status": "pass" if event_order_orphans == 0 else "fail",
            "orphans": event_order_orphans,
        }
    else:
        report["contract_checks"]["fact_order_events_order_fk_coverage"] = {
            "status": "skipped",
            "reason": "missing fact_order_events/fact_orders",
        }

    # Completeness thresholds for critical event columns
    completeness_threshold = float((config.get("dq", {}) or {}).get("completeness_threshold", 0.99))
    if order_events_silver is not None:
        total_events = order_events_silver.count()
        for required_col in ("order_id", "event_type", "event_ts"):
            if required_col not in order_events_silver.columns:
                report["contract_checks"][f"order_events_{required_col}_completeness"] = {
                    "status": "fail",
                    "reason": f"missing column {required_col}",
                }
                continue
            non_null = order_events_silver.filter(F.col(required_col).isNotNull()).count()
            pct = (non_null / total_events) if total_events else 0.0
            report["contract_checks"][f"order_events_{required_col}_completeness"] = {
                "status": "pass" if pct >= completeness_threshold else "fail",
                "non_null_rows": non_null,
                "total_rows": total_events,
                "non_null_pct": pct,
                "threshold": completeness_threshold,
            }
    else:
        for required_col in ("order_id", "event_type", "event_ts"):
            report["contract_checks"][f"order_events_{required_col}_completeness"] = {
                "status": "skipped",
                "reason": "missing order_events_silver",
            }

    # Future-date bounds checks
    if fact_orders is not None and "order_date" in fact_orders.columns:
        future_orders = fact_orders.filter(
            F.col("order_date") > F.date_add(F.current_date(), 1)
        ).count()
        report["contract_checks"]["fact_orders_future_order_date"] = {
            "status": "pass" if future_orders == 0 else "fail",
            "rows": future_orders,
        }
    else:
        report["contract_checks"]["fact_orders_future_order_date"] = {
            "status": "skipped",
            "reason": "missing fact_orders.order_date",
        }

    if fact_order_events is not None and "event_ts" in fact_order_events.columns:
        future_events = fact_order_events.filter(
            F.col("event_ts") > (F.current_timestamp() + F.expr("INTERVAL 1 DAY"))
        ).count()
        report["contract_checks"]["fact_order_events_future_event_ts"] = {
            "status": "pass" if future_events == 0 else "fail",
            "rows": future_events,
        }
    else:
        report["contract_checks"]["fact_order_events_future_event_ts"] = {
            "status": "skipped",
            "reason": "missing fact_order_events.event_ts",
        }

    # Surrogate-key referential checks (*_sk)
    if fact_orders is not None and dim_customer is not None and dim_product is not None:
        customer_sk_orphans = (
            fact_orders.filter(F.col("customer_sk").isNotNull() & (F.col("customer_sk") != -1))
            .select("customer_sk")
            .dropDuplicates(["customer_sk"])
            .join(
                dim_customer.select(F.col("customer_sk").alias("_customer_sk")).dropDuplicates(
                    ["_customer_sk"]
                ),
                F.col("customer_sk") == F.col("_customer_sk"),
                "left_anti",
            )
            .count()
        )
        product_sk_orphans = (
            fact_orders.filter(F.col("product_sk").isNotNull() & (F.col("product_sk") != -1))
            .select("product_sk")
            .dropDuplicates(["product_sk"])
            .join(
                dim_product.select(F.col("product_sk").alias("_product_sk")).dropDuplicates(
                    ["_product_sk"]
                ),
                F.col("product_sk") == F.col("_product_sk"),
                "left_anti",
            )
            .count()
        )
        report["contract_checks"]["fact_orders_customer_sk_ri"] = {
            "status": "pass" if customer_sk_orphans == 0 else "fail",
            "orphans": customer_sk_orphans,
        }
        report["contract_checks"]["fact_orders_product_sk_ri"] = {
            "status": "pass" if product_sk_orphans == 0 else "fail",
            "orphans": product_sk_orphans,
        }
    else:
        report["contract_checks"]["fact_orders_customer_sk_ri"] = {
            "status": "skipped",
            "reason": "missing fact_orders/dim_customer",
        }
        report["contract_checks"]["fact_orders_product_sk_ri"] = {
            "status": "skipped",
            "reason": "missing fact_orders/dim_product",
        }

    if fact_opportunity is not None and dim_account is not None and dim_contact is not None:
        account_sk_orphans = (
            fact_opportunity.filter(F.col("account_sk").isNotNull() & (F.col("account_sk") != -1))
            .select("account_sk")
            .dropDuplicates(["account_sk"])
            .join(
                dim_account.select(F.col("account_sk").alias("_account_sk")).dropDuplicates(
                    ["_account_sk"]
                ),
                F.col("account_sk") == F.col("_account_sk"),
                "left_anti",
            )
            .count()
        )
        contact_sk_orphans = (
            fact_opportunity.filter(F.col("contact_sk").isNotNull() & (F.col("contact_sk") != -1))
            .select("contact_sk")
            .dropDuplicates(["contact_sk"])
            .join(
                dim_contact.select(F.col("contact_sk").alias("_contact_sk")).dropDuplicates(
                    ["_contact_sk"]
                ),
                F.col("contact_sk") == F.col("_contact_sk"),
                "left_anti",
            )
            .count()
        )
        report["contract_checks"]["fact_opportunity_account_sk_ri"] = {
            "status": "pass" if account_sk_orphans == 0 else "fail",
            "orphans": account_sk_orphans,
        }
        report["contract_checks"]["fact_opportunity_contact_sk_ri"] = {
            "status": "pass" if contact_sk_orphans == 0 else "fail",
            "orphans": contact_sk_orphans,
        }
    else:
        report["contract_checks"]["fact_opportunity_account_sk_ri"] = {
            "status": "skipped",
            "reason": "missing fact_opportunity/dim_account",
        }
        report["contract_checks"]["fact_opportunity_contact_sk_ri"] = {
            "status": "skipped",
            "reason": "missing fact_opportunity/dim_contact",
        }

    if fact_order_events is not None and dim_customer is not None:
        customer_sk_orphans = (
            fact_order_events.filter(
                F.col("customer_sk").isNotNull() & (F.col("customer_sk") != -1)
            )
            .select("customer_sk")
            .dropDuplicates(["customer_sk"])
            .join(
                dim_customer.select(F.col("customer_sk").alias("_customer_sk")).dropDuplicates(
                    ["_customer_sk"]
                ),
                F.col("customer_sk") == F.col("_customer_sk"),
                "left_anti",
            )
            .count()
        )
        report["contract_checks"]["fact_order_events_customer_sk_ri"] = {
            "status": "pass" if customer_sk_orphans == 0 else "fail",
            "orphans": customer_sk_orphans,
        }
    else:
        report["contract_checks"]["fact_order_events_customer_sk_ri"] = {
            "status": "skipped",
            "reason": "missing fact_order_events/dim_customer",
        }

    # FX coverage + currency normalization checks
    if fact_orders is not None:
        non_usd = fact_orders.filter(F.col("currency") != F.lit("USD"))
        missing_fx = non_usd.filter(F.col("fx_rate").isNull() | (F.col("fx_rate") <= 0)).count()
        non_usd_count = non_usd.count()
        report["fx_coverage"]["fact_orders"] = {
            "non_usd_rows": non_usd_count,
            "missing_fx": missing_fx,
            "missing_fx_pct": (missing_fx / non_usd_count) * 100 if non_usd_count else 0.0,
        }
        inconsistent = fact_orders.filter(
            F.abs(F.col("order_amount_usd") - (F.col("total_amount") * F.col("fx_rate")))
            > F.lit(0.01)
        ).count()
        report["fx_coverage"]["fact_orders"]["amount_usd_inconsistent"] = inconsistent
    else:
        report["fx_coverage"]["fact_orders"] = {"status": "skipped", "reason": "missing data"}

    if fact_order_events is not None:
        non_usd = fact_order_events.filter(F.col("currency") != F.lit("USD"))
        missing_fx = non_usd.filter(F.col("fx_rate").isNull() | (F.col("fx_rate") <= 0)).count()
        non_usd_count = non_usd.count()
        report["fx_coverage"]["fact_order_events"] = {
            "non_usd_rows": non_usd_count,
            "missing_fx": missing_fx,
            "missing_fx_pct": (missing_fx / non_usd_count) * 100 if non_usd_count else 0.0,
        }
        inconsistent = fact_order_events.filter(
            F.abs(F.col("amount_usd") - (F.col("amount") * F.col("fx_rate"))) > F.lit(0.01)
        ).count()
        report["fx_coverage"]["fact_order_events"]["amount_usd_inconsistent"] = inconsistent
    else:
        report["fx_coverage"]["fact_order_events"] = {"status": "skipped", "reason": "missing data"}

    # Cross-layer reconciliation
    if orders_silver is not None and customers_silver is not None and fact_orders is not None:
        orders = orders_silver.withColumn("order_date", F.to_date("order_date"))
        if "currency" not in orders.columns:
            orders = orders.withColumn("currency", F.lit("USD"))
        orders = orders.withColumn("currency", F.upper(F.coalesce(F.col("currency"), F.lit("USD"))))

        customers_unique = customers_silver.select("customer_id", "country").dropDuplicates(
            ["customer_id"]
        )
        orders = orders.join(customers_unique, on="customer_id", how="left")

        if window_days is not None:
            max_date = orders.select(F.max("order_date").alias("max_date")).collect()[0]["max_date"]
            if max_date is not None:
                window_start = orders.select(
                    F.date_sub(F.lit(max_date), window_days).alias("window_start")
                ).collect()[0]["window_start"]
                orders = orders.filter(
                    (F.col("order_date") >= F.lit(window_start))
                    & (F.col("order_date") <= F.lit(max_date))
                )
                fact_orders_filtered = fact_orders.filter(
                    (F.col("order_date") >= F.lit(window_start))
                    & (F.col("order_date") <= F.lit(max_date))
                )
            else:
                fact_orders_filtered = fact_orders
        else:
            fact_orders_filtered = fact_orders

        if fx_rates_silver is not None:
            fx_rates = _prepare_fx_rates(fx_rates_silver)
            orders = (
                orders.join(
                    fx_rates,
                    (orders.order_date == fx_rates.fx_date)
                    & (orders.currency == fx_rates.fx_currency),
                    "left",
                )
                .withColumn(
                    "fx_rate",
                    F.when(F.col("currency") == F.lit("USD"), F.lit(1.0)).otherwise(
                        F.col("fx_rate")
                    ),
                )
                .drop("fx_date", "fx_currency")
            )
            orders = orders.withColumn(
                "order_amount_usd",
                (F.col("total_amount").cast("double") * F.col("fx_rate")).cast("double"),
            )
        else:
            orders = orders.withColumn("fx_rate", F.lit(None).cast("double"))
            orders = orders.withColumn("order_amount_usd", F.lit(None).cast("double"))

        def reconcile(grain_cols: list[str], label: str) -> None:
            silver_agg = orders.groupBy(*grain_cols).agg(
                F.sum("total_amount").alias("silver_sum"),
                F.sum("order_amount_usd").alias("silver_usd_sum"),
                F.countDistinct("order_id").alias("silver_count"),
            )
            gold_agg = fact_orders_filtered.groupBy(*grain_cols).agg(
                F.sum("total_amount").alias("gold_sum"),
                F.sum("order_amount_usd").alias("gold_usd_sum"),
                F.countDistinct("order_id").alias("gold_count"),
            )

            joined = silver_agg.join(gold_agg, on=grain_cols, how="full")
            joined = joined.withColumn("silver_sum", F.coalesce(F.col("silver_sum"), F.lit(0.0)))
            joined = joined.withColumn("gold_sum", F.coalesce(F.col("gold_sum"), F.lit(0.0)))
            joined = joined.withColumn(
                "silver_usd_sum", F.coalesce(F.col("silver_usd_sum"), F.lit(0.0))
            )
            joined = joined.withColumn(
                "gold_usd_sum", F.coalesce(F.col("gold_usd_sum"), F.lit(0.0))
            )
            joined = joined.withColumn("silver_count", F.coalesce(F.col("silver_count"), F.lit(0)))
            joined = joined.withColumn("gold_count", F.coalesce(F.col("gold_count"), F.lit(0)))
            joined = joined.withColumn("sum_diff", F.col("gold_sum") - F.col("silver_sum"))
            joined = joined.withColumn(
                "usd_sum_diff", F.col("gold_usd_sum") - F.col("silver_usd_sum")
            )
            joined = joined.withColumn("count_diff", F.col("gold_count") - F.col("silver_count"))

            total_groups = joined.count()
            mismatch_total = joined.filter(
                (F.abs(F.col("sum_diff")) > F.lit(0.01)) | (F.col("count_diff") != 0)
            ).count()
            mismatch_usd = joined.filter(F.abs(F.col("usd_sum_diff")) > F.lit(0.01)).count()
            max_abs_sum_diff = joined.agg(F.max(F.abs(F.col("sum_diff")))).collect()[0][0]
            max_abs_usd_diff = joined.agg(F.max(F.abs(F.col("usd_sum_diff")))).collect()[0][0]
            max_abs_count_diff = joined.agg(F.max(F.abs(F.col("count_diff")))).collect()[0][0]

            result = {
                "groups": total_groups,
                "mismatched_groups_total": mismatch_total,
                "mismatched_groups_usd": mismatch_usd,
                "max_abs_sum_diff": float(max_abs_sum_diff)
                if max_abs_sum_diff is not None
                else 0.0,
                "max_abs_usd_diff": float(max_abs_usd_diff)
                if max_abs_usd_diff is not None
                else 0.0,
                "max_abs_count_diff": int(max_abs_count_diff)
                if max_abs_count_diff is not None
                else 0,
            }

            if mismatch_total > 0:
                top = (
                    joined.filter(
                        (F.abs(F.col("sum_diff")) > F.lit(0.01)) | (F.col("count_diff") != 0)
                    )
                    .orderBy(F.abs(F.col("sum_diff")).desc())
                    .limit(5)
                )
                result["top_mismatches_total"] = [
                    {
                        **{c: row[c] for c in grain_cols},
                        "sum_diff": row["sum_diff"],
                        "count_diff": row["count_diff"],
                    }
                    for row in top.collect()
                ]

            if mismatch_usd > 0:
                top = (
                    joined.filter(F.abs(F.col("usd_sum_diff")) > F.lit(0.01))
                    .orderBy(F.abs(F.col("usd_sum_diff")).desc())
                    .limit(5)
                )
                result["top_mismatches_usd"] = [
                    {**{c: row[c] for c in grain_cols}, "usd_sum_diff": row["usd_sum_diff"]}
                    for row in top.collect()
                ]

            report["reconciliation"][label] = result

        reconcile(["order_date"], "by_order_date")
        reconcile(["product_id"], "by_product_id")
        reconcile(["country"], "by_country")
    else:
        report["reconciliation"]["status"] = "skipped"
        report["reconciliation"]["reason"] = "missing orders_silver/customers_silver/fact_orders"

    spark.stop()

    if output_path is None:
        output_path = f"artifacts/run_audit/gold_truth_{run_id}.json"

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, default=_json_safe)

    logger.info("Gold truth report written to %s", output_file)
    return report


def main(args: argparse.Namespace | None = None) -> None:
    if args is None or not hasattr(args, "config"):
        args = parse_args()

    report = run_gold_truth_tests(
        config_path=args.config,
        env=args.env,
        output_path=getattr(args, "output", None),
        window_days=getattr(args, "window_days", None),
    )

    # Print a short summary for CLI users
    failures = 0
    for _, result in report.get("null_duplicate_checks", {}).items():
        if isinstance(result, dict) and result.get("status") == "fail":
            failures += 1
    for result in report.get("join_checks", []):
        if isinstance(result, dict) and result.get("status") == "fail":
            failures += 1
    for _, result in report.get("contract_checks", {}).items():
        if isinstance(result, dict) and result.get("status") == "fail":
            failures += 1
    for _, result in report.get("reconciliation", {}).items():
        if isinstance(result, dict) and result.get("mismatched_groups_total"):
            failures += int(result.get("mismatched_groups_total", 0))

    logger.info("Gold truth tests complete. Failures flagged: %s", failures)


if __name__ == "__main__":
    main()
