"""Build a customer_360 table from silver inputs (canonical + backward compatible)."""

from __future__ import annotations

import logging
from typing import Any, Iterable

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DecimalType, IntegerType, StringType, StructField, StructType

from project_a.dq.great_expectations_runner import GreatExpectationsRunner
from project_a.monitoring.metrics_collector import emit_metrics

logger = logging.getLogger(__name__)

def _storage_format(config: dict[str, Any]) -> str:
    storage_cfg = config.get("storage", {}) or {}
    fmt = storage_cfg.get("format")
    if fmt:
        return str(fmt).lower()
    if (config.get("iceberg") or {}).get("enabled"):
        return "iceberg"
    return "parquet"


def _get_root_paths(config: dict[str, Any]) -> tuple[str, str]:
    paths_cfg = config.get("paths", {}) or {}
    data_lake_cfg = config.get("data_lake", {}) or {}
    silver_root = (
        paths_cfg.get("silver_root")
        or paths_cfg.get("silver")
        or data_lake_cfg.get("silver_path")
        or "data/silver"
    )
    gold_root = (
        paths_cfg.get("gold_root")
        or paths_cfg.get("gold")
        or data_lake_cfg.get("gold_path")
        or "data/gold"
    )
    return silver_root, gold_root


def _read_with_fallbacks(spark, fmt: str, path: str, table_name: str | None = None):
    attempts: list[str] = []
    if fmt == "iceberg":
        attempts.extend(["iceberg", "delta", "parquet"])
    elif fmt == "delta":
        attempts.extend(["delta", "parquet"])
    else:
        attempts.extend(["parquet", "delta"])

    last_error: Exception | None = None
    for attempt in attempts:
        try:
            if attempt == "iceberg":
                if not table_name:
                    raise ValueError("Missing table_name for iceberg read")
                return spark.read.format("iceberg").load(table_name)
            if attempt == "delta":
                return spark.read.format("delta").load(path)
            return spark.read.parquet(path)
        except Exception as exc:
            last_error = exc
            logger.debug("Read attempt failed (%s): %s", attempt, exc)
    return None, last_error


def _safe_read(
    spark, fmt: str, path: str, table_name: str | None = None
) -> DataFrame | None:
    result = _read_with_fallbacks(spark, fmt, path, table_name)
    if isinstance(result, tuple):
        df, _err = result
        return df
    return result


def _coalesce_amount_column(df: DataFrame, candidates: Iterable[str]) -> F.Column:
    col = None
    for name in candidates:
        if name in df.columns:
            col = F.col(name).cast("decimal(18,2)")
            break
    if col is None:
        col = F.lit(0).cast("decimal(18,2)")
    return col


def build_customer_360(spark, config: dict):
    fmt = _storage_format(config)
    silver_base, gold_base = _get_root_paths(config)
    strict_mode = bool(
        (config.get("dq", {}) or {}).get("fail_on_error", False)
        or config.get("strict_mode", False)
    )

    iceberg_cfg = config.get("iceberg", {}) or {}
    catalog = iceberg_cfg.get("catalog_name", "local")

    # Canonical silver tables (preferred)
    customers = _safe_read(
        spark, fmt, f"{silver_base}/customers_silver", f"{catalog}.customers_silver"
    )
    orders = _safe_read(
        spark, fmt, f"{silver_base}/orders_silver", f"{catalog}.orders_silver"
    )
    if orders is None:
        orders = _safe_read(spark, fmt, f"{silver_base}/snowflake/orders")

    behavior = _safe_read(
        spark, fmt, f"{silver_base}/customer_behavior_silver", f"{catalog}.customer_behavior_silver"
    )
    if behavior is None:
        behavior = _safe_read(spark, fmt, f"{silver_base}/behavior")

    # Legacy CRM fallback (if canonical customer table missing)
    contacts = _safe_read(spark, fmt, f"{silver_base}/crm/contacts")
    accounts = _safe_read(spark, fmt, f"{silver_base}/crm/accounts")

    if customers is None and (contacts is None or accounts is None):
        if strict_mode:
            raise ValueError(
                "Missing customers_silver (preferred) and CRM contacts/accounts fallback inputs"
            )
        logger.warning("Missing contacts or accounts input; returning empty customer_360")
        empty_schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("account_id", StringType(), True),
                StructField("lifetime_value_usd", DecimalType(18, 2), True),
                StructField("total_orders", IntegerType(), True),
                StructField("behavior_events", IntegerType(), True),
            ]
        )
        return spark.createDataFrame([], empty_schema)

    if customers is not None:
        base_df = customers.select(
            "customer_id",
            F.col("country").alias("customer_country"),
        )
    else:
        base_df = (
            contacts.join(accounts, "account_id", "left")
            .select(
                F.col("contact_id").alias("customer_id"),
                F.col("account_id"),
                F.col("industry").alias("customer_country"),
            )
            .dropDuplicates(["customer_id"])
        )

    if orders is not None:
        amount_col = _coalesce_amount_column(
            orders, ["order_amount_usd", "total_amount_usd", "total_amount", "amount_usd", "amount"]
        )
        order_agg = orders.groupBy("customer_id").agg(
            F.sum(amount_col).alias("lifetime_value_usd"),
            F.countDistinct("order_id").cast("int").alias("total_orders"),
        )
        base_df = base_df.join(order_agg, "customer_id", "left")
    else:
        logger.warning("Orders input missing; defaulting order metrics to 0")
        base_df = base_df.withColumn("lifetime_value_usd", F.lit(0).cast("decimal(18,2)")).withColumn(
            "total_orders", F.lit(0).cast("int")
        )

    if behavior is not None and "customer_id" in behavior.columns:
        behavior_agg = behavior.groupBy("customer_id").agg(
            F.count("*").cast("int").alias("behavior_events")
        )
        base_df = base_df.join(behavior_agg, "customer_id", "left")
    elif behavior is None:
        logger.warning("Behavior input missing; skipping behavior aggregation")

    result = base_df.select(
        "customer_id",
        F.col("account_id"),
        F.coalesce(F.col("lifetime_value_usd"), F.lit(0).cast("decimal(18,2)")).alias(
            "lifetime_value_usd"
        ),
        F.coalesce(F.col("total_orders"), F.lit(0).cast("int")).alias("total_orders"),
        F.coalesce(F.col("behavior_events"), F.lit(0).cast("int")).alias("behavior_events"),
    )

    # Optional DQ gate via GE checkpoint if configured
    dq_cfg = config.get("dq", {}) or {}
    checkpoint_name = dq_cfg.get("checkpoint_customer_360") or dq_cfg.get("checkpoint")
    if checkpoint_name:
        ge_runner = GreatExpectationsRunner()
        ge_runner.init_context()
        ge_runner.run_checkpoint(checkpoint_name, fail_on_error=bool(dq_cfg.get("fail_on_error", True)))

    output_path = f"{gold_base}/customer_360"
    if fmt == "iceberg":
        try:
            from project_a.iceberg_utils import IcebergWriter

            IcebergWriter(spark, catalog).write_overwrite(result, "customer_360")
        except Exception as exc:
            logger.warning("Iceberg write failed, falling back to parquet: %s", exc)
            result.write.mode("overwrite").parquet(output_path)
    elif fmt == "delta":
        result.write.format("delta").mode("overwrite").save(output_path)
    else:
        result.write.mode("overwrite").parquet(output_path)

    emit_metrics("silver_build_customer_360", 0, result.count(), 0.0, "pass", config=config)
    return result
