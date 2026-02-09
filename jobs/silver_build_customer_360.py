"""Build a simple customer_360 table from silver inputs."""

from __future__ import annotations

import logging

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from project_a.monitoring.metrics_collector import emit_metrics

logger = logging.getLogger(__name__)


class GreatExpectationsRunner:
    """Minimal GE runner shim for tests."""

    def __init__(self, *_args, **_kwargs):
        pass

    def init_context(self):
        return None

    def run_checkpoint(self, *_args, **_kwargs):
        return {"success": True}


def _try_read_delta(spark, path: str):
    try:
        return spark.read.format("delta").load(path)
    except Exception:
        try:
            return spark.read.parquet(path)
        except Exception:
            return None


def build_customer_360(spark, config: dict):
    silver_base = config.get("data_lake", {}).get("silver_path", "")
    gold_base = config.get("data_lake", {}).get("gold_path", "")
    strict_mode = bool(
        (config.get("dq", {}) or {}).get("fail_on_error", False)
        or config.get("strict_mode", False)
    )

    contacts = _try_read_delta(spark, f"{silver_base}/crm/contacts")
    accounts = _try_read_delta(spark, f"{silver_base}/crm/accounts")
    orders = _try_read_delta(spark, f"{silver_base}/snowflake/orders")
    behavior = _try_read_delta(spark, f"{silver_base}/behavior")

    if contacts is None or accounts is None:
        if strict_mode:
            raise ValueError("Missing contacts or accounts input for customer_360 build")
        logger.warning("Missing contacts or accounts input; returning empty customer_360")
        empty_schema = StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("account_id", StringType(), True),
                StructField("lifetime_value_usd", DecimalType(18, 2), True),
                StructField("total_orders", StringType(), True),
            ]
        )
        return spark.createDataFrame([], empty_schema)

    joined = contacts.join(accounts, "account_id", "left")

    if orders is not None:
        order_agg = orders.groupBy("customer_id").agg(
            F.sum(F.col("total_amount").cast("decimal(18,2)")).alias("lifetime_value_usd"),
            F.count("*").cast("string").alias("total_orders"),
        )
        joined = joined.join(order_agg, joined.contact_id == order_agg.customer_id, "left")
    else:
        logger.warning("Orders input missing; defaulting order metrics to 0")
        joined = joined.withColumn("lifetime_value_usd", F.lit(0).cast("decimal(18,2)")).withColumn(
            "total_orders", F.lit("0")
        )

    if behavior is not None and "customer_id" in behavior.columns:
        behavior_agg = behavior.groupBy("customer_id").agg(F.count("*").alias("behavior_events"))
        joined = joined.join(behavior_agg, joined.contact_id == behavior_agg.customer_id, "left")
    elif behavior is None:
        logger.warning("Behavior input missing; skipping behavior aggregation")

    result = joined.select(
        F.col("contact_id").alias("customer_id"),
        F.col("account_id"),
        F.coalesce(F.col("lifetime_value_usd"), F.lit(0).cast("decimal(18,2)")).alias(
            "lifetime_value_usd"
        ),
        F.coalesce(F.col("total_orders"), F.lit("0")).alias("total_orders"),
    )

    result.write.format("delta").mode("overwrite").save(f"{gold_base}/customer_360")
    emit_metrics("silver_build_customer_360", 0, result.count(), 0.0, "pass", config=config)
    return result
