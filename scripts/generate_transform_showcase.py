#!/usr/bin/env python3
"""
Generate a transformation showcase with realistic data samples and PySpark optimizations.

Outputs:
  - artifacts/transform_demo/*.csv (sample outputs)
  - artifacts/transform_demo/explain_join_plan.txt
  - artifacts/transform_demo/optimization_settings.json
  - docs/reports/TRANSFORMATION_SHOWCASE.md
"""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from project_a.schemas.bronze_schemas import (
    SNOWFLAKE_CUSTOMERS_SCHEMA,
    SNOWFLAKE_ORDERS_SCHEMA,
    SNOWFLAKE_PRODUCTS_SCHEMA,
)
from project_a.utils.config import load_config_resolved
from project_a.utils.spark_session import build_spark


def _write_sample(df, out_path: Path, limit: int = 10):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.limit(limit).coalesce(1).write.mode("overwrite").option("header", "true").csv(str(out_path))


def _markdown_table_from_csv(csv_dir: Path, max_rows: int = 8) -> str:
    # Read the first CSV file from Spark output directory
    files = list(csv_dir.glob("*.csv"))
    if not files:
        return "_No sample available._"
    csv_path = files[0]
    rows = csv_path.read_text(encoding="utf-8").splitlines()
    if not rows:
        return "_No sample available._"
    header = rows[0].split(",")
    body = [r.split(",") for r in rows[1 : max_rows + 1]]
    md = []
    md.append("| " + " | ".join(header) + " |")
    md.append("| " + " | ".join(["---"] * len(header)) + " |")
    for row in body:
        md.append("| " + " | ".join(row) + " |")
    return "\n".join(md)


def main() -> None:
    cfg = load_config_resolved("local/config/local.yaml")
    spark = build_spark(app_name="transform_showcase", config=cfg)

    try:
        bronze_root = cfg.get("paths", {}).get("bronze_root") or cfg.get("paths", {}).get("bronze") or "data/bronze"
        snowflake_base = cfg.get("sources", {}).get("snowflake", {}).get("base_path", f"{bronze_root}/snowflake")
        snowflake_files = cfg.get("sources", {}).get("snowflake", {}).get("files", {})

        customers_path = f"{snowflake_base}/{snowflake_files.get('customers', 'snowflake_customers_50000.csv')}"
        orders_path = f"{snowflake_base}/{snowflake_files.get('orders', 'snowflake_orders_100000.csv')}"
        products_path = f"{snowflake_base}/{snowflake_files.get('products', 'snowflake_products_10000.csv')}"

        customers_bronze = (
            spark.read.schema(SNOWFLAKE_CUSTOMERS_SCHEMA)
            .option("header", "true")
            .csv(customers_path)
        )
        orders_bronze = (
            spark.read.schema(SNOWFLAKE_ORDERS_SCHEMA)
            .option("header", "true")
            .csv(orders_path)
        )
        products_bronze = (
            spark.read.schema(SNOWFLAKE_PRODUCTS_SCHEMA)
            .option("header", "true")
            .csv(products_path)
        )

        # --- Silver-style cleaning ---
        customers_clean = customers_bronze.select(
            F.col("customer_id"),
            F.trim(F.lower(F.col("first_name"))).alias("first_name"),
            F.trim(F.lower(F.col("last_name"))).alias("last_name"),
            F.lower(F.regexp_replace(F.col("email"), r"\s+", "")).alias("email"),
            F.upper(F.col("country")).alias("country"),
            F.to_date("registration_date").alias("registration_date"),
        )

        orders_clean = orders_bronze.select(
            F.col("order_id"),
            F.col("customer_id"),
            F.col("product_id"),
            F.to_date("order_date").alias("order_date"),
            F.col("total_amount").cast("double").alias("total_amount"),
            F.col("quantity").cast("int").alias("quantity"),
            F.col("status"),
            F.coalesce(F.col("updated_at"), F.col("order_timestamp")).alias("updated_at"),
        )

        # Deduplicate using window (latest update wins)
        window = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc_nulls_last())
        orders_dedup = orders_clean.withColumn("rn", F.row_number().over(window)).filter(F.col("rn") == 1).drop("rn")

        # Optimization: cache + repartition
        orders_dedup = orders_dedup.repartition("order_date").cache()
        orders_dedup.count()

        # --- Gold-style fact table ---
        products_dim = products_bronze.select(
            F.col("product_id"),
            F.col("product_name"),
            F.col("category"),
            F.col("price_usd").cast("double").alias("price_usd"),
        )

        fact_orders = (
            orders_dedup.join(F.broadcast(products_dim), on="product_id", how="left")
            .join(customers_clean.select("customer_id", "country"), on="customer_id", how="left")
            .withColumn("order_month", F.month("order_date"))
            .withColumn("order_year", F.year("order_date"))
        )

        # Customer metrics
        customer_metrics = (
            fact_orders.groupBy("customer_id")
            .agg(
                F.count("*").alias("order_count"),
                F.sum("total_amount").alias("total_revenue"),
                F.avg("total_amount").alias("avg_order_value"),
                F.max("order_date").alias("last_order_date"),
            )
            .orderBy(F.desc("total_revenue"))
        )

        # Write samples
        out_root = Path("artifacts/transform_demo")
        _write_sample(orders_bronze, out_root / "bronze_orders_sample")
        _write_sample(orders_dedup, out_root / "silver_orders_sample")
        _write_sample(fact_orders, out_root / "gold_fact_orders_sample")
        _write_sample(customer_metrics, out_root / "gold_customer_metrics_sample")

        # Explain plan
        explain_path = out_root / "explain_join_plan.txt"
        explain_path.write_text(fact_orders._jdf.queryExecution().toString(), encoding="utf-8")

        # Optimization settings
        opt_settings = {
            "spark.sql.adaptive.enabled": spark.conf.get("spark.sql.adaptive.enabled", "unknown"),
            "spark.sql.shuffle.partitions": spark.conf.get("spark.sql.shuffle.partitions", "unknown"),
            "spark.sql.autoBroadcastJoinThreshold": spark.conf.get(
                "spark.sql.autoBroadcastJoinThreshold", "unknown"
            ),
        }
        (out_root / "optimization_settings.json").write_text(
            json.dumps(opt_settings, indent=2), encoding="utf-8"
        )

        # Realism stats
        realism = {
            "orders_min_date": str(orders_dedup.select(F.min("order_date")).collect()[0][0]),
            "orders_max_date": str(orders_dedup.select(F.max("order_date")).collect()[0][0]),
            "distinct_customers": int(orders_dedup.select(F.approx_count_distinct("customer_id")).collect()[0][0]),
            "distinct_products": int(orders_dedup.select(F.approx_count_distinct("product_id")).collect()[0][0]),
        }

        # Build markdown report
        report_path = Path("docs/reports/TRANSFORMATION_SHOWCASE.md")
        report_path.parent.mkdir(parents=True, exist_ok=True)

        report_md = []
        report_md.append("# Transformation Showcase (PySpark)\n")
        report_md.append("This report shows realistic transformations and PySpark optimizations on Project A data.\n")
        report_md.append("## Data realism summary\n")
        report_md.append(f"- Orders date range: {realism['orders_min_date']} → {realism['orders_max_date']}")
        report_md.append(f"- Approx distinct customers: {realism['distinct_customers']:,}")
        report_md.append(f"- Approx distinct products: {realism['distinct_products']:,}\n")

        report_md.append("## Bronze sample (orders)\n")
        report_md.append(_markdown_table_from_csv(out_root / "bronze_orders_sample"))
        report_md.append("\n## Silver sample (deduped + cleaned)\n")
        report_md.append(_markdown_table_from_csv(out_root / "silver_orders_sample"))
        report_md.append("\n## Gold sample (fact_orders)\n")
        report_md.append(_markdown_table_from_csv(out_root / "gold_fact_orders_sample"))
        report_md.append("\n## Gold sample (customer metrics)\n")
        report_md.append(_markdown_table_from_csv(out_root / "gold_customer_metrics_sample"))

        report_md.append("\n## Optimizations used\n")
        report_md.append("- Broadcast join for product dimension (`broadcast`)")
        report_md.append("- Repartition on `order_date` before heavy joins")
        report_md.append("- Cache to avoid recomputation")
        report_md.append("- AQE enabled (adaptive query execution)")

        report_md.append("\n## Explain plan (join)\n")
        report_md.append(f"See: `{explain_path}`\n")

        report_path.write_text("\n".join(report_md), encoding="utf-8")

        print(f"Wrote report to {report_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
