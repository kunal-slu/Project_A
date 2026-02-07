"""
Publish Gold to Snowflake Job

Reads Gold Delta/Parquet tables from S3 and writes to Snowflake.
"""

import argparse
import logging

from project_a.utils.config import load_config
from project_a.utils.spark_session import get_spark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Publish Gold to Snowflake")
    parser.add_argument("--env", required=True, help="Environment (dev/staging/prod)")
    parser.add_argument("--config", required=True, help="Config file path (local or s3://...)")
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    cfg = load_config(args.config, args.env)

    spark = get_spark(app_name="publish_gold_to_snowflake", config=cfg)

    gold_root = cfg["paths"]["gold_root"]
    sf_cfg = cfg.get("sinks", {}).get("snowflake", {})

    if not sf_cfg:
        raise ValueError("Snowflake sink configuration not found in config")

    options = {
        "sfURL": sf_cfg["url"],
        "sfUser": sf_cfg["user"],
        "sfPassword": sf_cfg["password"],
        "sfDatabase": sf_cfg["database"],
        "sfSchema": sf_cfg["schema"],
        "sfWarehouse": sf_cfg["warehouse"],
        "sfRole": sf_cfg.get("role", "SYSADMIN"),
    }

    logger.info("📥 Reading Gold tables from S3...")

    # Read Gold tables
    fact_orders = spark.read.parquet(f"{gold_root}/fact_orders/")
    dim_customer = spark.read.parquet(f"{gold_root}/dim_customer/")
    dim_product = spark.read.parquet(f"{gold_root}/dim_product/")

    logger.info(f"📊 fact_orders: {fact_orders.count():,} rows")
    logger.info(f"📊 dim_customer: {dim_customer.count():,} rows")
    logger.info(f"📊 dim_product: {dim_product.count():,} rows")

    logger.info("💾 Writing to Snowflake...")

    # Write to Snowflake
    (
        fact_orders.write.mode("overwrite")
        .format("snowflake")
        .options(**options)
        .option("dbtable", "FACT_ORDERS")
        .save()
    )

    logger.info("✅ Published fact_orders to Snowflake")

    (
        dim_customer.write.mode("overwrite")
        .format("snowflake")
        .options(**options)
        .option("dbtable", "DIM_CUSTOMER")
        .save()
    )

    logger.info("✅ Published dim_customer to Snowflake")

    (
        dim_product.write.mode("overwrite")
        .format("snowflake")
        .options(**options)
        .option("dbtable", "DIM_PRODUCT")
        .save()
    )

    logger.info("✅ Published dim_product to Snowflake")
    logger.info("✅ All Gold tables published to Snowflake successfully")


if __name__ == "__main__":
    main()
