"""
Publish Gold to Redshift Job

Reads Gold Delta/Parquet tables from S3 and writes to Redshift using JDBC.
"""

import argparse
import logging

from project_a.utils.config import load_config
from project_a.utils.spark_session import get_spark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Publish Gold to Redshift")
    parser.add_argument("--env", required=True, help="Environment (dev/staging/prod)")
    parser.add_argument("--config", required=True, help="Config file path (local or s3://...)")
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    cfg = load_config(args.config, args.env)

    spark = get_spark(app_name="publish_gold_to_redshift", config=cfg)

    gold_root = cfg["paths"]["gold_root"]
    redshift_cfg = cfg.get("sinks", {}).get("redshift", {})

    if not redshift_cfg:
        raise ValueError("Redshift sink configuration not found in config")

    jdbc_url = redshift_cfg["jdbc_url"]
    user = redshift_cfg["user"]
    password = redshift_cfg["password"]

    common_opts = {
        "url": jdbc_url,
        "user": user,
        "password": password,
        "driver": "com.amazon.redshift.jdbc.Driver",
    }

    logger.info("📥 Reading Gold tables from S3...")

    # Read Gold tables
    fact_orders = spark.read.parquet(f"{gold_root}/fact_orders/")
    dim_customer = spark.read.parquet(f"{gold_root}/dim_customer/")
    dim_product = spark.read.parquet(f"{gold_root}/dim_product/")

    logger.info(f"📊 fact_orders: {fact_orders.count():,} rows")
    logger.info(f"📊 dim_customer: {dim_customer.count():,} rows")
    logger.info(f"📊 dim_product: {dim_product.count():,} rows")

    logger.info("💾 Writing to Redshift...")

    # Write to Redshift
    fact_orders.write.mode("overwrite").format("jdbc").options(
        **common_opts, dbtable="public.fact_orders"
    ).save()

    logger.info("✅ Published fact_orders to Redshift")

    dim_customer.write.mode("overwrite").format("jdbc").options(
        **common_opts, dbtable="public.dim_customer"
    ).save()

    logger.info("✅ Published dim_customer to Redshift")

    dim_product.write.mode("overwrite").format("jdbc").options(
        **common_opts, dbtable="public.dim_product"
    ).save()

    logger.info("✅ Published dim_product to Redshift")
    logger.info("✅ All Gold tables published to Redshift successfully")


if __name__ == "__main__":
    main()
