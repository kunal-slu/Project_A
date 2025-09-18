#!/usr/bin/env python3
"""
Simple ETL Pipeline Runner
Runs a simplified ETL pipeline that works well with mock Spark environments.
"""

import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from pyspark_interview_project import (
    build_spark,
    load_config_resolved,
    extract_customers,
    extract_products,
    extract_orders_json,
    extract_returns,
    extract_exchange_rates,
    extract_inventory_snapshots,
    write_delta,
    write_parquet
)

def run_simple_etl():
    """Run a simplified ETL pipeline suitable for mock environments."""

    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        logger.info("🚀 Starting Simple ETL Pipeline")

        # Step 1: Load configuration
        logger.info("📋 Step 1: Loading configuration...")
        config = load_config_resolved("config/config-dev.yaml")
        logger.info("✅ Configuration loaded")

        # Step 2: Initialize Spark
        logger.info("⚡ Step 2: Initializing Spark session...")
        spark = build_spark(config)
        logger.info("✅ Spark session ready")

        # Step 3: Extract data
        logger.info("📥 Step 3: Extracting data...")
        customers = extract_customers(spark, config["input"]["customer_path"])
        products = extract_products(spark, config["input"]["product_path"])
        orders = extract_orders_json(spark, config["input"]["orders_path"])
        returns = extract_returns(spark, config["input"].get("returns_path", "data/input_data/returns.json"))
        rates = extract_exchange_rates(spark, config["input"].get("exchange_rates_path", "data/input_data/exchange_rates.csv"))
        inventory = extract_inventory_snapshots(spark, config["input"].get("inventory_path", "data/input_data/inventory_snapshots.csv"))
        logger.info("✅ Data extraction complete")

        # Step 4: Load to Bronze layer (raw data)
        logger.info("🥉 Step 4: Loading to Bronze layer...")
        bronze_base = config["output"].get("bronze_path", "data/lakehouse/bronze")
        write_delta(customers, f"{bronze_base}/customers_raw", mode="overwrite")
        write_delta(products, f"{bronze_base}/products_raw", mode="overwrite")
        write_delta(orders, f"{bronze_base}/orders_raw", mode="overwrite")
        write_delta(returns, f"{bronze_base}/returns_raw", mode="overwrite")
        write_delta(rates, f"{bronze_base}/fx_rates", mode="overwrite")
        write_delta(inventory, f"{bronze_base}/inventory_snapshots", mode="overwrite")
        logger.info("✅ Bronze layer loaded")

        # Step 5: Simple transformations (avoiding UDFs)
        logger.info("🔄 Step 5: Simple data transformations...")

        # Basic column selection and filtering (no UDFs)
        customers_simple = customers.select("*")
        products_simple = products.select("*")
        orders_simple = orders.select("*")

        logger.info("✅ Simple transformations complete")

        # Step 6: Load to Silver layer
        logger.info("🥈 Step 6: Loading to Silver layer...")
        silver_base = config["output"].get("silver_path", "data/lakehouse/silver")
        write_delta(customers_simple, f"{silver_base}/customers_enriched", mode="overwrite")
        write_delta(products_simple, f"{silver_base}/products_enriched", mode="overwrite")
        write_delta(orders_simple, f"{silver_base}/orders_enriched", mode="overwrite")
        logger.info("✅ Silver layer loaded")

        # Step 7: Load to Gold layer and final output
        logger.info("🥇 Step 7: Loading to Gold layer...")
        gold_base = config["output"].get("gold_path", "data/lakehouse/gold")
        write_delta(orders_simple, f"{gold_base}/fact_orders", mode="overwrite")

        # Final output
        write_parquet(orders_simple, config["output"]["parquet_path"])
        write_delta(orders_simple, config["output"]["delta_path"], mode="overwrite")
        logger.info("✅ Gold layer and final output loaded")

        logger.info("🎉 Simple ETL Pipeline completed successfully!")
        return True

    except Exception as e:
        logger.error(f"❌ ETL Pipeline failed: {str(e)}")
        raise
    finally:
        try:
            spark.stop()
            logger.info("🔌 Spark session stopped")
        except:
            pass

if __name__ == "__main__":
    success = run_simple_etl()
    sys.exit(0 if success else 1)
