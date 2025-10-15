#!/usr/bin/env python3
"""
Step-by-Step ETL Pipeline Runner
Runs each stage of the ETL pipeline with detailed logging.
"""

import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from pyspark_interview_project import (
    get_spark_session,
    load_config_resolved,
    extract_customers,
    extract_products,
    extract_orders_json,
    extract_returns,
    extract_exchange_rates,
    extract_inventory_snapshots,
    enrich_customers,
    enrich_products,
    clean_orders,
    normalize_currency,
    join_returns,
    join_inventory,
    write_delta,
    write_parquet,
    create_performance_optimizer
)

def run_step_by_step_etl():
    """Run ETL pipeline step by step with detailed logging."""

    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        logger.info("🚀 Starting Step-by-Step ETL Pipeline")

        # Step 1: Load configuration
        logger.info("📋 Step 1: Loading configuration...")
        config = load_config_resolved("config/config-dev.yaml")
        logger.info("✅ Configuration loaded")

        # Step 2: Initialize Spark
        logger.info("⚡ Step 2: Initializing Spark session...")
        spark = get_spark_session(config)
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

        # Step 4: Performance optimization
        logger.info("⚡ Step 4: Running performance optimization...")
        optimizer = create_performance_optimizer(spark, config)
        datasets_to_optimize = {
            "customers": customers,
            "products": products,
            "orders": orders,
            "returns_raw": returns,
            "rates": rates,
            "inventory": inventory
        }
        optimized_datasets = optimizer.run_full_optimization_pipeline(datasets_to_optimize)
        logger.info("✅ Performance optimization complete")

        # Step 5: Load to Bronze layer
        logger.info("🥉 Step 5: Loading to Bronze layer...")
        bronze_base = config["output"].get("bronze_path", "data/lakehouse/bronze")
        write_delta(optimized_datasets["customers"], f"{bronze_base}/customers_raw", mode="overwrite")
        write_delta(optimized_datasets["products"], f"{bronze_base}/products_raw", mode="overwrite")
        write_delta(optimized_datasets["orders"], f"{bronze_base}/orders_raw", mode="overwrite")
        write_delta(optimized_datasets["returns_raw"], f"{bronze_base}/returns_raw", mode="overwrite")
        write_delta(optimized_datasets["rates"], f"{bronze_base}/fx_rates", mode="overwrite")
        write_delta(optimized_datasets["inventory"], f"{bronze_base}/inventory_snapshots", mode="overwrite")
        logger.info("✅ Bronze layer loaded")

        # Step 6: Transform data
        logger.info("🔄 Step 6: Transforming data...")
        enriched_customers = enrich_customers(optimized_datasets["customers"])
        enriched_products = enrich_products(optimized_datasets["products"])
        enriched_products = join_inventory(enriched_products, optimized_datasets["inventory"])
        cleaned_orders = clean_orders(optimized_datasets["orders"])
        normalized_orders = normalize_currency(cleaned_orders, optimized_datasets["rates"])
        final_orders = join_returns(normalized_orders, optimized_datasets["returns_raw"])
        logger.info("✅ Data transformation complete")

        # Step 7: Load to Silver layer
        logger.info("🥈 Step 7: Loading to Silver layer...")
        silver_base = config["output"].get("silver_path", "data/lakehouse/silver")
        write_delta(enriched_customers, f"{silver_base}/customers_enriched", mode="overwrite")
        write_delta(enriched_products, f"{silver_base}/products_enriched", mode="overwrite")
        write_delta(final_orders, f"{silver_base}/orders_enriched", mode="overwrite")
        logger.info("✅ Silver layer loaded")

        # Step 8: Load to Gold layer and final output
        logger.info("🥇 Step 8: Loading to Gold layer...")
        gold_base = config["output"].get("gold_path", "data/lakehouse/gold")
        write_delta(final_orders, f"{gold_base}/fact_orders", mode="overwrite")

        # Final output
        write_parquet(final_orders, config["output"]["parquet_path"])
        write_delta(final_orders, config["output"]["delta_path"], mode="overwrite")
        logger.info("✅ Gold layer and final output loaded")

        logger.info("🎉 Step-by-Step ETL Pipeline completed successfully!")
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
    success = run_step_by_step_etl()
    sys.exit(0 if success else 1)
