#!/usr/bin/env python3
"""
Local ETL pipeline runner - end-to-end bronze->silver->gold + DQ.
"""

import argparse
import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.utils.spark import get_spark_session, is_local
from pyspark_interview_project.utils.io import read_delta, write_delta, ensure_directory_exists
from pyspark_interview_project.utils.logging import setup_json_logging, get_trace_id, log_with_trace
from pyspark_interview_project.transforms.bronze_to_silver import (
    transform_customers_bronze_to_silver,
    transform_orders_bronze_to_silver,
    transform_products_bronze_to_silver
)
from pyspark_interview_project.transforms.silver_to_gold import (
    create_dim_customers,
    create_dim_products,
    create_fact_orders
)
from pyspark_interview_project.transforms.scd2_customers import scd2_merge_customers
from pyspark_interview_project.dq.runner import run_yaml_policy, print_dq_summary
from pyspark_interview_project.schema.validator import SchemaValidator


def main():
    """Main ETL pipeline execution."""
    parser = argparse.ArgumentParser(description="Run local ETL pipeline")
    parser.add_argument("--conf", required=True, help="Configuration file path")
    parser.add_argument("--limit", type=int, help="Limit number of rows for testing")
    args = parser.parse_args()
    
    # Setup logging
    trace_id = get_trace_id()
    logger = setup_json_logging(include_trace_id=True)
    log_with_trace(logger, "info", "Starting local ETL pipeline", trace_id)
    
    try:
        # Load configuration
        config = load_conf(args.conf)
        log_with_trace(logger, "info", f"Configuration loaded: {args.conf}", trace_id)
        
        # Create Spark session
        spark_conf = {
            "spark.sql.shuffle.partitions": config["runtime"]["shuffle_partitions"]
        }
        spark = get_spark_session(config["runtime"]["app_name"], spark_conf)
        log_with_trace(logger, "info", "Spark session created", trace_id)
        
        # Initialize schema validator
        schema_validator = SchemaValidator(spark)
        
        # Create data directories
        bronze_path = config["lake"]["bronze_path"]
        silver_path = config["lake"]["silver_path"]
        gold_path = config["lake"]["gold_path"]
        
        if is_local(config):
            ensure_directory_exists(bronze_path)
            ensure_directory_exists(silver_path)
            ensure_directory_exists(gold_path)
        
        # Step 1: Read raw data and write to bronze
        log_with_trace(logger, "info", "Step 1: Creating bronze layer", trace_id)
        
        # Read sample data (in real scenario, this would be from various sources)
        customers_df = spark.read.option("header", "true").csv("data/input_data/customers.csv")
        
        # Read JSON data with proper schema handling
        orders_df = spark.read.option("multiline", "true").json("data/input_data/orders.json")
        
        products_df = spark.read.option("header", "true").csv("data/input_data/products.csv")
        
        if args.limit:
            customers_df = customers_df.limit(args.limit)
            orders_df = orders_df.limit(args.limit)
            products_df = products_df.limit(args.limit)
        
        # Write to bronze
        write_delta(customers_df, f"{bronze_path}/customers_raw")
        write_delta(orders_df, f"{bronze_path}/orders_raw")
        write_delta(products_df, f"{bronze_path}/products_raw")
        
        bronze_counts = {
            "customers": customers_df.count(),
            "orders": orders_df.count(),
            "products": products_df.count()
        }
        log_with_trace(logger, "info", f"Bronze layer created: {bronze_counts}", trace_id)
        
        # Step 2: Transform bronze to silver
        log_with_trace(logger, "info", "Step 2: Creating silver layer", trace_id)
        
        customers_silver = transform_customers_bronze_to_silver(spark, customers_df)
        orders_silver = transform_orders_bronze_to_silver(spark, orders_df)
        products_silver = transform_products_bronze_to_silver(spark, products_df)
        
        # Write to silver
        write_delta(customers_silver, f"{silver_path}/customers_cleansed")
        write_delta(orders_silver, f"{silver_path}/orders_cleansed")
        write_delta(products_silver, f"{silver_path}/products_cleansed")
        
        silver_counts = {
            "customers": customers_silver.count(),
            "orders": orders_silver.count(),
            "products": products_silver.count()
        }
        log_with_trace(logger, "info", f"Silver layer created: {silver_counts}", trace_id)
        
        # Step 3: Run data quality checks on silver
        log_with_trace(logger, "info", "Step 3: Running data quality checks", trace_id)
        
        dq_policy_path = f"{config['dq']['policies_path']}/orders_policy.yaml"
        dq_summary = run_yaml_policy(spark, dq_policy_path, orders_silver)
        print_dq_summary(dq_summary)
        
        # Step 4: Transform silver to gold
        log_with_trace(logger, "info", "Step 4: Creating gold layer", trace_id)
        
        dim_customers = create_dim_customers(spark, customers_silver)
        dim_products = create_dim_products(spark, products_silver)
        fact_orders = create_fact_orders(spark, orders_silver, customers_silver, products_silver)
        
        # SCD2 for customers
        customers_scd2 = scd2_merge_customers(spark, customers_silver, f"{silver_path}/customers_scd2")
        
        # Write to gold
        write_delta(dim_customers, f"{gold_path}/dim_customers")
        write_delta(dim_products, f"{gold_path}/dim_products")
        write_delta(fact_orders, f"{gold_path}/fact_orders")
        write_delta(customers_scd2, f"{silver_path}/customers_scd2")
        
        gold_counts = {
            "dim_customers": dim_customers.count(),
            "dim_products": dim_products.count(),
            "fact_orders": fact_orders.count(),
            "customers_scd2": customers_scd2.count()
        }
        log_with_trace(logger, "info", f"Gold layer created: {gold_counts}", trace_id)
        
        # Print final summary
        print("\n" + "="*60)
        print("ETL PIPELINE SUMMARY")
        print("="*60)
        print(f"Bronze Layer: {bronze_counts}")
        print(f"Silver Layer: {silver_counts}")
        print(f"Gold Layer: {gold_counts}")
        print("="*60)
        
        log_with_trace(logger, "info", "ETL pipeline completed successfully", trace_id)
        
    except Exception as e:
        log_with_trace(logger, "error", f"ETL pipeline failed: {str(e)}", trace_id, error=str(e))
        sys.exit(1)
    
    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()