#!/usr/bin/env python3
"""
Comprehensive Data Quality Validation Job

Runs all DQ checks:
1. Schema drift
2. Referential integrity
3. Primary key uniqueness
4. Null analysis
5. Timestamp validation
6. Semantic validation
7. Distribution profiling
8. Incremental ETL readiness
9. Kafka streaming fitness
10. Performance optimization
"""
import sys
import argparse
import logging
from pathlib import Path
from typing import Dict, Any

# Add src to path
project_root = Path(__file__).parent.parent.parent
src_path = project_root / "src"
if src_path.exists() and str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from pyspark.sql import SparkSession
from project_a.utils.spark_session import build_spark
from project_a.config_loader import load_config_resolved
from project_a.utils.path_resolver import resolve_data_path
from project_a.dq.comprehensive_validator import ComprehensiveValidator
from project_a.schemas.bronze_schemas import (
    CRM_ACCOUNTS_SCHEMA, CRM_CONTACTS_SCHEMA, CRM_OPPORTUNITIES_SCHEMA,
    REDSHIFT_BEHAVIOR_SCHEMA, SNOWFLAKE_CUSTOMERS_SCHEMA,
    SNOWFLAKE_ORDERS_SCHEMA, SNOWFLAKE_PRODUCTS_SCHEMA,
    KAFKA_EVENTS_SCHEMA, FX_RATES_SCHEMA
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_bronze_tables(spark: SparkSession, config: Dict[str, Any]) -> Dict[str, Any]:
    """Load all bronze tables."""
    bronze_root = resolve_data_path(config, "bronze")
    sources = config.get("sources", {})
    
    bronze_data = {}
    
    # Load CRM
    try:
        crm_base = sources.get("crm", {}).get("base_path", f"{bronze_root}/crm")
        bronze_data["accounts"] = spark.read.format("delta").load(f"{crm_base}/accounts").option("mergeSchema", "true")
        bronze_data["contacts"] = spark.read.format("delta").load(f"{crm_base}/contacts").option("mergeSchema", "true")
        bronze_data["opportunities"] = spark.read.format("delta").load(f"{crm_base}/opportunities").option("mergeSchema", "true")
    except Exception as e:
        logger.warning(f"Could not load CRM bronze: {e}")
    
    # Load Snowflake
    try:
        snowflake_base = sources.get("snowflake", {}).get("base_path", f"{bronze_root}/snowflake")
        bronze_data["customers"] = spark.read.format("delta").load(f"{snowflake_base}/customers").option("mergeSchema", "true")
        bronze_data["orders"] = spark.read.format("delta").load(f"{snowflake_base}/orders").option("mergeSchema", "true")
        bronze_data["products"] = spark.read.format("delta").load(f"{snowflake_base}/products").option("mergeSchema", "true")
    except Exception as e:
        logger.warning(f"Could not load Snowflake bronze: {e}")
    
    # Load Redshift
    try:
        redshift_base = sources.get("redshift", {}).get("base_path", f"{bronze_root}/redshift")
        bronze_data["behavior"] = spark.read.format("delta").load(f"{redshift_base}/behavior").option("mergeSchema", "true")
    except Exception as e:
        logger.warning(f"Could not load Redshift bronze: {e}")
    
    # Load Kafka
    try:
        kafka_base = sources.get("kafka_sim", {}).get("base_path", f"{bronze_root}/kafka")
        bronze_data["kafka_events"] = spark.read.format("delta").load(f"{kafka_base}/events").option("mergeSchema", "true")
    except Exception as e:
        logger.warning(f"Could not load Kafka bronze: {e}")
    
    # Load FX
    try:
        fx_base = sources.get("fx", {}).get("base_path", f"{bronze_root}/fx")
        bronze_data["fx_rates"] = spark.read.format("delta").load(f"{fx_base}/rates").option("mergeSchema", "true")
    except Exception as e:
        logger.warning(f"Could not load FX bronze: {e}")
    
    return bronze_data


def load_silver_tables(spark: SparkSession, config: Dict[str, Any]) -> Dict[str, Any]:
    """Load all silver tables."""
    silver_root = resolve_data_path(config, "silver")
    tables_config = config.get("tables", {}).get("silver", {})
    
    silver_data = {}
    table_names = {
        "customers": tables_config.get("customers", "customers_silver"),
        "orders": tables_config.get("orders", "orders_silver"),
        "products": tables_config.get("products", "products_silver"),
        "behavior": tables_config.get("behavior", "customer_behavior_silver"),
        "fx_rates": tables_config.get("fx_rates", "fx_rates_silver"),
        "order_events": tables_config.get("order_events", "order_events_silver")
    }
    
    for key, table_name in table_names.items():
        try:
            silver_data[key] = spark.read.format("delta").load(f"{silver_root}/{table_name}").option("mergeSchema", "true")
        except Exception as e:
            logger.warning(f"Could not load {table_name}: {e}")
    
    return silver_data


def load_gold_tables(spark: SparkSession, config: Dict[str, Any]) -> Dict[str, Any]:
    """Load all gold tables."""
    gold_root = resolve_data_path(config, "gold")
    tables_config = config.get("tables", {}).get("gold", {})
    
    gold_data = {}
    table_names = {
        "fact_orders": tables_config.get("fact_orders", "fact_orders"),
        "dim_customer": tables_config.get("dim_customer", "dim_customer"),
        "dim_product": tables_config.get("dim_product", "dim_product"),
        "customer_360": tables_config.get("fact_customer_24m", "customer_360")
    }
    
    for key, table_name in table_names.items():
        try:
            gold_data[key] = spark.read.format("delta").load(f"{gold_root}/{table_name}").option("mergeSchema", "true")
        except Exception as e:
            logger.warning(f"Could not load {table_name}: {e}")
    
    return gold_data


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Comprehensive Data Quality Validation")
    parser.add_argument("--env", default="dev", help="Environment")
    parser.add_argument("--config", help="Config file path")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "all"], default="all", help="Layer to validate")
    parser.add_argument("--output", help="Output path for report")
    args = parser.parse_args()
    
    # Load config
    if args.config:
        config = load_config_resolved(args.config)
    else:
        config_path = Path(f"config/{args.env}.yaml")
        if not config_path.exists():
            config_path = Path("config/local.yaml")
        config = load_config_resolved(str(config_path))
    
    # Build Spark
    spark = build_spark(config)
    
    try:
        validator = ComprehensiveValidator(spark)
        
        # Load expected schemas
        expected_schemas = {
            "accounts": CRM_ACCOUNTS_SCHEMA,
            "contacts": CRM_CONTACTS_SCHEMA,
            "opportunities": CRM_OPPORTUNITIES_SCHEMA,
            "behavior": REDSHIFT_BEHAVIOR_SCHEMA,
            "customers": SNOWFLAKE_CUSTOMERS_SCHEMA,
            "orders": SNOWFLAKE_ORDERS_SCHEMA,
            "products": SNOWFLAKE_PRODUCTS_SCHEMA,
            "kafka_events": KAFKA_EVENTS_SCHEMA,
            "fx_rates": FX_RATES_SCHEMA
        }
        
        # Validate based on layer
        if args.layer in ["bronze", "all"]:
            logger.info("Loading bronze tables...")
            bronze_data = load_bronze_tables(spark, config)
            validator.validate_bronze_layer(bronze_data, expected_schemas)
        
        if args.layer in ["silver", "all"]:
            logger.info("Loading silver tables...")
            silver_data = load_silver_tables(spark, config)
            bronze_data = load_bronze_tables(spark, config) if args.layer == "silver" else bronze_data
            validator.validate_silver_layer(silver_data, bronze_data)
        
        if args.layer in ["gold", "all"]:
            logger.info("Loading gold tables...")
            gold_data = load_gold_tables(spark, config)
            silver_data = load_silver_tables(spark, config) if args.layer == "gold" else silver_data
            validator.validate_gold_layer(gold_data, silver_data)
        
        # Generate report
        report = validator.generate_comprehensive_report()
        summary = validator.get_summary()
        
        logger.info("\n" + report)
        logger.info(f"\nSummary: {summary}")
        
        # Write report to file if output specified
        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(report, encoding="utf-8")
            logger.info(f"Report written to {args.output}")
        
        # Exit with error code if any layer failed
        if summary["layers_failed"] > 0:
            sys.exit(1)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

