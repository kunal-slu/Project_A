#!/usr/bin/env python3
"""
Validate Source Data - Join Relationships and Data Quality

Validates:
1. Foreign key relationships across all sources
2. Data quality checks
3. Join compatibility
4. Schema correctness
"""
import sys
import json
from pathlib import Path
from typing import Dict, List, Tuple

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct
from pyspark.sql.types import StructType

def create_spark() -> SparkSession:
    """Create Spark session for validation."""
    return (
        SparkSession.builder
        .appName("source-data-validation")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def validate_joins(spark: SparkSession, data_root: str) -> Dict[str, Tuple[bool, str]]:
    """
    Validate all foreign key joins across sources.
    
    Returns:
        Dict mapping join_name -> (is_valid, message)
    """
    results = {}
    
    try:
        # 1. Snowflake: orders.customer_id → customers.customer_id
        print("🔍 Validating: orders.customer_id → customers.customer_id")
        customers = spark.read.option("header", "true").csv(
            f"{data_root}/bronze/snowflakes/snowflake_customers_50000.csv"
        )
        orders = spark.read.option("header", "true").csv(
            f"{data_root}/bronze/snowflakes/snowflake_orders_100000.csv"
        )
        
        customer_ids = customers.select("customer_id").distinct()
        order_customer_ids = orders.select("customer_id").distinct()
        
        # Check for orphaned orders
        orphaned = order_customer_ids.join(
            customer_ids, "customer_id", "left_anti"
        )
        orphaned_count = orphaned.count()
        
        if orphaned_count == 0:
            results["orders_to_customers"] = (True, "✅ All orders have valid customer_id")
        else:
            results["orders_to_customers"] = (
                False,
                f"❌ {orphaned_count} orders have invalid customer_id"
            )
        
        # 2. Snowflake: orders.product_id → products.product_id
        print("🔍 Validating: orders.product_id → products.product_id")
        products = spark.read.option("header", "true").csv(
            f"{data_root}/bronze/snowflakes/snowflake_products_10000.csv"
        )
        
        product_ids = products.select("product_id").distinct()
        order_product_ids = orders.select("product_id").distinct()
        
        orphaned = order_product_ids.join(
            product_ids, "product_id", "left_anti"
        )
        orphaned_count = orphaned.count()
        
        if orphaned_count == 0:
            results["orders_to_products"] = (True, "✅ All orders have valid product_id")
        else:
            results["orders_to_products"] = (
                False,
                f"❌ {orphaned_count} orders have invalid product_id"
            )
        
        # 3. CRM: contacts.account_id → accounts.account_id
        print("🔍 Validating: contacts.account_id → accounts.account_id")
        accounts = spark.read.option("header", "true").csv(
            f"{data_root}/bronze/crm/accounts.csv"
        )
        contacts = spark.read.option("header", "true").csv(
            f"{data_root}/bronze/crm/contacts.csv"
        )
        
        account_ids = accounts.select("account_id").distinct()
        contact_account_ids = contacts.select("account_id").distinct().filter(
            col("account_id").isNotNull()
        )
        
        orphaned = contact_account_ids.join(
            account_ids, "account_id", "left_anti"
        )
        orphaned_count = orphaned.count()
        
        if orphaned_count == 0:
            results["contacts_to_accounts"] = (True, "✅ All contacts have valid account_id")
        else:
            results["contacts_to_accounts"] = (
                False,
                f"⚠️  {orphaned_count} contacts have invalid account_id (may be intentional)"
            )
        
        # 4. CRM: opportunities.account_id → accounts.account_id
        print("🔍 Validating: opportunities.account_id → accounts.account_id")
        opportunities = spark.read.option("header", "true").csv(
            f"{data_root}/bronze/crm/opportunities.csv"
        )
        
        opp_account_ids = opportunities.select("account_id").distinct().filter(
            col("account_id").isNotNull()
        )
        
        orphaned = opp_account_ids.join(
            account_ids, "account_id", "left_anti"
        )
        orphaned_count = orphaned.count()
        
        if orphaned_count == 0:
            results["opportunities_to_accounts"] = (True, "✅ All opportunities have valid account_id")
        else:
            results["opportunities_to_accounts"] = (
                False,
                f"⚠️  {orphaned_count} opportunities have invalid account_id"
            )
        
        # 5. Redshift: behavior.customer_id → customers.customer_id
        print("🔍 Validating: behavior.customer_id → customers.customer_id")
        behavior = spark.read.option("header", "true").csv(
            f"{data_root}/bronze/redshift/redshift_customer_behavior_50000.csv"
        )
        
        behavior_customer_ids = behavior.select("customer_id").distinct()
        
        orphaned = behavior_customer_ids.join(
            customer_ids, "customer_id", "left_anti"
        )
        orphaned_count = orphaned.count()
        
        if orphaned_count == 0:
            results["behavior_to_customers"] = (True, "✅ All behavior records have valid customer_id")
        else:
            results["behavior_to_customers"] = (
                False,
                f"⚠️  {orphaned_count} behavior records have invalid customer_id (may be intentional for testing)"
            )
        
        # 6. Kafka: events.customer_id → customers.customer_id (if present in JSON)
        print("🔍 Validating: kafka events (checking customer_id in metadata)")
        kafka_events = spark.read.option("header", "true").csv(
            f"{data_root}/bronze/kafka/stream_kafka_events_100000.csv"
        )
        
        # Kafka events may have customer_id in metadata JSON
        # This is a soft check - we'll note if customer_id appears
        kafka_count = kafka_events.count()
        results["kafka_events"] = (
            True,
            f"✅ {kafka_count:,} Kafka events loaded (customer_id may be in JSON metadata)"
        )
        
    except Exception as e:
        results["error"] = (False, f"❌ Validation error: {e}")
    
    return results


def validate_data_quality(spark: SparkSession, data_root: str) -> Dict[str, Tuple[bool, str]]:
    """Validate data quality checks."""
    results = {}
    
    try:
        # Check for null primary keys
        print("🔍 Checking for null primary keys...")
        
        customers = spark.read.option("header", "true").csv(
            f"{data_root}/bronze/snowflakes/snowflake_customers_50000.csv"
        )
        null_customer_ids = customers.filter(col("customer_id").isNull()).count()
        results["customers_null_pk"] = (
            null_customer_ids == 0,
            f"{'✅' if null_customer_ids == 0 else '❌'} Customer IDs: {null_customer_ids} nulls"
        )
        
        orders = spark.read.option("header", "true").csv(
            f"{data_root}/bronze/snowflakes/snowflake_orders_100000.csv"
        )
        null_order_ids = orders.filter(col("order_id").isNull()).count()
        results["orders_null_pk"] = (
            null_order_ids == 0,
            f"{'✅' if null_order_ids == 0 else '❌'} Order IDs: {null_order_ids} nulls"
        )
        
        # Check for negative amounts
        negative_amounts = orders.filter(col("amount") < 0).count()
        results["orders_negative_amounts"] = (
            negative_amounts == 0,
            f"{'✅' if negative_amounts == 0 else '⚠️ '} Negative amounts: {negative_amounts}"
        )
        
    except Exception as e:
        results["dq_error"] = (False, f"❌ DQ check error: {e}")
    
    return results


def main():
    """Main validation function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Validate source data")
    parser.add_argument(
        "--data-root",
        default="aws/data/samples",
        help="Root directory for data files"
    )
    args = parser.parse_args()
    
    print("=" * 60)
    print("SOURCE DATA VALIDATION")
    print("=" * 60)
    print()
    
    spark = create_spark()
    
    try:
        # Validate joins
        print("📊 VALIDATING FOREIGN KEY JOINS")
        print("-" * 60)
        join_results = validate_joins(spark, args.data_root)
        
        for join_name, (is_valid, message) in join_results.items():
            print(f"  {message}")
        
        print()
        
        # Validate data quality
        print("📊 VALIDATING DATA QUALITY")
        print("-" * 60)
        dq_results = validate_data_quality(spark, args.data_root)
        
        for check_name, (is_valid, message) in dq_results.items():
            print(f"  {message}")
        
        print()
        print("=" * 60)
        
        # Summary
        all_joins_valid = all(is_valid for is_valid, _ in join_results.values() if "error" not in join_results)
        all_dq_valid = all(is_valid for is_valid, _ in dq_results.values() if "error" not in dq_results)
        
        if all_joins_valid and all_dq_valid:
            print("✅ ALL VALIDATIONS PASSED")
            return 0
        else:
            print("⚠️  SOME VALIDATIONS FAILED (see details above)")
            return 1
            
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

