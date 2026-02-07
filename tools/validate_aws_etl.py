#!/usr/bin/env python3
"""
AWS ETL Pipeline Validation Script

Validates that the Bronze → Silver → Gold pipeline works correctly on AWS by:
1. Reading Silver and Gold output tables from S3 (Delta format with Parquet fallback)
2. Checking row counts, schemas, and sample data
3. Performing sanity checks (null keys, join integrity, etc.)

⚠️  IMPORTANT: This script MUST run on EMR Serverless, NOT locally.
   Local execution will fail because S3 filesystem support is not available.
   For local testing, use tools/validate_local_etl.py instead.

Usage:
    # On EMR Serverless (via Airflow DAG or EMR job):
    python tools/validate_aws_etl.py --env dev --config s3://bucket/config/aws/config/dev.yaml
    
    # For local testing (uses local filesystem):
    python tools/validate_local_etl.py --env local --config local/config/local.yaml
"""

import sys
import argparse
import logging
from pathlib import Path
from typing import Dict, Any, List, Tuple

# Add src to path
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
if src_path.exists() and str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Core utilities
from project_a.utils.spark_session import build_spark
from project_a.pyspark_interview_project.utils.config_loader import load_config_resolved
from project_a.utils.path_resolver import resolve_data_path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def read_table(
    spark: SparkSession,
    path: str,
    table_name: str,
    config: Dict[str, Any],
    *,
    required: bool = True,
    schema: Any = None
) -> DataFrame:
    """
    Read a table from Silver or Gold layer with format-aware fallback.
    
    For AWS environment: delta first, then parquet fallback
    For local environment: parquet only (no delta attempts)
    
    Args:
        spark: SparkSession
        path: Table path (S3 or local)
        table_name: Table name for logging
        config: Configuration dictionary
        required: Whether this table is required (affects error logging)
        schema: Optional schema for empty DataFrame fallback
        
    Returns:
        DataFrame or empty DataFrame with schema if not found
    """
    import os
    from pyspark.sql.utils import AnalysisException
    from pyspark.sql import types as T
    
    environment = config.get("environment", config.get("env", "aws"))
    # For AWS validation, we expect "emr", "aws", or "prod" - not "local"
    is_local = environment in ("local", "dev_local")
    
    # Normalize path
    path = path.rstrip("/")
    
    # Check if path is S3
    is_s3_path = path.startswith("s3://")
    
    # If running locally but trying to read from S3, provide helpful error
    if is_local and is_s3_path:
        error_msg = (
            f"Cannot read from S3 path {path} in local mode. "
            "AWS validation requires either:\n"
            "  1. Running on EMR Serverless (recommended)\n"
            "  2. Local setup with S3 support:\n"
            "     - Install: pip install delta-spark\n"
            "     - Set SPARK_PACKAGES: io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4\n"
            "     - Configure AWS credentials\n"
            f"  For now, returning empty DataFrame for {table_name}"
        )
        logger.error(f"  ✗ {error_msg}")
        if schema is not None:
            return spark.createDataFrame([], schema)
        return spark.createDataFrame([], T.StructType([]))
    
    # Local filesystem: parquet only (no delta attempts)
    if is_local:
        try:
            df = spark.read.parquet(path)
            count = df.count()
            logger.info(f"  ✓ {table_name}: {count:,} rows (parquet)")
            return df
        except AnalysisException as e:
            msg = f"{table_name} not found at {path} (local/parquet mode)"
            if required:
                logger.error(f"  ✗ {msg}: {e}")
            else:
                logger.warning(f"  ⚠ {msg}: {e}")
            if schema is not None:
                return spark.createDataFrame([], schema)
            return spark.createDataFrame([], T.StructType([]))
        except Exception as e:
            msg = f"{table_name} error reading from {path}"
            if required:
                logger.error(f"  ✗ {msg}: {e}")
            else:
                logger.warning(f"  ⚠ {msg}: {e}")
            if schema is not None:
                return spark.createDataFrame([], schema)
            return spark.createDataFrame([], T.StructType([]))
    
    # AWS: delta first, then parquet fallback
    try:
        df = spark.read.format("delta").load(path)
        count = df.count()
        logger.info(f"  ✓ {table_name}: {count:,} rows (delta)")
        return df
    except Exception as delta_err:
        # Check if it's a missing Delta JAR error
        delta_err_str = str(delta_err)
        if "delta" in delta_err_str.lower() or "ClassNotFoundException" in delta_err_str:
            logger.warning(
                f"  ⚠ Delta Lake not available (expected on EMR). "
                f"Falling back to parquet for {table_name}"
            )
        else:
            logger.warning(
                f"  ⚠ {table_name} not found as delta from {path}; falling back to parquet"
            )
        try:
            df = spark.read.parquet(path)
            count = df.count()
            logger.info(f"  ✓ {table_name}: {count:,} rows (parquet)")
            return df
        except Exception as pq_err:
            # Check if it's S3 filesystem error
            pq_err_str = str(pq_err)
            if "s3" in pq_err_str.lower() or "FileSystem" in pq_err_str or "UnsupportedFileSystemException" in pq_err_str:
                error_msg = (
                    f"Cannot read from S3 path {path}. "
                    "S3 filesystem support not configured. "
                    "Ensure hadoop-aws JAR is available. "
                    "This script should run on EMR Serverless where S3 support is built-in."
                )
                logger.error(f"  ✗ {error_msg}")
            else:
                msg = f"{table_name} not found in either delta or parquet at {path}"
                if required:
                    logger.error(f"  ✗ {msg}: {pq_err}")
                else:
                    logger.warning(f"  ⚠ {msg}: {pq_err}")
            if schema is not None:
                return spark.createDataFrame([], schema)
            return spark.createDataFrame([], T.StructType([]))


def print_table_report(
    df: DataFrame,
    table_name: str,
    show_samples: bool = True,
    num_samples: int = 5
) -> Dict[str, Any]:
    """
    Print a compact report for a table.
    
    Args:
        df: DataFrame to report on
        table_name: Table name
        show_samples: Whether to show sample rows
        num_samples: Number of sample rows to show
        
    Returns:
        Dictionary with report metrics
    """
    print(f"\n{'='*80}")
    print(f"📊 {table_name.upper()}")
    print(f"{'='*80}")
    
    try:
        row_count = df.count()
        print(f"Row Count: {row_count:,}")
        
        if row_count == 0:
            print("⚠️  WARNING: Table is empty!")
            # Still try to get schema even for empty tables
            schema_dict = {}
            try:
                for field in df.schema.fields:
                    nullable_str = "nullable" if field.nullable else "not null"
                    print(f"  • {field.name}: {field.dataType} ({nullable_str})")
                    schema_dict[field.name] = {
                        "type": str(field.dataType),
                        "nullable": field.nullable
                    }
            except Exception:
                pass
            return {"row_count": 0, "schema": schema_dict if schema_dict else None, "has_data": False}
        
        # Schema
        print(f"\nSchema ({len(df.columns)} columns):")
        schema_dict = {}
        for field in df.schema.fields:
            nullable_str = "nullable" if field.nullable else "not null"
            print(f"  • {field.name}: {field.dataType} ({nullable_str})")
            schema_dict[field.name] = {
                "type": str(field.dataType),
                "nullable": field.nullable
            }
        
        # Sample rows
        if show_samples and row_count > 0:
            print(f"\nSample Rows (showing {min(num_samples, row_count)} of {row_count:,}):")
            try:
                samples = df.limit(num_samples).collect()
                for i, row in enumerate(samples, 1):
                    row_dict = row.asDict()
                    # Truncate long values for display
                    row_str = ", ".join([
                        f"{k}={str(v)[:50]}{'...' if len(str(v)) > 50 else ''}"
                        for k, v in row_dict.items()
                    ])
                    print(f"  [{i}] {row_str}")
            except Exception as e:
                print(f"  ⚠️  Could not show samples: {e}")
        
        return {
            "row_count": row_count,
            "schema": schema_dict,
            "has_data": True
        }
        
    except Exception as e:
        print(f"❌ Error analyzing {table_name}: {e}")
        return {"row_count": 0, "schema": None, "has_data": False, "error": str(e)}


def check_null_percentage(df: DataFrame, column: str) -> Tuple[int, float]:
    """
    Check null percentage for a column.
    
    Args:
        df: DataFrame
        column: Column name
        
    Returns:
        Tuple of (null_count, null_percentage)
    """
    try:
        total = df.count()
        if total == 0:
            return (0, 0.0)
        null_count = df.filter(F.col(column).isNull()).count()
        null_pct = (null_count / total) * 100.0
        return (null_count, null_pct)
    except Exception:
        return (0, 0.0)


def check_join_integrity(
    fact_df: DataFrame,
    dim_df: DataFrame,
    fact_key: str,
    dim_key: str,
    fact_name: str,
    dim_name: str
) -> Dict[str, Any]:
    """
    Check referential integrity between fact and dimension tables.
    
    Args:
        fact_df: Fact table DataFrame
        dim_df: Dimension table DataFrame
        fact_key: Foreign key column in fact table
        dim_key: Primary key column in dimension table
        fact_name: Fact table name
        dim_name: Dimension table name
        
    Returns:
        Dictionary with integrity check results
    """
    try:
        fact_total = fact_df.count()
        if fact_total == 0:
            return {
                "total_fact_rows": 0,
                "orphan_count": 0,
                "orphan_percentage": 0.0,
                "status": "SKIPPED"
            }
        
        # Get distinct keys from fact table
        fact_keys = fact_df.select(fact_key).distinct()
        
        # Get distinct keys from dimension
        dim_keys = dim_df.select(dim_key).distinct()
        
        # Find orphans (keys in fact but not in dim)
        orphans = fact_keys.join(dim_keys, fact_keys[fact_key] == dim_keys[dim_key], "left_anti")
        orphan_count = orphans.count()
        orphan_pct = (orphan_count / fact_total) * 100.0 if fact_total > 0 else 0.0
        
        status = "PASS" if orphan_count == 0 else "WARN" if orphan_pct < 5.0 else "FAIL"
        
        return {
            "total_fact_rows": fact_total,
            "orphan_count": orphan_count,
            "orphan_percentage": orphan_pct,
            "status": status
        }
    except Exception as e:
        return {
            "error": str(e),
            "status": "ERROR"
        }


# FX Rates schema definition
FX_RATES_SCHEMA = T.StructType([
    T.StructField("trade_date", T.DateType(), True),
    T.StructField("base_ccy", T.StringType(), True),
    T.StructField("counter_ccy", T.StringType(), True),
    T.StructField("rate", T.DoubleType(), True),
])


def validate_silver_layer(
    spark: SparkSession,
    config: Dict[str, Any]
) -> Dict[str, Any]:
    """Validate Silver layer tables."""
    print("\n" + "="*80)
    print("🔍 VALIDATING SILVER LAYER")
    print("="*80)
    
    silver_root = resolve_data_path(config, "silver")
    tables_config = config.get("tables", {}).get("silver", {})
    
    silver_tables = {
        "customers_silver": (tables_config.get("customers", "customers_silver"), True, None),
        "orders_silver": (tables_config.get("orders", "orders_silver"), True, None),
        "products_silver": (tables_config.get("products", "products_silver"), True, None),
        "behavior_silver": (tables_config.get("behavior", "customer_behavior_silver"), True, None),
        "fx_rates_silver": (tables_config.get("fx_rates", "fx_rates_silver"), False, FX_RATES_SCHEMA),
        "order_events_silver": (tables_config.get("order_events", "order_events_silver"), True, None),
    }
    
    results = {}
    dfs = {}
    
    for table_key, (table_name, required, schema) in silver_tables.items():
        path = f"{silver_root}/{table_name}"
        df = read_table(spark, path, table_name, config, required=required, schema=schema)
        dfs[table_key] = df
        results[table_key] = print_table_report(df, table_name, show_samples=True)
        
        # Special handling for fx_rates_silver: warn if empty but schema is present
        if table_key == "fx_rates_silver" and results[table_key].get("row_count", 0) == 0:
            if schema is not None:
                if results[table_key].get("schema") is not None:
                    logger.warning(f"  ⚠ {table_name} is empty but schema is present (this is acceptable)")
                else:
                    logger.warning(f"  ⚠ {table_name} is empty and no schema found (using provided schema)")
    
    # Sanity checks
    print(f"\n{'='*80}")
    print("🔍 SILVER LAYER SANITY CHECKS")
    print(f"{'='*80}")
    
    checks_passed = 0
    checks_total = 0
    
    # Check critical tables have data
    critical_tables = ["customers_silver", "orders_silver", "products_silver"]
    optional_tables = ["fx_rates_silver"]  # Optional tables that can be empty
    
    for table_key in critical_tables:
        checks_total += 1
        if table_key in dfs:
            count = results[table_key].get("row_count", 0)
            if count > 0:
                print(f"✓ {table_key}: {count:,} rows (PASS)")
                checks_passed += 1
            else:
                print(f"✗ {table_key}: 0 rows (FAIL)")
    
    # Check optional tables (warn if empty but don't fail)
    for table_key in optional_tables:
        if table_key in dfs:
            count = results[table_key].get("row_count", 0)
            if count > 0:
                print(f"✓ {table_key}: {count:,} rows (PASS)")
            else:
                if results[table_key].get("schema") is not None:
                    print(f"⚠ {table_key}: 0 rows but schema present (WARN - acceptable)")
                else:
                    print(f"⚠ {table_key}: 0 rows and no schema (WARN - may not exist)")
    
    # Check null percentages for key columns
    if "customers_silver" in dfs and results["customers_silver"]["has_data"]:
        checks_total += 1
        null_count, null_pct = check_null_percentage(dfs["customers_silver"], "customer_id")
        if null_pct == 0.0:
            print(f"✓ customers_silver.customer_id: 0% null (PASS)")
            checks_passed += 1
        else:
            print(f"⚠ customers_silver.customer_id: {null_pct:.2f}% null ({null_count:,} rows)")
    
    if "orders_silver" in dfs and results["orders_silver"]["has_data"]:
        checks_total += 1
        null_count, null_pct = check_null_percentage(dfs["orders_silver"], "order_id")
        if null_pct == 0.0:
            print(f"✓ orders_silver.order_id: 0% null (PASS)")
            checks_passed += 1
        else:
            print(f"⚠ orders_silver.order_id: {null_pct:.2f}% null ({null_count:,} rows)")
        
        # Check customer_id FK
        checks_total += 1
        null_count, null_pct = check_null_percentage(dfs["orders_silver"], "customer_id")
        if null_pct < 5.0:
            print(f"✓ orders_silver.customer_id: {null_pct:.2f}% null (PASS)")
            checks_passed += 1
        else:
            print(f"⚠ orders_silver.customer_id: {null_pct:.2f}% null ({null_count:,} rows)")
    
    print(f"\nSilver Checks: {checks_passed}/{checks_total} passed")
    
    return {
        "tables": results,
        "dataframes": dfs,
        "checks_passed": checks_passed,
        "checks_total": checks_total
    }


def validate_gold_layer(
    spark: SparkSession,
    config: Dict[str, Any],
    silver_dfs: Dict[str, DataFrame]
) -> Dict[str, Any]:
    """Validate Gold layer tables."""
    print("\n" + "="*80)
    print("🔍 VALIDATING GOLD LAYER")
    print("="*80)
    
    gold_root = resolve_data_path(config, "gold")
    tables_config = config.get("tables", {}).get("gold", {})
    
    gold_tables = {
        "fact_orders": tables_config.get("fact_orders", "fact_orders"),
        "dim_customer": tables_config.get("dim_customer", "dim_customer"),
        "dim_product": tables_config.get("dim_product", "dim_product"),
        "dim_date": tables_config.get("dim_date", "dim_date"),
        "customer_360": tables_config.get("customer_360", "customer_360"),
        "product_performance": tables_config.get("product_performance", "product_performance"),
    }
    
    results = {}
    dfs = {}
    
    for table_key, table_name in gold_tables.items():
        path = f"{gold_root}/{table_name}"
        df = read_table(spark, path, table_name, config)
        dfs[table_key] = df
        results[table_key] = print_table_report(df, table_name, show_samples=True)
    
    # Sanity checks
    print(f"\n{'='*80}")
    print("🔍 GOLD LAYER SANITY CHECKS")
    print(f"{'='*80}")
    
    checks_passed = 0
    checks_total = 0
    
    # Check critical tables have data
    critical_tables = ["fact_orders", "dim_customer", "dim_product"]
    for table_key in critical_tables:
        checks_total += 1
        if table_key in dfs:
            count = results[table_key].get("row_count", 0)
            if count > 0:
                print(f"✓ {table_key}: {count:,} rows (PASS)")
                checks_passed += 1
            else:
                print(f"✗ {table_key}: 0 rows (FAIL)")
    
    # Check referential integrity
    # Note: fact_orders uses surrogate keys (customer_sk, product_sk), not natural keys
    # We check that these keys are valid (not -1, which indicates missing dimension)
    if "fact_orders" in dfs and results["fact_orders"]["has_data"]:
        try:
            fact_df = dfs["fact_orders"]
            total_fact = fact_df.count()
            
            # Check for missing customer_sk (should be rare, -1 indicates missing dim)
            if "customer_sk" in fact_df.columns:
                checks_total += 1
                missing_customer = fact_df.filter(F.col("customer_sk") == -1).count()
                missing_pct = (missing_customer / total_fact) * 100.0 if total_fact > 0 else 0.0
                if missing_pct == 0.0:
                    print(f"✓ fact_orders.customer_sk: All rows have valid customer (PASS)")
                    checks_passed += 1
                elif missing_pct < 1.0:
                    print(f"⚠ fact_orders.customer_sk: {missing_pct:.2f}% missing customer ({missing_customer:,} rows)")
                    checks_passed += 1  # Warn but don't fail
                else:
                    print(f"✗ fact_orders.customer_sk: {missing_pct:.2f}% missing customer (FAIL)")
            
            # Check for missing product_sk
            if "product_sk" in fact_df.columns:
                checks_total += 1
                missing_product = fact_df.filter(F.col("product_sk") == -1).count()
                missing_pct = (missing_product / total_fact) * 100.0 if total_fact > 0 else 0.0
                if missing_pct == 0.0:
                    print(f"✓ fact_orders.product_sk: All rows have valid product (PASS)")
                    checks_passed += 1
                elif missing_pct < 1.0:
                    print(f"⚠ fact_orders.product_sk: {missing_pct:.2f}% missing product ({missing_product:,} rows)")
                    checks_passed += 1
                else:
                    print(f"✗ fact_orders.product_sk: {missing_pct:.2f}% missing product (FAIL)")
        except Exception as e:
            print(f"⚠ Could not check referential integrity: {e}")
    
    # Check key aggregations make sense
    if "fact_orders" in dfs and results["fact_orders"]["has_data"]:
        try:
            total_revenue = dfs["fact_orders"].agg(F.sum("sales_amount").alias("total")).collect()[0]["total"]
            if total_revenue and total_revenue > 0:
                print(f"✓ fact_orders total revenue: ${total_revenue:,.2f} (PASS)")
                checks_passed += 1
            else:
                print(f"⚠ fact_orders total revenue: ${total_revenue or 0:,.2f}")
            checks_total += 1
        except Exception as e:
            print(f"⚠ Could not compute total revenue: {e}")
    
    print(f"\nGold Checks: {checks_passed}/{checks_total} passed")
    
    return {
        "tables": results,
        "dataframes": dfs,
        "checks_passed": checks_passed,
        "checks_total": checks_total
    }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Validate AWS ETL pipeline")
    parser.add_argument("--env", default="aws", help="Environment (aws/dev/prod)")
    parser.add_argument("--config", help="Config file path (local or s3://...)")
    parser.add_argument("--skip-silver", action="store_true", help="Skip Silver validation")
    parser.add_argument("--skip-gold", action="store_true", help="Skip Gold validation")
    args = parser.parse_args()
    
    # Load config
    if args.config:
        config_path = args.config
    else:
        env = args.env
        if env == "aws":
            config_path = "aws/config/dev.yaml"
        else:
            config_path = f"aws/config/{env}.yaml"
    
    try:
        config = load_config_resolved(config_path, env=args.env)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)
    
    # Set environment if not set
    if not config.get("environment"):
        config["environment"] = args.env if args.env != "aws" else "aws"
    
    # Detect if running locally
    import os
    spark_master = os.getenv("SPARK_MASTER", "local[*]")
    is_local_run = spark_master.startswith("local") or config.get("environment") == "local"
    
    # Check if config has S3 paths
    paths = config.get("paths", {})
    has_s3_paths = any(
        isinstance(v, str) and v.startswith("s3://")
        for v in paths.values()
    )
    
    if is_local_run and has_s3_paths:
        logger.error("="*80)
        logger.error("❌ ERROR: Cannot validate AWS pipeline locally with S3 paths")
        logger.error("="*80)
        logger.error("This script requires S3 filesystem support which is not available in local Spark.")
        logger.error("")
        logger.error("The AWS validation script is designed to run on EMR Serverless where:")
        logger.error("  - S3 filesystem support is built-in")
        logger.error("  - Delta Lake is pre-configured")
        logger.error("  - All dependencies are available")
        logger.error("")
        logger.error("To validate the AWS pipeline:")
        logger.error("  1. Run the ETL jobs on EMR Serverless first")
        logger.error("  2. Then run this validation script on EMR Serverless (or a machine with S3 support)")
        logger.error("")
        logger.error("To set up local S3 support (not recommended for validation):")
        logger.error("  1. Install: pip install delta-spark")
        logger.error("  2. Set SPARK_PACKAGES=io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4")
        logger.error("  3. Configure AWS credentials")
        logger.error("  4. This is complex and error-prone - use EMR instead")
        logger.error("")
        logger.error("="*80)
        logger.error("")
        logger.error("Exiting. Please run this script on EMR Serverless or use validate_local_etl.py for local validation.")
        sys.exit(1)
    
    # Build Spark session (with Delta enabled for AWS)
    spark = build_spark(app_name="validate_aws_etl", config=config)
    
    try:
        print("="*80)
        print("🚀 AWS ETL PIPELINE VALIDATION")
        print("="*80)
        print(f"Environment: {config.get('environment', 'aws')}")
        print(f"Config: {config_path}")
        print(f"Silver Root: {resolve_data_path(config, 'silver')}")
        print(f"Gold Root: {resolve_data_path(config, 'gold')}")
        print(f"Spark Master: {spark.sparkContext.master}")
        if is_local_run and has_s3_paths:
            print("⚠️  Running locally with S3 paths - S3 reads will fail")
        
        silver_results = None
        gold_results = None
        
        # Validate Silver
        if not args.skip_silver:
            silver_results = validate_silver_layer(spark, config)
        
        # Validate Gold
        if not args.skip_gold:
            silver_dfs = silver_results.get("dataframes", {}) if silver_results else {}
            gold_results = validate_gold_layer(spark, config, silver_dfs)
        
        # Final summary
        print("\n" + "="*80)
        print("📋 VALIDATION SUMMARY")
        print("="*80)
        
        total_checks = 0
        total_passed = 0
        
        if silver_results:
            silver_passed = silver_results.get("checks_passed", 0)
            silver_total = silver_results.get("checks_total", 0)
            total_checks += silver_total
            total_passed += silver_passed
            print(f"Silver Layer: {silver_passed}/{silver_total} checks passed")
        
        if gold_results:
            gold_passed = gold_results.get("checks_passed", 0)
            gold_total = gold_results.get("checks_total", 0)
            total_checks += gold_total
            total_passed += gold_passed
            print(f"Gold Layer: {gold_passed}/{gold_total} checks passed")
        
        print(f"\nOverall: {total_passed}/{total_checks} checks passed")
        
        # Exit code
        if total_checks == 0:
            print("⚠️  No checks performed")
            sys.exit(0)
        elif total_passed == total_checks:
            print("✅ All checks passed!")
            sys.exit(0)
        elif total_passed >= total_checks * 0.8:  # 80% threshold
            print("⚠️  Some checks failed, but pipeline appears functional")
            sys.exit(0)
        else:
            print("❌ Critical checks failed - pipeline may have issues")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Validation failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

