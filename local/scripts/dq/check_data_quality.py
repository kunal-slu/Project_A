#!/usr/bin/env python3
"""
Data Quality Check Script

Runs data quality checks on both local and AWS data sources.
Compares results to ensure consistency.
"""
import sys
import os
from pathlib import Path

# Add src to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import logging
from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from project_a.utils.spark_session import build_spark
from project_a.config_loader import load_config_resolved
from project_a.utils.logging import setup_json_logging, get_trace_id

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def check_table_quality(
    spark: SparkSession,
    table_name: str,
    table_path: str,
    format_type: str = "parquet"
) -> Dict[str, Any]:
    """
    Run basic data quality checks on a table.
    
    Returns:
        Dictionary with quality metrics
    """
    logger.info(f"🔍 Checking quality for {table_name} at {table_path}")
    
    try:
        # Read table
        if format_type == "delta":
            df = spark.read.format("delta").load(table_path)
        elif format_type == "parquet":
            df = spark.read.format("parquet").load(table_path)
        else:
            df = spark.read.format(format_type).load(table_path)
        
        # Basic quality metrics
        total_rows = df.count()
        
        # Check for nulls in key columns
        null_counts = {}
        for col_name in df.columns[:10]:  # Check first 10 columns
            null_count = df.filter(F.col(col_name).isNull()).count()
            null_counts[col_name] = null_count
        
        # Check for duplicates (on all columns)
        distinct_rows = df.distinct().count()
        duplicate_count = total_rows - distinct_rows
        
        # Check for empty table
        is_empty = total_rows == 0
        
        # Schema info
        schema_fields = len(df.schema.fields)
        
        quality_metrics = {
            "table_name": table_name,
            "table_path": table_path,
            "total_rows": total_rows,
            "distinct_rows": distinct_rows,
            "duplicate_count": duplicate_count,
            "is_empty": is_empty,
            "schema_fields": schema_fields,
            "null_counts": null_counts,
            "status": "PASS" if not is_empty and duplicate_count == 0 else "WARN"
        }
        
        logger.info(f"  ✅ {table_name}: {total_rows:,} rows, {duplicate_count} duplicates")
        return quality_metrics
        
    except Exception as e:
        logger.error(f"  ❌ Failed to check {table_name}: {e}")
        return {
            "table_name": table_name,
            "table_path": table_path,
            "status": "ERROR",
            "error": str(e)
        }


def check_bronze_quality(spark: SparkSession, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Check quality of bronze layer tables."""
    logger.info("=" * 80)
    logger.info("BRONZE LAYER DATA QUALITY CHECK")
    logger.info("=" * 80)
    
    bronze_root = config["paths"]["bronze_root"]
    sources = config.get("sources", {})
    
    results = []
    
    # Check CRM data
    if "crm" in sources:
        crm_cfg = sources["crm"]
        base_path = crm_cfg.get("base_path", f"{bronze_root}/crm")
        files = crm_cfg.get("files", {})
        
        for file_type, filename in files.items():
            file_path = f"{base_path}/{filename}"
            result = check_table_quality(spark, f"bronze.crm.{file_type}", file_path, "csv")
            results.append(result)
    
    # Check Snowflake data
    if "snowflake" in sources:
        sf_cfg = sources["snowflake"]
        base_path = sf_cfg.get("base_path", f"{bronze_root}/snowflakes")
        files = sf_cfg.get("files", {})
        
        for file_type, filename in files.items():
            file_path = f"{base_path}/{filename}"
            result = check_table_quality(spark, f"bronze.snowflake.{file_type}", file_path, "csv")
            results.append(result)
    
    # Check Redshift data
    if "redshift" in sources:
        rs_cfg = sources["redshift"]
        base_path = rs_cfg.get("base_path", f"{bronze_root}/redshift")
        files = rs_cfg.get("files", {})
        
        for file_type, filename in files.items():
            file_path = f"{base_path}/{filename}"
            result = check_table_quality(spark, f"bronze.redshift.{file_type}", file_path, "csv")
            results.append(result)
    
    # Check Kafka data
    if "kafka_sim" in sources:
        kafka_cfg = sources["kafka_sim"]
        base_path = kafka_cfg.get("base_path", f"{bronze_root}/kafka")
        files = kafka_cfg.get("files", {})
        
        for file_type, filename in files.items():
            file_path = f"{base_path}/{filename}"
            result = check_table_quality(spark, f"bronze.kafka.{file_type}", file_path, "csv")
            results.append(result)
    
    # Check FX data
    if "fx" in sources:
        fx_cfg = sources["fx"]
        base_path = fx_cfg.get("base_path", f"{bronze_root}/fx")
        files = fx_cfg.get("files", {})
        
        for file_type, filename in files.items():
            if filename.endswith(".json"):
                file_path = f"{base_path}/{filename}"
                result = check_table_quality(spark, f"bronze.fx.{file_type}", file_path, "json")
                results.append(result)
    
    return results


def check_silver_quality(spark: SparkSession, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Check quality of silver layer tables."""
    logger.info("=" * 80)
    logger.info("SILVER LAYER DATA QUALITY CHECK")
    logger.info("=" * 80)
    
    silver_root = config["paths"]["silver_root"]
    tables = config.get("tables", {}).get("silver", {})
    
    results = []
    
    # Determine format (delta or parquet)
    format_type = "delta" if "delta" in silver_root.lower() or silver_root.startswith("s3://") else "parquet"
    
    for table_key, table_name in tables.items():
        table_path = f"{silver_root}/{table_name}"
        result = check_table_quality(spark, f"silver.{table_key}", table_path, format_type)
        results.append(result)
    
    return results


def check_gold_quality(spark: SparkSession, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Check quality of gold layer tables."""
    logger.info("=" * 80)
    logger.info("GOLD LAYER DATA QUALITY CHECK")
    logger.info("=" * 80)
    
    gold_root = config["paths"]["gold_root"]
    tables = config.get("tables", {}).get("gold", {})
    
    results = []
    
    # Determine format (delta or parquet)
    format_type = "delta" if "delta" in gold_root.lower() or gold_root.startswith("s3://") else "parquet"
    
    for table_key, table_name in tables.items():
        table_path = f"{gold_root}/{table_name}"
        result = check_table_quality(spark, f"gold.{table_key}", table_path, format_type)
        results.append(result)
    
    return results


def print_quality_report(results: List[Dict[str, Any]], layer: str):
    """Print formatted quality report."""
    logger.info("")
    logger.info("=" * 80)
    logger.info(f"{layer.upper()} LAYER QUALITY REPORT")
    logger.info("=" * 80)
    logger.info("")
    
    for result in results:
        status_icon = "✅" if result.get("status") == "PASS" else "⚠️" if result.get("status") == "WARN" else "❌"
        logger.info(f"{status_icon} {result['table_name']}")
        logger.info(f"   Path: {result['table_path']}")
        
        if result.get("status") == "ERROR":
            logger.info(f"   Error: {result.get('error', 'Unknown error')}")
        else:
            logger.info(f"   Rows: {result.get('total_rows', 0):,}")
            logger.info(f"   Distinct: {result.get('distinct_rows', 0):,}")
            logger.info(f"   Duplicates: {result.get('duplicate_count', 0):,}")
            logger.info(f"   Schema Fields: {result.get('schema_fields', 0)}")
            
            null_counts = result.get('null_counts', {})
            if null_counts:
                logger.info(f"   Nulls: {sum(null_counts.values()):,} total across checked columns")
        
        logger.info("")


def compare_local_vs_aws(local_results: List[Dict[str, Any]], aws_results: List[Dict[str, Any]]):
    """Compare local and AWS quality results."""
    logger.info("")
    logger.info("=" * 80)
    logger.info("LOCAL vs AWS COMPARISON")
    logger.info("=" * 80)
    logger.info("")
    
    # Create lookup dictionaries
    local_lookup = {r["table_name"]: r for r in local_results}
    aws_lookup = {r["table_name"]: r for r in aws_results}
    
    all_tables = set(local_lookup.keys()) | set(aws_lookup.keys())
    
    for table_name in sorted(all_tables):
        local_result = local_lookup.get(table_name)
        aws_result = aws_lookup.get(table_name)
        
        logger.info(f"📊 {table_name}")
        
        if local_result and aws_result:
            local_rows = local_result.get("total_rows", 0)
            aws_rows = aws_result.get("total_rows", 0)
            
            if local_rows == aws_rows:
                logger.info(f"   ✅ Row count match: {local_rows:,}")
            else:
                logger.info(f"   ⚠️  Row count mismatch: Local={local_rows:,}, AWS={aws_rows:,}")
            
            local_dups = local_result.get("duplicate_count", 0)
            aws_dups = aws_result.get("duplicate_count", 0)
            
            if local_dups == aws_dups:
                logger.info(f"   ✅ Duplicate count match: {local_dups}")
            else:
                logger.info(f"   ⚠️  Duplicate count mismatch: Local={local_dups}, AWS={aws_dups}")
        
        elif local_result:
            logger.info(f"   ⚠️  Only in local: {local_result.get('total_rows', 0):,} rows")
        elif aws_result:
            logger.info(f"   ⚠️  Only in AWS: {aws_result.get('total_rows', 0):,} rows")
        
        logger.info("")


def main():
    """Main entry point for DQ checks."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Check data quality for local and AWS")
    parser.add_argument("--env", default="local", choices=["local", "aws"], help="Environment to check")
    parser.add_argument("--config", help="Config file path")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "all"], default="all", help="Layer to check")
    parser.add_argument("--compare", action="store_true", help="Compare local vs AWS results")
    args = parser.parse_args()
    
    # Determine config path
    if args.config:
        config_path = args.config
    elif args.env == "local":
        config_path = str(PROJECT_ROOT / "local/config/local.yaml")
    else:
        config_path = str(PROJECT_ROOT / "aws/config/dev.yaml")
    
    logger.info("=" * 80)
    logger.info("DATA QUALITY CHECK")
    logger.info("=" * 80)
    logger.info(f"Environment: {args.env}")
    logger.info(f"Config: {config_path}")
    logger.info(f"Layer: {args.layer}")
    logger.info("")
    
    # Load config
    config = load_config_resolved(config_path)
    config["environment"] = args.env
    
    # Setup logging
    trace_id = get_trace_id()
    setup_json_logging(include_trace_id=True)
    
    # Build Spark session
    try:
        spark = build_spark(config=config)
    except Exception as e:
        logger.error(f"Failed to create SparkSession: {e}")
        logger.error("")
        logger.error("💡 TIP: Use check_s3_data_quality.py for S3-only checks (no Spark required):")
        logger.error(f"   python3 {PROJECT_ROOT}/local/scripts/dq/check_s3_data_quality.py --env {args.env} --layer {args.layer}")
        logger.error("")
        sys.exit(1)
    
    try:
        all_results = []
        
        # Check requested layers
        if args.layer in ["bronze", "all"]:
            bronze_results = check_bronze_quality(spark, config)
            all_results.extend(bronze_results)
            print_quality_report(bronze_results, "bronze")
        
        if args.layer in ["silver", "all"]:
            silver_results = check_silver_quality(spark, config)
            all_results.extend(silver_results)
            print_quality_report(silver_results, "silver")
        
        if args.layer in ["gold", "all"]:
            gold_results = check_gold_quality(spark, config)
            all_results.extend(gold_results)
            print_quality_report(gold_results, "gold")
        
        # Summary
        logger.info("")
        logger.info("=" * 80)
        logger.info("QUALITY SUMMARY")
        logger.info("=" * 80)
        
        pass_count = sum(1 for r in all_results if r.get("status") == "PASS")
        warn_count = sum(1 for r in all_results if r.get("status") == "WARN")
        error_count = sum(1 for r in all_results if r.get("status") == "ERROR")
        
        logger.info(f"✅ Pass: {pass_count}")
        logger.info(f"⚠️  Warn: {warn_count}")
        logger.info(f"❌ Error: {error_count}")
        logger.info(f"📊 Total Tables: {len(all_results)}")
        
        # Compare if requested
        if args.compare:
            logger.info("")
            logger.info("Note: To compare local vs AWS, run:")
            logger.info("  python local/scripts/dq/check_data_quality.py --env local > local_dq.txt")
            logger.info("  python local/scripts/dq/check_data_quality.py --env aws > aws_dq.txt")
            logger.info("  # Then compare the files")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

