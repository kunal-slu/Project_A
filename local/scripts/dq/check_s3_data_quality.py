#!/usr/bin/env python3
"""
S3 Data Quality Check Script (No Local Spark Required)

Uses AWS CLI and boto3 to check data quality on S3 without requiring local Spark.
This is useful when local Spark has compatibility issues.
"""
import sys
import os
from pathlib import Path

# Add src to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import logging
import boto3
from typing import Dict, Any, List
from botocore.exceptions import ClientError

from project_a.config_loader import load_config_resolved

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def check_s3_path_exists(s3_client, bucket: str, prefix: str) -> bool:
    """Check if S3 path exists."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return response.get("KeyCount", 0) > 0
    except ClientError as e:
        logger.warning(f"Error checking S3 path {prefix}: {e}")
        return False


def count_s3_objects(s3_client, bucket: str, prefix: str) -> int:
    """Count objects in S3 prefix."""
    try:
        count = 0
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            count += page.get("KeyCount", 0)
        return count
    except ClientError as e:
        logger.warning(f"Error counting S3 objects {prefix}: {e}")
        return 0


def get_s3_path_size(s3_client, bucket: str, prefix: str) -> int:
    """Get total size of objects in S3 prefix (bytes)."""
    try:
        total_size = 0
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                total_size += obj.get("Size", 0)
        return total_size
    except ClientError as e:
        logger.warning(f"Error getting S3 size {prefix}: {e}")
        return 0


def check_bronze_s3_quality(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Check quality of bronze layer in S3."""
    logger.info("=" * 80)
    logger.info("BRONZE LAYER S3 DATA QUALITY CHECK")
    logger.info("=" * 80)
    
    s3_client = boto3.client('s3', region_name=config.get("aws", {}).get("region", "us-east-1"))
    bronze_root = config["paths"]["bronze_root"]
    
    # Parse S3 path
    if not bronze_root.startswith("s3://"):
        logger.error(f"Bronze root is not an S3 path: {bronze_root}")
        return []
    
    bucket = bronze_root.replace("s3://", "").split("/")[0]
    base_prefix = "/".join(bronze_root.replace("s3://", "").split("/")[1:])
    
    sources = config.get("sources", {})
    results = []
    
    # Check CRM data
    if "crm" in sources:
        crm_cfg = sources["crm"]
        base_path = crm_cfg.get("base_path", f"{bronze_root}/crm")
        files = crm_cfg.get("files", {})
        
        for file_type, filename in files.items():
            if base_path.startswith("s3://"):
                file_bucket = base_path.replace("s3://", "").split("/")[0]
                file_prefix = "/".join(base_path.replace("s3://", "").split("/")[1:]) + f"/{filename}"
            else:
                file_bucket = bucket
                file_prefix = f"{base_prefix}/crm/{filename}"
            
            exists = check_s3_path_exists(s3_client, file_bucket, file_prefix)
            size = get_s3_path_size(s3_client, file_bucket, file_prefix) if exists else 0
            
            results.append({
                "table_name": f"bronze.crm.{file_type}",
                "table_path": f"s3://{file_bucket}/{file_prefix}",
                "exists": exists,
                "size_bytes": size,
                "status": "PASS" if exists and size > 0 else "ERROR"
            })
    
    # Check Snowflake data
    if "snowflake" in sources:
        sf_cfg = sources["snowflake"]
        base_path = sf_cfg.get("base_path", f"{bronze_root}/snowflakes")
        files = sf_cfg.get("files", {})
        
        for file_type, filename in files.items():
            if base_path.startswith("s3://"):
                file_bucket = base_path.replace("s3://", "").split("/")[0]
                file_prefix = "/".join(base_path.replace("s3://", "").split("/")[1:]) + f"/{filename}"
            else:
                file_bucket = bucket
                file_prefix = f"{base_prefix}/snowflakes/{filename}"
            
            exists = check_s3_path_exists(s3_client, file_bucket, file_prefix)
            size = get_s3_path_size(s3_client, file_bucket, file_prefix) if exists else 0
            
            results.append({
                "table_name": f"bronze.snowflake.{file_type}",
                "table_path": f"s3://{file_bucket}/{file_prefix}",
                "exists": exists,
                "size_bytes": size,
                "status": "PASS" if exists and size > 0 else "ERROR"
            })
    
    # Check Redshift data
    if "redshift" in sources:
        rs_cfg = sources["redshift"]
        base_path = rs_cfg.get("base_path", f"{bronze_root}/redshift")
        files = rs_cfg.get("files", {})
        
        for file_type, filename in files.items():
            if base_path.startswith("s3://"):
                file_bucket = base_path.replace("s3://", "").split("/")[0]
                file_prefix = "/".join(base_path.replace("s3://", "").split("/")[1:]) + f"/{filename}"
            else:
                file_bucket = bucket
                file_prefix = f"{base_prefix}/redshift/{filename}"
            
            exists = check_s3_path_exists(s3_client, file_bucket, file_prefix)
            size = get_s3_path_size(s3_client, file_bucket, file_prefix) if exists else 0
            
            results.append({
                "table_name": f"bronze.redshift.{file_type}",
                "table_path": f"s3://{file_bucket}/{file_prefix}",
                "exists": exists,
                "size_bytes": size,
                "status": "PASS" if exists and size > 0 else "ERROR"
            })
    
    # Check Kafka data
    if "kafka_sim" in sources:
        kafka_cfg = sources["kafka_sim"]
        base_path = kafka_cfg.get("base_path", f"{bronze_root}/kafka")
        files = kafka_cfg.get("files", {})
        
        for file_type, filename in files.items():
            if base_path.startswith("s3://"):
                file_bucket = base_path.replace("s3://", "").split("/")[0]
                file_prefix = "/".join(base_path.replace("s3://", "").split("/")[1:]) + f"/{filename}"
            else:
                file_bucket = bucket
                file_prefix = f"{base_prefix}/kafka/{filename}"
            
            exists = check_s3_path_exists(s3_client, file_bucket, file_prefix)
            size = get_s3_path_size(s3_client, file_bucket, file_prefix) if exists else 0
            
            results.append({
                "table_name": f"bronze.kafka.{file_type}",
                "table_path": f"s3://{file_bucket}/{file_prefix}",
                "exists": exists,
                "size_bytes": size,
                "status": "PASS" if exists and size > 0 else "ERROR"
            })
    
    # Check FX data
    if "fx" in sources:
        fx_cfg = sources["fx"]
        base_path = fx_cfg.get("base_path", f"{bronze_root}/fx")
        files = fx_cfg.get("files", {})
        
        for file_type, filename in files.items():
            if base_path.startswith("s3://"):
                file_bucket = base_path.replace("s3://", "").split("/")[0]
                file_prefix = "/".join(base_path.replace("s3://", "").split("/")[1:]) + f"/{filename}"
            else:
                file_bucket = bucket
                file_prefix = f"{base_prefix}/fx/{filename}"
            
            exists = check_s3_path_exists(s3_client, file_bucket, file_prefix)
            size = get_s3_path_size(s3_client, file_bucket, file_prefix) if exists else 0
            
            results.append({
                "table_name": f"bronze.fx.{file_type}",
                "table_path": f"s3://{file_bucket}/{file_prefix}",
                "exists": exists,
                "size_bytes": size,
                "status": "PASS" if exists and size > 0 else "ERROR"
            })
    
    return results


def check_silver_s3_quality(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Check quality of silver layer in S3."""
    logger.info("=" * 80)
    logger.info("SILVER LAYER S3 DATA QUALITY CHECK")
    logger.info("=" * 80)
    
    s3_client = boto3.client('s3', region_name=config.get("aws", {}).get("region", "us-east-1"))
    silver_root = config["paths"]["silver_root"]
    
    # Parse S3 path
    if not silver_root.startswith("s3://"):
        logger.error(f"Silver root is not an S3 path: {silver_root}")
        return []
    
    bucket = silver_root.replace("s3://", "").split("/")[0]
    base_prefix = "/".join(silver_root.replace("s3://", "").split("/")[1:])
    
    tables = config.get("tables", {}).get("silver", {})
    results = []
    
    for table_key, table_name in tables.items():
        table_prefix = f"{base_prefix}/{table_name}"
        
        exists = check_s3_path_exists(s3_client, bucket, table_prefix)
        object_count = count_s3_objects(s3_client, bucket, table_prefix) if exists else 0
        size = get_s3_path_size(s3_client, bucket, table_prefix) if exists else 0
        
        results.append({
            "table_name": f"silver.{table_key}",
            "table_path": f"s3://{bucket}/{table_prefix}",
            "exists": exists,
            "object_count": object_count,
            "size_bytes": size,
            "status": "PASS" if exists and object_count > 0 else "ERROR"
        })
    
    return results


def check_gold_s3_quality(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Check quality of gold layer in S3."""
    logger.info("=" * 80)
    logger.info("GOLD LAYER S3 DATA QUALITY CHECK")
    logger.info("=" * 80)
    
    s3_client = boto3.client('s3', region_name=config.get("aws", {}).get("region", "us-east-1"))
    gold_root = config["paths"]["gold_root"]
    
    # Parse S3 path
    if not gold_root.startswith("s3://"):
        logger.error(f"Gold root is not an S3 path: {gold_root}")
        return []
    
    bucket = gold_root.replace("s3://", "").split("/")[0]
    base_prefix = "/".join(gold_root.replace("s3://", "").split("/")[1:])
    
    tables = config.get("tables", {}).get("gold", {})
    results = []
    
    for table_key, table_name in tables.items():
        table_prefix = f"{base_prefix}/{table_name}"
        
        exists = check_s3_path_exists(s3_client, bucket, table_prefix)
        object_count = count_s3_objects(s3_client, bucket, table_prefix) if exists else 0
        size = get_s3_path_size(s3_client, bucket, table_prefix) if exists else 0
        
        results.append({
            "table_name": f"gold.{table_key}",
            "table_path": f"s3://{bucket}/{table_prefix}",
            "exists": exists,
            "object_count": object_count,
            "size_bytes": size,
            "status": "PASS" if exists and object_count > 0 else "ERROR"
        })
    
    return results


def print_quality_report(results: List[Dict[str, Any]], layer: str):
    """Print formatted quality report."""
    logger.info("")
    logger.info("=" * 80)
    logger.info(f"{layer.upper()} LAYER QUALITY REPORT")
    logger.info("=" * 80)
    logger.info("")
    
    for result in results:
        status_icon = "✅" if result.get("status") == "PASS" else "❌"
        logger.info(f"{status_icon} {result['table_name']}")
        logger.info(f"   Path: {result['table_path']}")
        
        if result.get("status") == "ERROR":
            logger.info(f"   Status: ERROR - File/table not found or empty")
        else:
            size_mb = result.get("size_bytes", 0) / (1024 * 1024)
            logger.info(f"   Size: {size_mb:.2f} MB")
            if "object_count" in result:
                logger.info(f"   Objects: {result['object_count']}")
        
        logger.info("")


def main():
    """Main entry point for S3 DQ checks."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Check S3 data quality (no Spark required)")
    parser.add_argument("--env", default="local", choices=["local", "aws"], help="Environment")
    parser.add_argument("--config", help="Config file path")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "all"], default="all")
    args = parser.parse_args()
    
    # Determine config path
    if args.config:
        config_path = args.config
    elif args.env == "local":
        # For local, use AWS config since both use S3 bronze
        config_path = str(PROJECT_ROOT / "aws/config/dev.yaml")
    else:
        config_path = str(PROJECT_ROOT / "aws/config/dev.yaml")
    
    logger.info("=" * 80)
    logger.info("S3 DATA QUALITY CHECK (No Spark Required)")
    logger.info("=" * 80)
    logger.info(f"Environment: {args.env}")
    logger.info(f"Config: {config_path}")
    logger.info(f"Layer: {args.layer}")
    logger.info("")
    
    # Load config
    config = load_config_resolved(config_path)
    config["environment"] = args.env
    
    all_results = []
    
    # Check requested layers
    if args.layer in ["bronze", "all"]:
        bronze_results = check_bronze_s3_quality(config)
        all_results.extend(bronze_results)
        print_quality_report(bronze_results, "bronze")
    
    if args.layer in ["silver", "all"]:
        silver_results = check_silver_s3_quality(config)
        all_results.extend(silver_results)
        print_quality_report(silver_results, "silver")
    
    if args.layer in ["gold", "all"]:
        gold_results = check_gold_s3_quality(config)
        all_results.extend(gold_results)
        print_quality_report(gold_results, "gold")
    
    # Summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("QUALITY SUMMARY")
    logger.info("=" * 80)
    
    pass_count = sum(1 for r in all_results if r.get("status") == "PASS")
    error_count = sum(1 for r in all_results if r.get("status") == "ERROR")
    
    logger.info(f"✅ Pass: {pass_count}")
    logger.info(f"❌ Error: {error_count}")
    logger.info(f"📊 Total Tables: {len(all_results)}")
    
    if error_count > 0:
        logger.warning("")
        logger.warning("⚠️  Some tables failed quality checks!")
        logger.warning("   Run ETL pipeline to create missing tables.")


if __name__ == "__main__":
    main()

