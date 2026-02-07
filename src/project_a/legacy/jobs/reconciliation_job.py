"""
Data reconciliation between source and target systems.

Compares row counts and hash sums for data integrity validation.
"""

import logging
from typing import Dict, Any, List, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, sha2, concat_ws, col, sum as spark_sum

logger = logging.getLogger(__name__)


def compute_table_hash(df: DataFrame, key_columns: List[str] = None) -> str:
    """
    Compute hash sum of entire table for reconciliation.
    
    Args:
        df: DataFrame to hash
        key_columns: Key columns (if None, uses all columns)
        
    Returns:
        SHA-256 hash of table
    """
    if key_columns is None:
        key_columns = df.columns
    
    # Create hash from all key columns
    hash_expr = sha2(
        concat_ws("|", *[col(c) for c in key_columns]),
        256
    )
    
    # Sum all hashes
    hash_sum = df.agg(spark_sum(hash_expr).alias("hash_sum")).collect()[0]["hash_sum"]
    
    return str(hash_sum) if hash_sum else ""


def reconcile_tables(
    source_df: DataFrame,
    target_df: DataFrame,
    key_columns: List[str],
    source_name: str = "source",
    target_name: str = "target"
) -> Dict[str, Any]:
    """
    Reconcile two tables by comparing row counts and hash sums.
    
    Args:
        source_df: Source DataFrame
        target_df: Target DataFrame
        key_columns: Key columns for comparison
        source_name: Name of source system
        target_name: Name of target system
        
    Returns:
        Reconciliation results dictionary
    """
    logger.info(f"Reconciling {source_name} ↔ {target_name}")
    
    # Row counts
    source_count = source_df.count()
    target_count = target_df.count()
    
    logger.info(f"  {source_name} rows: {source_count:,}")
    logger.info(f"  {target_name} rows: {target_count:,}")
    
    # Hash sums
    source_hash = compute_table_hash(source_df, key_columns)
    target_hash = compute_table_hash(target_df, key_columns)
    
    logger.info(f"  {source_name} hash: {source_hash[:16]}...")
    logger.info(f"  {target_name} hash: {target_hash[:16]}...")
    
    # Compare
    row_count_match = source_count == target_count
    hash_match = source_hash == target_hash
    
    reconciliation_result = {
        "source_name": source_name,
        "target_name": target_name,
        "source_row_count": source_count,
        "target_row_count": target_count,
        "row_count_match": row_count_match,
        "source_hash": source_hash,
        "target_hash": target_hash,
        "hash_match": hash_match,
        "reconciled": row_count_match and hash_match
    }
    
    if reconciliation_result["reconciled"]:
        logger.info("✅ Reconciliation PASSED")
    else:
        logger.error("❌ Reconciliation FAILED")
        if not row_count_match:
            logger.error(f"   Row count mismatch: {source_count:,} vs {target_count:,}")
        if not hash_match:
            logger.error(f"   Hash mismatch detected")
    
    return reconciliation_result


def reconcile_snowflake_to_s3(
    spark: SparkSession,
    snowflake_table: str,
    s3_path: str,
    config: Dict[str, Any],
    key_columns: List[str]
) -> Dict[str, Any]:
    """
    Reconcile Snowflake table with S3 Delta table.
    
    Args:
        spark: SparkSession
        snowflake_table: Snowflake table name
        s3_path: S3 path to Delta table
        config: Configuration dictionary
        key_columns: Key columns for reconciliation
        
    Returns:
        Reconciliation results
    """
    logger.info(f"Reconciling Snowflake {snowflake_table} ↔ S3 {s3_path}")
    
    # Read from Snowflake
    from project_a.utils.secrets import get_snowflake_credentials
    
    sf_creds = get_snowflake_credentials(config)
    
    source_df = spark.read \
        .format("snowflake") \
        .options(**{
            "sfURL": f"{sf_creds.get('account')}.snowflakecomputing.com",
            "sfUser": sf_creds.get('user'),
            "sfPassword": sf_creds.get('password'),
            "sfDatabase": sf_creds.get('database'),
            "sfSchema": sf_creds.get('schema'),
            "sfWarehouse": sf_creds.get('warehouse'),
            "dbtable": snowflake_table
        }) \
        .load()
    
    # Read from S3
    target_df = spark.read.format("delta").load(s3_path)
    
    return reconcile_tables(
        source_df,
        target_df,
        key_columns,
        source_name="Snowflake",
        target_name="S3"
    )


def reconcile_redshift_to_s3(
    spark: SparkSession,
    redshift_table: str,
    s3_path: str,
    config: Dict[str, Any],
    key_columns: List[str]
) -> Dict[str, Any]:
    """
    Reconcile Redshift table with S3 Delta table.
    
    Args:
        spark: SparkSession
        redshift_table: Redshift table name
        s3_path: S3 path to Delta table
        config: Configuration dictionary
        key_columns: Key columns
        
    Returns:
        Reconciliation results
    """
    logger.info(f"Reconciling Redshift {redshift_table} ↔ S3 {s3_path}")
    
    # Read from Redshift
    from project_a.utils.secrets import get_redshift_credentials
    
    rs_creds = get_redshift_credentials(config)
    
    source_df = spark.read \
        .format("jdbc") \
        .option("url", rs_creds.get('url')) \
        .option("dbtable", redshift_table) \
        .option("user", rs_creds.get('user')) \
        .option("password", rs_creds.get('password')) \
        .load()
    
    # Read from S3
    target_df = spark.read.format("delta").load(s3_path)
    
    return reconcile_tables(
        source_df,
        target_df,
        key_columns,
        source_name="Redshift",
        target_name="S3"
    )


if __name__ == "__main__":
    import sys
    import argparse
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    
    from project_a.utils.spark_session import build_spark
    from project_a.utils.config import load_conf
    
    parser = argparse.ArgumentParser(description="Data reconciliation job")
    parser.add_argument("--source", choices=["snowflake", "redshift"], required=True, help="Source system")
    parser.add_argument("--target", required=True, help="Target S3 path")
    parser.add_argument("--source-table", required=True, help="Source table name")
    parser.add_argument("--key-columns", nargs="+", required=True, help="Key columns for reconciliation")
    parser.add_argument("--config", default="config/prod.yaml", help="Config file")
    
    args = parser.parse_args()
    
    config = load_conf(args.config)
    spark = build_spark(app_name="reconciliation", config=config)
    
    try:
        if args.source == "snowflake":
            result = reconcile_snowflake_to_s3(
                spark,
                snowflake_table=args.source_table,
                s3_path=args.target,
                config=config,
                key_columns=args.key_columns
            )
        elif args.source == "redshift":
            result = reconcile_redshift_to_s3(
                spark,
                redshift_table=args.source_table,
                s3_path=args.target,
                config=config,
                key_columns=args.key_columns
            )
        else:
            logger.error(f"Unknown source: {args.source}")
            sys.exit(1)
        
        if not result["reconciled"]:
            logger.error("❌ Reconciliation FAILED")
            sys.exit(1)
        
        logger.info("✅ Reconciliation PASSED")
        
    finally:
        spark.stop()

