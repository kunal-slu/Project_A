"""
Dead Letter Queue (DLQ) Handler

Handles failed/rejected records by writing them to DLQ with metadata.
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, current_timestamp, col
import json

logger = logging.getLogger(__name__)


def write_to_dlq(
    spark: SparkSession,
    rejected_df: DataFrame,
    dataset_name: str,
    error_reason: str,
    error_type: str,
    config: Dict[str, Any],
    processing_date: Optional[str] = None
) -> None:
    """
    Write rejected records to Dead Letter Queue (DLQ).
    
    Args:
        spark: SparkSession
        rejected_df: DataFrame with rejected records
        dataset_name: Name of the dataset (e.g., 'crm_contacts')
        error_reason: Description of why records were rejected
        error_type: Type of error (e.g., 'schema_validation', 'dq_failure')
        config: Configuration dict
        processing_date: Processing date in YYYY-MM-DD format (default: today)
    """
    if rejected_df.isEmpty():
        logger.info("No records to write to DLQ")
        return
    
    # Get DLQ path from config
    dlq_base = config.get("data_lake", {}).get("dlq_prefix", "s3://bucket/_errors")
    if not dlq_base.startswith("s3://"):
        dlq_base = f"s3://{dlq_base}"
    
    # Set processing date
    if processing_date is None:
        processing_date = datetime.now().strftime("%Y-%m-%d")
    
    # DLQ path structure: s3://bucket/_errors/{dataset}/dt={date}/
    dlq_path = f"{dlq_base}/{dataset_name}/dt={processing_date}/"
    
    # Add error metadata columns
    rejected_with_metadata = rejected_df.withColumn(
        "_dlq_error_type", lit(error_type)
    ).withColumn(
        "_dlq_error_reason", lit(error_reason)
    ).withColumn(
        "_dlq_timestamp", current_timestamp()
    ).withColumn(
        "_dlq_processing_date", lit(processing_date)
    )
    
    # Write rejected records
    logger.warning(f"Writing {rejected_df.count()} rejected records to DLQ: {dlq_path}")
    rejected_with_metadata.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .partitionBy("_dlq_processing_date") \
        .save(f"{dlq_path}rejected_rows")
    
    # Write error metadata JSON
    error_metadata = {
        "error_type": error_type,
        "error_reason": error_reason,
        "row_count": rejected_df.count(),
        "timestamp": datetime.utcnow().isoformat(),
        "dataset": dataset_name,
        "processing_date": processing_date,
        "dlq_path": dlq_path
    }
    
    # Save metadata (in real implementation, would use boto3 to write JSON to S3)
    logger.info(f"Error metadata: {json.dumps(error_metadata, indent=2)}")
    
    # Create success marker
    success_df = spark.createDataFrame(
        [(processing_date, datetime.utcnow().isoformat())],
        ["processing_date", "completed_at"]
    )
    success_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{dlq_path}_SUCCESS")


def quarantine_data(
    spark: SparkSession,
    df: DataFrame,
    dataset_name: str,
    reason: str,
    config: Dict[str, Any],
    processing_date: Optional[str] = None
) -> None:
    """
    Move data to quarantine for investigation.
    
    Args:
        spark: SparkSession
        df: DataFrame to quarantine
        dataset_name: Name of the dataset
        reason: Reason for quarantine
        config: Configuration dict
        processing_date: Processing date
    """
    if df.isEmpty():
        return
    
    quarantine_base = config.get("data_lake", {}).get("quarantine_prefix", "s3://bucket/_quarantine")
    if not quarantine_base.startswith("s3://"):
        quarantine_base = f"s3://{quarantine_base}"
    
    if processing_date is None:
        processing_date = datetime.now().strftime("%Y-%m-%d")
    
    quarantine_path = f"{quarantine_base}/{dataset_name}/dt={processing_date}/{reason}/"
    
    logger.warning(f"Quarantining {df.count()} records: {quarantine_path}")
    
    df.withColumn("_quarantine_reason", lit(reason)) \
      .withColumn("_quarantine_timestamp", current_timestamp()) \
      .write \
      .format("delta") \
      .mode("overwrite") \
      .partitionBy("_quarantine_timestamp") \
      .save(quarantine_path)

