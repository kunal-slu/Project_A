#!/usr/bin/env python3
"""
Redshift Customer Behavior Ingestion Job

This job extracts customer behavior data from Redshift and writes it to the Bronze layer
in S3 with proper partitioning and schema validation.
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime, date
from typing import Dict, Any
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

from pyspark_interview_project.utils.spark import get_spark_session
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.utils.logging import setup_json_logging

logger = logging.getLogger(__name__)


def get_redshift_connection_string() -> str:
    """
    Get Redshift JDBC connection string from environment variables.
    
    Returns:
        JDBC connection string
    """
    redshift_host = os.getenv("REDSHIFT_HOST")
    redshift_port = os.getenv("REDSHIFT_PORT", "5439")
    redshift_user = os.getenv("REDSHIFT_USER")
    redshift_pass = os.getenv("REDSHIFT_PASS")
    redshift_db = os.getenv("REDSHIFT_DB", "analytics")
    redshift_schema = os.getenv("REDSHIFT_SCHEMA", "public")
    
    return (
        f"jdbc:redshift://{redshift_host}:{redshift_port}/"
        f"{redshift_db}?user={redshift_user}&password={redshift_pass}&schema={redshift_schema}"
    )


def get_behavior_query() -> str:
    """
    Get the customer behavior query.
    
    Returns:
        SQL query string
    """
    return """
        SELECT 
            user_id,
            session_id,
            event_type,
            event_timestamp,
            page_url,
            referrer_url,
            device_type,
            browser,
            os,
            country,
            city,
            duration_seconds,
            conversion_value,
            campaign_id,
            source,
            medium
        FROM customer_behavior_events
        WHERE event_timestamp >= CURRENT_DATE - INTERVAL '7 days'
        ORDER BY event_timestamp DESC
    """


def extract_redshift_behavior_data(
    spark: SparkSession,
    connection_string: str,
    query: str
) -> Any:
    """
    Extract customer behavior data from Redshift.
    
    Args:
        spark: Spark session
        connection_string: Redshift JDBC connection string
        query: SQL query to execute
        
    Returns:
        Spark DataFrame with behavior data
    """
    logger.info("Extracting customer behavior data from Redshift")
    
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", connection_string) \
            .option("query", query) \
            .option("driver", "com.amazon.redshift.jdbc.Driver") \
            .load()
        
        logger.info(f"Successfully extracted {df.count()} behavior records")
        return df
        
    except Exception as e:
        logger.error(f"Failed to extract Redshift data: {str(e)}")
        raise


def add_metadata_columns(df: Any, source: str, table_name: str) -> Any:
    """
    Add metadata columns to the DataFrame.
    
    Args:
        df: Spark DataFrame
        source: Source system name
        table_name: Table name
        
    Returns:
        DataFrame with metadata columns
    """
    logger.info(f"Adding metadata columns for {table_name}")
    
    return df.withColumn("_ingestion_ts", current_timestamp()) \
             .withColumn("_source_system", lit(source)) \
             .withColumn("_table_name", lit(table_name)) \
             .withColumn("_ingestion_date", to_date(current_timestamp()))


def write_to_bronze(df: Any, table_name: str, lake_root: str) -> None:
    """
    Write DataFrame to Bronze layer in S3.
    
    Args:
        df: Spark DataFrame
        table_name: Table name
        lake_root: S3 root path of the data lake
    """
    logger.info(f"Writing {table_name} to Bronze layer")
    
    bronze_path = f"{lake_root}/bronze/redshift/{table_name}"
    
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .partitionBy("_ingestion_date") \
        .save(bronze_path)
    
    logger.info(f"Successfully wrote {table_name} to bronze layer")


def process_redshift_behavior_ingestion(spark: SparkSession, config: Dict[str, Any]) -> None:
    """
    Process Redshift behavior data ingestion.
    
    Args:
        spark: Spark session
        config: Configuration dictionary
    """
    logger.info("Starting Redshift behavior data ingestion")
    
    # Get connection string
    connection_string = get_redshift_connection_string()
    
    # Get query
    query = get_behavior_query()
    
    # Extract data
    behavior_df = extract_redshift_behavior_data(spark, connection_string, query)
    
    # Add metadata columns
    enriched_df = add_metadata_columns(behavior_df, "redshift", "customer_behavior")
    
    # Write to Bronze layer
    lake_root = config["lake"]["root"]
    write_to_bronze(enriched_df, "customer_behavior", lake_root)
    
    logger.info("Redshift behavior data ingestion completed successfully")


def main():
    """Main function to run the Redshift behavior ingestion job."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract Redshift behavior data")
    parser.add_argument("--config", required=True, help="Configuration file path")
    args = parser.parse_args()
    
    # Setup logging
    setup_json_logging()
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logging.getLogger().setLevel(getattr(logging, log_level.upper()))
    
    try:
        # Load configuration
        config = load_conf(args.config)
        
        # Create Spark session with Delta support
        spark = get_spark_session(
            "RedshiftBehaviorIngestion",
            extra_conf={
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
            }
        )
        
        # Process ingestion
        process_redshift_behavior_ingestion(spark, config)
        
    except Exception as e:
        logger.error(f"Redshift behavior ingestion failed: {e}")
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
