"""
FX rates transformation job - Bronze to Silver layer with deduplication.
"""

import argparse
import logging
import os
import sys
from typing import Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, row_number, max as spark_max, when, lit
from pyspark.sql.window import Window
from datetime import date, timedelta

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.utils.logging import setup_json_logging

logger = logging.getLogger(__name__)


def deduplicate_fx_rates(spark: SparkSession, bronze_df) -> DataFrame:
    """
    Deduplicate FX rates by (as_of_date, ccy) keeping the latest record.
    
    Args:
        spark: Spark session
        bronze_df: Bronze FX rates DataFrame
        
    Returns:
        Deduplicated DataFrame
    """
    logger.info("Deduplicating FX rates by (as_of_date, ccy)")
    
    # FX rates freshness guard - check if rates are stale
    latest = bronze_df.agg(F.max("as_of_date")).first()[0]
    if latest < (date.today() - timedelta(days=2)):
        raise RuntimeError(f"FX rates stale: {latest}")
    
    # Define window for deduplication
    window_spec = Window.partitionBy("as_of_date", "ccy").orderBy(col("ingestion_timestamp").desc())
    
    # Add row number and filter latest records
    deduplicated_df = bronze_df.withColumn(
        "row_num", 
        row_number().over(window_spec)
    ).filter(col("row_num") == 1).drop("row_num")
    
    logger.info(f"Deduplication completed. Records: {deduplicated_df.count()}")
    return deduplicated_df


def add_rate_categories(spark: SparkSession, df) -> DataFrame:
    """
    Add rate categories based on currency type.
    
    Args:
        spark: Spark session
        df: FX rates DataFrame
        
    Returns:
        DataFrame with rate categories
    """
    logger.info("Adding rate categories")
    
    # Define major currencies
    major_currencies = ["USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD"]
    
    # Add category
    df_with_category = df.withColumn(
        "rate_category",
        when(col("ccy").isin(major_currencies), "major")
        .when(col("ccy").rlike("^[A-Z]{3}$"), "minor")
        .otherwise("exotic")
    )
    
    logger.info("Rate categories added successfully")
    return df_with_category


def validate_fx_rates(df) -> bool:
    """
    Validate FX rates data quality.
    
    Args:
        df: FX rates DataFrame
        
    Returns:
        True if validation passes, False otherwise
    """
    logger.info("Validating FX rates data quality")
    
    # Check for null values in critical columns
    null_checks = [
        df.filter(col("ccy").isNull()).count(),
        df.filter(col("rate_to_base").isNull()).count(),
        df.filter(col("as_of_date").isNull()).count()
    ]
    
    if any(null_checks):
        logger.error(f"Found null values in critical columns: {null_checks}")
        return False
    
    # Check for negative rates
    negative_rates = df.filter(col("rate_to_base") <= 0).count()
    if negative_rates > 0:
        logger.error(f"Found {negative_rates} negative or zero rates")
        return False
    
    # Check for duplicate (as_of_date, ccy) combinations
    total_count = df.count()
    unique_count = df.select("as_of_date", "ccy").distinct().count()
    if total_count != unique_count:
        logger.error(f"Found duplicate (as_of_date, ccy) combinations: {total_count} total, {unique_count} unique")
        return False
    
    logger.info("FX rates validation passed")
    return True


def main():
    """Main entry point for FX Bronze to Silver job."""
    parser = argparse.ArgumentParser(description="FX rates transformation Bronze to Silver")
    parser.add_argument("--config", required=True, help="Configuration file path")
    parser.add_argument("--lake-root", required=True, help="Data lake root path")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_json_logging()
    logger.info("Starting FX Bronze to Silver transformation job")
    
    # Load configuration
    config = load_conf(args.config)
    lake_root = args.lake_root
    
    # Create Spark session with Delta config
    spark = get_spark_session(
        app_name="fx-bronze-to-silver",
        config=get_delta_config()
    )
    
    try:
        # Read Bronze data
        bronze_path = f"{lake_root}/bronze/fx_rates"
        bronze_df = spark.read.format("delta").load(bronze_path)
        
        logger.info(f"Read {bronze_df.count()} records from Bronze layer")
        
        # Deduplicate data
        deduplicated_df = deduplicate_fx_rates(spark, bronze_df)
        
        # Add rate categories
        silver_df = add_rate_categories(spark, deduplicated_df)
        
        # Validate data quality
        if not validate_fx_rates(silver_df):
            raise ValueError("Data quality validation failed")
        
        # Write to Silver layer
        silver_path = f"{lake_root}/silver/fx_rates"
        silver_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(silver_path)
        
        logger.info(f"Successfully wrote {silver_df.count()} records to {silver_path}")
        
        # Show sample data
        silver_df.show(5, truncate=False)
        
        # Show data quality summary
        silver_df.groupBy("rate_category").count().show()
        
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
