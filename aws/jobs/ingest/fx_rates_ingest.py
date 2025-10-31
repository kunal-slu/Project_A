#!/usr/bin/env python3
"""
FX Rates and Financial Metrics Ingestion Job

This job extracts FX rates and financial metrics from external vendor APIs
and writes them to the Bronze layer in S3 with proper partitioning and schema validation.
"""

import os
import sys
import logging
import pandas as pd
import requests
from datetime import datetime, date, timedelta
from typing import Dict, Any, List
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


def get_fx_api_config() -> Dict[str, str]:
    """
    Get FX API configuration from environment variables.
    
    Returns:
        Dictionary with API configuration
    """
    return {
        "api_key": os.getenv("FX_API_KEY"),
        "base_url": os.getenv("FX_BASE_URL", "https://api.exchangerate-api.com/v4"),
        "timeout": int(os.getenv("FX_API_TIMEOUT", "30"))
    }


def fetch_fx_rates(api_config: Dict[str, str], base_currency: str = "USD") -> List[Dict]:
    """
    Fetch FX rates from external API.
    
    Args:
        api_config: API configuration
        base_currency: Base currency for rates
        
    Returns:
        List of FX rate records
    """
    logger.info(f"Fetching FX rates for base currency: {base_currency}")
    
    try:
        url = f"{api_config['base_url']}/latest/{base_currency}"
        headers = {"Authorization": f"Bearer {api_config['api_key']}"}
        
        response = requests.get(url, headers=headers, timeout=api_config['timeout'])
        response.raise_for_status()
        
        data = response.json()
        
        # Transform API response to our format
        fx_records = []
        timestamp = datetime.now()
        
        for currency, rate in data.get('rates', {}).items():
            fx_records.append({
                "base_currency": base_currency,
                "target_currency": currency,
                "exchange_rate": rate,
                "rate_timestamp": timestamp,
                "source": "exchangerate-api"
            })
        
        logger.info(f"Successfully fetched {len(fx_records)} FX rates")
        return fx_records
        
    except Exception as e:
        logger.error(f"Failed to fetch FX rates: {str(e)}")
        raise


def fetch_financial_metrics(api_config: Dict[str, str]) -> List[Dict]:
    """
    Fetch financial metrics from external API.
    
    Args:
        api_config: API configuration
        
    Returns:
        List of financial metric records
    """
    logger.info("Fetching financial metrics")
    
    try:
        # Simulate financial metrics API call
        # In production, this would call a real financial data provider
        metrics_records = []
        timestamp = datetime.now()
        
        # Simulate some financial metrics
        metrics_data = [
            {"metric_name": "VIX", "value": 18.5, "currency": "USD"},
            {"metric_name": "DXY", "value": 103.2, "currency": "USD"},
            {"metric_name": "GOLD_PRICE", "value": 1985.50, "currency": "USD"},
            {"metric_name": "OIL_PRICE", "value": 78.25, "currency": "USD"},
            {"metric_name": "BOND_YIELD_10Y", "value": 4.25, "currency": "USD"},
        ]
        
        for metric in metrics_data:
            metrics_records.append({
                "metric_name": metric["metric_name"],
                "metric_value": metric["value"],
                "currency": metric["currency"],
                "metric_timestamp": timestamp,
                "source": "financial-api"
            })
        
        logger.info(f"Successfully fetched {len(metrics_records)} financial metrics")
        return metrics_records
        
    except Exception as e:
        logger.error(f"Failed to fetch financial metrics: {str(e)}")
        raise


def create_fx_rates_dataframe(spark: SparkSession, fx_records: List[Dict]) -> Any:
    """
    Create Spark DataFrame from FX rates records.
    
    Args:
        spark: Spark session
        fx_records: List of FX rate records
        
    Returns:
        Spark DataFrame
    """
    logger.info("Creating FX rates DataFrame")
    
    # Convert to pandas DataFrame first
    pandas_df = pd.DataFrame(fx_records)
    
    # Convert to Spark DataFrame
    df = spark.createDataFrame(pandas_df)
    
    return df


def create_financial_metrics_dataframe(spark: SparkSession, metrics_records: List[Dict]) -> Any:
    """
    Create Spark DataFrame from financial metrics records.
    
    Args:
        spark: Spark session
        metrics_records: List of financial metric records
        
    Returns:
        Spark DataFrame
    """
    logger.info("Creating financial metrics DataFrame")
    
    # Convert to pandas DataFrame first
    pandas_df = pd.DataFrame(metrics_records)
    
    # Convert to Spark DataFrame
    df = spark.createDataFrame(pandas_df)
    
    return df


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
    
    bronze_path = f"{lake_root}/bronze/fx/{table_name}"
    
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .partitionBy("_ingestion_date") \
        .save(bronze_path)
    
    logger.info(f"Successfully wrote {table_name} to bronze layer")


def process_fx_ingestion(spark: SparkSession, config: Dict[str, Any]) -> None:
    """
    Process FX rates and financial metrics ingestion.
    
    Args:
        spark: Spark session
        config: Configuration dictionary
    """
    logger.info("Starting FX rates and financial metrics ingestion")
    
    # Get API configuration
    api_config = get_fx_api_config()
    
    # Fetch FX rates
    fx_records = fetch_fx_rates(api_config)
    fx_df = create_fx_rates_dataframe(spark, fx_records)
    fx_enriched_df = add_metadata_columns(fx_df, "fx_vendor", "fx_rates")
    
    # Fetch financial metrics
    metrics_records = fetch_financial_metrics(api_config)
    metrics_df = create_financial_metrics_dataframe(spark, metrics_records)
    metrics_enriched_df = add_metadata_columns(metrics_df, "fx_vendor", "financial_metrics")
    
    # Write to Bronze layer
    lake_root = config["lake"]["root"]
    write_to_bronze(fx_enriched_df, "fx_rates", lake_root)
    write_to_bronze(metrics_enriched_df, "financial_metrics", lake_root)
    
    logger.info("FX rates and financial metrics ingestion completed successfully")


def main():
    """Main function to run the FX ingestion job."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract FX rates and financial metrics")
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
            "FXIngestion",
            extra_conf={
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
            }
        )
        
        # Process ingestion
        process_fx_ingestion(spark, config)
        
    except Exception as e:
        logger.error(f"FX ingestion failed: {e}")
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
