"""
Data extraction utilities for the PySpark pipeline.
"""

import os
import logging
from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp

logger = logging.getLogger(__name__)

def extract_csv_data(
    spark: SparkSession,
    file_path: str,
    source_name: str,
    table_name: str
) -> DataFrame:
    """
    Extract data from CSV files.
    
    Args:
        spark: SparkSession object
        file_path: Path to the CSV file
        source_name: Name of the data source
        table_name: Name of the table
        
    Returns:
        DataFrame with extracted data
    """
    logger.info(f"Extracting data from {source_name} - {table_name}")
    
    try:
        df = spark.read             .option("header", "true")             .option("inferSchema", "true")             .csv(file_path)
        
        # Add metadata columns
        df = df.withColumn("record_source", lit(source_name))                .withColumn("record_table", lit(table_name))                .withColumn("ingest_timestamp", current_timestamp())
        
        logger.info(f"Successfully extracted {df.count()} records from {source_name} - {table_name}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to extract data from {source_name} - {table_name}: {e}")
        raise

def extract_all_data_sources(
    spark: SparkSession,
    config: Dict[str, Any]
) -> Dict[str, DataFrame]:
    """
    Extract data from all configured data sources.
    
    Args:
        spark: SparkSession object
        config: Configuration dictionary
        
    Returns:
        Dictionary of DataFrames keyed by source name
    """
    datasets = {}
    
    # Extract HubSpot data
    hubspot_files = [
        ("aws/data_fixed/01_hubspot_crm/hubspot_contacts_25000.csv", "contacts"),
        ("aws/data_fixed/01_hubspot_crm/hubspot_deals_30000.csv", "deals")
    ]
    
    for file_path, table_name in hubspot_files:
        if os.path.exists(file_path):
            df = extract_csv_data(spark, file_path, "hubspot", table_name)
            datasets[f"hubspot_{table_name}"] = df
    
    # Extract Snowflake data
    snowflake_files = [
        ("aws/data_fixed/02_snowflake_warehouse/snowflake_customers_50000.csv", "customers"),
        ("aws/data_fixed/02_snowflake_warehouse/snowflake_orders_100000.csv", "orders"),
        ("aws/data_fixed/02_snowflake_warehouse/snowflake_products_10000.csv", "products")
    ]
    
    for file_path, table_name in snowflake_files:
        if os.path.exists(file_path):
            df = extract_csv_data(spark, file_path, "snowflake", table_name)
            datasets[f"snowflake_{table_name}"] = df
    
    # Extract Redshift data
    redshift_files = [
        ("aws/data_fixed/03_redshift_analytics/redshift_customer_behavior_50000.csv", "customer_behavior")
    ]
    
    for file_path, table_name in redshift_files:
        if os.path.exists(file_path):
            df = extract_csv_data(spark, file_path, "redshift", table_name)
            datasets[f"redshift_{table_name}"] = df
    
    # Extract Stream data
    stream_files = [
        ("aws/data_fixed/04_stream_data/stream_kafka_events_100000.csv", "kafka_events")
    ]
    
    for file_path, table_name in stream_files:
        if os.path.exists(file_path):
            df = extract_csv_data(spark, file_path, "stream", table_name)
            datasets[f"stream_{table_name}"] = df
    
    # Extract FX Rates data
    fx_files = [
        ("aws/data_fixed/05_fx_rates/fx_rates_historical_730_days.csv", "historical_rates")
    ]
    
    for file_path, table_name in fx_files:
        if os.path.exists(file_path):
            df = extract_csv_data(spark, file_path, "fx_rates", table_name)
            datasets[f"fx_rates_{table_name}"] = df
    
    return datasets


def extract_customers(spark: SparkSession, path: str) -> DataFrame:
    """
    Extract customer data from CSV file.
    
    Args:
        spark: SparkSession object
        path: Path to the CSV file
        
    Returns:
        DataFrame with customer data
    """
    return extract_csv_data(spark, path, "customers", "customers")


def extract_products(spark: SparkSession, path: str) -> DataFrame:
    """
    Extract product data from CSV file.
    
    Args:
        spark: SparkSession object
        path: Path to the CSV file
        
    Returns:
        DataFrame with product data
    """
    return extract_csv_data(spark, path, "products", "products")


def extract_orders_json(spark: SparkSession, path: str) -> DataFrame:
    """
    Extract orders data from JSON file.
    
    Args:
        spark: SparkSession object
        path: Path to the JSON file
        
    Returns:
        DataFrame with orders data
    """
    logger.info(f"Extracting orders data from {path}")
    
    try:
        df = spark.read.option("multiline", "true").json(path)
        
        # Add metadata columns
        df = df.withColumn("record_source", lit("orders")) \
               .withColumn("record_table", lit("orders")) \
               .withColumn("ingest_timestamp", current_timestamp())
        
        logger.info(f"Successfully extracted {df.count()} orders")
        return df
        
    except Exception as e:
        logger.error(f"Failed to extract orders data from {path}: {e}")
        raise


def extract_returns(spark: SparkSession, path: str) -> DataFrame:
    """
    Extract returns data from JSON file.
    
    Args:
        spark: SparkSession object
        path: Path to the JSON file
        
    Returns:
        DataFrame with returns data
    """
    logger.info(f"Extracting returns data from {path}")
    
    try:
        df = spark.read.option("multiline", "true").json(path)
        
        # Add metadata columns
        df = df.withColumn("record_source", lit("returns")) \
               .withColumn("record_table", lit("returns")) \
               .withColumn("ingest_timestamp", current_timestamp())
        
        logger.info(f"Successfully extracted {df.count()} returns")
        return df
        
    except Exception as e:
        logger.error(f"Failed to extract returns data from {path}: {e}")
        raise


def extract_exchange_rates(spark: SparkSession, path: str) -> DataFrame:
    """
    Extract exchange rates data from CSV file.
    
    Args:
        spark: SparkSession object
        path: Path to the CSV file
        
    Returns:
        DataFrame with exchange rates data
    """
    return extract_csv_data(spark, path, "exchange_rates", "exchange_rates")


def extract_inventory_snapshots(spark: SparkSession, path: str) -> DataFrame:
    """
    Extract inventory snapshots data from CSV file.
    
    Args:
        spark: SparkSession object
        path: Path to the CSV file
        
    Returns:
        DataFrame with inventory snapshots data
    """
    return extract_csv_data(spark, path, "inventory", "inventory_snapshots")
