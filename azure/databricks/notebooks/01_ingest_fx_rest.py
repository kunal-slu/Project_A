# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Ingest FX Data from REST API
# MAGIC 
# MAGIC This notebook ingests foreign exchange rate data from a public REST API and stores it in the bronze layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import os
import requests
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Get configuration from environment variables
lake_root = os.getenv("LAKE_ROOT", "abfss://lake@yourstorageaccount.dfs.core.windows.net")
log_level = os.getenv("LOG_LEVEL", "INFO")

# Set log level
spark.sparkContext.setLogLevel(log_level)

print(f"Lake root: {lake_root}")
print(f"Log level: {log_level}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch FX Data from REST API

# COMMAND ----------

def fetch_fx_data(base_currency: str = "USD", target_currencies: list = None) -> dict:
    """
    Fetch foreign exchange rate data from a public API.
    
    Args:
        base_currency: Base currency for exchange rates
        target_currencies: List of target currencies
        
    Returns:
        Dictionary containing exchange rate data
    """
    if target_currencies is None:
        target_currencies = ["EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY"]
    
    # Use a free FX API (example)
    api_url = f"https://api.exchangerate-api.com/v4/latest/{base_currency}"
    
    try:
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Extract rates for target currencies
        rates = {}
        for currency in target_currencies:
            if currency in data.get("rates", {}):
                rates[currency] = data["rates"][currency]
        
        return {
            "base_currency": base_currency,
            "as_of_date": data.get("date", datetime.now().strftime("%Y-%m-%d")),
            "rates": rates,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"Error fetching FX data: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process and Store Data

# COMMAND ----------

def process_fx_data(fx_data: dict) -> None:
    """
    Process FX data and store in bronze layer.
    
    Args:
        fx_data: Dictionary containing FX data
    """
    # Create DataFrame from FX data
    rows = []
    for currency, rate in fx_data["rates"].items():
        rows.append({
            "ccy": currency,
            "rate_to_base": rate,
            "as_of_date": fx_data["as_of_date"],
            "base_currency": fx_data["base_currency"],
            "timestamp": fx_data["timestamp"]
        })
    
    if not rows:
        print("No FX data to process")
        return
    
    # Create DataFrame
    df = spark.createDataFrame(rows)
    
    # Add metadata columns
    df_with_meta = df.withColumn("_source", lit("rest_api")) \
                     .withColumn("_extracted_at", current_timestamp()) \
                     .withColumn("_proc_date", to_date(current_timestamp()))
    
    # Write to bronze layer
    output_path = f"{lake_root}/bronze/fx_rates"
    
    print(f"Writing FX data to: {output_path}")
    
    df_with_meta.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("_proc_date") \
        .save(output_path)
    
    print(f"Successfully wrote {len(rows)} FX rate records to bronze layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution

# COMMAND ----------

try:
    # Fetch FX data
    print("Fetching FX data from REST API...")
    fx_data = fetch_fx_data()
    
    # Process and store data
    print("Processing and storing FX data...")
    process_fx_data(fx_data)
    
    print("FX data ingestion completed successfully!")
    
except Exception as e:
    print(f"FX data ingestion failed: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data

# COMMAND ----------

# Read and display the ingested data
fx_df = spark.read.format("delta").load(f"{lake_root}/bronze/fx_rates")
fx_df.show(10)

print(f"Total records: {fx_df.count()}")
print(f"Date range: {fx_df.select('as_of_date').distinct().collect()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("✅ FX data ingestion completed successfully!")
print(f"📊 Records processed: {fx_df.count()}")
print(f"📅 Date: {fx_df.select('as_of_date').distinct().collect()[0][0]}")
print(f"💾 Storage location: {lake_root}/bronze/fx_rates")
