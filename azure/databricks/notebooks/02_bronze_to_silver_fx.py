# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze to Silver FX Data Processing
# MAGIC 
# MAGIC This notebook processes FX data from bronze to silver layer with deduplication, type conversion, and latest rate per currency.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_date, max as spark_max, row_number, desc
from pyspark.sql.window import Window

# Get configuration from environment variables
lake_root = os.getenv("LAKE_ROOT", "abfss://lake@yourstorageaccount.dfs.core.windows.net")
log_level = os.getenv("LOG_LEVEL", "INFO")

# Set log level
spark.sparkContext.setLogLevel(log_level)

print(f"Lake root: {lake_root}")
print(f"Log level: {log_level}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Data

# COMMAND ----------

def read_bronze_fx_data() -> SparkSession.DataFrame:
    """
    Read FX data from bronze layer.
    
    Returns:
        Spark DataFrame with bronze FX data
    """
    bronze_path = f"{lake_root}/bronze/fx_rates"
    
    print(f"Reading bronze FX data from: {bronze_path}")
    
    df = spark.read.format("delta").load(bronze_path)
    
    print(f"Bronze data count: {df.count()}")
    print(f"Bronze data schema:")
    df.printSchema()
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality and Cleaning

# COMMAND ----------

def clean_fx_data(df: SparkSession.DataFrame) -> SparkSession.DataFrame:
    """
    Clean and validate FX data.
    
    Args:
        df: Input DataFrame with bronze FX data
        
    Returns:
        Cleaned DataFrame
    """
    print("Cleaning FX data...")
    
    # Filter out null values
    df_clean = df.filter(
        col("ccy").isNotNull() &
        col("rate_to_base").isNotNull() &
        col("as_of_date").isNotNull()
    )
    
    # Convert data types
    df_clean = df_clean.withColumn("rate_to_base", col("rate_to_base").cast("double"))
    df_clean = df_clean.withColumn("as_of_date", col("as_of_date").cast("date"))
    
    # Filter out invalid rates (negative or zero)
    df_clean = df_clean.filter(col("rate_to_base") > 0)
    
    # Remove duplicates based on (ccy, as_of_date)
    df_clean = df_clean.dropDuplicates(["ccy", "as_of_date"])
    
    print(f"Cleaned data count: {df_clean.count()}")
    
    return df_clean

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Latest Rate Per Currency

# COMMAND ----------

def get_latest_rates(df: SparkSession.DataFrame) -> SparkSession.DataFrame:
    """
    Get the latest rate per currency.
    
    Args:
        df: Input DataFrame with cleaned FX data
        
    Returns:
        DataFrame with latest rates per currency
    """
    print("Getting latest rates per currency...")
    
    # Define window for ranking by date
    window_spec = Window.partitionBy("ccy").orderBy(desc("as_of_date"))
    
    # Add row number to get latest record per currency
    df_with_rank = df.withColumn("row_num", row_number().over(window_spec))
    
    # Filter to get only the latest record per currency
    df_latest = df_with_rank.filter(col("row_num") == 1).drop("row_num")
    
    print(f"Latest rates count: {df_latest.count()}")
    
    return df_latest

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Business Logic

# COMMAND ----------

def add_business_logic(df: SparkSession.DataFrame) -> SparkSession.DataFrame:
    """
    Add business logic and calculated fields.
    
    Args:
        df: Input DataFrame with latest rates
        
    Returns:
        DataFrame with business logic applied
    """
    print("Adding business logic...")
    
    # Add calculated fields
    df_business = df.withColumn("rate_category", 
        when(col("rate_to_base") < 1, "below_par")
        .when(col("rate_to_base") < 2, "near_par")
        .otherwise("above_par")
    ).withColumn("is_major_currency",
        col("ccy").isin(["EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY"])
    ).withColumn("_processed_at", current_timestamp())
    
    print(f"Business logic applied to {df_business.count()} records")
    
    return df_business

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Layer

# COMMAND ----------

def write_to_silver(df: SparkSession.DataFrame) -> None:
    """
    Write processed data to silver layer.
    
    Args:
        df: Input DataFrame with processed data
    """
    silver_path = f"{lake_root}/silver/fx_rates"
    
    print(f"Writing to silver layer: {silver_path}")
    
    # Write to silver layer with partitioning
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .partitionBy("_proc_date") \
        .save(silver_path)
    
    print("Successfully wrote to silver layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Processing Pipeline

# COMMAND ----------

try:
    # Step 1: Read bronze data
    print("Step 1: Reading bronze data...")
    bronze_df = read_bronze_fx_data()
    
    # Step 2: Clean data
    print("Step 2: Cleaning data...")
    clean_df = clean_fx_data(bronze_df)
    
    # Step 3: Get latest rates
    print("Step 3: Getting latest rates...")
    latest_df = get_latest_rates(clean_df)
    
    # Step 4: Add business logic
    print("Step 4: Adding business logic...")
    business_df = add_business_logic(latest_df)
    
    # Step 5: Write to silver
    print("Step 5: Writing to silver layer...")
    write_to_silver(business_df)
    
    print("✅ Bronze to Silver FX processing completed successfully!")
    
except Exception as e:
    print(f"❌ Bronze to Silver FX processing failed: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Silver Data

# COMMAND ----------

# Read and display the silver data
silver_df = spark.read.format("delta").load(f"{lake_root}/silver/fx_rates")
silver_df.show(10)

print(f"Silver data count: {silver_df.count()}")
print(f"Silver data schema:")
silver_df.printSchema()

# Show summary statistics
silver_df.select("ccy", "rate_to_base", "rate_category", "is_major_currency").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("✅ Bronze to Silver FX processing completed successfully!")
print(f"📊 Silver records: {silver_df.count()}")
print(f"💾 Storage location: {lake_root}/silver/fx_rates")
print(f"📅 Processing date: {silver_df.select('_processed_at').first()[0]}")
