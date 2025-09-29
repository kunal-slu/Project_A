# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Salesforce to Bronze Data Ingestion
# MAGIC 
# MAGIC This notebook ingests data from Salesforce using incremental loading with SystemModstamp and stores it in the bronze layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import os
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_date, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, DateType

# Get configuration from environment variables
lake_root = os.getenv("LAKE_ROOT", "abfss://lake@yourstorageaccount.dfs.core.windows.net")
log_level = os.getenv("LOG_LEVEL", "INFO")

# Set log level
spark.sparkContext.setLogLevel(log_level)

print(f"Lake root: {lake_root}")
print(f"Log level: {log_level}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Salesforce Credentials from Key Vault

# COMMAND ----------

def get_salesforce_credentials() -> dict:
    """
    Get Salesforce credentials from Azure Key Vault.
    
    Returns:
        Dictionary with Salesforce credentials
    """
    try:
        # Use Databricks secrets (backed by Key Vault)
        username = dbutils.secrets.get("salesforce", "username")
        password = dbutils.secrets.get("salesforce", "password")
        security_token = dbutils.secrets.get("salesforce", "security_token")
        domain = dbutils.secrets.get("salesforce", "domain")
        
        return {
            "username": username,
            "password": password,
            "security_token": security_token,
            "domain": domain or "login"
        }
        
    except Exception as e:
        print(f"Error getting credentials from Key Vault: {e}")
        # Fall back to environment variables
        return {
            "username": os.getenv("SF_USER"),
            "password": os.getenv("SF_PASS"),
            "security_token": os.getenv("SF_TOKEN"),
            "domain": os.getenv("SF_DOMAIN", "login")
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salesforce Data Extraction

# COMMAND ----------

def extract_salesforce_data(object_name: str, last_checkpoint: str = None) -> SparkSession.DataFrame:
    """
    Extract data from Salesforce using SOQL.
    
    Args:
        object_name: Salesforce object name (Lead, Account, etc.)
        last_checkpoint: Last checkpoint timestamp for incremental loading
        
    Returns:
        Spark DataFrame with Salesforce data
    """
    print(f"Extracting {object_name} data from Salesforce...")
    
    # Get credentials
    creds = get_salesforce_credentials()
    
    # Build SOQL query based on object
    if object_name == "Lead":
        soql_query = build_lead_soql(last_checkpoint)
    elif object_name == "Account":
        soql_query = build_account_soql(last_checkpoint)
    else:
        raise ValueError(f"Unsupported object: {object_name}")
    
    print(f"SOQL Query: {soql_query}")
    
    # Use Salesforce connector (if available) or JDBC
    try:
        # Try using Salesforce connector
        df = spark.read \
            .format("com.springml.spark.salesforce") \
            .option("username", creds["username"]) \
            .option("password", creds["password"]) \
            .option("securityToken", creds["security_token"]) \
            .option("soql", soql_query) \
            .load()
        
        print(f"Successfully extracted {df.count()} {object_name} records")
        return df
        
    except Exception as e:
        print(f"Salesforce connector failed, trying JDBC: {e}")
        
        # Fall back to JDBC
        connection_string = f"jdbc:salesforce:user={creds['username']};password={creds['password']};securityToken={creds['security_token']};serverURL=https://{creds['domain']}.salesforce.com"
        
        df = spark.read \
            .format("jdbc") \
            .option("url", connection_string) \
            .option("query", soql_query) \
            .option("driver", "com.salesforce.jdbc.SalesforceDriver") \
            .load()
        
        print(f"Successfully extracted {df.count()} {object_name} records via JDBC")
        return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## SOQL Query Builders

# COMMAND ----------

def build_lead_soql(last_checkpoint: str = None) -> str:
    """
    Build SOQL query for Lead object.
    
    Args:
        last_checkpoint: Last checkpoint timestamp
        
    Returns:
        SOQL query string
    """
    if last_checkpoint:
        where_clause = f"WHERE SystemModstamp >= {last_checkpoint}"
    else:
        where_clause = "WHERE SystemModstamp >= LAST_N_DAYS:1"
    
    return f"""
    SELECT 
        Id,
        FirstName,
        LastName,
        Email,
        Phone,
        Company,
        Status,
        LeadSource,
        CreatedDate,
        LastModifiedDate,
        SystemModstamp
    FROM Lead 
    {where_clause}
    ORDER BY SystemModstamp
    """

def build_account_soql(last_checkpoint: str = None) -> str:
    """
    Build SOQL query for Account object.
    
    Args:
        last_checkpoint: Last checkpoint timestamp
        
    Returns:
        SOQL query string
    """
    if last_checkpoint:
        where_clause = f"WHERE SystemModstamp >= {last_checkpoint}"
    else:
        where_clause = "WHERE SystemModstamp >= LAST_N_DAYS:1"
    
    return f"""
    SELECT 
        Id,
        Name,
        Type,
        Industry,
        BillingStreet,
        BillingCity,
        BillingState,
        BillingPostalCode,
        BillingCountry,
        Phone,
        Website,
        CreatedDate,
        LastModifiedDate,
        SystemModstamp
    FROM Account 
    {where_clause}
    ORDER BY SystemModstamp
    """

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint Management

# COMMAND ----------

def get_last_checkpoint(object_name: str) -> str:
    """
    Get the last checkpoint timestamp for incremental loading.
    
    Args:
        object_name: Salesforce object name
        
    Returns:
        Last checkpoint timestamp or None
    """
    checkpoint_path = f"{lake_root}/_checkpoints/salesforce/{object_name.lower()}"
    
    try:
        # Try to read the last checkpoint
        checkpoint_df = spark.read.format("delta").load(checkpoint_path)
        last_checkpoint = checkpoint_df.select("last_checkpoint").collect()[0][0]
        print(f"Found last checkpoint for {object_name}: {last_checkpoint}")
        return last_checkpoint
    except Exception:
        print(f"No checkpoint found for {object_name}, using LAST_N_DAYS:1")
        return None

def save_checkpoint(object_name: str, checkpoint_time: str) -> None:
    """
    Save the checkpoint timestamp for incremental loading.
    
    Args:
        object_name: Salesforce object name
        checkpoint_time: Checkpoint timestamp
    """
    checkpoint_path = f"{lake_root}/_checkpoints/salesforce/{object_name.lower()}"
    
    # Create checkpoint DataFrame
    checkpoint_df = spark.createDataFrame(
        [(checkpoint_time,)], 
        ["last_checkpoint"]
    )
    
    # Write checkpoint
    checkpoint_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(checkpoint_path)
    
    print(f"Saved checkpoint for {object_name}: {checkpoint_time}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process and Store Data

# COMMAND ----------

def process_and_store_salesforce_data(object_name: str) -> None:
    """
    Process and store Salesforce data for a specific object.
    
    Args:
        object_name: Salesforce object name
    """
    print(f"Processing {object_name} data...")
    
    # Get last checkpoint
    last_checkpoint = get_last_checkpoint(object_name)
    
    # Extract data
    df = extract_salesforce_data(object_name, last_checkpoint)
    
    if df.count() == 0:
        print(f"No new {object_name} data to process")
        return
    
    # Add metadata columns
    df_with_meta = df.withColumn("_source", lit("salesforce")) \
                     .withColumn("_object", lit(object_name.lower())) \
                     .withColumn("_extracted_at", current_timestamp()) \
                     .withColumn("_proc_date", to_date(current_timestamp()))
    
    # Write to bronze layer
    output_path = f"{lake_root}/bronze/salesforce/{object_name.lower()}"
    
    print(f"Writing {object_name} data to: {output_path}")
    
    df_with_meta.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("_proc_date") \
        .save(output_path)
    
    # Save checkpoint
    if df.count() > 0:
        max_timestamp = df.select("SystemModstamp").rdd.max()[0]
        save_checkpoint(object_name, max_timestamp)
    
    print(f"Successfully processed {df.count()} {object_name} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Processing Pipeline

# COMMAND ----------

try:
    # Process Lead data
    print("Processing Lead data...")
    process_and_store_salesforce_data("Lead")
    
    # Process Account data
    print("Processing Account data...")
    process_and_store_salesforce_data("Account")
    
    print("✅ Salesforce data ingestion completed successfully!")
    
except Exception as e:
    print(f"❌ Salesforce data ingestion failed: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data

# COMMAND ----------

# Read and display the ingested data
lead_df = spark.read.format("delta").load(f"{lake_root}/bronze/salesforce/lead")
account_df = spark.read.format("delta").load(f"{lake_root}/bronze/salesforce/account")

print("Lead data:")
lead_df.show(5)
print(f"Lead records: {lead_df.count()}")

print("\nAccount data:")
account_df.show(5)
print(f"Account records: {account_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("✅ Salesforce data ingestion completed successfully!")
print(f"📊 Lead records: {lead_df.count()}")
print(f"📊 Account records: {account_df.count()}")
print(f"💾 Storage location: {lake_root}/bronze/salesforce/")
print(f"📅 Processing date: {datetime.now().isoformat()}")
