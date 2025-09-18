# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Simple Azure ETL - Minimal Code Upload
# MAGIC
# MAGIC This notebook runs your ETL pipeline with **minimal code upload**.
# MAGIC It uses built-in Databricks functions and only requires:
# MAGIC - Configuration file
# MAGIC - Data files (auto-uploaded)
# MAGIC
# MAGIC **No need to upload your entire codebase!**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install Required Libraries

# COMMAND ----------

# Install only the essential packages
dbutils.library.installPyPI("delta-spark", "3.0.0")
dbutils.library.installPyPI("pyyaml", "6.0.1")

# Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Configuration

# COMMAND ----------

import yaml
from pyspark.sql.functions import *

# Load configuration from uploaded file
config_path = "/dbfs/pyspark-etl/config/config-azure-dev.yaml"

with open(config_path, 'r') as file:
    config = yaml.safe_load(file)

print("✅ Configuration loaded")
print(f"Storage Account: {config['azure']['storage']['account_name']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Initialize Spark Session

# COMMAND ----------

# Configure Spark for Azure
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Set Azure storage configuration
storage_account = config['azure']['storage']['account_name']
storage_key = config['azure']['storage']['account_key']

spark.conf.set(f"spark.hadoop.fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key)
spark.conf.set(f"spark.hadoop.fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SharedKey")

print("✅ Spark session configured for Azure")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Extract Data (Bronze Layer)

# COMMAND ----------

print("📥 Extracting data to Bronze layer...")

# Extract customers
customers_df = spark.read.csv(
    config['input']['customer_path'],
    header=True,
    inferSchema=True
)

# Extract products
products_df = spark.read.csv(
    config['input']['product_path'],
    header=True,
    inferSchema=True
)

# Extract orders
orders_df = spark.read.json(config['input']['orders_path'])

# Extract returns
returns_df = spark.read.json(config['input']['returns_path'])

# Extract exchange rates
rates_df = spark.read.csv(
    config['input']['exchange_rates_path'],
    header=True,
    inferSchema=True
)

# Extract inventory
inventory_df = spark.read.csv(
    config['input']['inventory_path'],
    header=True,
    inferSchema=True
)

print("✅ Data extraction complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Load to Bronze Layer

# COMMAND ----------

print("🥉 Loading to Bronze layer...")

# Write to bronze layer
customers_df.write.format("delta").mode("overwrite").save(f"{config['output']['bronze_path']}/customers_raw")
products_df.write.format("delta").mode("overwrite").save(f"{config['output']['bronze_path']}/products_raw")
orders_df.write.format("delta").mode("overwrite").save(f"{config['output']['bronze_path']}/orders_raw")
returns_df.write.format("delta").mode("overwrite").save(f"{config['output']['bronze_path']}/returns_raw")
rates_df.write.format("delta").mode("overwrite").save(f"{config['output']['bronze_path']}/fx_rates")
inventory_df.write.format("delta").mode("overwrite").save(f"{config['output']['bronze_path']}/inventory_snapshots")

print("✅ Bronze layer loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Transform Data (Silver Layer)

# COMMAND ----------

print("🔄 Transforming data for Silver layer...")

# Clean and enrich customers
customers_silver = customers_df.select(
    col("customer_id"),
    trim(col("name")).alias("customer_name"),
    trim(col("email")).alias("email"),
    col("age"),
    trim(col("address")).alias("address"),
    col("registration_date"),
    current_timestamp().alias("processed_at")
).filter(col("customer_id").isNotNull())

# Clean and enrich products
products_silver = products_df.select(
    col("product_id"),
    trim(col("name")).alias("product_name"),
    col("category"),
    col("price"),
    col("supplier_id"),
    current_timestamp().alias("processed_at")
).filter(col("product_id").isNotNull())

# Clean and enrich orders
orders_silver = orders_df.select(
    col("order_id"),
    col("customer_id"),
    col("product_id"),
    col("quantity"),
    col("order_date"),
    col("total_amount"),
    current_timestamp().alias("processed_at")
).filter(col("order_id").isNotNull())

print("✅ Data transformation complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Load to Silver Layer

# COMMAND ----------

print("🥈 Loading to Silver layer...")

# Write to silver layer
customers_silver.write.format("delta").mode("overwrite").save(f"{config['output']['silver_path']}/customers_enriched")
products_silver.write.format("delta").mode("overwrite").save(f"{config['output']['silver_path']}/products_enriched")
orders_silver.write.format("delta").mode("overwrite").save(f"{config['output']['silver_path']}/orders_enriched")

print("✅ Silver layer loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Create Gold Layer (Analytics)

# COMMAND ----------

print("🥇 Creating Gold layer...")

# Join orders with customers and products
fact_orders = orders_silver.join(
    customers_silver, "customer_id", "left"
).join(
    products_silver, "product_id", "left"
).select(
    col("order_id"),
    col("customer_id"),
    col("customer_name"),
    col("product_id"),
    col("product_name"),
    col("category"),
    col("quantity"),
    col("price"),
    col("total_amount"),
    col("order_date"),
    col("processed_at")
)

# Add year-month partition
fact_orders_with_partition = fact_orders.withColumn(
    "order_ym",
    date_format(col("order_date"), "yyyy-MM")
)

# Write to gold layer
fact_orders_with_partition.write.format("delta").mode("overwrite").partitionBy("order_ym").save(f"{config['output']['gold_path']}/fact_orders")

print("✅ Gold layer created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Create SCD2 Dimension (Simple Version)

# COMMAND ----------

print("🔄 Creating SCD2 dimension...")

# Read customer changes if available
try:
    customer_changes = spark.read.csv(
        config['input']['customers_changes_path'],
        header=True,
        inferSchema=True
    )

    # Create SCD2 structure
    scd2_customers = customer_changes.withColumn(
        "effective_from", current_timestamp()
    ).withColumn(
        "effective_to", lit(None)
    ).withColumn(
        "is_current", lit(True)
    ).withColumn(
        "surrogate_key", expr("uuid()")
    )

    # Write SCD2 dimension
    scd2_customers.write.format("delta").mode("overwrite").save(f"{config['output']['silver_path']}/dim_customers_scd2")

    print("✅ SCD2 dimension created")

except Exception as e:
    print(f"⚠️ SCD2 creation skipped: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Validate Results

# COMMAND ----------

print("📊 Validating results...")

# Check bronze layer
bronze_customers = spark.read.format("delta").load(f"{config['output']['bronze_path']}/customers_raw")
print(f"✅ Bronze customers: {bronze_customers.count()} records")

# Check silver layer
silver_customers = spark.read.format("delta").load(f"{config['output']['silver_path']}/customers_enriched")
print(f"✅ Silver customers: {silver_customers.count()} records")

# Check gold layer
gold_fact_orders = spark.read.format("delta").load(f"{config['output']['gold_path']}/fact_orders")
print(f"✅ Gold fact_orders: {gold_fact_orders.count()} records")

# Show sample data
print("\n📋 Sample Gold Layer Data:")
gold_fact_orders.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Create Summary Statistics

# COMMAND ----------

print("📈 Creating summary statistics...")

# Sales by category
sales_by_category = gold_fact_orders.groupBy("category").agg(
    sum("total_amount").alias("total_sales"),
    count("order_id").alias("order_count"),
    avg("total_amount").alias("avg_order_value")
)

print("📊 Sales by Category:")
sales_by_category.show()

# Sales by customer
top_customers = gold_fact_orders.groupBy("customer_id", "customer_name").agg(
    sum("total_amount").alias("total_spent"),
    count("order_id").alias("order_count")
).orderBy(col("total_spent").desc())

print("👥 Top 10 Customers:")
top_customers.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 ETL Pipeline Complete!

# MAGIC
# MAGIC ### ✅ **What Was Accomplished:**
# MAGIC - **Bronze Layer**: Raw data ingested from Azure Storage
# MAGIC - **Silver Layer**: Cleaned and enriched data
# MAGIC - **Gold Layer**: Business-ready analytics data
# MAGIC - **SCD2 Dimension**: Historical tracking (if data available)
# MAGIC - **Summary Statistics**: Key business metrics
# MAGIC
# MAGIC ### 📁 **Data Locations:**
# MAGIC - **Bronze**: `abfss://lakehouse@pysparketlstorage.dfs.core.windows.net/bronze/`
# MAGIC - **Silver**: `abfss://lakehouse@pysparketlstorage.dfs.core.windows.net/silver/`
# MAGIC - **Gold**: `abfss://lakehouse@pysparketlstorage.dfs.core.windows.net/gold/`
# MAGIC
# MAGIC ### 🚀 **Next Steps:**
# MAGIC 1. **Schedule**: Set up automated runs using Databricks Jobs
# MAGIC 2. **Monitor**: Set up alerts for pipeline failures
# MAGIC 3. **Optimize**: Fine-tune performance for larger datasets
# MAGIC 4. **Scale**: Increase cluster size as needed
# MAGIC
# MAGIC **Your ETL pipeline is now running on Azure with minimal code upload!** 🎯
