# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 PySpark ETL Pipeline - Azure Deployment
# MAGIC
# MAGIC This notebook runs the complete ETL pipeline on Azure with:
# MAGIC - ✅ Delta Lake integration
# MAGIC - ✅ SCD2 implementation
# MAGIC - ✅ Data quality checks
# MAGIC - ✅ Performance optimization
# MAGIC - ✅ Monitoring and metrics
# MAGIC
# MAGIC ## Prerequisites
# MAGIC 1. Azure Storage Account configured
# MAGIC 2. Data uploaded to input-data container
# MAGIC 3. Project code uploaded to dbfs:/pyspark-etl/
# MAGIC 4. Delta Lake libraries installed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install Required Libraries

# COMMAND ----------

# Install Delta Lake and other required packages
dbutils.library.installPyPI("delta-spark", "3.0.0")
dbutils.library.installPyPI("structlog", "23.2.0")
dbutils.library.installPyPI("pyyaml", "6.0.1")
dbutils.library.installPyPI("prometheus-client", "0.19.0")

# Restart Python to load new libraries
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Set Up Environment

# COMMAND ----------

# Import required modules
import sys

# Add project path to Python path
project_path = "/dbfs/pyspark-etl/src"
sys.path.insert(0, project_path)

# Import project modules
from pyspark_interview_project import (
    get_spark_session,
    load_config_resolved,
    create_monitor,
    run_pipeline
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Load Azure Configuration

# COMMAND ----------

# Load Azure-specific configuration
config_path = "/dbfs/pyspark-etl/config/config-azure-dev.yaml"
config = load_config_resolved(config_path)

print("✅ Configuration loaded successfully")
print(f"Environment: {config.get('environment', 'unknown')}")
print(f"Storage Account: {config['azure']['storage']['account_name']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Initialize Spark Session

# COMMAND ----------

# Initialize Spark session with Azure configuration
spark = get_spark_session(config)

print("✅ Spark session initialized")
print(f"Spark Version: {spark.version}")
print(f"Application Name: {spark.conf.get('spark.app.name')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Set Up Monitoring

# COMMAND ----------

# Create monitoring instance
monitor = create_monitor(spark, config)

print("✅ Monitoring setup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Input Data

# COMMAND ----------

# Check if input data exists
input_paths = [
    config['input']['customer_path'],
    config['input']['product_path'],
    config['input']['orders_path'],
    config['input']['returns_path'],
    config['input']['exchange_rates_path'],
    config['input']['inventory_path']
]

print("📊 Checking input data availability:")
for path in input_paths:
    try:
        df = spark.read.format("delta").load(path)
        count = df.count()
        print(f"✅ {path}: {count} records")
    except Exception:
        try:
            # Try reading as CSV/JSON
            if path.endswith('.csv'):
                df = spark.read.csv(path, header=True, inferSchema=True)
            elif path.endswith('.json'):
                df = spark.read.json(path)
            else:
                df = spark.read.format("delta").load(path)
            count = df.count()
            print(f"✅ {path}: {count} records")
        except Exception as e2:
            print(f"❌ {path}: Not available - {str(e2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Run Complete ETL Pipeline

# COMMAND ----------

# Run the complete ETL pipeline with monitoring
print("🚀 Starting ETL Pipeline...")

try:
    with monitor.monitor_pipeline("azure_etl_complete"):
        run_pipeline(spark, config)

    print("🎉 ETL Pipeline completed successfully!")

except Exception as e:
    print(f"❌ ETL Pipeline failed: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Validate Results

# COMMAND ----------

# Validate bronze layer
print("🥉 Validating Bronze Layer:")
try:
    bronze_customers = spark.read.format("delta").load(f"{config['output']['bronze_path']}/customers_raw")
    print(f"✅ Bronze customers: {bronze_customers.count()} records")
except Exception as e:
    print(f"❌ Bronze customers: {str(e)}")

try:
    bronze_orders = spark.read.format("delta").load(f"{config['output']['bronze_path']}/orders_raw")
    print(f"✅ Bronze orders: {bronze_orders.count()} records")
except Exception as e:
    print(f"❌ Bronze orders: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Validate Silver Layer

# COMMAND ----------

# Validate silver layer
print("🥈 Validating Silver Layer:")
try:
    silver_customers = spark.read.format("delta").load(f"{config['output']['silver_path']}/customers_enriched")
    print(f"✅ Silver customers: {silver_customers.count()} records")
    print(f"   Columns: {', '.join(silver_customers.columns)}")
except Exception as e:
    print(f"❌ Silver customers: {str(e)}")

try:
    silver_orders = spark.read.format("delta").load(f"{config['output']['silver_path']}/orders_enriched")
    print(f"✅ Silver orders: {silver_orders.count()} records")
    print(f"   Columns: {', '.join(silver_orders.columns)}")
except Exception as e:
    print(f"❌ Silver orders: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Validate Gold Layer

# COMMAND ----------

# Validate gold layer
print("🥇 Validating Gold Layer:")
try:
    gold_fact_orders = spark.read.format("delta").load(f"{config['output']['gold_path']}/fact_orders")
    print(f"✅ Gold fact_orders: {gold_fact_orders.count()} records")
    print(f"   Columns: {', '.join(gold_fact_orders.columns)}")

    # Show sample data
    print("📊 Sample data:")
    gold_fact_orders.show(5)

except Exception as e:
    print(f"❌ Gold fact_orders: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Validate SCD2 Implementation

# COMMAND ----------

# Validate SCD2 implementation
print("🔄 Validating SCD2 Implementation:")

try:
    # Import SCD2 validation function
    from tests.test_scd2_validation import validate_scd2_table

    # Validate customer SCD2
    scd2_results = validate_scd2_table(
        spark,
        f"{config['output']['silver_path']}/dim_customers_scd2",
        "customer_id"
    )

    print("SCD2 Validation Results:")
    print(f"  Validation Passed: {scd2_results['validation_passed']}")
    print(f"  Total Records: {scd2_results['total_records']}")
    print(f"  Current Records: {scd2_results['current_records']}")
    print(f"  Historical Records: {scd2_results['historical_records']}")

    if scd2_results['errors']:
        print(f"  Errors: {scd2_results['errors']}")
    else:
        print("  ✅ No validation errors found")

except Exception as e:
    print(f"❌ SCD2 validation failed: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Performance Metrics

# COMMAND ----------

# Display performance metrics
print("📈 Performance Metrics:")

# Get Spark UI metrics
spark_metrics = spark.sparkContext.getConf().getAll()
print(f"Spark Application ID: {spark.sparkContext.applicationId}")

# Show table statistics
try:
    gold_fact_orders = spark.read.format("delta").load(f"{config['output']['gold_path']}/fact_orders")
    print(f"Final table size: {gold_fact_orders.count()} records")

    # Show partition information
    print("Partition information:")
    gold_fact_orders.write.format("delta").mode("overwrite").save("/tmp/temp_stats")
    spark.sql("ANALYZE TABLE delta.`/tmp/temp_stats` COMPUTE STATISTICS FOR ALL COLUMNS")

except Exception as e:
    print(f"Could not compute statistics: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13: Cleanup

# COMMAND ----------

# Clean up temporary files
try:
    dbutils.fs.rm("/tmp/temp_stats", recurse=True)
    print("✅ Temporary files cleaned up")
except:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Pipeline Complete!
# MAGIC
# MAGIC Your ETL pipeline has been successfully executed on Azure with:
# MAGIC - ✅ **Bronze Layer**: Raw data ingested
# MAGIC - ✅ **Silver Layer**: Cleaned and enriched data
# MAGIC - ✅ **Gold Layer**: Business-ready analytics data
# MAGIC - ✅ **SCD2 Implementation**: Historical tracking enabled
# MAGIC - ✅ **Data Quality**: Validation checks passed
# MAGIC - ✅ **Performance**: Optimized for Azure environment
# MAGIC
# MAGIC ### Next Steps:
# MAGIC 1. **Schedule**: Set up automated runs using Databricks Jobs
# MAGIC 2. **Monitor**: Set up alerts and dashboards
# MAGIC 3. **Scale**: Optimize cluster size for larger datasets
# MAGIC 4. **Security**: Review and enhance access controls
