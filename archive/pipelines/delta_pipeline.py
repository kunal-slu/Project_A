#!/usr/bin/env python3
"""
Delta Lake Pipeline for Local Development
Demonstrates Delta Lake features with the project data.
"""

import sys
import os
sys.path.insert(0, 'src')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg, max as spark_max
from delta import configure_spark_with_delta_pip

def create_delta_spark_session():
    """Create SparkSession with Delta Lake support"""
    builder = SparkSession.builder         .appName("DeltaLakePipeline")         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

def main():
    """Main Delta Lake pipeline"""
    print("🏗️ DELTA LAKE PIPELINE")
    print("======================")
    
    spark = create_delta_spark_session()
    
    try:
        # Read data
        orders_df = spark.read.option('header', 'true').option('inferSchema', 'true')             .csv('aws/data_fixed/02_snowflake_warehouse/snowflake_orders_100000.csv')
        
        # Write to Delta Lake
        print("📝 Writing to Delta Lake...")
        orders_df.write.format('delta').mode('overwrite')             .save('data/delta/orders')
        
        # Read from Delta Lake
        print("📖 Reading from Delta Lake...")
        delta_orders = spark.read.format('delta').load('data/delta/orders')
        
        # Demonstrate Delta Lake features
        print("🔍 Delta Lake Analytics:")
        print(f"   Records: {delta_orders.count():,}")
        
        # Time travel (if we had multiple versions)
        print("⏰ Delta Lake features available:")
        print("   ✅ ACID transactions")
        print("   ✅ Time travel")
        print("   ✅ Schema evolution")
        print("   ✅ Upserts and merges")
        
        print("✅ Delta Lake pipeline completed successfully!")
        
    except Exception as e:
        print(f"❌ Delta Lake pipeline failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
