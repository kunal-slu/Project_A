#!/usr/bin/env python3
"""
Simple PySpark Pipeline for Local Testing
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, max as spark_max

def create_spark_session():
    """Create SparkSession for local testing"""
    return SparkSession.builder         .appName("PySparkDataEngineeringProject")         .config("spark.sql.adaptive.enabled", "true")         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")         .getOrCreate()

def main():
    """Main pipeline function"""
    print("🚀 STARTING SIMPLE PYSPARK PIPELINE")
    print("===================================")
    
    # Create SparkSession
    spark = create_spark_session()
    
    try:
        # Read data from all sources
        print("\n📊 READING DATA FROM ALL SOURCES:")
        print("==================================")
        
        # HubSpot Contacts
        if os.path.exists('aws/data_fixed/01_hubspot_crm/hubspot_contacts_25000.csv'):
            contacts_df = spark.read.option("header", "true").option("inferSchema", "true")                 .csv('aws/data_fixed/01_hubspot_crm/hubspot_contacts_25000.csv')
            print(f"✅ HubSpot Contacts: {contacts_df.count():,} records")
        else:
            print("❌ HubSpot Contacts file not found")
            return
        
        # HubSpot Deals
        if os.path.exists('aws/data_fixed/01_hubspot_crm/hubspot_deals_30000.csv'):
            deals_df = spark.read.option("header", "true").option("inferSchema", "true")                 .csv('aws/data_fixed/01_hubspot_crm/hubspot_deals_30000.csv')
            print(f"✅ HubSpot Deals: {deals_df.count():,} records")
        else:
            print("❌ HubSpot Deals file not found")
            return
        
        # Snowflake Customers
        if os.path.exists('aws/data_fixed/02_snowflake_warehouse/snowflake_customers_50000.csv'):
            customers_df = spark.read.option("header", "true").option("inferSchema", "true")                 .csv('aws/data_fixed/02_snowflake_warehouse/snowflake_customers_50000.csv')
            print(f"✅ Snowflake Customers: {customers_df.count():,} records")
        else:
            print("❌ Snowflake Customers file not found")
            return
        
        # Snowflake Orders
        if os.path.exists('aws/data_fixed/02_snowflake_warehouse/snowflake_orders_100000.csv'):
            orders_df = spark.read.option("header", "true").option("inferSchema", "true")                 .csv('aws/data_fixed/02_snowflake_warehouse/snowflake_orders_100000.csv')
            print(f"✅ Snowflake Orders: {orders_df.count():,} records")
        else:
            print("❌ Snowflake Orders file not found")
            return
        
        # Snowflake Products
        if os.path.exists('aws/data_fixed/02_snowflake_warehouse/snowflake_products_10000.csv'):
            products_df = spark.read.option("header", "true").option("inferSchema", "true")                 .csv('aws/data_fixed/02_snowflake_warehouse/snowflake_products_10000.csv')
            print(f"✅ Snowflake Products: {products_df.count():,} records")
        else:
            print("❌ Snowflake Products file not found")
            return
        
        # Redshift Behavior
        if os.path.exists('aws/data_fixed/03_redshift_analytics/redshift_customer_behavior_50000.csv'):
            behavior_df = spark.read.option("header", "true").option("inferSchema", "true")                 .csv('aws/data_fixed/03_redshift_analytics/redshift_customer_behavior_50000.csv')
            print(f"✅ Redshift Behavior: {behavior_df.count():,} records")
        else:
            print("❌ Redshift Behavior file not found")
            return
        
        # Stream Data
        if os.path.exists('aws/data_fixed/04_stream_data/stream_kafka_events_100000.csv'):
            stream_df = spark.read.option("header", "true").option("inferSchema", "true")                 .csv('aws/data_fixed/04_stream_data/stream_kafka_events_100000.csv')
            print(f"✅ Stream Data: {stream_df.count():,} records")
        else:
            print("❌ Stream Data file not found")
            return
        
        # FX Rates
        if os.path.exists('aws/data_fixed/05_fx_rates/fx_rates_historical_730_days.csv'):
            fx_df = spark.read.option("header", "true").option("inferSchema", "true")                 .csv('aws/data_fixed/05_fx_rates/fx_rates_historical_730_days.csv')
            print(f"✅ FX Rates: {fx_df.count():,} records")
        else:
            print("❌ FX Rates file not found")
            return
        
        # Run analytics
        print("\n📈 RUNNING ANALYTICS:")
        print("====================")
        
        # Customer Analytics
        print("\n1️⃣ Customer Analytics:")
        customer_stats = customers_df.agg(
            count("*").alias("total_customers"),
            count("customer_segment").alias("segmented_customers")
        ).collect()[0]
        print(f"   Total Customers: {customer_stats['total_customers']:,}")
        print(f"   Segmented Customers: {customer_stats['segmented_customers']:,}")
        
        # Order Analytics
        print("\n2️⃣ Order Analytics:")
        order_stats = orders_df.agg(
            count("*").alias("total_orders"),
            spark_sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value")
        ).collect()[0]
        print(f"   Total Orders: {order_stats['total_orders']:,}")
        print(f"   Total Revenue: ${order_stats['total_revenue']:,.2f}")
        print(f"   Average Order Value: ${order_stats['avg_order_value']:,.2f}")
        
        # Product Analytics
        print("\n3️⃣ Product Analytics:")
        product_stats = products_df.agg(
            count("*").alias("total_products"),
            count("category").alias("categorized_products")
        ).collect()[0]
        print(f"   Total Products: {product_stats['total_products']:,}")
        print(f"   Categorized Products: {product_stats['categorized_products']:,}")
        
        # HubSpot Analytics
        print("\n4️⃣ HubSpot Analytics:")
        hubspot_stats = contacts_df.agg(
            count("*").alias("total_contacts"),
            count("lead_status").alias("leads_with_status")
        ).collect()[0]
        print(f"   Total Contacts: {hubspot_stats['total_contacts']:,}")
        print(f"   Leads with Status: {hubspot_stats['leads_with_status']:,}")
        
        # Behavior Analytics
        print("\n5️⃣ Behavior Analytics:")
        behavior_stats = behavior_df.agg(
            count("*").alias("total_events"),
            count("event_name").alias("events_with_name")
        ).collect()[0]
        print(f"   Total Events: {behavior_stats['total_events']:,}")
        print(f"   Events with Name: {behavior_stats['events_with_name']:,}")
        
        # Stream Analytics
        print("\n6️⃣ Stream Analytics:")
        stream_stats = stream_df.agg(
            count("*").alias("total_events"),
            count("topic").alias("events_with_topic")
        ).collect()[0]
        print(f"   Total Stream Events: {stream_stats['total_events']:,}")
        print(f"   Events with Topic: {stream_stats['events_with_topic']:,}")
        
        # FX Analytics
        print("\n7️⃣ FX Analytics:")
        fx_stats = fx_df.agg(
            count("*").alias("total_rates"),
            count("target_currency").alias("rates_with_currency")
        ).collect()[0]
        print(f"   Total FX Rates: {fx_stats['total_rates']:,}")
        print(f"   Rates with Currency: {fx_stats['rates_with_currency']:,}")
        
        print("\n✅ PIPELINE EXECUTION COMPLETE!")
        print("===============================")
        print("🎯 All data sources processed successfully")
        print("🎯 Analytics completed successfully")
        print("🎯 Pipeline is working correctly")
        
    except Exception as e:
        print(f"\n❌ PIPELINE EXECUTION FAILED: {e}")
        return False
    
    finally:
        spark.stop()
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
