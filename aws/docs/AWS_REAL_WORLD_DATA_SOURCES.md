# 🌍 Real-World AWS ETL Data Sources & Processes

## 📊 **Real Data Sources for Production ETL**

### **1. 🛒 E-Commerce & Retail Data**
```yaml
# Real e-commerce data sources
ecommerce_sources:
  # Amazon S3 - Raw data from multiple sources
  raw_data:
    - source: "s3://company-raw-data/orders/"
      format: "JSON"
      frequency: "hourly"
      description: "Order data from web/mobile apps"
    
    - source: "s3://company-raw-data/customers/"
      format: "CSV"
      frequency: "daily"
      description: "Customer profiles and demographics"
    
    - source: "s3://company-raw-data/products/"
      format: "Parquet"
      frequency: "daily"
      description: "Product catalog and inventory"
    
    - source: "s3://company-raw-data/transactions/"
      format: "JSON"
      frequency: "real-time"
      description: "Payment and transaction logs"
  
  # External APIs
  external_apis:
    - source: "https://api.stripe.com/v1/charges"
      format: "JSON"
      frequency: "hourly"
      description: "Payment processing data"
    
    - source: "https://api.shipbob.com/v1/orders"
      format: "JSON"
      frequency: "daily"
      description: "Shipping and fulfillment data"
    
    - source: "https://api.mailchimp.com/3.0/reports"
      format: "JSON"
      frequency: "daily"
      description: "Email marketing campaign data"
```

### **2. 📱 Mobile App & User Behavior Data**
```yaml
# Mobile app analytics
mobile_analytics:
  # Firebase Analytics
  firebase:
    - source: "s3://firebase-analytics-export/events/"
      format: "BigQuery export"
      frequency: "daily"
      description: "User events, page views, conversions"
    
    - source: "s3://firebase-analytics-export/users/"
      format: "BigQuery export"
      frequency: "daily"
      description: "User demographics and behavior"
  
  # App Store Connect
  app_store:
    - source: "s3://app-store-reports/sales/"
      format: "CSV"
      frequency: "daily"
      description: "App downloads, revenue, reviews"
  
  # Crashlytics
  crashlytics:
    - source: "s3://crashlytics-export/crashes/"
      format: "JSON"
      frequency: "hourly"
      description: "App crash reports and stack traces"
```

### **3. 🌐 Web Analytics & Marketing Data**
```yaml
# Web analytics
web_analytics:
  # Google Analytics 4
  ga4:
    - source: "s3://ga4-export/events/"
      format: "BigQuery export"
      frequency: "daily"
      description: "Website traffic, conversions, user journey"
  
  # Google Ads
  google_ads:
    - source: "s3://google-ads-export/campaigns/"
      format: "CSV"
      frequency: "daily"
      description: "Ad performance, spend, conversions"
  
  # Facebook Ads
  facebook_ads:
    - source: "s3://facebook-ads-export/insights/"
      format: "JSON"
      frequency: "daily"
      description: "Ad performance, audience insights"
  
  # LinkedIn Ads
  linkedin_ads:
    - source: "s3://linkedin-ads-export/analytics/"
      format: "CSV"
      frequency: "daily"
      description: "B2B marketing performance data"
```

### **4. 💰 Financial & Payment Data**
```yaml
# Financial data
financial_data:
  # Stripe payments
  stripe:
    - source: "s3://stripe-export/charges/"
      format: "JSON"
      frequency: "hourly"
      description: "Payment transactions, refunds, disputes"
    
    - source: "s3://stripe-export/customers/"
      format: "JSON"
      frequency: "daily"
      description: "Customer payment methods, subscriptions"
  
  # PayPal
  paypal:
    - source: "s3://paypal-export/transactions/"
      format: "CSV"
      frequency: "daily"
      description: "Payment transactions and settlements"
  
  # Square
  square:
    - source: "s3://square-export/payments/"
      format: "JSON"
      frequency: "hourly"
      description: "Point-of-sale transactions"
```

### **5. 📧 Customer Support & CRM Data**
```yaml
# Customer support
customer_support:
  # Zendesk
  zendesk:
    - source: "s3://zendesk-export/tickets/"
      format: "JSON"
      frequency: "hourly"
      description: "Support tickets, customer interactions"
    
    - source: "s3://zendesk-export/users/"
      format: "JSON"
      frequency: "daily"
      description: "Customer profiles and preferences"
  
  # Intercom
  intercom:
    - source: "s3://intercom-export/conversations/"
      format: "JSON"
      frequency: "hourly"
      description: "Chat conversations, user messages"
  
  # HubSpot
  hubspot:
    - source: "s3://hubspot-export/contacts/"
      format: "CSV"
      frequency: "daily"
      description: "Lead and customer data"
    
    - source: "s3://hubspot-export/deals/"
      format: "CSV"
      frequency: "daily"
      description: "Sales pipeline and deal tracking"
```

### **6. 🏭 Internal Business Systems**
```yaml
# Internal systems
internal_systems:
  # ERP System (SAP/NetSuite)
  erp:
    - source: "s3://erp-export/inventory/"
      format: "CSV"
      frequency: "daily"
      description: "Inventory levels, SKU data"
    
    - source: "s3://erp-export/purchasing/"
      format: "CSV"
      frequency: "daily"
      description: "Purchase orders, vendor data"
  
  # HR System (Workday/BambooHR)
  hr:
    - source: "s3://hr-export/employees/"
      format: "CSV"
      frequency: "weekly"
      description: "Employee data, performance reviews"
  
  # Accounting System (QuickBooks/Xero)
  accounting:
    - source: "s3://accounting-export/transactions/"
      format: "CSV"
      frequency: "daily"
      description: "Financial transactions, GL accounts"
```

## 🔄 **Real ETL Pipeline Architecture**

### **Data Ingestion Layer (Bronze)**
```python
# Real-world data ingestion with AWS services
import boto3
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def ingest_ecommerce_data(spark, config):
    """Ingest real e-commerce data from multiple sources."""
    
    # 1. Ingest orders from S3 (real-time)
    orders_df = spark.read.json("s3://company-raw-data/orders/")
    orders_df = orders_df.withColumn("ingested_at", current_timestamp())
    orders_df.write.mode("append").partitionBy("year", "month", "day").parquet(
        f"{config['bronze_path']}/orders/"
    )
    
    # 2. Ingest customer data (daily batch)
    customers_df = spark.read.csv("s3://company-raw-data/customers/", header=True)
    customers_df = customers_df.withColumn("ingested_at", current_timestamp())
    customers_df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(
        f"{config['bronze_path']}/customers/"
    )
    
    # 3. Ingest product catalog (daily batch)
    products_df = spark.read.parquet("s3://company-raw-data/products/")
    products_df = products_df.withColumn("ingested_at", current_timestamp())
    products_df.write.mode("overwrite").partitionBy("category").parquet(
        f"{config['bronze_path']}/products/"
    )
    
    # 4. Ingest Stripe payment data (hourly)
    stripe_df = spark.read.json("s3://stripe-export/charges/")
    stripe_df = stripe_df.withColumn("ingested_at", current_timestamp())
    stripe_df.write.mode("append").partitionBy("year", "month", "day").parquet(
        f"{config['bronze_path']}/payments/"
    )

def ingest_analytics_data(spark, config):
    """Ingest analytics data from various platforms."""
    
    # 1. Google Analytics 4 data
    ga4_df = spark.read.parquet("s3://ga4-export/events/")
    ga4_df = ga4_df.withColumn("ingested_at", current_timestamp())
    ga4_df.write.mode("append").partitionBy("event_date").parquet(
        f"{config['bronze_path']}/analytics/ga4/"
    )
    
    # 2. Firebase Analytics data
    firebase_df = spark.read.parquet("s3://firebase-analytics-export/events/")
    firebase_df = firebase_df.withColumn("ingested_at", current_timestamp())
    firebase_df.write.mode("append").partitionBy("event_date").parquet(
        f"{config['bronze_path']}/analytics/firebase/"
    )
    
    # 3. Marketing campaign data
    marketing_df = spark.read.csv("s3://google-ads-export/campaigns/", header=True)
    marketing_df = marketing_df.withColumn("ingested_at", current_timestamp())
    marketing_df.write.mode("append").partitionBy("date").parquet(
        f"{config['bronze_path']}/marketing/"
    )

def ingest_support_data(spark, config):
    """Ingest customer support data."""
    
    # 1. Zendesk tickets
    zendesk_df = spark.read.json("s3://zendesk-export/tickets/")
    zendesk_df = zendesk_df.withColumn("ingested_at", current_timestamp())
    zendesk_df.write.mode("append").partitionBy("year", "month", "day").parquet(
        f"{config['bronze_path']}/support/tickets/"
    )
    
    # 2. Intercom conversations
    intercom_df = spark.read.json("s3://intercom-export/conversations/")
    intercom_df = intercom_df.withColumn("ingested_at", current_timestamp())
    intercom_df.write.mode("append").partitionBy("year", "month", "day").parquet(
        f"{config['bronze_path']}/support/conversations/"
    )
```

### **Data Processing Layer (Silver)**
```python
def process_orders_silver(spark, config):
    """Process and clean order data."""
    
    # Read bronze orders
    orders_df = spark.read.parquet(f"{config['bronze_path']}/orders/")
    
    # Clean and transform
    clean_orders = orders_df.select(
        col("order_id"),
        col("customer_id"),
        col("order_date"),
        col("total_amount"),
        col("status"),
        col("payment_method"),
        col("shipping_address"),
        col("items"),
        col("ingested_at")
    ).filter(
        col("order_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("total_amount") > 0
    ).withColumn("processed_at", current_timestamp())
    
    # Write to silver
    clean_orders.write.mode("append").partitionBy("year", "month", "day").parquet(
        f"{config['silver_path']}/orders/"
    )

def process_customers_silver(spark, config):
    """Process and enrich customer data."""
    
    # Read bronze customers
    customers_df = spark.read.parquet(f"{config['bronze_path']}/customers/")
    
    # Clean and enrich
    clean_customers = customers_df.select(
        col("customer_id"),
        concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"),
        col("email"),
        col("phone"),
        col("address"),
        col("city"),
        col("state"),
        col("country"),
        col("zip_code"),
        col("registration_date"),
        col("ingested_at")
    ).filter(
        col("customer_id").isNotNull() &
        col("email").isNotNull()
    ).withColumn("processed_at", current_timestamp())
    
    # Write to silver
    clean_customers.write.mode("overwrite").partitionBy("country", "state").parquet(
        f"{config['silver_path']}/customers/"
    )

def process_analytics_silver(spark, config):
    """Process analytics data for insights."""
    
    # Process GA4 data
    ga4_df = spark.read.parquet(f"{config['bronze_path']}/analytics/ga4/")
    
    # Aggregate user sessions
    sessions_df = ga4_df.groupBy(
        "user_pseudo_id",
        "session_id",
        date_trunc("day", col("event_timestamp")).alias("session_date")
    ).agg(
        count("*").alias("event_count"),
        min("event_timestamp").alias("session_start"),
        max("event_timestamp").alias("session_end"),
        collect_list("event_name").alias("events")
    ).withColumn("session_duration", 
        unix_timestamp(col("session_end")) - unix_timestamp(col("session_start"))
    ).withColumn("processed_at", current_timestamp())
    
    # Write to silver
    sessions_df.write.mode("append").partitionBy("session_date").parquet(
        f"{config['silver_path']}/analytics/sessions/"
    )
```

### **Data Warehouse Layer (Gold)**
```python
def build_sales_fact_table(spark, config):
    """Build sales fact table for analytics."""
    
    # Read silver data
    orders_df = spark.read.parquet(f"{config['silver_path']}/orders/")
    customers_df = spark.read.parquet(f"{config['silver_path']}/customers/")
    products_df = spark.read.parquet(f"{config['silver_path']}/products/")
    
    # Build sales fact table
    sales_fact = orders_df.join(customers_df, "customer_id", "left") \
        .select(
            col("order_id"),
            col("customer_id"),
            col("full_name").alias("customer_name"),
            col("order_date"),
            col("total_amount"),
            col("status"),
            col("payment_method"),
            col("country"),
            col("state"),
            col("city"),
            col("processed_at")
        ).withColumn("order_year", year(col("order_date"))) \
        .withColumn("order_month", month(col("order_date"))) \
        .withColumn("order_day", dayofmonth(col("order_date")))
    
    # Write to gold
    sales_fact.write.mode("append").partitionBy("order_year", "order_month").parquet(
        f"{config['gold_path']}/fact_sales/"
    )

def build_customer_dimension(spark, config):
    """Build customer dimension table with SCD Type 2."""
    
    # Read silver customers
    customers_df = spark.read.parquet(f"{config['silver_path']}/customers/")
    
    # Add SCD Type 2 columns
    customer_dim = customers_df.withColumn("effective_from", col("processed_at")) \
        .withColumn("effective_to", lit(None).cast("timestamp")) \
        .withColumn("is_current", lit(True)) \
        .withColumn("surrogate_key", expr("uuid()"))
    
    # Write to gold
    customer_dim.write.mode("overwrite").parquet(
        f"{config['gold_path']}/dim_customers/"
    )

def build_marketing_attribution(spark, config):
    """Build marketing attribution model."""
    
    # Read analytics data
    sessions_df = spark.read.parquet(f"{config['silver_path']}/analytics/sessions/")
    marketing_df = spark.read.parquet(f"{config['silver_path']}/marketing/")
    orders_df = spark.read.parquet(f"{config['silver_path']}/orders/")
    
    # Join sessions with marketing data
    attribution_df = sessions_df.join(marketing_df, 
        sessions_df.session_date == marketing_df.date, "left") \
        .join(orders_df, 
            sessions_df.user_pseudo_id == orders_df.customer_id, "left") \
        .select(
            col("user_pseudo_id"),
            col("session_id"),
            col("session_date"),
            col("campaign_name"),
            col("ad_group"),
            col("keyword"),
            col("clicks"),
            col("impressions"),
            col("cost"),
            col("order_id"),
            col("total_amount"),
            col("processed_at")
        ).withColumn("conversion", col("order_id").isNotNull()) \
        .withColumn("attribution_date", current_date())
    
    # Write to gold
    attribution_df.write.mode("append").partitionBy("session_date").parquet(
        f"{config['gold_path']}/marketing_attribution/"
    )
```

## 📈 **Real Business Intelligence Queries**

### **Sales Analytics**
```sql
-- Daily sales performance
SELECT 
    order_date,
    COUNT(*) as order_count,
    SUM(total_amount) as daily_revenue,
    AVG(total_amount) as avg_order_value,
    COUNT(DISTINCT customer_id) as unique_customers
FROM gold_fact_sales
WHERE order_date >= DATE_SUB(CURRENT_DATE, 30)
GROUP BY order_date
ORDER BY order_date DESC;

-- Customer lifetime value
SELECT 
    customer_id,
    customer_name,
    COUNT(*) as total_orders,
    SUM(total_amount) as total_spent,
    AVG(total_amount) as avg_order_value,
    MAX(order_date) as last_order_date,
    DATEDIFF(CURRENT_DATE, MAX(order_date)) as days_since_last_order
FROM gold_fact_sales
GROUP BY customer_id, customer_name
HAVING total_spent > 1000
ORDER BY total_spent DESC;

-- Geographic sales analysis
SELECT 
    country,
    state,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value
FROM gold_fact_sales
WHERE order_date >= DATE_SUB(CURRENT_DATE, 90)
GROUP BY country, state
ORDER BY total_revenue DESC;
```

### **Marketing Performance**
```sql
-- Campaign performance
SELECT 
    campaign_name,
    ad_group,
    SUM(clicks) as total_clicks,
    SUM(impressions) as total_impressions,
    SUM(cost) as total_cost,
    SUM(CASE WHEN conversion THEN 1 ELSE 0 END) as conversions,
    SUM(CASE WHEN conversion THEN total_amount ELSE 0 END) as conversion_revenue,
    ROUND(SUM(clicks) * 100.0 / SUM(impressions), 2) as ctr,
    ROUND(SUM(cost) * 1000.0 / SUM(impressions), 2) as cpm,
    ROUND(SUM(cost) / SUM(CASE WHEN conversion THEN 1 ELSE 0 END), 2) as cpa
FROM gold_marketing_attribution
WHERE session_date >= DATE_SUB(CURRENT_DATE, 30)
GROUP BY campaign_name, ad_group
ORDER BY conversion_revenue DESC;

-- Attribution analysis
SELECT 
    session_date,
    COUNT(DISTINCT user_pseudo_id) as unique_users,
    COUNT(DISTINCT session_id) as total_sessions,
    SUM(CASE WHEN conversion THEN 1 ELSE 0 END) as conversions,
    ROUND(SUM(CASE WHEN conversion THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT user_pseudo_id), 2) as conversion_rate
FROM gold_marketing_attribution
WHERE session_date >= DATE_SUB(CURRENT_DATE, 7)
GROUP BY session_date
ORDER BY session_date DESC;
```

### **Customer Support Analytics**
```sql
-- Support ticket analysis
SELECT 
    DATE(created_at) as ticket_date,
    priority,
    status,
    COUNT(*) as ticket_count,
    AVG(DATEDIFF(resolved_at, created_at)) as avg_resolution_time_hours
FROM bronze_support_tickets
WHERE created_at >= DATE_SUB(CURRENT_DATE, 30)
GROUP BY DATE(created_at), priority, status
ORDER BY ticket_date DESC;

-- Customer satisfaction trends
SELECT 
    DATE(created_at) as ticket_date,
    AVG(satisfaction_score) as avg_satisfaction,
    COUNT(*) as ticket_count
FROM bronze_support_tickets
WHERE satisfaction_score IS NOT NULL
    AND created_at >= DATE_SUB(CURRENT_DATE, 90)
GROUP BY DATE(created_at)
ORDER BY ticket_date DESC;
```

## 🔧 **Real AWS ETL Implementation**

### **AWS Glue ETL Job**
```python
# Real AWS Glue ETL job for production
import sys
import os
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'config_path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load configuration
config = {
    'bronze_path': 's3://company-data-lake/bronze',
    'silver_path': 's3://company-data-lake/silver', 
    'gold_path': 's3://company-data-lake/gold'
}

# Real ETL pipeline
def run_production_etl():
    """Run production ETL pipeline."""
    
    # 1. Ingest data from multiple sources
    print("Ingesting e-commerce data...")
    ingest_ecommerce_data(spark, config)
    
    print("Ingesting analytics data...")
    ingest_analytics_data(spark, config)
    
    print("Ingesting support data...")
    ingest_support_data(spark, config)
    
    # 2. Process and clean data
    print("Processing orders...")
    process_orders_silver(spark, config)
    
    print("Processing customers...")
    process_customers_silver(spark, config)
    
    print("Processing analytics...")
    process_analytics_silver(spark, config)
    
    # 3. Build data warehouse
    print("Building sales fact table...")
    build_sales_fact_table(spark, config)
    
    print("Building customer dimension...")
    build_customer_dimension(spark, config)
    
    print("Building marketing attribution...")
    build_marketing_attribution(spark, config)
    
    print("ETL pipeline completed successfully!")

# Run the pipeline
run_production_etl()
job.commit()
```

### **AWS Step Functions Workflow**
```json
{
  "Comment": "Real Production ETL Pipeline",
  "StartAt": "ValidateDataSources",
  "States": {
    "ValidateDataSources": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:ACCOUNT_ID:function:validate-data-sources",
      "Next": "StartEMRCluster"
    },
    "StartEMRCluster": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:ACCOUNT_ID:function:start-emr-cluster",
      "Next": "WaitForEMRCluster"
    },
    "WaitForEMRCluster": {
      "Type": "Wait",
      "Seconds": 300,
      "Next": "CheckEMRStatus"
    },
    "CheckEMRStatus": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:ACCOUNT_ID:function:check-emr-status",
      "Next": "EMRClusterReady?"
    },
    "EMRClusterReady?": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.clusterStatus",
          "StringEquals": "WAITING",
          "Next": "RunETLJob"
        }
      ],
      "Default": "WaitForEMRCluster"
    },
    "RunETLJob": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:ACCOUNT_ID:function:run-etl-job",
      "Next": "MonitorETLJob"
    },
    "MonitorETLJob": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:ACCOUNT_ID:function:monitor-etl-job",
      "Next": "ETLJobComplete?"
    },
    "ETLJobComplete?": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.jobStatus",
          "StringEquals": "COMPLETED",
          "Next": "RunDataQualityChecks"
        },
        {
          "Variable": "$.jobStatus",
          "StringEquals": "FAILED",
          "Next": "HandleFailure"
        }
      ],
      "Default": "MonitorETLJob"
    },
    "RunDataQualityChecks": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:ACCOUNT_ID:function:run-data-quality",
      "Next": "QualityChecksPassed?"
    },
    "QualityChecksPassed?": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.qualityStatus",
          "StringEquals": "PASSED",
          "Next": "UpdateDataCatalog"
        }
      ],
      "Default": "HandleQualityFailure"
    },
    "UpdateDataCatalog": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:ACCOUNT_ID:function:update-data-catalog",
      "Next": "TerminateEMRCluster"
    },
    "TerminateEMRCluster": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:ACCOUNT_ID:function:terminate-emr-cluster",
      "Next": "SendSuccessNotification"
    },
    "SendSuccessNotification": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:ACCOUNT_ID:function:send-notification",
      "Parameters": {
        "message": "Production ETL Pipeline completed successfully",
        "status": "SUCCESS"
      },
      "End": true
    },
    "HandleFailure": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:ACCOUNT_ID:function:handle-failure",
      "Next": "SendFailureNotification"
    },
    "HandleQualityFailure": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:ACCOUNT_ID:function:handle-quality-failure",
      "Next": "SendFailureNotification"
    },
    "SendFailureNotification": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:ACCOUNT_ID:function:send-notification",
      "Parameters": {
        "message": "Production ETL Pipeline failed",
        "status": "FAILED"
      },
      "End": true
    }
  }
}
```

## 📊 **Real Data Quality Checks**

```python
def run_data_quality_checks(spark, config):
    """Run comprehensive data quality checks."""
    
    quality_results = {}
    
    # Check sales data quality
    sales_df = spark.read.parquet(f"{config['gold_path']}/fact_sales/")
    
    # Completeness checks
    completeness = sales_df.select([
        (count(when(col(c).isNull(), c)) / count("*") * 100).alias(f"{c}_null_pct")
        for c in sales_df.columns
    ]).collect()[0]
    
    # Validity checks
    validity_checks = sales_df.select(
        count(when(col("total_amount") <= 0, True)).alias("invalid_amounts"),
        count(when(col("order_date") > current_date(), True)).alias("future_dates"),
        count(when(col("customer_id").isNull(), True)).alias("missing_customer_ids")
    ).collect()[0]
    
    # Uniqueness checks
    uniqueness_checks = sales_df.select(
        count("*").alias("total_rows"),
        countDistinct("order_id").alias("unique_orders")
    ).collect()[0]
    
    quality_results['sales'] = {
        'completeness': completeness,
        'validity': validity_checks,
        'uniqueness': uniqueness_checks
    }
    
    return quality_results
```

## 🎯 **Real Production Monitoring**

### **CloudWatch Metrics**
```python
import boto3
import time

def send_custom_metrics():
    """Send custom metrics to CloudWatch."""
    
    cloudwatch = boto3.client('cloudwatch')
    
    # ETL job metrics
    cloudwatch.put_metric_data(
        Namespace='PySparkETL',
        MetricData=[
            {
                'MetricName': 'RecordsProcessed',
                'Value': 1000000,
                'Unit': 'Count'
            },
            {
                'MetricName': 'ProcessingTime',
                'Value': 1800,
                'Unit': 'Seconds'
            },
            {
                'MetricName': 'DataQualityScore',
                'Value': 98.5,
                'Unit': 'Percent'
            }
        ]
    )
```

This is a **real-world ETL pipeline** that data engineers actually build and maintain in production! 🚀
