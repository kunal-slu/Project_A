# 🚀 Complete AWS Deployment Guide - End to End

## Overview

This guide provides step-by-step instructions to deploy the PySpark Data Engineering project on AWS with production-grade features including EMR Serverless, Delta Lake, data quality gates, and comprehensive monitoring.

---

## 📋 Table of Contents

1. [Prerequisites](#prerequisites)
2. [Architecture Overview](#architecture-overview)
3. [Step 1: AWS Account Setup](#step-1-aws-account-setup)
4. [Step 2: Infrastructure as Code (Terraform)](#step-2-infrastructure-as-code-terraform)
5. [Step 3: Deploy AWS Resources](#step-3-deploy-aws-resources)
6. [Step 4: Configure Secrets](#step-4-configure-secrets)
7. [Step 5: Upload Code & Data](#step-5-upload-code--data)
8. [Step 6: Run ETL Pipeline](#step-6-run-etl-pipeline)
9. [Step 7: Query with Athena](#step-7-query-with-athena)
10. [Step 8: Monitoring & Alerts](#step-8-monitoring--alerts)
11. [Step 9: Daily Operations](#step-9-daily-operations)
12. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required AWS Services
- AWS Account with admin access
- AWS CLI installed and configured
- Terraform >= 1.5.0
- Python 3.10+
- Git

### AWS Permissions Required
```bash
# Core permissions needed
- S3: Full access to create buckets and manage objects
- EMR Serverless: Create applications and run jobs
- IAM: Create roles and policies
- Secrets Manager: Store and retrieve secrets
- Glue: Create databases and tables
- CloudWatch: Create logs and metrics
- KMS: Create and manage encryption keys
```

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                     │
├─────────────────────────┬───────────────────┬───────────────────────────┤
│   Snowflake (External)  │  Redshift         │   Kafka (Streaming)       │
│   • orders              │  • behavior       │   • orders_events         │
│   • customers           │  • analytics      │   • user_events           │
│   • products            │                   │                           │
└───────────┬─────────────┴────────┬──────────┴──────────┬────────────────┘
            │                      │                      │
            ▼                      ▼                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    ETL PROCESSING LAYER                                 │
├─────────────────────────────────────────────────────────────────────────┤
│  EMR Serverless Application                                             │
│  • Spark 3.5+                                                           │
│  • Delta Lake 3.2                                                       │
│  • Auto-scaling                                                         │
│                                                                          │
│  Jobs:                                                                   │
│  ├── Extract (Bronze)     ├── Transform (Silver)  ├── Analytics (Gold) │
│  │   • snowflake_ingest   │   • bronze_to_silver  │   • customer_360    │
│  │   • redshift_ingest    │   • data_quality      │   • product_perf    │
│  │   • kafka_stream       │   • deduplicate       │   • revenue_attr    │
│  │   • fx_rates_ingest    │                       │                     │
│  └────────────────────────┴───────────────────────┴─────────────────────┤
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
┌────────────────────────────────────▼────────────────────────────────────┐
│                      DATA LAKE (S3)                                     │
├─────────────────────────────────────────────────────────────────────────┤
│  s3://data-lake-bucket/                                                │
│  ├── bronze/              # Raw ingested data                          │
│  │   ├── snowflake/                                                    │
│  │   ├── redshift/                                                     │
│  │   ├── kafka/                                                        │
│  │   └── fx/                                                           │
│  ├── silver/              # Cleaned & conformed data                   │
│  │   ├── orders/                                                       │
│  │   ├── behavior/                                                     │
│  │   └── fx_rates/                                                     │
│  ├── gold/                # Analytics & aggregates                     │
│  │   ├── customer_360/                                                 │
│  │   ├── product_perf/                                                 │
│  │   └── revenue_by_geo/                                               │
│  ├── _dq_results/         # Data quality results                       │
│  └── _checkpoints/        # Streaming checkpoints                      │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
┌────────────────────────────────────▼────────────────────────────────────┐
│                     QUERY & ANALYTICS LAYER                             │
├─────────────────────────────────────────────────────────────────────────┤
│  • AWS Glue Catalog      # Metastore                                   │
│  • Amazon Athena         # SQL Queries                                 │
│  • Amazon QuickSight     # BI Dashboards                               │
│  • CloudWatch            # Monitoring                                  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Step 1: AWS Account Setup

### 1.1 Configure AWS CLI
```bash
# Install AWS CLI (if not already installed)
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# Configure credentials
aws configure
# Enter:
# - AWS Access Key ID: YOUR_ACCESS_KEY
# - AWS Secret Access Key: YOUR_SECRET_KEY
# - Default region: us-east-1
# - Default output format: json

# Verify configuration
aws sts get-caller-identity
```

### 1.2 Set Environment Variables
```bash
# Set your AWS account and region
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export AWS_REGION=us-east-1
export PROJECT_NAME=pyspark-etl
export ENVIRONMENT=prod

# Bucket names
export DATA_LAKE_BUCKET="${PROJECT_NAME}-${ENVIRONMENT}-data-lake-${AWS_ACCOUNT_ID}"
export ARTIFACTS_BUCKET="${PROJECT_NAME}-${ENVIRONMENT}-artifacts-${AWS_ACCOUNT_ID}"
export LOGS_BUCKET="${PROJECT_NAME}-${ENVIRONMENT}-logs-${AWS_ACCOUNT_ID}"

echo "Account ID: ${AWS_ACCOUNT_ID}"
echo "Region: ${AWS_REGION}"
echo "Data Lake Bucket: ${DATA_LAKE_BUCKET}"
```

---

## Step 2: Infrastructure as Code (Terraform)

### 2.1 Navigate to Terraform Directory
```bash
cd aws/terraform
```

### 2.2 Initialize Terraform
```bash
# Initialize Terraform with AWS provider
terraform init

# Verify Terraform installation
terraform version
```

### 2.3 Review Configuration
```bash
# Review main configuration
cat terraform.tfvars

# Expected output:
# aws_region = "us-east-1"
# project_name = "pyspark-etl"
# environment = "prod"
```

---

## Step 3: Deploy AWS Resources

### 3.1 Plan Infrastructure Deployment
```bash
# Review what will be created (no changes made yet)
terraform plan

# Expected output:
# Plan: 25 to add, 0 to change, 0 to destroy
# Resources:
#   - S3 buckets (data lake, artifacts, logs)
#   - IAM roles and policies
#   - EMR Serverless application
#   - Glue databases (bronze, silver, gold)
#   - CloudWatch log groups
#   - Secrets Manager placeholders
#   - KMS encryption keys
```

### 3.2 Apply Infrastructure
```bash
# Deploy infrastructure (type 'yes' when prompted)
terraform apply

# This will create:
# ✅ S3 Buckets with versioning and encryption
# ✅ IAM Roles for EMR Serverless and job execution
# ✅ EMR Serverless Application
# ✅ Glue Databases (bronze, silver, gold)
# ✅ CloudWatch Log Groups
# ✅ KMS Keys for encryption
# ✅ Secrets Manager placeholders
```

### 3.3 Verify Resources Created
```bash
# List S3 buckets
aws s3 ls | grep ${PROJECT_NAME}

# List EMR Serverless applications
aws emr-serverless list-applications --region ${AWS_REGION}

# List Glue databases
aws glue get-databases --region ${AWS_REGION}

# Get EMR application ID (you'll need this)
export EMR_APP_ID=$(aws emr-serverless list-applications --region ${AWS_REGION} \
  --query 'applications[0].applicationId' --output text)

echo "EMR Application ID: ${EMR_APP_ID}"
```

### 3.4 Get Execution Role ARN
```bash
# Get EMR execution role ARN
export EMR_EXECUTION_ROLE=$(aws emr-serverless list-applications \
  --region ${AWS_REGION} \
  --query 'applications[0].arn' --output text | sed 's/application/execution-role/g')

echo "Execution Role ARN: ${EMR_EXECUTION_ROLE}"
```

---

## Step 4: Configure Secrets

### 4.1 Snowflake Credentials
```bash
# Store Snowflake credentials
aws secretsmanager create-secret \
  --name "pyspark-etl/snowflake/credentials" \
  --description "Snowflake credentials for ETL" \
  --secret-string '{
    "account": "YOUR_ACCOUNT",
    "username": "YOUR_USERNAME",
    "password": "YOUR_PASSWORD",
    "warehouse": "COMPUTE_WH",
    "database": "ETL_PROJECT_DB",
    "schema": "RAW"
  }' \
  --region ${AWS_REGION}

echo "✅ Snowflake secrets stored"
```

### 4.2 Redshift Credentials
```bash
# Store Redshift credentials
aws secretsmanager create-secret \
  --name "pyspark-etl/redshift/credentials" \
  --description "Redshift credentials for ETL" \
  --secret-string '{
    "host": "your-redshift-cluster.amazonaws.com",
    "port": "5439",
    "database": "dev",
    "username": "admin",
    "password": "YOUR_PASSWORD",
    "iam_role": "arn:aws:iam::'${AWS_ACCOUNT_ID}':role/RedshiftLoadRole"
  }' \
  --region ${AWS_REGION}

echo "✅ Redshift secrets stored"
```

### 4.3 Kafka Credentials
```bash
# Store Kafka credentials
aws secretsmanager create-secret \
  --name "pyspark-etl/kafka/credentials" \
  --description "Kafka credentials for streaming" \
  --secret-string '{
    "bootstrap_servers": "your-kafka-bootstrap-servers:9092",
    "security_protocol": "SASL_SSL",
    "sasl_mechanism": "PLAIN",
    "sasl_username": "YOUR_KAFKA_API_KEY",
    "sasl_password": "YOUR_KAFKA_SECRET",
    "topic": "orders_events"
  }' \
  --region ${AWS_REGION}

echo "✅ Kafka secrets stored"
```

---

## Step 5: Upload Code & Data

### 5.1 Prepare Code Package
```bash
# Go back to project root
cd ../..

# Create code package
zip -r pyspark-etl-code.zip \
  src/ \
  config/ \
  requirements.txt \
  aws/jobs/ \
  aws/scripts/ \
  -x "*.pyc" -x "__pycache__/*" -x "*.DS_Store"

# Upload to S3
aws s3 cp pyspark-etl-code.zip \
  s3://${ARTIFACTS_BUCKET}/code/

aws s3 cp config/prod.yaml \
  s3://${ARTIFACTS_BUCKET}/config/

echo "✅ Code uploaded to S3"
```

### 5.2 Upload Source Data
```bash
# Upload sample data to S3 input bucket
aws s3 sync data/input_data/ \
  s3://${DATA_LAKE_BUCKET}/input-data/

# Verify upload
aws s3 ls s3://${DATA_LAKE_BUCKET}/input-data/

echo "✅ Source data uploaded"
```

### 5.3 Create S3 Folder Structure
```bash
# Create Bronze/Silver/Gold structure
aws s3api put-object --bucket ${DATA_LAKE_BUCKET} --key bronze/
aws s3api put-object --bucket ${DATA_LAKE_BUCKET} --key silver/
aws s3api put-object --bucket ${DATA_LAKE_BUCKET} --key gold/
aws s3api put-object --bucket ${DATA_LAKE_BUCKET} --key _dq_results/
aws s3api put-object --bucket ${DATA_LAKE_BUCKET} --key _checkpoints/

echo "✅ S3 folder structure created"
```

---

## Step 6: Run ETL Pipeline

### 6.1 Extract: Snowflake to Bronze
```bash
# Run Snowflake ingestion job
aws emr-serverless start-job-run \
  --application-id ${EMR_APP_ID} \
  --execution-role-arn ${EMR_EXECUTION_ROLE} \
  --name "extract-snowflake-bronze" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://'${ARTIFACTS_BUCKET}'/code/pyspark-etl-code.zip",
      "entryPointArguments": [
        "--config", "s3://'${ARTIFACTS_BUCKET}'/config/prod.yaml",
        "--source", "snowflake",
        "--job", "aws/jobs/ingest/snowflake_to_bronze.py"
      ],
      "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --packages io.delta:delta-spark_2.12:3.2.0,net.snowflake:snowflake-jdbc:3.14.3"
    }
  }' \
  --configuration-overrides '{
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {
        "logUri": "s3://'${LOGS_BUCKET}'/emr-logs/"
      }
    }
  }' \
  --region ${AWS_REGION}

# Save job run ID
export SNOWFLAKE_JOB_ID=$(aws emr-serverless list-job-runs \
  --application-id ${EMR_APP_ID} \
  --region ${AWS_REGION} \
  --query 'jobRuns[0].id' --output text)

echo "Snowflake job run ID: ${SNOWFLAKE_JOB_ID}"
```

### 6.2 Extract: Redshift to Bronze
```bash
# Run Redshift ingestion job
aws emr-serverless start-job-run \
  --application-id ${EMR_APP_ID} \
  --execution-role-arn ${EMR_EXECUTION_ROLE} \
  --name "extract-redshift-bronze" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://'${ARTIFACTS_BUCKET}'/code/pyspark-etl-code.zip",
      "entryPointArguments": [
        "--config", "s3://'${ARTIFACTS_BUCKET}'/config/prod.yaml",
        "--source", "redshift",
        "--job", "aws/jobs/ingest/redshift_behavior_ingest.py"
      ],
      "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --packages io.delta:delta-spark_2.12:3.2.0,com.amazon.redshift:redshift-jdbc42:2.1.0.12"
    }
  }' \
  --configuration-overrides '{
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {
        "logUri": "s3://'${LOGS_BUCKET}'/emr-logs/"
      }
    }
  }' \
  --region ${AWS_REGION}

echo "✅ Redshift extraction started"
```

### 6.3 Transform: Bronze to Silver
```bash
# Run Bronze to Silver transformation
aws emr-serverless start-job-run \
  --application-id ${EMR_APP_ID} \
  --execution-role-arn ${EMR_EXECUTION_ROLE} \
  --name "bronze-to-silver" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://'${ARTIFACTS_BUCKET}'/code/pyspark-etl-code.zip",
      "entryPointArguments": [
        "--config", "s3://'${ARTIFACTS_BUCKET}'/config/prod.yaml",
        "--job", "aws/jobs/transform/snowflake_bronze_to_silver_merge.py"
      ],
      "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --packages io.delta:delta-spark_2.12:3.2.0"
    }
  }' \
  --configuration-overrides '{
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {
        "logUri": "s3://'${LOGS_BUCKET}'/emr-logs/"
      }
    }
  }' \
  --region ${AWS_REGION}

echo "✅ Bronze to Silver transformation started"
```

### 6.4 Data Quality Check (HARD GATE)
```bash
# Run data quality checks (this is a hard gate)
aws emr-serverless start-job-run \
  --application-id ${EMR_APP_ID} \
  --execution-role-arn ${EMR_EXECUTION_ROLE} \
  --name "dq-check-silver" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://'${ARTIFACTS_BUCKET}'/code/pyspark-etl-code.zip",
      "entryPointArguments": [
        "--config", "s3://'${ARTIFACTS_BUCKET}'/config/prod.yaml",
        "--suite", "silver",
        "--job", "aws/jobs/transform/dq_check_silver.py"
      ],
      "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --packages io.delta:delta-spark_2.12:3.2.0"
    }
  }' \
  --configuration-overrides '{
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {
        "logUri": "s3://'${LOGS_BUCKET}'/emr-logs/"
      }
    }
  }' \
  --region ${AWS_REGION}

echo "✅ Data quality check started"
```

### 6.5 Analytics: Silver to Gold
```bash
# Run Silver to Gold analytics
aws emr-serverless start-job-run \
  --application-id ${EMR_APP_ID} \
  --execution-role-arn ${EMR_EXECUTION_ROLE} \
  --name "silver-to-gold" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://'${ARTIFACTS_BUCKET}'/code/pyspark-etl-code.zip",
      "entryPointArguments": [
        "--config", "s3://'${ARTIFACTS_BUCKET}'/config/prod.yaml",
        "--job", "aws/jobs/analytics/build_customer_dimension.py"
      ],
      "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --packages io.delta:delta-spark_2.12:3.2.0"
    }
  }' \
  --configuration-overrides '{
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {
        "logUri": "s3://'${LOGS_BUCKET}'/emr-logs/"
      }
    }
  }' \
  --region ${AWS_REGION}

echo "✅ Silver to Gold analytics started"
```

### 6.6 Monitor Job Status
```bash
# Check job status
aws emr-serverless get-job-run \
  --application-id ${EMR_APP_ID} \
  --job-run-id ${SNOWFLAKE_JOB_ID} \
  --region ${AWS_REGION} \
  --query 'jobRun.state' --output text

# Expected states: SUBMITTED, RUNNING, SUCCESS, FAILED
# Continue checking until SUCCESS or FAILED
```

### 6.7 View Job Logs
```bash
# List available logs
aws s3 ls s3://${LOGS_BUCKET}/emr-logs/

# Download and view driver logs
aws s3 cp s3://${LOGS_BUCKET}/emr-logs/applications/${EMR_APP_ID}/jobs/${SNOWFLAKE_JOB_ID}/driver/stdout.gz - | gunzip | less

# View stderr
aws s3 cp s3://${LOGS_BUCKET}/emr-logs/applications/${EMR_APP_ID}/jobs/${SNOWFLAKE_JOB_ID}/driver/stderr.gz - | gunzip | less
```

---

## Step 7: Query with Athena

### 7.1 Register Glue Tables
```bash
# Register Silver layer tables in Glue catalog
python aws/scripts/register_glue_tables.py \
  --db silver \
  --lake-bucket ${DATA_LAKE_BUCKET} \
  --region ${AWS_REGION} \
  --format delta

# Register Gold layer tables
python aws/scripts/register_glue_tables.py \
  --db gold \
  --lake-bucket ${DATA_LAKE_BUCKET} \
  --region ${AWS_REGION} \
  --format delta

echo "✅ Glue tables registered"
```

### 7.2 Query Data in Athena
```bash
# Open Athena console and run queries
# Or use AWS CLI to run a query

aws athena start-query-execution \
  --query-string "SELECT COUNT(*) as record_count FROM silver.orders" \
  --result-configuration "OutputLocation=s3://${DATA_LAKE_BUCKET}/query-results/" \
  --region ${AWS_REGION}

# Business Intelligence Query
aws athena start-query-execution \
  --query-string "
    SELECT 
      c.name AS customer_name,
      COUNT(o.order_id) as total_orders,
      SUM(o.amount_usd) as total_revenue
    FROM silver.orders o
    JOIN silver.customers c ON o.customer_id = c.customer_id
    GROUP BY c.name
    ORDER BY total_revenue DESC
    LIMIT 10
  " \
  --result-configuration "OutputLocation=s3://${DATA_LAKE_BUCKET}/query-results/" \
  --region ${AWS_REGION}
```

### 7.3 Sample Analytics Queries
```sql
-- Check data volume in each layer
SELECT 'bronze' as layer, 'orders' as table, COUNT(*) as count 
FROM bronze.snowflake_orders
UNION ALL
SELECT 'silver' as layer, 'orders' as table, COUNT(*) as count 
FROM silver.orders
UNION ALL
SELECT 'gold' as layer, 'customer_360' as table, COUNT(*) as count 
FROM gold.customer_360;

-- Data freshness check
SELECT 
  table_name,
  MAX(last_updated_ts) as latest_update,
  CURRENT_TIMESTAMP - MAX(last_updated_ts) as age_hours
FROM (
  SELECT 'orders' as table_name, MAX(order_date) as last_updated_ts FROM silver.orders
  UNION ALL
  SELECT 'behavior' as table_name, MAX(event_ts) as last_updated_ts FROM silver.behavior
  UNION ALL
  SELECT 'fx_rates' as table_name, MAX(as_of_date) as last_updated_ts FROM silver.fx_rates
)
GROUP BY table_name;

-- Top customers by revenue
SELECT 
  c.name,
  COUNT(DISTINCT o.order_id) as order_count,
  ROUND(SUM(o.amount_usd), 2) as total_revenue_usd,
  ROUND(AVG(o.amount_usd), 2) as avg_order_value
FROM gold.customer_360 c
JOIN silver.orders o ON c.customer_id = o.customer_id
WHERE o.amount_usd IS NOT NULL
GROUP BY c.name
ORDER BY total_revenue_usd DESC
LIMIT 20;
```

---

## Step 8: Monitoring & Alerts

### 8.1 View CloudWatch Metrics
```bash
# List custom metrics
aws cloudwatch list-metrics \
  --namespace "etl/data-pipeline" \
  --region ${AWS_REGION}

# Get recent metric data
aws cloudwatch get-metric-statistics \
  --namespace "etl/data-pipeline" \
  --metric-name "records_processed_total" \
  --dimensions Name=job,Value=bronze_to_silver \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Sum \
  --region ${AWS_REGION}
```

### 8.2 Create CloudWatch Alarms
```bash
# Alarm for pipeline failures
aws cloudwatch put-metric-alarm \
  --alarm-name "etl-pipeline-failure" \
  --alarm-description "Alert when ETL pipeline fails" \
  --metric-name "job_run_failures" \
  --namespace "etl/data-pipeline" \
  --statistic "Sum" \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator "GreaterThanOrEqualToThreshold" \
  --alarm-actions "arn:aws:sns:${AWS_REGION}:${AWS_ACCOUNT_ID}:etl-alerts" \
  --region ${AWS_REGION}

echo "✅ CloudWatch alarm created"
```

### 8.3 Check Data Quality Results
```bash
# List DQ results
aws s3 ls s3://${DATA_LAKE_BUCKET}/_dq_results/

# Download and view latest DQ report
aws s3 cp s3://${DATA_LAKE_BUCKET}/_dq_results/silver/orders/$(date +%Y%m%d)_* - | jq .

# Check for critical failures
aws s3 cp s3://${DATA_LAKE_BUCKET}/_dq_results/silver/summary.json - | jq '.critical_failures'
```

---

## Step 9: Daily Operations

### 9.1 Scheduled Daily Pipeline
```bash
# Set up EventBridge rule to trigger daily pipeline
aws events put-rule \
  --name "daily-etl-pipeline" \
  --schedule-expression "cron(0 2 * * ? *)" \
  --state "ENABLED" \
  --region ${AWS_REGION}

# Add target to start EMR job
aws events put-targets \
  --rule "daily-etl-pipeline" \
  --targets "Id"="1","Arn"="arn:aws:lambda:${AWS_REGION}:${AWS_ACCOUNT_ID}:function:StartETLPipeline" \
  --region ${AWS_REGION}

echo "✅ Daily pipeline scheduled"
```

### 9.2 Delta Lake Maintenance (Weekly)
```bash
# Run weekly Delta optimization
aws emr-serverless start-job-run \
  --application-id ${EMR_APP_ID} \
  --execution-role-arn ${EMR_EXECUTION_ROLE} \
  --name "weekly-delta-optimize" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://'${ARTIFACTS_BUCKET}'/code/pyspark-etl-code.zip",
      "entryPointArguments": [
        "--config", "s3://'${ARTIFACTS_BUCKET}'/config/prod.yaml",
        "--job", "aws/jobs/maintenance/delta_optimize_vacuum.py",
        "--action", "optimize"
      ],
      "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    }
  }' \
  --region ${AWS_REGION}

echo "✅ Delta optimization started"
```

### 9.3 Backfill Data
```bash
# Backfill last 30 days of data
python aws/scripts/maintenance/backfill_bronze_for_date.sh \
  --start-date $(date -d '30 days ago' +%Y-%m-%d) \
  --end-date $(date +%Y-%m-%d) \
  --source snowflake \
  --region ${AWS_REGION}

echo "✅ Backfill completed"
```

---

## Troubleshooting

### Issue 1: S3 Permissions Denied
```bash
# Check bucket policy
aws s3api get-bucket-policy --bucket ${DATA_LAKE_BUCKET}

# Check IAM role permissions
aws iam get-role-policy \
  --role-name EMRServerlessExecutionRole \
  --policy-name EMRServerlessExecutionPolicy

# Fix: Update IAM policy to grant S3 read/write access
```

### Issue 2: Delta Table Not Found
```bash
# Check if Delta table exists
aws s3 ls s3://${DATA_LAKE_BUCKET}/silver/orders/_delta_log/

# Fix: Re-run ingestion job
```

### Issue 3: Job Failing with Memory Errors
```bash
# Check job configuration
aws emr-serverless get-job-run \
  --application-id ${EMR_APP_ID} \
  --job-run-id ${SNOWFLAKE_JOB_ID} \
  --region ${AWS_REGION} \
  --query 'configurationOverrides' --output json

# Fix: Increase worker memory or reduce data partition size
```

### Issue 4: Secrets Not Found
```bash
# List all secrets
aws secretsmanager list-secrets --region ${AWS_REGION}

# Get secret value (for testing)
aws secretsmanager get-secret-value \
  --secret-id "pyspark-etl/snowflake/credentials" \
  --region ${AWS_REGION} \
  --query 'SecretString' --output text | jq .

# Fix: Recreate secret with correct name
```

### Issue 5: Streaming Checkpoint Issues
```bash
# Check checkpoint directory
aws s3 ls s3://${DATA_LAKE_BUCKET}/_checkpoints/kafka-stream/

# Reset checkpoint (start from beginning)
aws s3 rm s3://${DATA_LAKE_BUCKET}/_checkpoints/kafka-stream/ --recursive

# Fix: Checkpoint will be recreated on next run
```

### Issue 6: Data Quality Checks Failing
```bash
# View DQ results
aws s3 cp s3://${DATA_LAKE_BUCKET}/_dq_results/silver/orders/latest.json - | jq .

# Check for specific failed expectations
aws s3 cp s3://${DATA_LAKE_BUCKET}/_dq_results/silver/orders/latest.json - | jq '.results[] | select(.success == false)'

# Fix: Investigate root cause and fix data or update DQ rules
```

---

## 🎯 Success Criteria

Your AWS deployment is successful when:

- [x] All Terraform resources created without errors
- [x] Secrets stored securely in Secrets Manager
- [x] Code and data uploaded to S3
- [x] EMR Serverless jobs complete successfully
- [x] Data visible in Bronze, Silver, and Gold layers
- [x] Glue tables registered and queryable in Athena
- [x] CloudWatch metrics and logs working
- [x] Data quality gates functioning
- [x] Delta Lake optimizations running
- [x] Monitoring and alerting configured

---

## 📊 Cost Optimization Tips

### 1. Use EMR Serverless Auto-scaling
- Configured automatically in Terraform
- Pay only for job execution time

### 2. Implement S3 Lifecycle Policies
- Transition to Infrequent Access after 30 days
- Archive to Glacier after 90 days

### 3. Delta Lake Optimization
- ZORDER frequently queried columns
- Compaction for small files
- Vacuum old versions

### 4. Partition Data Appropriately
- Partition by date for time-series data
- Partition by source for multi-source data
- Avoid over-partitioning

### 5. Use Athena Partition Projection
- For frequently queried partitioned tables
- Reduces query costs

---

## 🔐 Security Checklist

- [x] S3 buckets with encryption enabled
- [x] IAM roles with least privilege
- [x] Secrets stored in Secrets Manager
- [x] VPC endpoints for private connectivity (optional)
- [x] CloudWatch audit logging enabled
- [x] MFA enabled on AWS account
- [x] Key rotation policies configured
- [x] Network ACLs and security groups configured

---

## 📚 Additional Resources

### AWS Documentation
- [EMR Serverless Documentation](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/)
- [Delta Lake on AWS](https://docs.delta.io/latest/aws-integration.html)
- [AWS Glue Catalog](https://docs.aws.amazon.com/glue/latest/dg/catalog-overview.html)
- [Amazon Athena](https://docs.aws.amazon.com/athena/)

### Project Documentation
- `aws/RUNBOOK_AWS_2025.md` - Operational runbook
- `aws/docs/AWS_DEPLOYMENT_GUIDE.md` - Detailed deployment guide
- `aws/terraform/README_TERRAFORM.md` - Terraform setup

---

## 🎉 Next Steps

1. **Set up Airflow/MWAA** for orchestration
2. **Create BI Dashboards** in QuickSight
3. **Implement ML Models** on Gold layer data
4. **Add More Data Sources** as needed
5. **Optimize for Performance** and cost

---

**🚀 Your production-ready data pipeline is now running on AWS!**

For support or questions, refer to the troubleshooting section or AWS documentation.

