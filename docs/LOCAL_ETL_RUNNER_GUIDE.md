# Local ETL Runner Guide

## Overview

This guide explains how to run the ETL pipeline locally. Since both local and AWS ETL use **S3 as the data source**, the pipeline reads from and writes to S3.

## Quick Start

### Option 1: Python Script (Recommended)

```bash
# Run full pipeline
python3 local/scripts/run_local_etl_safe.py --config local/config/local.yaml

# Check environment first
python3 local/scripts/run_local_etl_safe.py --check-env

# Skip steps
python3 local/scripts/run_local_etl_safe.py --skip-bronze  # Skip Bronze→Silver
python3 local/scripts/run_local_etl_safe.py --skip-gold    # Skip Silver→Gold

# Verify only (no ETL)
python3 local/scripts/run_local_etl_safe.py --verify-only
```

### Option 2: Bash Script

```bash
# Run full pipeline
./local/scripts/run_local_etl.sh local/config/local.yaml
```

### Option 3: Direct Python Entry Point

```bash
# Run full pipeline
python3 local/jobs/run_etl_pipeline.py --config local/config/local.yaml

# Run individual steps
python3 local/jobs/transform/bronze_to_silver.py --env local --config local/config/local.yaml
python3 local/jobs/transform/silver_to_gold.py --env local --config local/config/local.yaml
```

## Prerequisites

### 1. Python Environment

```bash
# Activate virtual environment
source .venv/bin/activate  # or your venv path

# Install dependencies
pip install -r requirements.txt
```

### 2. Java (Required for Spark)

```bash
# Check Java version
java -version  # Should be Java 8 or 11

# Set JAVA_HOME (macOS)
export JAVA_HOME=$(/usr/libexec/java_home)

# Set JAVA_HOME (Linux)
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

### 3. AWS Credentials

Both local and AWS ETL use S3, so you need AWS credentials:

```bash
# Check AWS credentials
aws configure list --profile kunal21

# Or set environment variables
export AWS_PROFILE=kunal21
export AWS_REGION=us-east-1
```

### 4. PySpark

```bash
# Check PySpark
pip show pyspark

# Install/upgrade if needed
pip install --upgrade pyspark
```

## Common Issues

### Issue 1: Py4JError / SparkSession Error

**Error:**
```
py4j.protocol.Py4JError: An error occurred while calling None.org.apache.spark.sql.SparkSession
```

**Solutions:**

1. **Check Java version:**
   ```bash
   java -version  # Should be Java 8 or 11, not Java 17+
   ```

2. **Set JAVA_HOME:**
   ```bash
   export JAVA_HOME=$(/usr/libexec/java_home -v 11)  # macOS
   ```

3. **Reinstall PySpark:**
   ```bash
   pip uninstall pyspark
   pip install pyspark==3.4.4
   ```

4. **Use AWS EMR instead (recommended):**
   - Both local and AWS use S3 data
   - AWS EMR is already working
   - No local Spark setup needed

### Issue 2: ModuleNotFoundError

**Error:**
```
ModuleNotFoundError: No module named 'project_a'
```

**Solution:**
```bash
# Set PYTHONPATH
export PYTHONPATH="${PROJECT_ROOT}/src:${PYTHONPATH}"

# Or use the safe runner (sets PYTHONPATH automatically)
python3 local/scripts/run_local_etl_safe.py
```

### Issue 3: S3 Access Denied

**Error:**
```
AccessDenied: Access Denied
```

**Solution:**
```bash
# Check AWS credentials
aws s3 ls s3://my-etl-lake-demo-424570854632/bronze/ --profile kunal21

# Set profile
export AWS_PROFILE=kunal21
export AWS_REGION=us-east-1
```

## Alternative: Check Data Without Running ETL

If local ETL fails due to SparkSession issues, you can still check the data:

### 1. S3 Data Quality Check (No Spark Required)

```bash
# Check all layers
python3 local/scripts/dq/check_s3_data_quality.py --env aws --layer all --config aws/config/dev.yaml

# Check specific layer
python3 local/scripts/dq/check_s3_data_quality.py --layer bronze
python3 local/scripts/dq/check_s3_data_quality.py --layer silver
python3 local/scripts/dq/check_s3_data_quality.py --layer gold
```

### 2. AWS CLI

```bash
# List Silver tables
aws s3 ls s3://my-etl-lake-demo-424570854632/silver/ --recursive --profile kunal21

# List Gold tables
aws s3 ls s3://my-etl-lake-demo-424570854632/gold/ --recursive --profile kunal21
```

### 3. Use AWS EMR (Recommended)

Since both local and AWS use S3 data, you can run ETL on AWS EMR:

```bash
# Submit Bronze→Silver job
aws emr add-steps \
  --cluster-id j-3N2JXYADSENNU \
  --steps file://steps_bronze_to_silver.json \
  --profile kunal21 \
  --region us-east-1

# Submit Silver→Gold job
aws emr add-steps \
  --cluster-id j-3N2JXYADSENNU \
  --steps file://steps_silver_to_gold.json \
  --profile kunal21 \
  --region us-east-1
```

## Pipeline Steps

### Step 1: Bronze → Silver

**What it does:**
- Reads from S3 Bronze layer (CRM, Snowflake, Redshift, Kafka, FX)
- Transforms and cleans data
- Writes to S3 Silver layer (Delta format)

**Output tables:**
- `customers_silver`
- `orders_silver`
- `products_silver`
- `customer_behavior_silver`
- `fx_rates_silver`
- `order_events_silver`

### Step 2: Silver → Gold

**What it does:**
- Reads from S3 Silver layer
- Builds dimensions and facts
- Writes to S3 Gold layer (Delta format)

**Output tables:**
- `fact_orders`
- `dim_customer`
- `dim_product`
- `dim_date`
- `customer_360`
- `product_performance`

### Step 3: Verification

**What it does:**
- Checks that all output tables exist
- Shows row counts for each table
- Validates data quality

## Configuration

### Local Config (`local/config/local.yaml`)

```yaml
env: local
environment: local

paths:
  bronze_root: "s3://my-etl-lake-demo-424570854632/bronze"
  silver_root: "s3://my-etl-lake-demo-424570854632/silver"
  gold_root: "s3://my-etl-lake-demo-424570854632/gold"
```

**Note:** Local config uses S3 paths (same as AWS) because both environments share the same data source.

## Troubleshooting

### Check Environment

```bash
# Run with environment check
python3 local/scripts/run_local_etl_safe.py --check-env --config local/config/local.yaml
```

### View Logs

```bash
# Run with verbose output
python3 local/scripts/run_local_etl_safe.py --config local/config/local.yaml 2>&1 | tee etl.log
```

### Test Individual Steps

```bash
# Test Bronze→Silver only
python3 local/jobs/transform/bronze_to_silver.py \
  --env local \
  --config local/config/local.yaml

# Test Silver→Gold only
python3 local/jobs/transform/silver_to_gold.py \
  --env local \
  --config local/config/local.yaml
```

## Best Practices

1. **Use AWS EMR for Production ETL** (recommended)
   - More reliable
   - No local Spark setup needed
   - Both use same S3 data source

2. **Use Local for Development**
   - Faster iteration
   - Easier debugging
   - Same code as AWS (95% identical)

3. **Check Data Quality Regularly**
   ```bash
   python3 local/scripts/dq/check_s3_data_quality.py --layer all
   ```

4. **Verify Outputs After ETL**
   ```bash
   python3 local/scripts/run_local_etl_safe.py --verify-only
   ```

## Summary

- ✅ **Local ETL**: Uses S3 data (same as AWS)
- ✅ **Code Alignment**: 95% identical between local and AWS
- ✅ **AWS EMR**: Working perfectly (recommended for production)
- ⚠️ **Local Spark**: May have compatibility issues (use AWS EMR if needed)

For production ETL, use AWS EMR. For development and testing, use local ETL with the safe runner.

