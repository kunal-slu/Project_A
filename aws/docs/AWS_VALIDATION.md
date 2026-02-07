# AWS ETL Pipeline Validation Guide

This guide explains how to validate the AWS ETL pipeline (Bronze → Silver → Gold) running on S3 and EMR.

## Overview

The AWS validation script (`tools/validate_aws_etl.py`) performs the same checks as the local validator but reads from S3 paths and uses Delta Lake format.

## Prerequisites

1. **Run on EMR Serverless**: ⚠️ **This script MUST run on EMR Serverless, NOT locally**. Local execution will fail because S3 filesystem support is not available in local Spark. See [AWS Validation Local Execution](../../docs/AWS_VALIDATION_LOCAL_EXECUTION.md) for details.
2. **AWS Credentials**: Configure AWS credentials (via `~/.aws/credentials` or environment variables) - handled automatically by EMR Serverless
3. **S3 Access**: Read access to the lake bucket (`my-etl-lake-demo-424570854632`) - handled automatically by EMR Serverless IAM roles
4. **ETL Jobs Executed**: The Bronze-to-Silver and Silver-to-Gold ETL jobs must have been successfully run on EMR Serverless first
5. **Delta Lake**: Delta Lake extensions enabled (automatic for AWS environment on EMR Serverless)

## Quick Start

⚠️ **IMPORTANT**: The AWS validation script **cannot run locally** when the config contains S3 paths. It must run on EMR Serverless. See [AWS Validation Local Execution](../../docs/AWS_VALIDATION_LOCAL_EXECUTION.md) for details.

### Validate AWS Pipeline on EMR Serverless

```bash
# Run on EMR Serverless (via Airflow DAG or EMR job submission):
python tools/validate_aws_etl.py --env dev --config s3://my-etl-artifacts-demo-424570854632/config/aws/config/dev.yaml
```

### For Local Testing

```bash
# Use the local validation script instead:
python tools/validate_local_etl.py --env local --config local/config/local.yaml
```

## Validation Checks

The validator performs the following checks:

### Silver Layer

1. **Table Existence & Row Counts**
   - `customers_silver`: Must have > 0 rows
   - `orders_silver`: Must have > 0 rows
   - `products_silver`: Must have > 0 rows
   - `customer_behavior_silver`: Must have > 0 rows
   - `order_events_silver`: Must have > 0 rows
   - `fx_rates_silver`: Optional (can be empty)

2. **Schema Validation**
   - All tables must have correct column names and types
   - Matches local validation schemas exactly

3. **Data Quality**
   - `customers_silver.customer_id`: 0% null (PASS)
   - `orders_silver.order_id`: 0% null (PASS)
   - `orders_silver.customer_id`: < 5% null (PASS)

### Gold Layer

1. **Table Existence & Row Counts**
   - `fact_orders`: Must have > 0 rows
   - `dim_customer`: Must have > 0 rows
   - `dim_product`: Must have > 0 rows
   - `dim_date`: Must have > 0 rows
   - `customer_360`: Must have > 0 rows
   - `product_performance`: Must have > 0 rows

2. **Referential Integrity**
   - `fact_orders.customer_sk`: All rows have valid customer (not -1)
   - `fact_orders.product_sk`: All rows have valid product (not -1)

3. **Business Logic**
   - `fact_orders.total_revenue`: > $0 (PASS)

## Expected Output

```
================================================================================
🚀 AWS ETL PIPELINE VALIDATION
================================================================================
Environment: aws
Config: aws/config/dev.yaml
Silver Root: s3://my-etl-lake-demo-424570854632/silver
Gold Root: s3://my-etl-lake-demo-424570854632/gold
Spark Master: local[*]

================================================================================
🔍 VALIDATING SILVER LAYER
================================================================================

================================================================================
📊 CUSTOMERS_SILVER
================================================================================
Row Count: 50,000

Schema (14 columns):
  • customer_id: StringType (nullable)
  • customer_name: StringType (nullable)
  ...

Sample Rows (showing 5 of 50,000):
  [1] customer_id=CUST-000001, customer_name=John Doe, ...

...

================================================================================
📋 VALIDATION SUMMARY
================================================================================
Silver Layer: 6/6 checks passed
Gold Layer: 6/6 checks passed

Overall: 12/12 checks passed
✅ All checks passed!
```

## Troubleshooting

### Issue: "Failed to find the data source: delta"

**Cause**: Delta Lake extensions not enabled.

**Solution**: Ensure Spark session has Delta extensions:
```python
spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
```

### Issue: "Table not found at s3://..."

**Cause**: Table doesn't exist or path is incorrect.

**Solution**: 
1. Check S3 path: `aws s3 ls s3://my-etl-lake-demo-424570854632/silver/`
2. Verify config paths match actual S3 structure
3. Run ETL jobs first: `bronze_to_silver.py` and `silver_to_gold.py`

### Issue: "All customer_sk = -1"

**Cause**: Customer dimension not matching fact table keys.

**Solution**:
1. Check `customers_silver` has correct `customer_id` values
2. Verify `orders_silver.customer_id` matches `customers_silver.customer_id`
3. Check join logic in `silver_to_gold.py` → `build_fact_orders()`

### Issue: "Total revenue = $0"

**Cause**: `amount_usd` or `sales_amount` not calculated correctly.

**Solution**:
1. Check `orders_silver.amount_usd` has values > 0
2. Verify FX rates join in `build_orders_silver()`
3. Check `build_fact_orders()` uses `amount_usd` for `sales_amount`

## S3 Data Structure

Expected S3 structure:

```
s3://my-etl-lake-demo-424570854632/
├── bronze/
│   ├── crm/
│   │   ├── accounts.csv
│   │   ├── contacts.csv
│   │   └── opportunities.csv
│   ├── snowflakes/  # Note: plural "snowflakes"
│   │   ├── snowflake_customers_50000.csv
│   │   ├── snowflake_orders_100000.csv
│   │   └── snowflake_products_10000.csv
│   ├── redshift/
│   │   └── redshift_customer_behavior_50000.csv
│   ├── kafka/
│   │   └── stream_kafka_events_100000.csv
│   └── fx/
│       └── fx_rates_historical.json
├── silver/
│   ├── customers_silver/
│   ├── orders_silver/
│   ├── products_silver/
│   ├── customer_behavior_silver/
│   ├── fx_rates_silver/
│   └── order_events_silver/
└── gold/
    ├── fact_orders/
    ├── dim_customer/
    ├── dim_product/
    ├── dim_date/
    ├── customer_360/
    └── product_performance/
```

## Commands Reference

### List S3 Data

```bash
# List bronze data
aws s3 ls s3://my-etl-lake-demo-424570854632/bronze/ --recursive

# List silver tables
aws s3 ls s3://my-etl-lake-demo-424570854632/silver/ --recursive

# List gold tables
aws s3 ls s3://my-etl-lake-demo-424570854632/gold/ --recursive
```

### Check Table Row Counts (via Spark SQL)

```bash
# Start Spark shell with Delta
pyspark --packages io.delta:delta-core_2.12:2.4.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

# In Spark shell:
df = spark.read.format("delta").load("s3://my-etl-lake-demo-424570854632/silver/orders_silver")
df.count()
```

### Compare Local vs AWS

```bash
# Run local validation
python tools/validate_local_etl.py --config local/config/local.yaml > local_results.txt

# Run AWS validation
python tools/validate_aws_etl.py --config aws/config/dev.yaml > aws_results.txt

# Compare row counts
diff local_results.txt aws_results.txt
```

## Next Steps

After validation passes:

1. **Deploy to Production**: Update `config/aws/prod.yaml` with production paths
2. **Set Up Monitoring**: Configure CloudWatch alarms for ETL job failures
3. **Schedule Jobs**: Use Airflow/MWAA to schedule daily ETL runs
4. **Data Quality**: Set up Great Expectations checks on Silver/Gold tables

## Related Documentation

- [Local ETL Validation](LOCAL_ETL_VALIDATION.md)
- [AWS Deployment Guide](../aws/docs/AWS_DEPLOYMENT_GUIDE.md)
- [ETL Pipeline Architecture](../README.md)

