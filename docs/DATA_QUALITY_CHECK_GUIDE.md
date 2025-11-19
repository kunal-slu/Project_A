# Data Quality Check Guide: Local vs AWS

## Overview

This guide explains how to run data quality checks on both local and AWS environments and compare results.

## Quick Start

### Check Local Data Quality

```bash
# Check all layers
python local/scripts/dq/check_data_quality.py --env local --layer all

# Check specific layer
python local/scripts/dq/check_data_quality.py --env local --layer bronze
python local/scripts/dq/check_data_quality.py --env local --layer silver
python local/scripts/dq/check_data_quality.py --env local --layer gold
```

### Check AWS Data Quality

```bash
# Run on EMR Serverless
./aws/scripts/dq/check_data_quality.sh

# Or manually
aws emr-serverless start-job-run \
  --application-id $EMR_APP_ID \
  --execution-role-arn $EMR_ROLE_ARN \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://bucket/scripts/dq/check_data_quality.py",
      "entryPointArguments": ["--env", "aws", "--config", "s3://bucket/config/dev.yaml"]
    }
  }'
```

### Compare Local vs AWS

```bash
python local/scripts/dq/compare_local_aws_dq.py --layer all
```

## Quality Checks Performed

### Bronze Layer

**Checks:**
- ✅ Row counts
- ✅ Duplicate detection
- ✅ Null value analysis
- ✅ Schema validation
- ✅ File accessibility

**Tables Checked:**
- `bronze.crm.accounts`
- `bronze.crm.contacts`
- `bronze.crm.opportunities`
- `bronze.snowflake.customers`
- `bronze.snowflake.orders`
- `bronze.snowflake.products`
- `bronze.redshift.behavior`
- `bronze.kafka.orders_seed`
- `bronze.fx.daily_rates_json`

### Silver Layer

**Checks:**
- ✅ Row counts
- ✅ Duplicate detection
- ✅ Null value analysis
- ✅ Schema validation
- ✅ Data completeness

**Tables Checked:**
- `silver.customers_silver`
- `silver.orders_silver`
- `silver.products_silver`
- `silver.customer_behavior_silver`
- `silver.fx_rates_silver`
- `silver.order_events_silver`

### Gold Layer

**Checks:**
- ✅ Row counts
- ✅ Duplicate detection
- ✅ Referential integrity (surrogate keys)
- ✅ Business rule validation

**Tables Checked:**
- `gold.fact_orders`
- `gold.dim_customer`
- `gold.dim_product`
- `gold.dim_date`
- `gold.customer_360`
- `gold.product_performance`

## Quality Metrics

### Pass Criteria

- ✅ **Row Count > 0**: Table is not empty
- ✅ **Duplicates = 0**: No duplicate rows
- ✅ **Nulls < 5%**: Less than 5% null values in key columns
- ✅ **Schema Valid**: All expected columns present

### Warning Criteria

- ⚠️ **Duplicates > 0**: Duplicate rows detected
- ⚠️ **Nulls 5-10%**: Moderate null values
- ⚠️ **Schema Mismatch**: Missing or extra columns

### Error Criteria

- ❌ **Table Empty**: No rows found
- ❌ **Cannot Read**: File/table not accessible
- ❌ **Schema Error**: Schema validation failed

## Expected Results

### Bronze Layer (Both Local and AWS)

Since both use the same S3 source:
- ✅ **Same row counts**
- ✅ **Same duplicate counts**
- ✅ **Same null patterns**

### Silver/Gold Layers

Results may differ if:
- Local and AWS ran at different times
- Different transformation logic (should not happen)
- Data corruption in one environment

## Troubleshooting

### Local Check Fails

**Error:** `AccessDenied` or `No credentials found`
- **Fix:** Configure AWS credentials: `aws configure --profile kunal21`

**Error:** `Table not found`
- **Fix:** Run ETL pipeline first to create tables

### AWS Check Fails

**Error:** `Job failed`
- **Fix:** Check EMR logs: `aws emr-serverless get-job-run --application-id $APP_ID --job-run-id $RUN_ID`

**Error:** `ModuleNotFoundError`
- **Fix:** Ensure wheel is uploaded to S3

### Results Don't Match

**Issue:** Local and AWS show different row counts
- **Check:** Are both using same S3 source?
- **Check:** Did both run on same data?
- **Check:** Are configs identical?

## Advanced Usage

### Custom DQ Rules

Edit `config/dq.yaml` to add custom rules:

```yaml
dq:
  rules:
    - name: "customer_id_not_null"
      table: "silver.customers_silver"
      check: "customer_id IS NOT NULL"
      severity: "error"
    
    - name: "order_amount_positive"
      table: "silver.orders_silver"
      check: "amount_usd > 0"
      severity: "warning"
```

### Integration with Great Expectations

The project supports Great Expectations for advanced DQ:

```python
from project_a.dq.run_ge import run_great_expectations_check

results = run_great_expectations_check(
    spark=spark,
    table_path="s3://bucket/silver/customers_silver",
    suite_name="silver_customers_suite"
)
```

## Reporting

DQ results are logged to:
- **Local:** Console output + optional JSON file
- **AWS:** CloudWatch logs + S3 DQ results bucket

## Next Steps

1. ✅ Run DQ checks on both environments
2. ✅ Compare results
3. ✅ Fix any quality issues found
4. ✅ Set up automated DQ in CI/CD
5. ✅ Configure alerts for quality failures

