# ✅ Phase 4: Bronze → Silver → Gold - Ready to Run

## Summary

All Phase 4 components are implemented and ready for EMR Serverless execution.

## ✅ Completed Components

### 1. Unified Entrypoint
- ✅ `src/project_a/pipeline/run_pipeline.py` - Dispatcher for all jobs
- ✅ `src/project_a/jobs/` - All jobs with `main(args)` signature
- ✅ Console script exposed in `pyproject.toml`

### 2. Bronze → Silver Job
- ✅ `jobs/transform/bronze_to_silver.py` - Reads all 5 sources:
  - CRM (accounts, contacts, opportunities)
  - Redshift (customer behavior)
  - Snowflake (customers, orders, products)
  - FX (rates from Delta)
  - Kafka (order events)
- ✅ Applies transformations:
  - Schema enforcement
  - Null checks and filtering
  - Deduplication
  - Business key alignment
  - FX normalization
- ✅ Writes to Silver:
  - `silver/customers_silver/`
  - `silver/orders_silver/`
  - `silver/products_silver/`
  - `silver/customer_behavior_silver/`
  - `silver/fx_rates_silver/`
  - `silver/order_events_silver/`

### 3. Silver → Gold Job
- ✅ `jobs/gold/silver_to_gold.py` - Builds star schema:
  - `dim_customer` (with SCD2-lite)
  - `dim_product`
  - `dim_date`
  - `fact_orders`
  - `customer_360` (analytics view)
  - `product_performance` (analytics view)
- ✅ Writes to Gold:
  - `gold/fact_orders/`
  - `gold/dim_customer/`
  - `gold/dim_product/`
  - `gold/dim_date/`
  - `gold/customer_360/`
  - `gold/product_performance/`

### 4. Run Audit Trail
- ✅ All jobs write audit logs to `s3://bucket/_audit/{env}/{job_name}/{date}/`
- ✅ Tracks: rows_in, rows_out, duration, status

### 5. Wheel Built
- ✅ `dist/project_a-0.1.0-py3-none-any.whl` (278KB)

## 🚀 Next Steps

### Step 1: Upload Wheel to S3

```bash
export AWS_PROFILE=kunal21
export AWS_REGION=us-east-1
export ARTIFACTS_BUCKET=my-etl-artifacts-demo-424570854632

cd ~/IdeaProjects/pyspark_data_engineer_project

# Upload wheel
aws s3 cp dist/project_a-0.1.0-py3-none-any.whl \
  "s3://${ARTIFACTS_BUCKET}/packages/" \
  --profile "$AWS_PROFILE" --region "$AWS_REGION"
```

### Step 2: Run Bronze → Silver Job

```bash
export EMR_APP_ID=00g0tm6kccmdcf09
export EMR_ROLE_ARN=arn:aws:iam::424570854632:role/project-a-dev-emr-exec

aws emr-serverless start-job-run \
  --application-id "$EMR_APP_ID" \
  --execution-role-arn "$EMR_ROLE_ARN" \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://'${ARTIFACTS_BUCKET}'/packages/project_a-0.1.0-py3-none-any.whl",
      "entryPointArguments": [
        "--job", "bronze_to_silver",
        "--env", "dev",
        "--config", "s3://'${ARTIFACTS_BUCKET}'/config/dev.yaml"
      ],
      "sparkSubmitParameters": "--py-files s3://'${ARTIFACTS_BUCKET}'/packages/project_a-0.1.0-py3-none-any.whl,s3://'${ARTIFACTS_BUCKET}'/packages/emr_deps_pyyaml.zip --packages io.delta:delta-core_2.12:2.4.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    }
  }' \
  --configuration-overrides '{
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {
        "logUri": "s3://'${ARTIFACTS_BUCKET}'/emr-logs/"
      }
    }
  }'
```

### Step 3: Verify Silver Tables

```bash
export LAKE_BUCKET=my-etl-lake-demo-424570854632

aws s3 ls "s3://${LAKE_BUCKET}/silver/" \
  --recursive --profile "$AWS_PROFILE" --region "$AWS_REGION" | head -20
```

Expected output:
```
silver/customers_silver/
silver/orders_silver/
silver/products_silver/
silver/customer_behavior_silver/
silver/fx_rates_silver/
silver/order_events_silver/
```

### Step 4: Run Silver → Gold Job

```bash
aws emr-serverless start-job-run \
  --application-id "$EMR_APP_ID" \
  --execution-role-arn "$EMR_ROLE_ARN" \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://'${ARTIFACTS_BUCKET}'/packages/project_a-0.1.0-py3-none-any.whl",
      "entryPointArguments": [
        "--job", "silver_to_gold",
        "--env", "dev",
        "--config", "s3://'${ARTIFACTS_BUCKET}'/config/dev.yaml"
      ],
      "sparkSubmitParameters": "--py-files s3://'${ARTIFACTS_BUCKET}'/packages/project_a-0.1.0-py3-none-any.whl,s3://'${ARTIFACTS_BUCKET}'/packages/emr_deps_pyyaml.zip --packages io.delta:delta-core_2.12:2.4.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    }
  }' \
  --configuration-overrides '{
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {
        "logUri": "s3://'${ARTIFACTS_BUCKET}'/emr-logs/"
      }
    }
  }'
```

### Step 5: Verify Gold Tables

```bash
aws s3 ls "s3://${LAKE_BUCKET}/gold/" \
  --recursive --profile "$AWS_PROFILE" --region "$AWS_REGION" | head -20
```

Expected output:
```
gold/fact_orders/
gold/dim_customer/
gold/dim_product/
gold/dim_date/
gold/customer_360/
gold/product_performance/
```

### Step 6: Check Job Status

```bash
# List recent job runs
aws emr-serverless list-job-runs \
  --application-id "$EMR_APP_ID" \
  --profile "$AWS_PROFILE" --region "$AWS_REGION" \
  --max-results 5 \
  --query 'jobRuns[*].[jobRunId,state,createdAt]' \
  --output table
```

### Step 7: Check Run Audit Logs

```bash
aws s3 ls "s3://${LAKE_BUCKET}/_audit/" \
  --recursive --profile "$AWS_PROFILE" --region "$AWS_REGION" | head -20
```

## 🎯 Phase 4 Completion Checklist

- [x] Unified entrypoint created (`project_a.pipeline.run_pipeline`)
- [x] Bronze → Silver job reads all 5 sources
- [x] Bronze → Silver applies schema, null checks, joins, dedupe
- [x] Bronze → Silver writes to silver/ tables
- [x] Silver → Gold builds fact + dimension tables
- [x] Silver → Gold writes to gold/ tables
- [x] Run audit trail implemented
- [x] Wheel built and ready to upload
- [ ] Wheel uploaded to S3
- [ ] Bronze → Silver job runs successfully on EMR
- [ ] Silver → Gold job runs successfully on EMR
- [ ] Silver tables verified in S3
- [ ] Gold tables verified in S3

## 📝 Quick Test Script

Use the provided script to run everything:

```bash
./scripts/run_phase4_jobs.sh
```

This script will:
1. Rebuild the wheel
2. Upload to S3
3. Run Bronze → Silver job
4. Wait for completion
5. Verify Silver tables
6. Run Silver → Gold job
7. Wait for completion
8. Verify Gold tables

## 🔍 Troubleshooting

### Job Fails with Import Error
- Ensure wheel includes all dependencies
- Check `emr_deps_pyyaml.zip` is in S3
- Verify `--py-files` includes both wheel and deps zip

### Job Fails with Config Error
- Verify `s3://${ARTIFACTS_BUCKET}/config/dev.yaml` exists
- Check config file has correct paths for bronze/silver/gold

### No Output Tables
- Check EMR logs: `s3://${ARTIFACTS_BUCKET}/emr-logs/`
- Verify IAM role has write permissions to lake bucket
- Check run audit logs for error messages

## 📊 Expected Row Counts

After successful runs, you should see:

**Silver:**
- customers_silver: ~50,000 rows
- orders_silver: ~100,000 rows
- products_silver: ~10,000 rows
- customer_behavior_silver: ~50,000 rows
- fx_rates_silver: ~730+ rows (one per day)
- order_events_silver: ~100,000 rows

**Gold:**
- fact_orders: ~100,000 rows
- dim_customer: ~50,000 rows
- dim_product: ~10,000 rows
- dim_date: ~730 rows
- customer_360: ~50,000 rows
- product_performance: ~10,000 rows

## 🎉 Success Criteria

Phase 4 is complete when:
1. ✅ Both jobs run successfully (state = SUCCESS)
2. ✅ Silver tables exist in S3 with expected row counts
3. ✅ Gold tables exist in S3 with expected row counts
4. ✅ Run audit logs show successful runs
5. ✅ You can query Gold tables in Athena/Spark

