# Unified Data Source Setup: AWS and Local Use Same Data

## Overview

Both local and AWS execution now use the **same S3 bronze layer** as the data source. This ensures:
- ✅ Same input data = same results
- ✅ Easier debugging (local matches production)
- ✅ Single source of truth
- ✅ Production-like testing

## Setup Complete

### 1. Data Uploaded to S3

All sample data has been uploaded to:
```
s3://my-etl-lake-demo-424570854632/bronze/
├── crm/
│   ├── accounts.csv
│   ├── contacts.csv
│   └── opportunities.csv
├── snowflakes/
│   ├── snowflake_customers_50000.csv
│   ├── snowflake_orders_100000.csv
│   └── snowflake_products_10000.csv
├── redshift/
│   └── redshift_customer_behavior_50000.csv
├── kafka/
│   └── stream_kafka_events_100000.csv
└── fx/
    ├── fx_rates_historical.json
    └── fx_rates_historical_730_days.csv
```

### 2. Configurations Updated

**Local Config (`local/config/local.yaml`):**
- ✅ All paths point to S3
- ✅ Same structure as AWS config
- ✅ Uses S3 bronze layer for input

**AWS Config (`aws/config/dev.yaml`):**
- ✅ Already configured for S3
- ✅ Same paths as local

### 3. Spark S3 Configuration

Spark automatically detects S3 paths and configures:
- S3A filesystem
- AWS credentials provider chain
- Works for both local (uses `~/.aws/credentials`) and EMR (uses instance profile)

## Usage

### Local Execution

```bash
# Run with S3 data (requires AWS credentials)
python local/jobs/run_etl_pipeline.py --config local/config/local.yaml
```

**Requirements:**
- AWS credentials configured (`aws configure --profile kunal21`)
- Or environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`

### AWS Execution

```bash
# Same data, same config structure
aws emr-serverless start-job-run \
  --application-id $EMR_APP_ID \
  --execution-role-arn $EMR_ROLE_ARN \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://bucket/jobs/transform/bronze_to_silver.py",
      "entryPointArguments": ["--env", "dev", "--config", "s3://bucket/config/dev.yaml"]
    }
  }'
```

## Benefits

1. **Consistency**
   - Local and AWS use identical input data
   - Same transformations produce same results
   - Easier to validate changes

2. **Debugging**
   - Issues found locally will appear in AWS
   - No "works locally but fails in AWS" surprises
   - Same data = predictable behavior

3. **Data Management**
   - Single source of truth (S3 bronze)
   - Update data once, both environments use it
   - No need to sync local files

4. **Testing**
   - Local tests use production-like data
   - Validates S3 access patterns
   - Catches S3-specific issues early

## Updating Data

To update the sample data:

```bash
# 1. Update local files
# Edit files in aws/data/samples/

# 2. Upload to S3
./scripts/upload_sample_data_to_s3.sh

# 3. Both local and AWS will use new data
```

## Verification

Check that both configs use same paths:

```bash
# Local config
grep -A 5 "bronze_root:" local/config/local.yaml

# AWS config  
grep -A 5 "bronze_root:" aws/config/dev.yaml

# Should both show: s3://my-etl-lake-demo-424570854632/bronze
```

## Troubleshooting

### Local Can't Read from S3

**Error:** `AccessDenied` or `No credentials found`

**Fix:**
```bash
# Configure AWS credentials
aws configure --profile kunal21

# Or set environment variables
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1
```

### Slow Local Execution

If S3 reads are too slow locally, you can temporarily use local files:

```yaml
# local/config/local.yaml (temporary override)
paths:
  bronze_root: "file:///Users/kunal/IdeaProjects/Project_A/aws/data/samples"
```

But remember: this means local and AWS will use different data!

## Next Steps

1. ✅ Data uploaded to S3
2. ✅ Configs updated
3. ✅ Spark S3 configuration added
4. ⏳ Test local execution with S3 data
5. ⏳ Verify AWS execution uses same paths
6. ⏳ Document any issues found

