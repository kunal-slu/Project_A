# AWS ETL Pipeline Alignment Summary

This document summarizes the work done to align the AWS ETL pipeline with the local ETL pipeline, ensuring both produce identical Silver and Gold tables.

## ✅ Completed Tasks

### 1. Created AWS Configuration File

**File**: `aws/config/dev.yaml`

- Mirrors `local/config/local.yaml` structure
- Uses S3 paths: `s3://my-etl-lake-demo-424570854632/{bronze|silver|gold}`
- **Key Fix**: Uses `snowflakes` (plural) in `base_path` to match actual S3 structure
- Table names match local config exactly
- Delta Lake enabled for AWS environment

### 2. Created AWS Validation Script

**File**: `tools/validate_aws_etl.py`

- Mirrors `tools/validate_local_etl.py` functionality
- Reads from S3 using Delta format (with Parquet fallback)
- Performs same 12 validation checks as local:
  - Silver layer: 6 tables, row counts, schemas, null checks
  - Gold layer: 6 tables, referential integrity, revenue validation
- Can run locally (with AWS credentials) or on EMR

### 3. Created Deployment Script

**File**: `aws/scripts/deploy_to_aws.sh`

- Builds Python wheel package
- Uploads wheel to S3 artifacts bucket
- Syncs job scripts to S3
- Syncs configs to S3
- Uploads validation script to S3

### 4. Updated Documentation

**Files Updated**:
- `README.md`: Added AWS deployment and validation instructions
- `aws/docs/AWS_VALIDATION.md`: Comprehensive validation guide

## 🔍 Key Findings

### S3 Path Mismatch (Fixed)

**Issue**: S3 uses `bronze/snowflakes/` (plural) but config used `bronze/snowflake/` (singular)

**Fix**: Updated `aws/config/dev.yaml` to use:
```yaml
sources:
  snowflake:
    base_path: "s3://my-etl-lake-demo-424570854632/bronze/snowflakes"
```

### Code Alignment

**Verified**: AWS jobs (`aws/jobs/transform/*.py`) import from the same shared library as local jobs:
- `from jobs.transform.bronze_to_silver import bronze_to_silver_complete`
- `from jobs.transform.silver_to_gold import silver_to_gold_complete`

This ensures **identical business logic** between local and AWS execution.

### Schema Consistency

**Verified**: Both local and AWS configs use identical table names:
- Silver: `customers_silver`, `orders_silver`, `products_silver`, `customer_behavior_silver`, `fx_rates_silver`, `order_events_silver`
- Gold: `fact_orders`, `dim_customer`, `dim_product`, `dim_date`, `customer_360`, `product_performance`

## 📊 Validation Results

### Expected Local Results (12/12 checks pass)
- `customers_silver`: 50,000 rows
- `orders_silver`: 100,000 rows
- `products_silver`: 10,000 rows
- `fact_orders`: 100,000 rows, revenue > $0
- All `customer_sk` and `product_sk` valid (not -1)

### Expected AWS Results (should match local)
- Same row counts
- Same schemas
- Same revenue totals
- Same referential integrity

## 🚀 Usage

### Deploy to AWS
```bash
export ARTIFACTS_BUCKET="my-etl-artifacts-demo-424570854632"
export AWS_REGION="us-east-1"
bash aws/scripts/deploy_to_aws.sh
```

### Validate AWS Pipeline
```bash
# From local machine (requires AWS credentials)
python tools/validate_aws_etl.py --config config/aws/dev.yaml

# Or from S3
python tools/validate_aws_etl.py --config s3://my-etl-artifacts-demo-424570854632/config/aws/config/dev.yaml
```

### Check S3 Data
```bash
# List silver tables
aws s3 ls s3://my-etl-lake-demo-424570854632/silver/ --recursive

# List gold tables
aws s3 ls s3://my-etl-lake-demo-424570854632/gold/ --recursive
```

## 🔧 Troubleshooting

### If validation fails:

1. **Check S3 paths match config**:
   ```bash
   aws s3 ls s3://my-etl-lake-demo-424570854632/bronze/snowflakes/
   ```

2. **Verify ETL jobs ran successfully**:
   - Check EMR job logs
   - Verify Delta tables exist in S3

3. **Compare schemas**:
   ```bash
   # Local
   python tools/validate_local_etl.py --config local/config/local.yaml > local.txt
   
   # AWS
   python tools/validate_aws_etl.py --config config/aws/dev.yaml > aws.txt
   
   # Compare
   diff local.txt aws.txt
   ```

## 📝 Next Steps

1. **Run ETL on AWS**: Submit `bronze_to_silver.py` and `silver_to_gold.py` jobs to EMR
2. **Validate**: Run `tools/validate_aws_etl.py` to confirm 12/12 checks pass
3. **Compare**: Verify AWS results match local results exactly
4. **Production**: Update `config/aws/prod.yaml` for production environment

## ✅ Verification Checklist

- [x] AWS config file created (`aws/config/dev.yaml`)
- [x] AWS validation script created (`tools/validate_aws_etl.py`)
- [x] Deployment script created (`aws/scripts/deploy_to_aws.sh`)
- [x] Documentation updated (`README.md`, `aws/docs/AWS_VALIDATION.md`)
- [x] S3 path mismatch fixed (`snowflakes` vs `snowflake`)
- [x] Code alignment verified (shared library imports)
- [x] Schema consistency verified (table names match)

## 📚 Related Documentation

- [AWS Validation Guide](AWS_VALIDATION.md)
- [Local ETL Validation Guide](../../local/docs/LOCAL_ETL_VALIDATION.md)
- [README](../README.md)

