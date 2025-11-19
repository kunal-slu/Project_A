# Data Source Strategy: Real Raw Data

## Overview

**Single Source of Truth:** S3 Bronze Layer
- All raw data lives in `s3://my-etl-lake-demo-424570854632/bronze/`
- Both local and AWS execution read from the same S3 paths
- Local `aws/data/samples/` is only for reference/upload purposes

## Data Architecture

### Source of Truth: S3 Bronze Layer

```
s3://my-etl-lake-demo-424570854632/bronze/
├── crm/
│   ├── accounts.csv          (7.5 MB - 50K accounts)
│   ├── contacts.csv           (20 MB - 200K contacts)
│   └── opportunities.csv     (33 MB - 300K opportunities)
│
├── snowflakes/
│   ├── snowflake_customers_50000.csv    (14 MB)
│   ├── snowflake_orders_100000.csv      (15 MB)
│   └── snowflake_products_10000.csv      (2.5 MB)
│
├── redshift/
│   └── redshift_customer_behavior_50000.csv  (17 MB)
│
├── kafka/
│   └── stream_kafka_events_100000.csv   (39 MB)
│
└── fx/
    └── fx_rates_historical.json          (3.3 MB - 730 days)
```

**Total:** ~150 MB of real production-like data

### Local Samples (Reference Only)

```
aws/data/samples/
├── crm/          # Source files for upload
├── snowflake/    # Source files for upload
├── redshift/     # Source files for upload
├── kafka/        # Source files for upload
└── fx/           # Source files for upload
```

**Purpose:** 
- Reference for data structure
- Source for uploading to S3
- NOT used directly by ETL (ETL reads from S3)

## Configuration

### Both Local and AWS Use Same S3 Paths

**local/config/local.yaml:**
```yaml
paths:
  bronze_root: "s3://my-etl-lake-demo-424570854632/bronze"
  silver_root: "s3://my-etl-lake-demo-424570854632/silver"
  gold_root: "s3://my-etl-lake-demo-424570854632/gold"

sources:
  crm:
    base_path: "s3://my-etl-lake-demo-424570854632/bronze/crm"
  snowflake:
    base_path: "s3://my-etl-lake-demo-424570854632/bronze/snowflakes"
  # ... same as AWS
```

**aws/config/dev.yaml:**
```yaml
paths:
  bronze_root: "s3://my-etl-lake-demo-424570854632/bronze"
  silver_root: "s3://my-etl-lake-demo-424570854632/silver"
  gold_root: "s3://my-etl-lake-demo-424570854632/gold"

sources:
  crm:
    base_path: "s3://my-etl-lake-demo-424570854632/bronze/crm"
  snowflake:
    base_path: "s3://my-etl-lake-demo-424570854632/bronze/snowflakes"
  # ... identical paths
```

## Data Quality

### Real Production-Like Data

1. **CRM Data:**
   - 50K accounts with realistic company data
   - 200K contacts with email/phone validation
   - 300K opportunities with sales pipeline data

2. **Snowflake Data:**
   - 50K customers with demographics
   - 100K orders with realistic amounts and dates
   - 10K products with categories and pricing

3. **Redshift Behavior:**
   - 50K customer behavior events
   - Login counts, page views, purchases
   - 24-month historical data

4. **Kafka Events:**
   - 100K order events
   - Real-time event stream simulation
   - Order status changes, payments, shipping

5. **FX Rates:**
   - 730 days of historical rates
   - 9 major currencies (USD, EUR, GBP, JPY, INR, CHF, CAD, AUD, CNY)
   - Bid/ask/mid rates with timestamps

## Benefits

1. **Consistency**
   - Same data = same results
   - Local matches production
   - No data sync issues

2. **Realistic Testing**
   - Production-like data volumes
   - Real data patterns and edge cases
   - Validates scalability

3. **Single Source of Truth**
   - Data lives in S3
   - Easy to update (upload once)
   - Version controlled via S3 versioning

4. **No Local Duplication**
   - No need for local bronze/silver/gold copies
   - S3 is the source
   - Local only has reference samples

## Maintenance

### Updating Data

```bash
# 1. Update local samples
# Edit files in aws/data/samples/

# 2. Upload to S3
./scripts/upload_sample_data_to_s3.sh

# 3. Both local and AWS automatically use new data
```

### Adding New Sources

1. Add sample data to `aws/data/samples/new_source/`
2. Upload to `s3://bucket/bronze/new_source/`
3. Update config files (local and AWS) with new source paths
4. ETL automatically picks up new source

## Cleanup Strategy

**Deleted:**
- ❌ `data/bronze/` - Duplicate, S3 is source
- ❌ `data/silver/` - Duplicate, S3 is source
- ❌ `data/gold/` - Duplicate, S3 is source
- ❌ `data/input_data/` - Duplicate of `aws/data/samples/`
- ❌ `data/backups/` - Not needed, S3 has versioning

**Kept:**
- ✅ `aws/data/samples/` - Reference samples for upload
- ✅ `data/checkpoints/` - Spark checkpoints (can be recreated)
- ✅ `data/_dq_results/` - DQ results (can be recreated)
- ✅ S3 bronze/silver/gold - Source of truth

## Verification

```bash
# Check S3 has all required files
aws s3 ls s3://my-etl-lake-demo-424570854632/bronze/ --recursive \
  --profile kunal21 --region us-east-1

# Verify local config uses S3 paths
grep "bronze_root" local/config/local.yaml
grep "bronze_root" aws/config/dev.yaml

# Both should show: s3://my-etl-lake-demo-424570854632/bronze
```

