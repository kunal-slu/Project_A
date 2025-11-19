# Local to S3 Mapping: `aws/data/` → `s3://my-etl-lake-demo-424570854632/bronze/`

## Overview

This document maps the local sample data directory (`aws/data/`) to the S3 bronze layer structure.

## Directory Structure Comparison

### Local Structure (`aws/data/`)

```
aws/data/
└── samples/
    ├── crm/
    │   ├── accounts.csv (7.5M)
    │   ├── contacts.csv (20M)
    │   └── opportunities.csv (33M)
    ├── redshift/
    │   └── redshift_customer_behavior_50000.csv (17M)
    ├── snowflake/
    │   ├── snowflake_customers_50000.csv (14M)
    │   ├── snowflake_orders_100000.csv (14M)
    │   └── snowflake_products_10000.csv (2.4M)
    ├── fx/
    │   └── fx_rates_historical.json (3.3M)
    └── kafka/
        └── stream_kafka_events_100000.csv (39M)
```

### S3 Structure (`s3://my-etl-lake-demo-424570854632/bronze/`)

```
s3://my-etl-lake-demo-424570854632/bronze/
├── crm/                                    ❌ MISSING
│   ├── accounts.csv
│   ├── contacts.csv
│   └── opportunities.csv
├── redshift/                               ❌ MISSING
│   └── redshift_customer_behavior_50000.csv
├── snowflakes/                             ✅ EXISTS
│   ├── snowflake_customers_50000.csv
│   ├── snowflake_orders_100000.csv
│   └── snowflake_products_10000.csv
├── fx/
│   └── json/                               ✅ EXISTS
│       └── fx_rates_historical.json
└── kafka/                                  ❌ MISSING
    └── stream_kafka_events_100000.csv
```

## File Mapping Table

| Local Path | S3 Path | Status | Notes |
|------------|---------|--------|-------|
| `aws/data/samples/crm/accounts.csv` | `s3://.../bronze/crm/accounts.csv` | ❌ Missing | Needs upload |
| `aws/data/samples/crm/contacts.csv` | `s3://.../bronze/crm/contacts.csv` | ❌ Missing | Needs upload |
| `aws/data/samples/crm/opportunities.csv` | `s3://.../bronze/crm/opportunities.csv` | ❌ Missing | Needs upload |
| `aws/data/samples/redshift/redshift_customer_behavior_50000.csv` | `s3://.../bronze/redshift/redshift_customer_behavior_50000.csv` | ❌ Missing | Needs upload |
| `aws/data/samples/snowflake/snowflake_customers_50000.csv` | `s3://.../bronze/snowflakes/snowflake_customers_50000.csv` | ✅ Exists | Already uploaded |
| `aws/data/samples/snowflake/snowflake_orders_100000.csv` | `s3://.../bronze/snowflakes/snowflake_orders_100000.csv` | ✅ Exists | Already uploaded |
| `aws/data/samples/snowflake/snowflake_products_10000.csv` | `s3://.../bronze/snowflakes/snowflake_products_10000.csv` | ✅ Exists | Already uploaded |
| `aws/data/samples/fx/fx_rates_historical.json` | `s3://.../bronze/fx/json/fx_rates_historical.json` | ✅ Exists | Already uploaded |
| `aws/data/samples/kafka/stream_kafka_events_100000.csv` | `s3://.../bronze/kafka/stream_kafka_events_100000.csv` | ❌ Missing | Needs upload |

## Key Differences

### 1. **Directory Naming**
- **Local**: `snowflake/` (singular)
- **S3**: `snowflakes/` (plural) ✅ Code handles this correctly

### 2. **FX File Location**
- **Local**: `fx/fx_rates_historical.json` (flat)
- **S3**: `fx/json/fx_rates_historical.json` (nested under `json/` subdirectory)

### 3. **Missing Directories**
- **S3 missing**: `crm/`, `redshift/`, `kafka/` directories don't exist yet
- These will be created automatically when files are uploaded

## Upload Commands

### Quick Upload (All Missing Files)

```bash
# Use the automated script
./scripts/upload_missing_bronze_files.sh
```

### Manual Upload (Per File)

```bash
export AWS_PROFILE=kunal21
export AWS_REGION=us-east-1
export BUCKET=my-etl-lake-demo-424570854632

# CRM files
aws s3 cp aws/data/samples/crm/accounts.csv \
  "s3://${BUCKET}/bronze/crm/accounts.csv" \
  --profile "${AWS_PROFILE}" --region "${AWS_REGION}"

aws s3 cp aws/data/samples/crm/contacts.csv \
  "s3://${BUCKET}/bronze/crm/contacts.csv" \
  --profile "${AWS_PROFILE}" --region "${AWS_REGION}"

aws s3 cp aws/data/samples/crm/opportunities.csv \
  "s3://${BUCKET}/bronze/crm/opportunities.csv" \
  --profile "${AWS_PROFILE}" --region "${AWS_REGION}"

# Redshift file
aws s3 cp aws/data/samples/redshift/redshift_customer_behavior_50000.csv \
  "s3://${BUCKET}/bronze/redshift/redshift_customer_behavior_50000.csv" \
  --profile "${AWS_PROFILE}" --region "${AWS_REGION}"

# Kafka file
aws s3 cp aws/data/samples/kafka/stream_kafka_events_100000.csv \
  "s3://${BUCKET}/bronze/kafka/stream_kafka_events_100000.csv" \
  --profile "${AWS_PROFILE}" --region "${AWS_REGION}"
```

## Config Mapping

### Local Config (`config/local.yaml`)

```yaml
sources:
  crm:
    base_path: "file:///Users/kunal/IdeaProjects/Project_A/aws/data/samples/crm"
  redshift:
    base_path: "file:///Users/kunal/IdeaProjects/Project_A/aws/data/samples/redshift"
  snowflake:
    base_path: "file:///Users/kunal/IdeaProjects/Project_A/aws/data/samples/snowflake"
  fx:
    base_path: "file:///Users/kunal/IdeaProjects/Project_A/aws/data/samples/fx"
  kafka_sim:
    base_path: "file:///Users/kunal/IdeaProjects/Project_A/aws/data/samples/kafka"
```

### AWS Config (`config/dev.yaml`)

```yaml
sources:
  crm:
    base_path: "s3://my-etl-lake-demo-424570854632/bronze/crm"
  redshift:
    base_path: "s3://my-etl-lake-demo-424570854632/bronze/redshift"
  snowflake:
    base_path: "s3://my-etl-lake-demo-424570854632/bronze/snowflakes"  # Note: plural
  fx:
    base_path: "s3://my-etl-lake-demo-424570854632/bronze/fx"
    raw_path: "s3://my-etl-lake-demo-424570854632/bronze/fx/json/"  # Note: nested
  kafka_sim:
    base_path: "s3://my-etl-lake-demo-424570854632/bronze/kafka"
```

## Summary

**Local (`aws/data/`)**: 9 files, all present
- ✅ Complete sample dataset
- ✅ Used for local development/testing

**S3 (`bronze/`)**: 4 files exist, 5 missing
- ✅ Snowflake files (3)
- ✅ FX JSON file (1)
- ❌ CRM files (3) - **NEEDS UPLOAD**
- ❌ Redshift file (1) - **NEEDS UPLOAD**
- ❌ Kafka file (1) - **NEEDS UPLOAD**

**Action Required**: Upload 5 missing files to achieve parity.

