# AWS Source Data: What Needs to Be Changed

## Summary

**5 files are missing from S3 bronze/** that are required for complete ETL execution. Additionally, there's a **path mismatch** for Snowflake files.

## Required Changes

### 1. ❌ Missing Files (Need to Upload)

These files exist locally but are **missing from S3**:

| Source | File | Expected S3 Path | Local Source |
|--------|------|------------------|--------------|
| **CRM** | `accounts.csv` | `s3://.../bronze/crm/accounts.csv` | `aws/data/samples/crm/accounts.csv` |
| **CRM** | `contacts.csv` | `s3://.../bronze/crm/contacts.csv` | `aws/data/samples/crm/contacts.csv` |
| **CRM** | `opportunities.csv` | `s3://.../bronze/crm/opportunities.csv` | `aws/data/samples/crm/opportunities.csv` |
| **Redshift** | `redshift_customer_behavior_50000.csv` | `s3://.../bronze/redshift/redshift_customer_behavior_50000.csv` | `aws/data/samples/redshift/redshift_customer_behavior_50000.csv` |
| **Kafka** | `stream_kafka_events_100000.csv` | `s3://.../bronze/kafka/stream_kafka_events_100000.csv` | `aws/data/samples/kafka/stream_kafka_events_100000.csv` |

### 2. ⚠️ Path Mismatch (Already Fixed in Code)

**Issue:** Config says `bronze/snowflakes/` but code handles this correctly.

- **Config (`dev.yaml`)**: `base_path: "s3://.../bronze/snowflakes"`
- **S3 Actual**: `bronze/snowflakes/` ✅ (matches)
- **Code**: Handles `snowflakes` path correctly ✅

**Status:** ✅ No change needed - code already handles this.

## Current S3 Structure

```
s3://my-etl-lake-demo-424570854632/bronze/
├── fx/
│   └── json/
│       └── fx_rates_historical.json ✅
├── snowflakes/
│   ├── snowflake_customers_50000.csv ✅
│   ├── snowflake_orders_100000.csv ✅
│   └── snowflake_products_10000.csv ✅
├── crm/ ❌ (MISSING - needs to be created)
├── redshift/ ❌ (MISSING - needs to be created)
└── kafka/ ❌ (MISSING - needs to be created)
```

## Required S3 Structure (After Fix)

```
s3://my-etl-lake-demo-424570854632/bronze/
├── fx/
│   └── json/
│       └── fx_rates_historical.json ✅
├── snowflakes/
│   ├── snowflake_customers_50000.csv ✅
│   ├── snowflake_orders_100000.csv ✅
│   └── snowflake_products_10000.csv ✅
├── crm/
│   ├── accounts.csv ✅ (NEEDS UPLOAD)
│   ├── contacts.csv ✅ (NEEDS UPLOAD)
│   └── opportunities.csv ✅ (NEEDS UPLOAD)
├── redshift/
│   └── redshift_customer_behavior_50000.csv ✅ (NEEDS UPLOAD)
└── kafka/
    └── stream_kafka_events_100000.csv ✅ (NEEDS UPLOAD)
```

## How to Fix

### Option 1: Use Upload Script (Recommended)

```bash
# Run the automated upload script
./scripts/upload_missing_bronze_files.sh
```

This script will:
1. ✅ Create missing directories (`crm/`, `redshift/`, `kafka/`)
2. ✅ Upload all 5 missing files
3. ✅ Verify uploads succeeded

### Option 2: Manual Upload

```bash
export AWS_PROFILE=kunal21
export AWS_REGION=us-east-1
export BUCKET=my-etl-lake-demo-424570854632

# Upload CRM files
aws s3 cp aws/data/samples/crm/accounts.csv \
  "s3://${BUCKET}/bronze/crm/accounts.csv" \
  --profile "${AWS_PROFILE}" --region "${AWS_REGION}"

aws s3 cp aws/data/samples/crm/contacts.csv \
  "s3://${BUCKET}/bronze/crm/contacts.csv" \
  --profile "${AWS_PROFILE}" --region "${AWS_REGION}"

aws s3 cp aws/data/samples/crm/opportunities.csv \
  "s3://${BUCKET}/bronze/crm/opportunities.csv" \
  --profile "${AWS_PROFILE}" --region "${AWS_REGION}"

# Upload Redshift file
aws s3 cp aws/data/samples/redshift/redshift_customer_behavior_50000.csv \
  "s3://${BUCKET}/bronze/redshift/redshift_customer_behavior_50000.csv" \
  --profile "${AWS_PROFILE}" --region "${AWS_REGION}"

# Upload Kafka file
aws s3 cp aws/data/samples/kafka/stream_kafka_events_100000.csv \
  "s3://${BUCKET}/bronze/kafka/stream_kafka_events_100000.csv" \
  --profile "${AWS_PROFILE}" --region "${AWS_REGION}"
```

## Verification

After uploading, verify all files exist:

```bash
# List all bronze files
aws s3 ls "s3://my-etl-lake-demo-424570854632/bronze/" \
  --recursive \
  --profile kunal21 \
  --region us-east-1

# Expected output should show 9 files:
# - 3 Snowflake files
# - 1 FX JSON file
# - 3 CRM files (NEW)
# - 1 Redshift file (NEW)
# - 1 Kafka file (NEW)
```

## Impact After Fix

### Before Fix (Current State)
- ✅ Snowflake: 3 files → ~100K orders, ~50K customers, ~10K products
- ✅ FX: 1 file → ~730 FX rate records
- ❌ CRM: 0 files → Empty DataFrames → No customer attributes
- ❌ Redshift: 0 files → Empty DataFrame → No behavior metrics
- ❌ Kafka: 0 files → Empty DataFrame → No order events

**Result:** Incomplete Silver/Gold tables with missing joins.

### After Fix (Target State)
- ✅ Snowflake: 3 files → Complete order/product data
- ✅ FX: 1 file → Complete FX rates
- ✅ CRM: 3 files → Complete customer attributes (name, email, phone, segment)
- ✅ Redshift: 1 file → Complete behavior metrics (logins, page views, purchases)
- ✅ Kafka: 1 file → Complete order events (100K events)

**Result:** Complete Silver/Gold tables with all joins working.

## Expected Row Counts After Fix

| Table | Current (Incomplete) | After Fix (Complete) |
|-------|---------------------|----------------------|
| `customers_silver` | ~50K (Snowflake only) | ~50K (Snowflake + CRM + Behavior) |
| `orders_silver` | ~100K (Snowflake only) | ~100K (Snowflake + Kafka events) |
| `products_silver` | ~10K | ~10K (no change) |
| `customer_behavior_silver` | 0 | ~50K |
| `fx_rates_silver` | ~730 | ~730 (no change) |
| `order_events_silver` | 0 | ~100K |
| `fact_orders` | ~100K (incomplete) | ~100K (complete with all joins) |
| `dim_customer` | ~50K (incomplete) | ~50K (complete with CRM attributes) |

## Code Changes Required

**✅ NONE** - The code already handles missing files gracefully and will automatically use the new files once uploaded.

The `bronze_to_silver.py` job:
1. Tries to read Delta tables first
2. Falls back to CSV if Delta doesn't exist
3. Creates empty DataFrames if neither exists (current behavior for missing files)

Once files are uploaded, the code will automatically:
- ✅ Read CRM files and populate `df_accounts`, `df_contacts`, `df_opps`
- ✅ Read Redshift file and populate `df_behavior`
- ✅ Read Kafka file and populate `df_kafka`
- ✅ Complete all joins in `build_customers_silver()` and `build_orders_silver()`

## Summary

**What needs to change:**
1. ✅ Upload 5 missing files to S3 (use script or manual commands above)
2. ✅ No code changes needed
3. ✅ No config changes needed

**After fix:**
- AWS ETL will have complete data matching local ETL
- All joins will work correctly
- Silver/Gold tables will be complete

