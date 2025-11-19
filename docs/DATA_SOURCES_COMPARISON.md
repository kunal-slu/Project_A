# Data Sources Comparison: Local vs AWS ETL

## Summary

**❌ NO - The data is NOT identical between local and AWS ETL.**

While the **same file names** are used, some source files are **missing from S3 bronze/** that exist locally. This means:

- ✅ **Local ETL** can read all sample data from `aws/data/samples/`
- ⚠️ **AWS ETL** will create empty DataFrames for missing sources (CRM, Redshift, Kafka)

## File Comparison

### ✅ Files Present in Both Locations

| Source | File Name | Local Path | S3 Path | Status |
|--------|-----------|------------|---------|--------|
| Snowflake | `snowflake_customers_50000.csv` | `aws/data/samples/snowflake/` | `s3://.../bronze/snowflakes/` | ✅ Both |
| Snowflake | `snowflake_orders_100000.csv` | `aws/data/samples/snowflake/` | `s3://.../bronze/snowflakes/` | ✅ Both |
| Snowflake | `snowflake_products_10000.csv` | `aws/data/samples/snowflake/` | `s3://.../bronze/snowflakes/` | ✅ Both |
| FX | `fx_rates_historical.json` | `aws/data/samples/fx/` | `s3://.../bronze/fx/json/` | ✅ Both |

### ❌ Files Missing from S3 Bronze

| Source | File Name | Local Path | S3 Path | Impact |
|--------|-----------|------------|---------|--------|
| CRM | `accounts.csv` | `aws/data/samples/crm/` | ❌ Missing | Empty `df_accounts` |
| CRM | `contacts.csv` | `aws/data/samples/crm/` | ❌ Missing | Empty `df_contacts` |
| CRM | `opportunities.csv` | `aws/data/samples/crm/` | ❌ Missing | Empty `df_opps` |
| Redshift | `redshift_customer_behavior_50000.csv` | `aws/data/samples/redshift/` | ❌ Missing | Empty `df_behavior` |
| Kafka | `stream_kafka_events_100000.csv` | `aws/data/samples/kafka/` | ❌ Missing | Empty `df_kafka` |

## Impact on ETL Results

### Local ETL (`config/local.yaml`)
- ✅ Reads all 5 sources (CRM, Redshift, Snowflake, FX, Kafka)
- ✅ Produces complete Silver tables with all joins
- ✅ Produces complete Gold tables with all dimensions

### AWS ETL (`config/dev.yaml`)
- ✅ Reads Snowflake and FX data
- ⚠️ Creates empty DataFrames for CRM, Redshift, Kafka (graceful fallback)
- ⚠️ Silver `customers_silver` will have fewer rows (no CRM/behavior data)
- ⚠️ Silver `orders_silver` will have fewer rows (no Kafka events)
- ⚠️ Gold `dim_customer` will be incomplete (missing CRM attributes)
- ⚠️ Gold `fact_orders` will have fewer rows (missing Kafka events)

## Code Behavior

The `bronze_to_silver.py` job handles missing files gracefully:

```python
def load_crm_bronze(...):
    try:
        # Try Delta first
        df = spark.read.format("delta").load(...)
    except:
        try:
            # Try CSV
            df = spark.read.schema(CRM_SCHEMA).csv(...)
        except:
            # Create empty DataFrame with expected schema
            df = spark.createDataFrame([], CRM_SCHEMA)
```

This means:
- ✅ **No crashes** - missing files create empty DataFrames
- ⚠️ **Different results** - local vs AWS will have different row counts
- ⚠️ **Incomplete joins** - some Silver/Gold tables will be sparse

## How to Fix

### Option 1: Upload Missing Files to S3 (Recommended)

```bash
# Upload CRM files
aws s3 cp aws/data/samples/crm/accounts.csv \
  s3://my-etl-lake-demo-424570854632/bronze/crm/accounts.csv \
  --profile kunal21 --region us-east-1

aws s3 cp aws/data/samples/crm/contacts.csv \
  s3://my-etl-lake-demo-424570854632/bronze/crm/contacts.csv \
  --profile kunal21 --region us-east-1

aws s3 cp aws/data/samples/crm/opportunities.csv \
  s3://my-etl-lake-demo-424570854632/bronze/crm/opportunities.csv \
  --profile kunal21 --region us-east-1

# Upload Redshift behavior
aws s3 cp aws/data/samples/redshift/redshift_customer_behavior_50000.csv \
  s3://my-etl-lake-demo-424570854632/bronze/redshift/redshift_customer_behavior_50000.csv \
  --profile kunal21 --region us-east-1

# Upload Kafka events
aws s3 cp aws/data/samples/kafka/stream_kafka_events_100000.csv \
  s3://my-etl-lake-demo-424570854632/bronze/kafka/stream_kafka_events_100000.csv \
  --profile kunal21 --region us-east-1
```

### Option 2: Use Local Config for AWS (Not Recommended)

Modify `config/dev.yaml` to point to local sample files (only works if running EMR from your local machine with S3 access to local files, which is not typical).

## Verification

After uploading missing files, verify:

```bash
# Check S3 bronze structure
aws s3 ls s3://my-etl-lake-demo-424570854632/bronze/ --recursive \
  --profile kunal21 --region us-east-1

# Run AWS ETL and compare row counts
# Local: python scripts/run_etl_local.py
# AWS: Submit EMR job and check Silver/Gold row counts
```

## Expected Row Counts (After Fix)

| Table | Expected Rows (Local) | Expected Rows (AWS) |
|-------|----------------------|---------------------|
| `customers_silver` | ~50,000 | ~50,000 |
| `orders_silver` | ~100,000 | ~100,000 |
| `products_silver` | ~10,000 | ~10,000 |
| `customer_behavior_silver` | ~50,000 | ~50,000 |
| `fx_rates_silver` | ~730 | ~730 |
| `order_events_silver` | ~100,000 | ~100,000 |
| `fact_orders` | ~100,000 | ~100,000 |
| `dim_customer` | ~50,000 | ~50,000 |
| `dim_product` | ~10,000 | ~10,000 |

## Conclusion

**To get identical results between local and AWS ETL:**

1. ✅ Upload missing CRM, Redshift, and Kafka files to S3 bronze/
2. ✅ Re-run AWS EMR jobs (Bronze→Silver, Silver→Gold)
3. ✅ Compare row counts between local and AWS outputs
4. ✅ Verify joins are complete (no missing customer/product IDs)

