# Data Source Unification - Local & AWS

**Date:** 2025-01-17  
**Status:** ✅ Complete

## Summary

Local and AWS environments now use **identical data source files** with the **same folder structure**.

## Structure

Both environments use the same structure:

```
aws/data/samples/          (AWS source of truth)
data/samples/              (Local - synced from AWS)
├── crm/
│   ├── accounts.csv
│   ├── contacts.csv
│   └── opportunities.csv
├── snowflake/
│   ├── snowflake_customers_50000.csv
│   ├── snowflake_orders_100000.csv
│   └── snowflake_products_10000.csv
├── redshift/
│   └── redshift_customer_behavior_50000.csv
├── fx/
│   └── fx_rates_historical.json
└── kafka/
    └── stream_kafka_events_100000.csv
```

## Configuration

### Local Config (`local/config/local.yaml`)

```yaml
sources:
  crm:
    base_path: "aws/data/samples/crm"  # Points to source files
    files:
      accounts: "accounts.csv"
      contacts: "contacts.csv"
      opportunities: "opportunities.csv"
  
  snowflake:
    base_path: "aws/data/samples/snowflake"
    files:
      customers: "snowflake_customers_50000.csv"
      orders: "snowflake_orders_100000.csv"
      products: "snowflake_products_10000.csv"
  
  redshift:
    base_path: "aws/data/samples/redshift"
    files:
      behavior: "redshift_customer_behavior_50000.csv"
  
  fx:
    base_path: "aws/data/samples/fx"
    files:
      daily_rates_json: "fx_rates_historical.json"
  
  kafka_sim:
    base_path: "aws/data/samples/kafka"
    files:
      orders_seed: "stream_kafka_events_100000.csv"
```

### AWS Config (`aws/config/dev.yaml`)

```yaml
sources:
  crm:
    base_path: "s3://my-etl-lake-demo-424570854632/bronze/crm"
    files:
      accounts: "accounts.csv"
      contacts: "contacts.csv"
      opportunities: "opportunities.csv"
  
  snowflake:
    base_path: "s3://my-etl-lake-demo-424570854632/bronze/snowflake"
    files:
      customers: "snowflake_customers_50000.csv"
      orders: "snowflake_orders_100000.csv"
      products: "snowflake_products_10000.csv"
  
  redshift:
    base_path: "s3://my-etl-lake-demo-424570854632/bronze/redshift"
    files:
      behavior: "redshift_customer_behavior_50000.csv"
  
  fx:
    base_path: "s3://my-etl-lake-demo-424570854632/bronze/fx"
    files:
      daily_rates_json: "fx_rates_historical.json"
  
  kafka_sim:
    base_path: "s3://my-etl-lake-demo-424570854632/bronze/kafka"
    files:
      orders_seed: "stream_kafka_events_100000.csv"
```

## File Synchronization

### Source of Truth

**`aws/data/samples/`** is the source of truth for:
- File structure
- File names
- File organization

**`data/samples/`** is synced from `aws/data/samples/` to ensure:
- Identical files locally
- Same folder structure
- Same file names

### Sync Script

Use `scripts/sync_data_sources.sh` to sync:

```bash
./scripts/sync_data_sources.sh
```

This script:
1. Creates directory structure in both locations
2. Syncs files from `aws/data/samples/` → `data/samples/`
3. Preserves additional local files (if any)
4. Verifies file counts match

## File Verification

### Current File Counts

| Source | CRM | Snowflake | Redshift | FX | Kafka | Total |
|--------|-----|-----------|----------|----|----|-------|
| AWS | 3 | 3 | 1 | 1 | 1 | 9 |
| Local | 3 | 3 | 1 | 1 | 1 | 9 |

✅ **All counts match!**

### File List

**CRM (3 files):**
- `accounts.csv`
- `contacts.csv`
- `opportunities.csv`

**Snowflake (3 files):**
- `snowflake_customers_50000.csv`
- `snowflake_orders_100000.csv`
- `snowflake_products_10000.csv`

**Redshift (1 file):**
- `redshift_customer_behavior_50000.csv`

**FX (1 file):**
- `fx_rates_historical.json`

**Kafka (1 file):**
- `stream_kafka_events_100000.csv`

## Verification Commands

```bash
# Compare file lists
diff <(find aws/data/samples -type f | sort) <(find data/samples -type f | sort)

# Compare file counts
for dir in crm snowflake redshift fx kafka; do
    echo "$dir:"
    echo "  AWS: $(find aws/data/samples/$dir -type f | wc -l)"
    echo "  Local: $(find data/samples/$dir -type f | wc -l)"
done

# Verify file hashes match
for file in $(find aws/data/samples -type f | sort); do
    rel_path=${file#aws/data/samples/}
    local_file="data/samples/$rel_path"
    if [ -f "$local_file" ]; then
        aws_hash=$(md5 -q "$file")
        local_hash=$(md5 -q "$local_file")
        if [ "$aws_hash" = "$local_hash" ]; then
            echo "✅ $rel_path"
        else
            echo "❌ $rel_path (hash mismatch)"
        fi
    fi
done
```

## Benefits

1. ✅ **Identical Data** - Same files in both environments
2. ✅ **Consistent Results** - ETL produces same output
3. ✅ **Easy Testing** - Test locally with production data
4. ✅ **Simple Sync** - One script to keep them aligned
5. ✅ **Clear Structure** - Same organization everywhere

## Maintenance

### When Adding New Source Files

1. Add file to `aws/data/samples/{source}/`
2. Run sync script: `./scripts/sync_data_sources.sh`
3. Verify file appears in `data/samples/{source}/`
4. Update config if needed

### When Updating Existing Files

1. Update file in `aws/data/samples/{source}/`
2. Run sync script: `./scripts/sync_data_sources.sh`
3. Verify hash matches: `md5 aws/data/samples/{source}/{file} data/samples/{source}/{file}`

## Status

✅ **Local and AWS data sources are now unified!**

- Same folder structure ✅
- Same file names ✅
- Same file contents ✅
- Same organization ✅
