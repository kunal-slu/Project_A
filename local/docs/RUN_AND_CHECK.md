# Run and Check Guide

## Quick Start

### 1. Regenerate Source Data (if needed)

```bash
# Regenerate all source data files with consistent, realistic data
python scripts/regenerate_source_data.py
```

This creates/updates:
- `aws/data/samples/crm/accounts.csv`, `contacts.csv`, `opportunities.csv`
- `aws/data/samples/snowflake/snowflake_customers_50000.csv`, `snowflake_orders_100000.csv`, `snowflake_products_10000.csv`
- `aws/data/samples/redshift/redshift_customer_behavior_50000.csv`
- `aws/data/samples/kafka/stream_kafka_events_100000.csv`
- `aws/data/samples/fx/fx_rates_historical_730_days.csv` and `fx_rates_historical.json`

### 2. Run Bronze → Silver ETL

```bash
# Run the Bronze to Silver transformation
python jobs/transform/bronze_to_silver.py --env local --config local/config/local.yaml
```

**Expected output:**
- ✅ Loading CRM, Snowflake, Redshift, FX, Kafka data
- ✅ Building silver tables (customers_silver, orders_silver, products_silver, etc.)
- ✅ Writing to `data/silver/` (parquet format for local)
- ✅ No delta.DefaultSource errors
- ✅ Row counts logged for each table

**Check for:**
- `✅ silver.customers: 30,153 rows` (or similar)
- `✅ silver.orders: 43,161 rows` (or similar)
- `✅ silver.fx_rates: X rows` (may be 0 if FX data is empty, but schema should be written)

### 3. Run Silver → Gold ETL

```bash
# Run the Silver to Gold transformation
python jobs/transform/silver_to_gold.py --env local --config local/config/local.yaml
```

**Expected output:**
- ✅ Reading Silver tables
- ✅ Building Gold dimensions and facts
- ✅ Writing to `data/gold/` (parquet format for local)
- ✅ Row counts for dim_customer, dim_product, fact_orders, etc.

**Check for:**
- `✅ dim_customer: 30,153 rows`
- `✅ fact_orders: 43,161 rows`
- `✅ customer_360: 30,153 rows`

### 4. Run Validation Script

```bash
# Validate the entire pipeline
python tools/validate_local_etl.py --env local --config local/config/local.yaml
```

**Expected output:**

```
================================================================================
🚀 LOCAL ETL PIPELINE VALIDATION
================================================================================
Environment: local
Config: local/config/local.yaml
Silver Root: data/silver
Gold Root: data/gold

================================================================================
🔍 VALIDATING SILVER LAYER
================================================================================
  ✓ customers_silver: 30,153 rows (parquet)
  ✓ orders_silver: 43,161 rows (parquet)
  ✓ products_silver: 10,000 rows (parquet)
  ✓ customer_behavior_silver: 31,643 rows (parquet)
  ⚠ fx_rates_silver path exists but appears empty; returning empty DF with explicit schema
  ⚠ fx_rates_silver: 0 rows (parquet)
  ✓ order_events_silver: 100,000 rows (parquet)

📊 CUSTOMERS_SILVER
================================================================================
Row Count: 30,153
Schema (14 columns):
  • customer_id: StringType (not null)
  • customer_name: StringType (nullable)
  • segment: StringType (nullable)
  ...

🔍 SILVER LAYER SANITY CHECKS
================================================================================
✓ customers_silver: 30,153 rows (PASS)
✓ orders_silver: 43,161 rows (PASS)
✓ products_silver: 10,000 rows (PASS)
✓ customers_silver.customer_id: 0% null (PASS)
✓ orders_silver.order_id: 0% null (PASS)
⚠ fx_rates_silver: 0 rows but schema present (WARN - acceptable)

Silver Checks: 5/5 passed

================================================================================
🔍 VALIDATING GOLD LAYER
================================================================================
  ✓ fact_orders: 43,161 rows (parquet)
  ✓ dim_customer: 30,153 rows (parquet)
  ✓ dim_product: 10,000 rows (parquet)
  ...

🔍 GOLD LAYER SANITY CHECKS
================================================================================
✓ fact_orders: 43,161 rows (PASS)
✓ dim_customer: 30,153 rows (PASS)
✓ dim_product: 10,000 rows (PASS)
✓ fact_orders.customer_sk: All rows have valid customer (PASS)
✓ fact_orders.product_sk: All rows have valid product (PASS)

Gold Checks: 5/5 passed

================================================================================
📋 VALIDATION SUMMARY
================================================================================
Silver Layer: 5/5 checks passed
Gold Layer: 5/5 checks passed

Overall: 10/10 checks passed
✅ All checks passed!
```

## Key Things to Verify

### ✅ No Delta Errors
- Should see: `(parquet)` in all read messages
- Should NOT see: `delta.DefaultSource`, `DATA_SOURCE_NOT_FOUND`, or `ClassNotFoundException: delta.DefaultSource`

### ✅ FX Rates Handling
- If `fx_rates_silver` is empty:
  - Should see: `⚠ fx_rates_silver: 0 rows but schema present (WARN - acceptable)`
  - Should NOT see: `UNABLE_TO_INFER_SCHEMA` errors
  - Schema should be displayed even for empty table

### ✅ Data Quality Checks
- `fact_orders.customer_sk` should NOT be 100% -1
- `fact_orders.product_sk` should be mostly valid
- Key columns should have low null percentages

## Troubleshooting

### If you see delta errors:
```bash
# Check environment is set correctly
grep -A 2 "environment:" local/config/local.yaml

# Should show: environment: local
```

### If fx_rates_silver has schema errors:
```bash
# Check if the table was written
ls -la data/silver/fx_rates_silver/

# Should see parquet files or at least a directory
```

### If validation fails:
```bash
# Run with verbose logging
python tools/validate_local_etl.py --env local --config local/config/local.yaml 2>&1 | tee validation.log

# Check the log for specific errors
```

## Quick Check Script

```bash
#!/bin/bash
# Quick validation check

echo "=== Running ETL Pipeline ==="
python jobs/transform/bronze_to_silver.py --env local --config local/config/local.yaml
echo ""

echo "=== Running Gold Transformation ==="
python jobs/transform/silver_to_gold.py --env local --config local/config/local.yaml
echo ""

echo "=== Running Validation ==="
python tools/validate_local_etl.py --env local --config local/config/local.yaml
```

Save as `scripts/run_full_pipeline.sh` and make executable:
```bash
chmod +x scripts/run_full_pipeline.sh
./scripts/run_full_pipeline.sh
```

