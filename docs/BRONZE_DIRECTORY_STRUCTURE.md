# Bronze Layer Directory Structure

## Overview

The Bronze layer stores raw-ish data from all sources with minimal transformation.
One directory per source system.

## Directory Structure

```
s3://my-etl-lake-demo-424570854632/bronze/
├── crm/                          # Salesforce/CRM data
│   ├── accounts.csv
│   ├── contacts.csv
│   └── opportunities.csv
│
├── snowflakes/                   # Snowflake data warehouse extracts
│   ├── snowflake_customers_50000.csv
│   ├── snowflake_orders_100000.csv
│   └── snowflake_products_10000.csv
│
├── redshift/                     # Redshift analytics data
│   └── redshift_customer_behavior_50000.csv
│
├── fx/                           # FX rates data
│   ├── json/                     # Raw JSON Lines format
│   │   └── fx_rates_historical.json
│   └── delta/                    # Normalized Delta table (after ingestion)
│       └── (Delta files)
│
└── kafka/                         # Kafka streaming events
    └── stream_kafka_events_100000.csv
```

## Source Details

### 1. CRM (`bronze/crm/`)

**Files:**
- `accounts.csv` - Account records
- `contacts.csv` - Contact records (FK: account_id → accounts.account_id)
- `opportunities.csv` - Sales opportunities (FK: account_id → accounts.account_id)

**Schema:** See `config/schema_definitions/bronze/crm_*.json`

**Partitioning:** None (small files, no partitioning needed)

### 2. Snowflake (`bronze/snowflakes/`)

**Files:**
- `snowflake_customers_50000.csv` - Customer master data
- `snowflake_orders_100000.csv` - Order transactions
  - FK: `customer_id` → customers.customer_id
  - FK: `product_id` → products.product_id
- `snowflake_products_10000.csv` - Product catalog

**Schema:** See `config/schema_definitions/bronze/snowflake_*.json`

**Partitioning:** Orders partitioned by `order_date` in Silver layer

### 3. Redshift (`bronze/redshift/`)

**Files:**
- `redshift_customer_behavior_50000.csv` - Customer behavior analytics
  - FK: `customer_id` → customers.customer_id

**Schema:** See `config/schema_definitions/bronze/redshift_behavior_bronze.json`

**Partitioning:** Partitioned by `event_date` in Silver layer

### 4. FX Rates (`bronze/fx/`)

**Structure:**
- `json/` - Raw JSON Lines format (one JSON object per line)
  - `fx_rates_historical.json` - 20,360 lines of JSON Lines
- `delta/` - Normalized Delta table (created by `fx_json_to_bronze` job)
  - Partitioned by `trade_date` and `_run_date`

**Schema:** See `config/schema_definitions/bronze/fx_rates.json`

**Format:** JSON Lines (NDJSON) - one JSON object per line
```json
{"date": "2023-01-01", "base_ccy": "USD", "quote_ccy": "EUR", "rate": 0.84059, ...}
{"date": "2023-01-01", "base_ccy": "USD", "quote_ccy": "GBP", "rate": 0.720874, ...}
```

**Validation:** ✅ All 20,360 lines are valid JSON

### 5. Kafka Events (`bronze/kafka/`)

**Files:**
- `stream_kafka_events_100000.csv` - Simulated Kafka events (CSV seed)

**Schema:** See `config/schema_definitions/bronze/kafka_events.json`

**Note:** In production, this would be streamed directly from Kafka.
The CSV file is a seed for testing.

**Fields:**
- `event_id` - Unique event identifier
- `order_id` - Order ID (if event is order-related)
- `customer_id` - Customer ID (may be in JSON metadata)
- `event_type` - Event type (page_view, purchase, etc.)
- `event_ts` - Event timestamp
- `status` - Event status
- `metadata` - JSON metadata string

## Data Flow

```
Source Systems
    ↓
Bronze Layer (Raw-ish, minimal cleaning)
    ↓
Silver Layer (Cleaned, conformed, deduplicated)
    ↓
Gold Layer (Star schema, analytics-ready)
```

## Ingestion Jobs

1. **CRM → Bronze**: Direct CSV copy (no job needed, files already in S3)
2. **Snowflake → Bronze**: Direct CSV copy (no job needed, files already in S3)
3. **Redshift → Bronze**: Direct CSV copy (no job needed, files already in S3)
4. **FX JSON → Bronze**: `fx_json_to_bronze` job
   - Reads: `bronze/fx/json/fx_rates_historical.json`
   - Writes: `bronze/fx/delta/` (Delta format)
5. **Kafka → Bronze**: Direct CSV copy (seed file) or streaming job

## Validation

Run validation script:
```bash
python scripts/validate_source_data.py --data-root aws/data/samples
```

This validates:
- ✅ Foreign key relationships
- ✅ Primary key uniqueness
- ✅ Data quality checks
- ✅ Join compatibility

## Schema Definitions

All schemas are defined in:
- `config/schema_definitions/bronze/*.json`

These schemas define:
- Column names and types
- Primary keys
- Foreign keys
- Data quality checks
- Partition columns

## Next Steps

1. ✅ Verify all source files exist in S3
2. ✅ Run FX JSON ingestion job to create Delta table
3. ✅ Validate joins using validation script
4. ✅ Run Bronze → Silver transformation
5. ✅ Run Silver → Gold transformation

