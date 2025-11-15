# Data Contracts for Bronze, Silver, and Gold Layers

This document defines the **data contracts** for each layer of our medallion architecture. These contracts ensure data quality, consistency, and enable reliable downstream processing.

---

## Overview

A **data contract** defines:
- **Schema**: Column names, types, nullability
- **Constraints**: Business rules (e.g., rate > 0, currency codes valid)
- **Partitioning**: How data is organized for efficient querying
- **Primary Keys**: Uniqueness guarantees
- **Retention**: How long data is kept

---

## 🥉 Bronze Layer Contract

**Purpose**: Raw-ish data ingestion with minimal transformation. One directory per source.

### Rules

1. **Minimal Cleaning Only**
   - Preserve source data as-is
   - Only apply critical validations (non-null PKs, basic type checks)
   - Route invalid rows to error lanes (quarantine)

2. **One Directory Per Source**
   - `bronze/crm/` - Salesforce data
   - `bronze/snowflake/` - Snowflake extracts
   - `bronze/redshift/` - Redshift behavior data
   - `bronze/fx/` - FX rates (JSON/CSV)
   - `bronze/kafka/` - Streaming events

3. **Schema Enforcement**
   - Explicit schemas (no inference)
   - Contract-driven validation (see `config/schema_definitions/bronze/`)
   - Primary key columns must be non-null

4. **Partitioning**
   - Partition by ingestion date (`ingestion_date=YYYY-MM-DD`)
   - For time-series: partition by date column (e.g., `trade_date`)

5. **Metadata Columns**
   - `_ingest_ts`: Timestamp of ingestion
   - `_run_date`: Processing date
   - `_batch_id`: Run identifier (UUID)
   - `_source_system`: Source identifier
   - `_format`: File format (json, csv, parquet)

### Example: FX Rates Bronze Contract

**Schema**: `config/schema_definitions/bronze/fx_rates.json`

```json
{
  "name": "bronze_fx_rates",
  "primary_key": ["trade_date", "base_ccy", "quote_ccy"],
  "columns": {
    "trade_date": { "type": "date", "nullable": false },
    "base_ccy": { "type": "string", "nullable": false },
    "quote_ccy": { "type": "string", "nullable": false },
    "rate": { "type": "double", "nullable": false },
    "source": { "type": "string", "nullable": true }
  },
  "constraints": {
    "rate_positive": "rate > 0",
    "rate_range": "rate BETWEEN 0.0001 AND 1000",
    "currency_non_empty": "base_ccy IS NOT NULL AND quote_ccy IS NOT NULL"
  },
  "partition_by": ["trade_date", "_run_date"]
}
```

**Validation**:
- Enforce non-null PK columns
- Route rows with `rate <= 0` or `rate > 1000` to error lane
- Partition by `trade_date` for downstream pruning

---

## 🥈 Silver Layer Contract

**Purpose**: Type-normalized, conformed dimensions, no duplicates on business key.

### Rules

1. **Type Normalization**
   - All dates as `DateType` (no strings)
   - All amounts as `DoubleType` or `DecimalType`
   - Currency codes standardized (e.g., "USD", "EUR", not "usd", "euro")

2. **Conformed Dimensions**
   - Customer IDs consistent across sources
   - Product IDs standardized
   - Currency codes normalized

3. **No Duplicates on Business Key**
   - Deduplicate on business key (e.g., `(customer_id, order_date)`)
   - Use window functions to pick latest record

4. **FX Normalization**
   - All monetary values converted to USD (or base currency)
   - Store original currency + converted amount

5. **Data Quality Gates**
   - DQ checks before writing to Silver
   - Fail pipeline on critical violations

6. **Partitioning**
   - Partition by business date (e.g., `order_date`, `event_date`)
   - Or by dimension key (e.g., `country` for customers)

### Example: Orders Silver Contract

**Business Key**: `(order_id, order_date)`

**Schema**:
- `order_id` (string, not null) - PK
- `customer_id` (string, not null)
- `product_id` (string, not null)
- `order_date` (date, not null)
- `amount_orig` (double, not null) - Original currency
- `currency` (string, not null) - Original currency code
- `amount_usd` (double, not null) - USD-normalized amount
- `quantity` (int, not null)

**Constraints**:
- `amount_usd > 0`
- `currency IN ('USD', 'EUR', 'GBP', ...)`
- `order_date >= '2020-01-01'` (business rule)

**Partitioning**: `order_date`

---

## 🥇 Gold Layer Contract

**Purpose**: One fact + conformed dimensions, SCD2, ready for BI.

### Rules

1. **Star Schema**
   - **Fact Tables**: Transactional data (e.g., `fact_orders`)
   - **Dimension Tables**: Descriptive data (e.g., `dim_customer`, `dim_product`, `dim_date`)

2. **SCD2 for Dimensions**
   - Track historical changes (effective_from, effective_to)
   - `is_current` flag for latest version
   - Surrogate keys (SK) for joins

3. **One Fact Table Per Business Process**
   - `fact_orders`: Order transactions
   - `fact_events`: Customer behavior events

4. **Analytics-Ready**
   - Pre-aggregated metrics (e.g., `customer_360`, `product_performance`)
   - Denormalized for query performance

5. **Partitioning**
   - Fact tables: partition by date (e.g., `order_date`)
   - Dimensions: typically unpartitioned (small tables)

### Example: Fact Orders Contract

**Schema**:
- `order_id` (string, not null) - Business key
- `customer_sk` (long, not null) - Surrogate key to dim_customer
- `product_sk` (long, not null) - Surrogate key to dim_product
- `date_sk` (int, not null) - Surrogate key to dim_date
- `order_date` (date, not null)
- `sales_amount` (double, not null) - USD amount
- `quantity` (int, not null)
- `currency` (string, not null)

**Primary Key**: `(order_id, order_date)`

**Partitioning**: `order_date`

**Constraints**:
- `sales_amount > 0`
- `quantity > 0`
- All SKs must exist in dimension tables (referential integrity)

### Example: Dim Customer Contract (SCD2)

**Schema**:
- `customer_sk` (long, not null) - Surrogate key (PK)
- `customer_id` (string, not null) - Business key
- `customer_name` (string, nullable)
- `email` (string, nullable)
- `country` (string, nullable)
- `segment` (string, nullable)
- `effective_from` (date, not null)
- `effective_to` (date, nullable)
- `is_current` (boolean, not null)

**Constraints**:
- For each `customer_id`, exactly one row with `is_current = true`
- `effective_from <= effective_to` (or `effective_to IS NULL`)

---

## Contract Enforcement

### Implementation

1. **Schema Contracts**: Defined in `config/schema_definitions/`
2. **Validation**: Enforced in job code (e.g., `fx_json_to_bronze.py`)
3. **Error Lanes**: Invalid rows routed to `_error_lanes/` for investigation
4. **Run Audit**: All runs logged to `s3://bucket/_audit/{env}/{job_name}/{date}/`

### Monitoring

- **Row Counts**: Tracked in run audit (`rows_in`, `rows_out`)
- **DQ Metrics**: Emitted to monitoring system
- **Lineage**: Tracked via `lineage_job` decorator

---

## Summary

| Layer | Purpose | Key Rules |
|-------|---------|-----------|
| **Bronze** | Raw-ish, minimal cleaning | One source per directory, explicit schemas, error lanes |
| **Silver** | Type-normalized, conformed | No duplicates, FX normalization, DQ gates |
| **Gold** | Star schema, BI-ready | SCD2 dimensions, surrogate keys, pre-aggregated metrics |

---

## References

- Schema definitions: `config/schema_definitions/`
- Run audit logs: `s3://bucket/_audit/`
- Error lanes: `s3://bucket/_error_lanes/`
- Data quality rules: `config/dq/dq_rules.yaml`
