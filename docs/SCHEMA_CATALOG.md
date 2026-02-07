# Schema Catalog - Complete Data Location Map

## Overview

This document provides a complete map of all tables, their locations, and access methods across the data platform.

## Table Locations

### Bronze Layer (Normalized Raw Data)

| Table | Location | Format | Access Method |
|-------|----------|--------|---------------|
| `bronze.customer_behavior` | `s3://my-etl-lake-demo/bronze/customer_behavior/` | Delta | Spark, Athena, Glue |
| `bronze.crm_accounts` | `s3://my-etl-lake-demo/bronze/crm_accounts/` | Delta | Spark, Athena, Glue |
| `bronze.crm_contacts` | `s3://my-etl-lake-demo/bronze/crm_contacts/` | Delta | Spark, Athena, Glue |
| `bronze.snowflake_orders` | `s3://my-etl-lake-demo/bronze/snowflake_orders/` | Delta | Spark, Athena, Glue |

**Redshift**: None (Bronze stays in S3 for cost efficiency)

---

### Silver Layer (Business-Cleaned)

| Table | Location | Format | Access Method |
|-------|----------|--------|---------------|
| `silver.dim_customer` | `s3://my-etl-lake-demo/silver/dim_customer/` | Delta | Spark, Athena, Glue |
| `silver.dim_account` | `s3://my-etl-lake-demo/silver/dim_account/` | Delta | Spark, Athena, Glue |
| `silver.dim_product` | `s3://my-etl-lake-demo/silver/dim_product/` | Delta | Spark, Athena, Glue |
| `silver.fact_orders` | `s3://my-etl-lake-demo/silver/fact_orders/` | Delta | Spark, Athena, Glue |
| `silver.fact_events` | `s3://my-etl-lake-demo/silver/fact_events/` | Delta | Spark, Athena, Glue |
| `silver.bridge_customer_account` | `s3://my-etl-lake-demo/silver/bridge_customer_account/` | Delta | Spark, Athena, Glue |

**Redshift**: None (Silver stays in S3)

**Glue Catalog**: `silver.*` (for Athena queries)

---

### Gold Layer (Analytics-Ready)

| Table | Location | Format | Access Method |
|-------|----------|--------|---------------|
| `gold.fact_customer_360` | `s3://my-etl-lake-demo/gold/fact_customer_360/` | Delta | Spark, Athena, Glue |
| `gold.fact_orders_daily` | `s3://my-etl-lake-demo/gold/fact_orders_daily/` | Delta | Spark, Athena, Glue |
| `gold.fact_marketing_events` | `s3://my-etl-lake-demo/gold/fact_marketing_events/` | Delta | Spark, Athena, Glue |

**Snowflake**: 
- `ANALYTICS.FACT_CUSTOMER_360` (synced from S3 Gold)
- `ANALYTICS.FACT_ORDERS_DAILY` (synced from S3 Gold)

**Redshift**:
- `redshift_analytics.gold_customer_360` (synced from S3 Gold)

**Glue Catalog**: `gold.*` (for Athena queries)

---

## Data Flow

### Ingestion Flow

```
Source Systems
  ↓
S3 Raw Zone (raw/)          # 1:1 dumps, no schema
  ↓
Bronze Layer (bronze/)     # Normalized, canonical columns
  ↓
Silver Layer (silver/)     # Business-cleaned, PKs enforced
  ↓
Gold Layer (gold/)         # Analytics-ready, joined
  ↓
Warehouses
  ├── Snowflake (ANALYTICS.*)
  └── Redshift (redshift_analytics.*)
```

### Query Patterns

**Ad-hoc Analysis (Athena)**:
- Query `silver.*` or `gold.*` directly from S3 via Glue Catalog

**BI Tools**:
- Snowflake for Tableau/PowerBI
- Redshift for Quicksight

**Spark Jobs**:
- Read from any layer (bronze/silver/gold) via Spark SQL

---

## Schema Definitions

All schemas are versioned in:
- `config/schema_definitions/bronze/`
- `config/schema_definitions/silver/`
- `config/schema_definitions/gold/`

Format: JSON Schema (JSON Schema Draft 7)

---

## Partitioning Strategy

### Bronze
- Partitioned by: `_proc_date` (daily)

### Silver
- Dimensions: `_proc_date` (daily)
- Facts: Partitioned by business date (e.g., `order_date`)

### Gold
- Partitioned by: `_proc_date` or business date (daily)

---

## Access Control

**Lake Formation**:
- `bronze.*`: `data_engineer_role` only
- `silver.*`: `data_engineer_role`, `data_analyst_role`
- `gold.*`: All roles (with PII masking for offshore teams)

---

## Table Naming Conventions

### Dimensions
- Prefix: `dim_*` (e.g., `dim_customer`, `dim_account`, `dim_product`)

### Facts
- Prefix: `fact_*` (e.g., `fact_orders`, `fact_events`, `fact_customer_360`)

### Bridges
- Prefix: `bridge_*` (e.g., `bridge_customer_account`)

### Bronze
- Source-based: `{source}_{table}` (e.g., `customer_behavior`, `crm_accounts`)

---

## Maintenance

- **Delta OPTIMIZE**: Weekly for all Delta tables
- **VACUUM**: Monthly (retention: 30 days)
- **Schema Evolution**: Documented in `config/schema_definitions/*/`

---

**Last Updated**: 2025-01-XX

