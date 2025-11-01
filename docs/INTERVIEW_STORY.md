# Interview Story: Enterprise Data Platform

## The Narrative

"We built a production-grade data platform that ingests from 5 upstream sources, processes through a lakehouse architecture (bronze/silver/gold), enforces data quality at every layer, tracks lineage end-to-end, and serves multiple consumption patterns (S3 for Spark, Snowflake for BI, Redshift for reporting)."

## Architecture Flow

```
5 Sources → Raw Zone → Bronze → Silver → Gold → Multiple Warehouses
    ↓          ↓         ↓        ↓       ↓            ↓
Snowflake    S3 raw   Normalized Clean   Analytics   Snowflake
Redshift               Delta      Delta   Delta       Redshift
CRM (HubSpot)          Partitioned PK/UK  Joined      S3/Athena
Kafka (Stream)        ingest_ts  DQ pass Aggregated  Glue Catalog
FX Rates (REST)       Schema     Types   Business    (for Athena)
                      validated  fixed   metrics
```

## Key Components

### 1. Ingestion (5 Sources → Raw Zone)

**Snowflake DW**:
- Extract: `jobs/ingest/ingest_snowflake_to_s3.py`
- Pattern: Full load daily
- Landing: `s3://.../raw/snowflake/orders/load_dt=YYYY-MM-DD/`

**Redshift Analytics**:
- Extract: `jobs/ingest/ingest_redshift_to_s3.py`
- Pattern: Append-only (immutable events)
- Landing: `s3://.../raw/redshift/behavior/load_dt=YYYY-MM-DD/`

**CRM (HubSpot/Salesforce)**:
- Extract: `jobs/ingest/ingest_crm_to_s3.py`
- Pattern: Incremental by `last_modified_date` (watermark)
- Landing: `s3://.../raw/crm/accounts/load_dt=YYYY-MM-DD/`

**Kafka (Streaming)**:
- Extract: `jobs/ingest/ingest_kafka_to_s3.py`
- Pattern: Stream replayable (checkpointed, idempotent)
- Landing: `s3://.../raw/kafka/orders/load_dt=YYYY-MM-DD/`

**FX Rates (REST API)**:
- Extract: `jobs/ingest/ingest_fx_rates_to_s3.py`
- Pattern: Daily REST pull, upsert by (date, currency)
- Landing: `s3://.../raw/fx_rates/load_dt=YYYY-MM-DD/`

### 2. Transformation (Raw → Bronze → Silver → Gold)

**Raw → Bronze** (`jobs/transform/raw_to_bronze.py`):
- Reads from `raw/` zone
- Normalizes to canonical schema
- Validates against schema contracts
- Writes to `bronze/` as Delta (partitioned by `_proc_date`)

**Bronze → Silver** (`jobs/transform/bronze_to_silver.py`):
- Data cleaning (trim, null handling)
- Type conversions
- PK/UK enforcement
- DQ checks (null %, referential integrity)
- Writes to `silver/` as Delta

**Silver → Gold** (`jobs/transform/silver_to_gold.py`):
- Joins across domains (customer + account + orders + events)
- Aggregations (customer_360, orders_daily)
- Business metrics
- PII masking
- Writes to `gold/` as Delta

### 3. Data Quality (Multi-Layer)

**Bronze DQ**:
- Schema validation (columns exist, types match)
- File presence check
- Row count thresholds
- Action: Fail fast

**Silver DQ**:
- Business rules (value in set, timestamp recent)
- Referential integrity (customer_id exists in CRM)
- Regex patterns (session_id format)
- Action: Quarantine bad rows

**Gold DQ**:
- PK uniqueness
- FK existence
- Business rules (no negative amounts)
- Action: Fail pipeline

**DQ Breaker**:
- Great Expectations runner in every DAG step
- Critical violations → ShortCircuitOperator → Stop pipeline
- Results → S3 + CloudWatch metrics

### 4. Lineage (End-to-End Tracking)

**Dataset-Level** (all jobs):
- Emitted via OpenLineage decorator
- Tracks: `bronze.customer_behavior → silver.fact_events → gold.fact_customer_360`

**Column-Level** (showcase):
- `gold.fact_customer_360.customer_id` ← `silver.dim_customer.customer_id`
- `gold.fact_customer_360.lifetime_value_usd` ← `silver.fact_orders.order_amount`

**Emission Points**:
- Every job entry point wraps with `@lineage_job` decorator
- Contract violations → lineage event with `status=FAILED_SCHEMA_MISMATCH`

### 5. Storage & Serving

**S3 Lakehouse** (primary):
- Bronze, Silver, Gold all in S3 as Delta
- Queryable via Athena (Glue Catalog)
- Cost-effective for large-scale analytics

**Snowflake** (BI teams):
- `ANALYTICS.FACT_CUSTOMER_360` (synced from Gold)
- MERGE on `customer_id` (upsert)
- PII masked

**Redshift** (reporting):
- `redshift_analytics.gold_customer_360` (synced from Gold)
- COPY from S3 (IAM role-based)

**Athena/Glue** (ad-hoc):
- Direct queries on S3 Delta tables
- Glue Catalog: `silver.*`, `gold.*`

### 6. Orchestration (Airflow)

**3 Separate DAGs**:

1. **`ingest_daily_sources_dag`**:
   - Tasks: Ingest all 5 sources → raw/
   - SLA: 30 minutes
   - Retries: 3, exponential backoff

2. **`build_analytics_dag`**:
   - Tasks: raw → bronze → silver → gold → publish
   - SLA: 1 hour
   - DQ checks at each layer

3. **`dq_watchdog_dag`**:
   - Tasks: Freshness checks, volume checks, GE suites
   - Schedule: Hourly
   - Alerts on SLA breaches

**Branching Logic**:
- File count check → skip if no files
- CDC vs full load branching
- Backfill support

### 7. Security & Governance

**Secrets Management**:
- Snowflake, HubSpot credentials in AWS Secrets Manager
- IAM roles with `secretsmanager:GetSecretValue`
- No hardcoded passwords

**Lake Formation**:
- Database: `bronze`, `silver`, `gold`
- Roles:
  - `data_engineer_role` → all layers
  - `data_analyst_role` → silver, gold
  - `ml_role` → gold only

**PII Masking**:
- Applied in Silver → Gold pipeline
- Email, phone, name masked
- Hashing for IDs

### 8. Observability

**Metrics**:
- Row counts (in/out per job)
- Duration (per stage)
- DQ status (pass/fail/quarantine)
- Cost estimates (EMR Serverless)

**Run Summaries**:
- Written to `s3://.../_runs/dt=YYYY-MM-DD/run_summary.json`
- Includes: counts, durations, DQ failures

**CloudWatch Dashboards**:
- Pipeline health
- Cost tracking
- Data freshness

**Alerting**:
- DQ violations → SNS → Slack
- SLA breaches → Email
- Job failures → Slack

### 9. Schema Evolution

**Version Control**:
- `config/schema_definitions/bronze/customer_behavior_v1.schema.json`
- `config/schema_definitions/bronze/customer_behavior_v2.schema.json`
- `config/schema_definitions/bronze/customer_behavior.schema.json` (latest)

**Handling**:
- Validate against latest schema
- Quarantine unknown columns
- Log schema drift to `s3://.../_schema_drift/`

**Backfill**:
- `scripts/backfill/backfill_customer_behavior.py --from 2025-10-01 --to 2025-10-31`
- Reprocess historical data with new schema

## Interview Answers (Quick Reference)

1. **"How do you handle late-arriving data?"**
   - Incremental watermark pattern for CRM
   - Delta MERGE for idempotent upserts
   - Backfill scripts for reprocessing

2. **"How do you ensure data quality?"**
   - Multi-layer DQ (bronze/silver/gold)
   - Great Expectations runner
   - DQ breaker stops pipeline on critical violations

3. **"Where is final data stored?"**
   - S3 Gold layer (primary)
   - Snowflake `ANALYTICS.*` (for BI)
   - Redshift `redshift_analytics.*` (for reporting)
   - Glue Catalog (for Athena)

4. **"How do you track lineage?"**
   - OpenLineage emits in every job
   - Dataset-level everywhere
   - Column-level for customer_360

5. **"How do you handle schema changes?"**
   - Schema versioning in `config/schema_definitions/`
   - Quarantine unknown columns
   - Backfill scripts for reprocessing

---

**This story demonstrates**: ELT pattern, multi-layer DQ, lineage tracking, multi-warehouse serving, security, observability, and schema evolution.

