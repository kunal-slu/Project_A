# Interview Walkthrough: Enterprise Data Platform

## Platform Overview

This is a production-ready PySpark + AWS data platform that demonstrates end-to-end ownership of a modern data lakehouse architecture.

## Architecture Summary

### Data Flow

```
Sources (5) → Landing (S3) → Bronze (Delta) → Silver (Delta) → Gold (Delta) → Warehouses (Snowflake/Redshift)
```

### 1. Sources: 5 Upstream Systems

1. **CRM** (Salesforce/HubSpot)
   - Accounts, Contacts, Opportunities
   - Pattern: Incremental by `updated_at`

2. **Snowflake DW** (Warehouse extracts)
   - Orders, Customers, Products
   - Pattern: Batch replace (full partition)

3. **Redshift Analytics** (Event stream)
   - Customer behavior events
   - Pattern: Append-only (immutable events)

4. **FX Vendor** (Daily rates)
   - Currency exchange rates
   - Pattern: Daily upsert (date, currency key)

5. **Kafka** (Real-time streaming)
   - Order events, user actions
   - Pattern: Stream replayable (checkpointed, idempotent)

### 2. Landing: S3 Bronze Layer

**Location**: `s3://my-etl-lake-demo/bronze/<source>/<table>/_proc_date=<YYYY-MM-DD>/`

**Characteristics**:
- Raw data as-is from sources
- Schema-on-write validation (quarantine unknown columns)
- Watermark state for incremental loads
- Partitioned by `_proc_date` for time-travel

### 3. Warehouse: Redshift Serverless

**Purpose**: Near-real-time analytics for behavior events

**Implementation**:
- COPY from S3 (IAM role-based access)
- Append-only tables for events
- Queryable via Redshift SQL

### 4. Transform: PySpark on EMR Serverless

**Bronze → Silver**:
- Data cleaning, deduplication
- Type conversions, null handling
- Business rule validation
- Quarantine bad rows

**Silver → Gold**:
- Join all sources (customer_360)
- Aggregations (product_perf_daily)
- Fact/dimension modeling
- SCD2 dimensions

**Storage**: Delta Lake (ACID, time travel, schema evolution)

### 5. Serve: Multi-Warehouse Publishing

**Snowflake**: `ANALYTICS.CUSTOMER_360`
- MERGE on `customer_id` (upsert)
- Used by analysts for dashboards

**Athena/Glue**: Direct queries on S3/Delta
- Ad-hoc analysis
- Cost-effective for large scans

**Redshift**: Behavior analytics
- Real-time querying of event streams

### 6. Govern: Lake Formation + PII Masking

**Lake Formation**:
- Database: `bronze`, `silver`, `gold`
- Roles:
  - `data_engineer_role` → all layers
  - `data_analyst_role` → silver, gold
  - `ml_role` → gold + features

**PII Masking**:
- Email: `user@example.com` → `u***@e***.com`
- Phone: `123-456-7890` → `***-***-7890`
- Name: `John Doe` → `J*** D***`
- Applied in Silver → Gold pipeline

**KMS**: Encryption at rest (S3 bucket, Delta files)

### 7. Observe: CloudWatch + OpenLineage

**Metrics**:
- Row counts (in/out per job)
- Duration (per stage)
- DQ status (pass/fail/quarantine)
- Cost estimates (EMR Serverless)

**Lineage**:
- Dataset-level: bronze → silver → gold
- Column-level: customer_360 (showcase)
- Emitted to OpenLineage/Marquez

**DQ Watchdog**:
- Independent DAG runs hourly
- Scans latest partitions for freshness
- Alerts on SLA breaches

---

## Interview Q&A (15 Common Questions)

### 1. How do you handle late-arriving CRM records?

**Answer**: 
- CRM uses **incremental watermark** pattern
- Watermark stored in `s3://.../_state/crm_contacts.json`
- Each run only processes `updated_at > watermark`
- Late records will be picked up in next run (or manual backfill)
- Silver layer uses **Delta MERGE** to handle updates/inserts idempotently

### 2. How do you guarantee idempotency if the source replays the same file?

**Answer**:
- **Bronze**: Write to `_proc_date={date}/run_id={run_id}/`, then append to main table
- **Silver**: Delta MERGE on primary key (e.g., `customer_id`)
- **Gold**: MERGE on `customer_id` when publishing to Snowflake
- **Streaming**: Dedupe by `(event_id, ingestion_timestamp)` before write

### 3. How do you detect schema drift from Snowflake files?

**Answer**:
- **Schema-on-write mode** (configurable)
- Validate against `config/schema_definitions/*.json`
- Unknown columns → quarantine to `s3://.../bronze/_unknown_cols/`
- Log drift to `s3://.../meta/schema_drift/`
- Option: `schema_on_read` for flexible ingestion (validate downstream)

### 4. How do you onboard a 6th source without redeploying everything?

**Answer**:
- **Extract pattern**: New file in `src/pyspark_interview_project/extract/<source>_<table>.py`
- **Config-driven**: Add to `config/lineage.yaml` dataset registry
- **Schema contract**: Add `config/schema_definitions/<source>_<table>_bronze.json`
- **DQ rules**: Add to `config/dq.yaml` (bronze layer checks)
- **Ingestion DAG**: Add task to `airflow/dags/ingest_daily_sources_dag.py`
- **No code changes** needed in transforms/publishers (they read by dataset name)

### 5. How do you keep Redshift and S3/Delta in sync?

**Answer**:
- **Redshift**: COPY from S3 bronze (append-only, near-real-time)
- **Snowflake**: MERGE from Gold Delta tables (daily batch)
- **Athena**: Direct query on S3 Delta (always in sync, no replication)
- **Sync strategy**:
  - Events → Redshift (streaming)
  - Aggregations → Snowflake (batch)
  - No bidirectional sync needed (S3 is source of truth)

### 6. How do you let analysts use Snowflake but keep storage in S3?

**Answer**:
- **Pattern**: External table or COPY (depending on Snowflake tier)
- **Option A**: External table on S3 (query S3 directly)
- **Option B**: COPY Gold tables to Snowflake (materialized, faster queries)
- **Cost**: S3 storage ($0.023/GB) vs Snowflake storage ($40/TB)
- **Trade-off**: Query performance vs cost (we COPY for high-traffic tables)

### 7. How do you mask PII for offshore teams?

**Answer**:
- **PII config**: `config/pii_masking.yaml` defines rules
- **Applied in**: Silver → Gold pipeline (`jobs/apply_data_masking.py`)
- **Methods**:
  - Email: `mask_email()` → `u***@e***.com`
  - Phone: `mask_phone()` → `***-***-7890`
  - Hash: `sha256(salt + value)` for IDs
- **Lake Formation**: Column-level permissions (offshore role sees masked columns)

### 8. How do you debug a failed COPY in Redshift?

**Answer**:
- **Check IAM role**: Redshift → S3 access (attach `RedshiftS3ReadRole`)
- **Check manifest**: Use manifest file for multi-file loads
- **Check errors**: `stl_load_errors` table for row-level issues
- **Check permissions**: S3 bucket policy allows `s3:GetObject`
- **Our setup**: IAM role attached to namespace (not SQL ALTER NAMESPACE)

### 9. How do you handle streaming data replay?

**Answer**:
- **Checkpointing**: `s3://.../_checkpoints/kafka/orders/` (Spark Structured Streaming)
- **Deduplication**: By `(event_id, ingestion_timestamp)`
- **Idempotent writes**: Delta MERGE or append with dedupe
- **Replay**: Reset checkpoint, reprocess from beginning

### 10. How do you ensure data freshness SLAs?

**Answer**:
- **DQ Watchdog DAG**: Runs hourly, checks latest partition date
- **Thresholds**: Bronze < 1hr, Silver < 2hr, Gold < 4hr
- **Alerting**: CloudWatch → SNS → Slack/Email
- **Airflow SLAs**: `sla=timedelta(minutes=30)` on ingestion DAG

### 11. How do you handle schema evolution in Delta?

**Answer**:
- **Delta `mergeSchema`**: Automatically adds new columns (null for old data)
- **Schema registry**: `config/schema_definitions/*.json` versioned
- **Backward compatibility**: New columns nullable by default
- **Breaking changes**: Manual migration (add new table version)

### 12. How do you optimize costs?

**Answer**:
- **EMR Serverless**: Spot instances for non-prod (`--spot`)
- **S3 Lifecycle**: Bronze → IA after 30 days, Glacier after 90
- **Delta OPTIMIZE/VACUUM**: Scheduled in `maintenance_dag.py`
- **Partition pruning**: Partition by `_proc_date`, `date`
- **Z-order**: For time-range queries on large tables

### 13. How do you test data pipelines?

**Answer**:
- **Unit tests**: Mock SparkSession, test transform logic
- **Integration tests**: Sample CSV → Bronze → Silver → Gold
- **Contract tests**: `tests/test_silver_behavior_contract.py` (assert schema, types)
- **DQ tests**: Run GE suites in CI (`aws/scripts/run_ge_checks.py`)

### 14. How do you handle disaster recovery?

**Answer**:
- **S3 Cross-region replication**: Bronze/Silver/Gold → DR region
- **Redshift snapshots**: Automated daily snapshots
- **Backfill tooling**: `aws/scripts/backfill_bronze_for_date.sh`
- **State recovery**: Watermarks stored in S3 (replayable)
- **Checkpoints**: Streaming checkpoints in S3 (replayable)

### 15. How do you scale to 100x data volume?

**Answer**:
- **Partitioning**: Increase partition granularity (hourly vs daily)
- **Delta OPTIMIZE**: More frequent (reduce small files)
- **Broadcast joins**: For small dimension tables
- **EMR Serverless**: Auto-scaling (adjust `maxCapacity`)
- **Athena/Glue**: Serverless querying (no cluster management)

---

## Key Differentiators

1. **Designed, not accumulated**: Ingestion strategy doc shows intentional architecture
2. **Multi-layer DQ**: Bronze (source), Silver (business), Gold (contract)
3. **Observability**: Metrics, lineage, DQ watchdog (not just logging)
4. **Security**: Lake Formation, PII masking, KMS (production-ready)
5. **Cost-aware**: Lifecycle policies, spot instances, optimization
6. **Testing**: Data contract tests, config validation, integration tests
7. **Documentation**: Interview walkthrough, runbooks, architecture docs

---

## Next Steps for Interview

1. **Demo**: Walk through `docs/INGESTION_STRATEGY.md` (shows design)
2. **Code**: Show `jobs/silver_build_customer_360.py` (enterprise view)
3. **Config**: Show `config/dq.yaml` multi-layer checks
4. **Lineage**: Show `config/lineage.yaml` dataset registry
5. **Security**: Show Lake Formation setup in `aws/terraform/`
6. **Ops**: Show `docs/runbooks/BACKFILL_AND_RECOVERY.md`

---

## Contact

For questions about this platform, see:
- `docs/INGESTION_STRATEGY.md` - Ingestion patterns
- `docs/RUNBOOK.md` - Operational runbook
- `docs/guides/DATA_GOVERNANCE.md` - Governance policies

