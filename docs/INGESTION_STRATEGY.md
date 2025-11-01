# Ingestion Strategy

## Overview

This document describes the per-source ingestion patterns implemented in the data platform. Each source has a tailored strategy based on its characteristics (update frequency, volume, schema stability, and business requirements).

## Source Ingestion Patterns

### 1. Snowflake-like CSVs → Batch Replace

**Source**: `snowflake_orders`, `snowflake_customers`, `snowflake_products`

**Pattern**: **Batch Replace** (land → overwrite bronze partition)

**Characteristics**:
- Large, bulk extracts from warehouse
- Full refresh approach
- Partitioned by `_proc_date` for historical tracking
- Overwrite behavior per partition

**Implementation**:
```
Input: s3://landing-bucket/snowflake/orders/2025-10-31/orders.csv
→ Write to: s3://my-etl-lake-demo/bronze/snowflake/orders/_proc_date=2025-10-31/
→ Mode: OVERWRITE (per partition)
```

**Why**: Snowflake extracts are typically full snapshots, and overwriting ensures data consistency without complex merge logic.

---

### 2. CRM Synthetic (Salesforce-style) → Incremental by `updated_at`

**Source**: `hubspot_contacts`, `hubspot_companies`, `crm_accounts`, `crm_contacts`

**Pattern**: **Incremental Load** (land only changed records)

**Characteristics**:
- Updates occur frequently but affect small subset
- Must preserve historical changes (SCD2)
- Uses watermark: `updated_at > last_watermark`

**Implementation**:
```
1. Read watermark from s3://my-etl-lake-demo/_state/crm_contacts.json
2. Extract only records WHERE updated_at > watermark
3. Write to: s3://my-etl-lake-demo/bronze/crm/contacts/_proc_date={execution_date}/
4. Update watermark: max(updated_at)
```

**Why**: CRM systems have high update frequency. Incremental loads reduce compute cost and latency while maintaining data freshness.

---

### 3. Redshift Behavior → Append-Only

**Source**: `redshift_customer_behavior`

**Pattern**: **Append-Only** (no updates, just new events)

**Characteristics**:
- Event stream (immutable events)
- No updates to historical records
- Time-series data partitioned by `event_date`

**Implementation**:
```
Input: s3://landing-bucket/redshift/behavior/2025-10-31/events.csv
→ Write to: s3://my-etl-lake-demo/bronze/redshift/behavior/_proc_date=2025-10-31/
→ Mode: APPEND
```

**Why**: Behavior events are append-only by nature. No need for merge/update logic, just new partitions per day.

---

### 4. FX Rates → Daily Upsert

**Source**: `fx_rates`

**Pattern**: **Daily Upsert** (1 file per day, key = `date, currency`)

**Characteristics**:
- One file per day
- Composite key: `(date, currency)`
- Updates existing rates if re-ingested

**Implementation**:
```
Input: s3://landing-bucket/fx/2025-10-31/rates.csv
→ Read existing: s3://my-etl-lake-demo/bronze/fx/rates/
→ MERGE INTO bronze.fx_rates USING new_data ON date = date AND currency = currency
→ Write merged result
```

**Why**: FX rates can be corrected retroactively. Upsert ensures latest rate for each `(date, currency)` pair.

---

### 5. Kafka Seed → Stream Replayable

**Source**: `kafka_orders_stream`, `kafka_events`

**Pattern**: **Stream Replayable** (idempotent writes, checkpointed)

**Characteristics**:
- Real-time streaming (Spark Structured Streaming)
- Idempotent writes via `(event_id, timestamp)` deduplication
- Checkpoint location: `s3://my-etl-lake-demo/_checkpoints/kafka/orders/`

**Implementation**:
```
Stream: kafka://broker:9092/orders-topic
→ Dedupe by (event_id, ingestion_timestamp)
→ Write to: s3://my-etl-lake-demo/bronze/kafka/orders/_proc_date={date}/
→ Checkpoint: s3://my-etl-lake-demo/_checkpoints/kafka/orders/
```

**Why**: Streaming data must be replayable for disaster recovery. Idempotency ensures no duplicates if reprocessed.

---

## Ingestion Configuration

### Watermark Storage

All incremental sources store watermarks in:
```
s3://my-etl-lake-demo/_state/<source_name>.json
```

Format:
```json
{
  "source": "crm_contacts",
  "watermark": "2025-10-31T19:20:00Z",
  "updated_at": "2025-10-31T19:25:00Z"
}
```

### Retry & Backoff

All ingestion tasks in Airflow use:
- `retries=3`
- `retry_delay=timedelta(minutes=5)`
- Exponential backoff for API calls

### Schema Handling

**Schema-on-Write Mode** (default):
- Validate against schema JSON at write time
- Quarantine unknown columns to `s3://.../bronze/_unknown_cols/`
- Fail fast on critical mismatches

**Schema-on-Read Mode** (optional):
- Accept any schema, validate downstream

Configure in `config/local.yaml`:
```yaml
ingestion:
  mode: schema_on_write  # or schema_on_read
  on_unknown_column: quarantine  # or drop, or fail
```

---

## Source-to-Bronze Mapping

| Source | Pattern | Partition Key | Update Strategy |
|--------|---------|---------------|-----------------|
| Snowflake | Batch Replace | `_proc_date` | Overwrite |
| CRM | Incremental | `_proc_date` | Append + Watermark |
| Redshift | Append-Only | `_proc_date` | Append |
| FX | Daily Upsert | `date, currency` | MERGE |
| Kafka | Stream Replay | `_proc_date` | Append + Dedupe |

---

## Monitoring

Each ingestion job emits metrics:
- `rows_extracted`: Count of records ingested
- `watermark_updated`: New watermark value
- `duration_ms`: Ingestion duration
- `dq_status`: Data quality status (pass/fail/quarantine)

Metrics are published to:
- CloudWatch (production)
- Local JSON logs (development)

---

## Future Enhancements

1. **CDC Support**: Add Debezium/Kafka Connect for real-time CDC from databases
2. **Multi-region Replication**: Cross-region S3 replication for disaster recovery
3. **Compression Strategy**: GZIP for CSV, Parquet/Delta for structured data
4. **Partition Optimization**: Z-order indexing for time-range queries

