# P0-P6 Implementation Reference Guide

## Quick Reference: What Exists vs What's Needed

This guide maps each P0-P6 requirement to existing code and gaps.

---

## P0: Safety, Traceability, Incremental

### ✅ Incremental Ingestion with Watermarks
**Status:** IMPLEMENTED  
**Location:** 
- `src/pyspark_interview_project/utils/watermark_utils.py`
- `src/pyspark_interview_project/utils/state_store.py`
- `src/pyspark_interview_project/extract/snowflake_orders.py` (lines 44-111)

**Usage Example:**
```python
# In snowflake_orders.py (already implemented)
watermark = get_watermark("snowflake_orders", config, spark)
if watermark:
    since_ts = watermark
    # Filter query by watermark

# After extraction
latest_ts = get_latest_timestamp_from_df(df, "order_date")
if latest_ts:
    upsert_watermark("snowflake_orders", latest_ts, config, spark)
```

**Gap:** Not wired into all extractors (redshift, kafka need updates)

---

### ⚠️ Run Metadata Columns Everywhere
**Status:** PARTIALLY IMPLEMENTED  
**Location:** 
- `src/pyspark_interview_project/extract/base_extractor.py` (lines 50-56)
- `src/pyspark_interview_project/extract/snowflake_orders.py` (lines 94-96)

**Current Implementation:**
```python
# BaseExtractor._add_metadata() adds:
df.withColumn("record_source", lit(self.source_name))
  .withColumn("record_table", lit(self.table_name))
  .withColumn("ingest_timestamp", current_timestamp())
```

**Missing:**
- `_batch_id` (UUID)
- `_run_date` (YYYY-MM-DD)

**Gap:** Contracts library created but not used yet

---

### ❌ Error Lanes for Bad Rows
**Status:** NOT IMPLEMENTED  
**Location:** Concept exists, needs implementation

**What's Needed:**
```python
# src/pyspark_interview_project/utils/error_lanes.py
class ErrorLaneHandler:
    def quarantine_bad_rows(self, df, validation_errors):
        # Write to s3://bucket/_errors/<layer>/<table>/dt=YYYY-MM-DD/
        # Return clean DataFrame
```

**Gap:** File doesn't exist yet

---

### ✅ Emit OpenLineage + Metrics on Every Job
**Status:** IMPLEMENTED  
**Location:** 
- `src/pyspark_interview_project/monitoring/lineage_decorator.py`
- `src/pyspark_interview_project/monitoring/metrics_collector.py`
- `src/pyspark_interview_project/extract/snowflake_orders.py` (lines 17-21, 104-105)

**Usage Example:**
```python
# Already in snowflake_orders.py
@lineage_job(
    name="extract_snowflake_orders",
    inputs=["snowflake://ORDERS"],
    outputs=["s3://bucket/bronze/snowflake/orders"]
)
def extract_snowflake_orders(...):
    # ... extraction logic ...
    
    # Emit metrics (already done)
    emit_rowcount("records_extracted", record_count, {...}, config)
    emit_duration("extraction_duration", duration_ms, {...}, config)
```

**Gap:** Some extractors missing decorator

---

## P1: Silver to Gold (Interview Expectations)

### ⚠️ Silver: Canonicalize + Dedupe + Multi-Source Join
**Status:** BASIC IMPLEMENTATION EXISTS  
**Location:** 
- `src/pyspark_interview_project/transform/bronze_to_silver.py`
- `src/pyspark_interview_project/pipeline/bronze_to_silver.py`

**What's Missing:**
- Multi-source join not fully implemented
- Need to join Snowflake + Redshift on customer_id
- Deduplication with window functions

**Gap:** Needs enhancement

---

### ⚠️ SCD Type-2 for dim_customer
**Status:** PATTERNS EXIST, NEEDS WIRING  
**Location:** 
- `src/pyspark_interview_project/common/scd2.py`
- `src/pyspark_interview_project/jobs/update_customer_dimension_scd2.py`
- `src/pyspark_interview_project/pipeline/scd2_customers.py`

**What Exists:**
- SCD2 utility functions
- Dim customer job (need to verify complete)

**Gap:** May need Delta MERGE logic completion

---

### ⚠️ Gold: Proper Star Schema
**Status:** CONCEPTUAL, NEEDS IMPLEMENTATION  
**Location:** 
- `src/pyspark_interview_project/gold_writer.py`
- `src/pyspark_interview_project/jobs/gold_writer.py`

**What's Missing:**
- dim_customer (SCD2)
- dim_product
- dim_date
- fact_sales (with surrogate keys)
- fact_behavior

**Gap:** Needs proper star schema implementation

---

## P2: Quality Gates

### ✅ Great Expectations Suites
**Status:** IMPLEMENTED  
**Location:** 
- `src/pyspark_interview_project/dq/ge_runner.py`
- `src/pyspark_interview_project/dq/great_expectations_runner.py`
- `config/dq.yaml`

**Usage:**
```python
from pyspark_interview_project.dq.ge_runner import GERunner

ge_runner = GERunner(config)
results = ge_runner.run_suite("silver.orders", df)

if results['critical_failures'] > 0:
    raise ValueError("DQ gate failed")
```

**Gap:** Not wired as hard gate in all transforms

---

## P3: Governance

### ⚠️ Glue + Lake Formation Tags
**Status:** PARTIAL  
**Location:** 
- `aws/scripts/register_glue_tables.py` (exists)
- `src/pyspark_interview_project/transform/pii_masking.py`

**What's Missing:**
- Lake Formation tagging automation
- Data filters
- Masked views

**Gap:** Needs enhancement

---

## P4: Orchestration

### ⚠️ Airflow DAGs with Task Boundaries
**Status:** EXISTS, NEEDS COMPLETION  
**Location:** 
- `aws/dags/` directory
- Existing DAGs

**What's Missing:**
- Complete daily_pipeline_dag.py
- SLA configuration
- Dataset dependencies

**Gap:** Need to wire all extractors

---

### ❌ Backfill and Replay Scripts
**Status:** NOT IMPLEMENTED  
**Location:** Doesn't exist yet

**What's Needed:**
```bash
# scripts/maintenance/backfill_range.py
python backfill_range.py --table orders --start 2025-10-01 --end 2025-10-31
```

**Gap:** File doesn't exist yet

---

## P5: Observability

### ✅ CloudWatch Metrics + Alarms
**Status:** IMPLEMENTED  
**Location:** 
- `src/pyspark_interview_project/monitoring/metrics_collector.py`
- `src/pyspark_interview_project/utils/metrics.py`

**Gap:** Alarms not created yet (Terraform needed)

---

### ✅ Structured Logging with Trace IDs
**Status:** IMPLEMENTED  
**Location:** 
- `src/pyspark_interview_project/utils/logging.py`

**Usage:**
```python
from pyspark_interview_project.utils.logging import setup_json_logging

logger = setup_json_logging(level="INFO", include_trace_id=True)
logger.info("Job started")
```

**Gap:** May need wiring in all jobs

---

### ❌ Runbooks
**Status:** NOT COMPLETE  
**Location:** 
- `RUNBOOK_AWS_2025.md` exists
- Missing specific runbooks

**What's Needed:**
- `runbooks/RUNBOOK_DQ_FAILOVER.md`
- `runbooks/RUNBOOK_STREAMING_RESTART.md`
- `runbooks/RUNBOOK_BACKFILL.md`

---

## P6: Cost & Performance

### ⚠️ Optimization Strategies
**Status:** DOCUMENTED  
**Location:** 
- `src/pyspark_interview_project/performance_optimizer.py`

**What's Missing:**
- Scripted OPTIMIZE + ZORDER
- Vacuum automation
- Cost monitoring

---

## Summary: Implementation Status

| Requirement | Status | Priority | Effort |
|-------------|--------|----------|--------|
| P0 Watermarks | ✅ Done | - | - |
| P0 Metadata | ⚠️ Partial | High | 2h |
| P0 Error Lanes | ❌ Missing | High | 3h |
| P0 Lineage/Metrics | ✅ Done | - | - |
| P1 Silver Multi-source | ⚠️ Needs work | Medium | 4h |
| P1 SCD2 | ⚠️ Partial | Medium | 3h |
| P1 Star Schema | ⚠️ Needs work | Medium | 4h |
| P2 DQ Suites | ✅ Done | - | - |
| P3 Governance | ⚠️ Partial | Low | 3h |
| P4 Airflow DAGs | ⚠️ Needs work | Medium | 4h |
| P4 Backfill | ❌ Missing | Low | 3h |
| P5 Observability | ✅ Mostly done | - | - |
| P5 Runbooks | ❌ Missing | Low | 4h |
| P6 Optimization | ⚠️ Partial | Low | 3h |

**Total Estimated Effort:** 32 hours

---

## Quick Wins (Do First)

1. **Add `_batch_id` and `_run_date` to metadata** (1h)
2. **Create error lanes handler** (3h)
3. **Wire contracts into snowflake_to_bronze.py** (2h)
4. **Create backfill script** (3h)
5. **Complete Airflow DAG** (4h)

**Total:** 13 hours for 80% completion

---

## Usage Instructions

### For Interviews
**Show existing documentation:**
1. README.md
2. BEGINNERS_AWS_DEPLOYMENT_GUIDE.md
3. DATA_SOURCES_AND_ARCHITECTURE.md
4. P0_P6_IMPLEMENTATION_PLAN.md

**Explain what exists:**
- "We have watermarks, lineage, metrics"
- "Here's the architecture"
- "Here's how to deploy"

### For Production
**Follow this guide:**
1. Complete P0 metadata columns
2. Implement error lanes
3. Wire contracts
4. Add backfill scripts
5. Complete Airflow DAG

---

## Getting Help

- **What exists?** Read this guide
- **How to use?** Check code examples above
- **What's missing?** See gaps column
- **Priority?** See effort/priority table

Good luck! 🚀

