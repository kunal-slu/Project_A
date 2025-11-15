# Code Quality Fixes Required for Senior-Level Standards

## Critical Issues Summary

### 1. Schema Inference (P0 - Fix Immediately)
**Impact**: Production instability, schema drift issues

**Files to Fix:**
- `jobs/ingest/ingest_crm_to_s3.py:66` - Uses `inferSchema=True`
- `jobs/redshift_to_bronze.py:54` - Uses `inferSchema=True`
- `jobs/ingest/snowflake_to_bronze.py:106,109,114` - Uses `inferSchema=True` in demo mode
- `jobs/ingest/snowflake_customers_to_bronze.py:54` - Uses `inferSchema=True`
- `jobs/ingest/ingest_snowflake_to_s3.py:64` - Uses `inferSchema=True`

**Fix Pattern:**
```python
# ❌ BEFORE
df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

# ✅ AFTER
from pyspark_interview_project.schemas.bronze_schemas import CRM_ACCOUNTS_SCHEMA
df = spark.read.schema(CRM_ACCOUNTS_SCHEMA).option("header", "true").csv(path)
```

### 2. Logging Inconsistency (P0 - Fix Immediately)
**Impact**: Poor observability, inconsistent log formats

**Files to Fix:**
- `jobs/redshift_to_bronze.py:69,105,109` - Uses `print()`
- `jobs/transform/bronze_to_silver.py:78,79,80,84` - Uses `print()`
- `jobs/gold/star_schema.py:135,198,206,209` - Uses `print()`

**Fix Pattern:**
```python
# ❌ BEFORE
print(f"✅ Job completed: {count} records")

# ✅ AFTER
logger.info(f"✅ Job completed: {count:,} records", extra={
    "records_processed": count,
    "job_name": "redshift_to_bronze"
})
```

### 3. Hardcoded Paths (P1 - High Priority)
**Impact**: Not configurable, breaks across environments

**Files to Fix:**
- `jobs/ingest/ingest_crm_to_s3.py:85` - Hardcoded `s3://my-etl-lake-demo/raw/crm/`
- `jobs/redshift_to_bronze.py:97` - Hardcoded `s3a://{lake_bucket}/bronze/redshift/behavior/`
- `jobs/gold/star_schema.py:71,72,132,192,193,197,200,204` - Multiple hardcoded paths

**Fix Pattern:**
```python
# ❌ BEFORE
bronze_path = f"s3a://{lake_bucket}/bronze/redshift/behavior/"

# ✅ AFTER
bronze_root = config["paths"]["bronze_root"]
source_cfg = config["sources"]["redshift"]
bronze_path = f"{bronze_root}/redshift/behavior/"
```

### 4. Missing Type Hints (P1 - High Priority)
**Impact**: Poor code maintainability, IDE support

**Files Needing Type Hints:**
- All job main() functions
- All transformation functions
- All utility functions

**Fix Pattern:**
```python
# ❌ BEFORE
def extract_data(spark, config, run_id):
    ...

# ✅ AFTER
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame

def extract_data(
    spark: SparkSession,
    config: Dict[str, Any],
    run_id: str
) -> DataFrame:
    """
    Extract data from source.
    
    Args:
        spark: SparkSession instance
        config: Configuration dictionary
        run_id: Unique run identifier
        
    Returns:
        DataFrame with extracted data
    """
    ...
```

### 5. Error Handling Gaps (P1 - High Priority)
**Impact**: Poor debugging, lack of context on failures

**Files Needing Better Error Handling:**
- `jobs/redshift_to_bronze.py` - Generic exception handling
- `jobs/transform/bronze_to_silver.py` - Basic try/except
- `jobs/gold/star_schema.py` - Generic exception handling

**Fix Pattern:**
```python
# ❌ BEFORE
except Exception as e:
    print(f"❌ Job failed: {e}")
    raise

# ✅ AFTER
except Exception as e:
    logger.error(
        f"Job failed: {e}",
        exc_info=True,
        extra={
            "job_name": "redshift_to_bronze",
            "run_id": run_id,
            "error_type": type(e).__name__,
            "source": "redshift"
        }
    )
    raise
```

## Implementation Priority

### Phase 1 (Immediate - This Week)
1. ✅ Replace all `inferSchema=True` with explicit schemas
2. ✅ Replace all `print()` with `logger` statements
3. ✅ Remove hardcoded paths, use config

### Phase 2 (High Priority - Next Week)
4. ✅ Add comprehensive error handling
5. ✅ Add type hints to all functions
6. ✅ Add docstrings with Args/Returns

### Phase 3 (Medium Priority - Following Week)
7. ✅ Standardize logging format across all jobs
8. ✅ Add metrics emission to all jobs
9. ✅ Improve error messages with context

## Files Created for Reference

- `src/pyspark_interview_project/schemas/bronze_schemas.py` - All bronze schemas defined
- `docs/markdown/SENIOR_LEVEL_CODE_REVIEW.md` - Detailed code review
- `docs/markdown/CODE_QUALITY_FIXES_REQUIRED.md` - This document

## Next Steps

1. Review this document
2. Prioritize fixes based on business impact
3. Implement fixes systematically
4. Add automated checks (linting, type checking) to prevent regressions

