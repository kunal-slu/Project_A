# 🚨 Critical Data Safety & Correctness - Implementation Guide

**Date**: 2025-10-18  
**Status**: ✅ Safety Patterns Implemented  
**Priority**: CRITICAL

---

## 📋 Executive Summary

This document outlines critical data safety improvements implemented to prevent data loss and ensure correctness in production environments.

### ✅ **Implementations Completed:**
1. ✅ Safe Delta Lake writer utilities (`utils/safe_writer.py`)
2. ✅ Fail-fast Great Expectations runner (`dq/great_expectations_runner.py`)
3. ✅ Explicit schema definitions (`schemas/production_schemas.py`)
4. ✅ Schema drift detection and validation

### 📋 **Remaining Actions:**
- Replace unsafe `.mode("overwrite")` calls with safe patterns
- Add explicit schemas to all source reads
- Replace `print()` with `logging`
- Enhance exception handling
- Remove hardcoded paths
- Add OPTIMIZE/VACUUM scheduling

---

## 🔧 1. Safe Delta Lake Writer (IMPLEMENTED)

### **Location**: `src/pyspark_interview_project/utils/safe_writer.py`

### **Key Features:**
✅ **Safe MERGE Operations** - Prevents data loss from overwrites  
✅ **Partition-Scoped Overwrites** - Uses `replaceWhere` for safety  
✅ **Row Count Validation** - Validates before/after writes  
✅ **Data Quality Metrics** - Emits DQ metrics to logs  
✅ **Fail-Fast Behavior** - Terminates on validation failures  

### **Usage Examples:**

#### **1. Safe MERGE (Recommended for Upserts)**
```python
from pyspark_interview_project.utils.safe_writer import SafeDeltaWriter

writer = SafeDeltaWriter(spark)

# Safe upsert with MERGE
metrics = writer.write_with_merge(
    df=source_df,
    target_path="data/lakehouse_delta/silver/customers",
    merge_keys=["customer_id"],
    partition_cols=["ingest_date"]
)

# Metrics returned:
# {
#   "success": True,
#   "operation": "MERGE",
#   "source_records": 1000,
#   "before_count": 5000,
#   "after_count": 5500,
#   "records_inserted": 500,
#   "records_updated": 500
# }
```

#### **2. Safe Partition Overwrite**
```python
# BEFORE (UNSAFE):
df.write.format("delta").mode("overwrite").save(target_path)  # ❌ DANGEROUS!

# AFTER (SAFE):
writer = SafeDeltaWriter(spark)
metrics = writer.write_partition_overwrite(
    df=df.filter(F.col("ingest_date") == "2025-10-18"),
    target_path="data/lakehouse_delta/silver/orders",
    partition_col="ingest_date",
    partition_value="2025-10-18",
    validate_partition=True  # Ensures source only has target partition
)
```

#### **3. Safe Append with Duplicate Checking**
```python
metrics = writer.write_append(
    df=source_df,
    target_path="data/lakehouse_delta/bronze/events",
    partition_cols=["event_date"],
    validate_duplicates=True,
    duplicate_keys=["event_id"]
)
```

---

## 🧪 2. Great Expectations Fail-Fast (IMPLEMENTED)

### **Location**: `src/pyspark_interview_project/dq/great_expectations_runner.py`

### **Key Features:**
✅ **Fail-Fast Validation** - Terminates job on DQ failures  
✅ **Compact Log Summaries** - Structured logging of results  
✅ **Data Docs Links** - Emits links to validation reports  
✅ **Version Controlled Suites** - Stores suites in git  

### **Usage Examples:**

#### **1. Run Checkpoint with Fail-Fast**
```python
from pyspark_interview_project.dq.great_expectations_runner import (
    GreatExpectationsRunner
)

runner = GreatExpectationsRunner()
runner.init_context()

# This will FAIL the job if validation fails
result = runner.run_checkpoint(
    checkpoint_name="customers_checkpoint",
    fail_on_error=True  # DEFAULT: fails job on DQ failure
)

# BEFORE (UNSAFE):
# result = checkpoint.run()
# logger.info(f"Results: {result}")  # ❌ Doesn't fail on error!

# AFTER (SAFE):
# if not result.get("success"):
#     raise RuntimeError("DQ gate failed")  # ✅ Fails fast!
```

#### **2. Validate DataFrame Directly**
```python
# Validate a DataFrame against an expectation suite
result = runner.validate_dataframe(
    df=silver_df,
    expectation_suite_name="silver_customers_suite",
    fail_on_error=True
)

# If validation fails, job terminates immediately
```

#### **3. Convenience Function**
```python
from pyspark_interview_project.dq.great_expectations_runner import (
    run_dq_checkpoint
)

# One-liner with fail-fast
run_dq_checkpoint(
    checkpoint_name="customers_checkpoint",
    fail_on_error=True
)
```

---

## 📊 3. Explicit Schemas (IMPLEMENTED)

### **Location**: `src/pyspark_interview_project/schemas/production_schemas.py`

### **Key Features:**
✅ **No Schema Inference** - All schemas explicitly defined  
✅ **Schema Drift Detection** - Validates actual vs expected  
✅ **Version Control** - Schemas stored in git  
✅ **Type Safety** - Enforces correct data types  

### **Available Schemas:**
```python
# Bronze Layer
- bronze.customers
- bronze.orders
- bronze.products

# Silver Layer
- silver.customers
- silver.orders

# Gold Layer
- gold.customer_analytics
- gold.monthly_revenue
```

### **Usage Examples:**

#### **1. Read with Explicit Schema**
```python
from pyspark_interview_project.schemas.production_schemas import get_schema

# BEFORE (UNSAFE):
df = spark.read.csv("data/input/customers.csv", header=True, inferSchema=True)  # ❌

# AFTER (SAFE):
schema = get_schema("bronze.customers")
df = spark.read.schema(schema).csv("data/input/customers.csv", header=True)  # ✅
```

#### **2. Validate DataFrame Schema**
```python
from pyspark_interview_project.schemas.production_schemas import (
    validate_dataframe_schema
)

# Validate before writing
result = validate_dataframe_schema(
    df=bronze_df,
    expected_table="bronze.customers",
    fail_on_mismatch=True  # Fails job on schema mismatch
)

# Result contains:
# {
#   "valid": True/False,
#   "issues": [{"type": "MISSING_COLUMN", "column": "email", ...}],
#   "issue_count": 0
# }
```

#### **3. Schema Drift Detection**
```python
from pyspark_interview_project.schemas.production_schemas import SchemaRegistry

registry = SchemaRegistry()

# Detect schema drift
result = registry.validate_schema(
    actual_schema=df.schema,
    expected_table="silver.customers",
    allow_additional_columns=False
)

if not result["valid"]:
    logger.error(f"Schema drift detected: {result['issues']}")
    raise RuntimeError("Breaking schema change detected")
```

---

## ✅ 4. Copy-Pasteable Safe Patterns

### **Pattern 1: Safe Partition Overwrite**
```python
from pyspark.sql import functions as F
from pyspark_interview_project.utils.safe_writer import SafeDeltaWriter

# Filter to target partition
run_date = "2025-10-18"
df_partition = df.filter(F.col("ingest_date") == F.lit(run_date))

# Safe overwrite of single partition
writer = SafeDeltaWriter(spark)
metrics = writer.write_partition_overwrite(
    df=df_partition,
    target_path=silver_path,
    partition_col="ingest_date",
    partition_value=run_date,
    validate_partition=True
)

logger.info(f"Partition overwrite metrics: {metrics}")
```

### **Pattern 2: Delta MERGE Upsert**
```python
from delta.tables import DeltaTable
from pyspark_interview_project.utils.safe_writer import SafeDeltaWriter

writer = SafeDeltaWriter(spark)

# Safe MERGE with validation
metrics = writer.write_with_merge(
    df=source_df,
    target_path=silver_path,
    merge_keys=["customer_id", "ingest_date"],
    partition_cols=["ingest_date"]
)

# Automatically handles:
# - New table creation
# - MERGE operation for existing tables
# - Row count validation
# - Metrics emission
```

### **Pattern 3: Module-Level Logging**
```python
import logging

# At top of module
logger = logging.getLogger(__name__)

# In functions
def process_data(df):
    logger.info(f"Processing {df.count()} records")
    
    try:
        result = transform(df)
        logger.info("Transform successful")
        return result
    except Exception as e:
        logger.error(f"Transform failed: {e}", exc_info=True)
        raise
```

### **Pattern 4: Fail-Fast CLI**
```python
import argparse
import sys
import logging

logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Processing date")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    
    args = parser.parse_args()
    
    try:
        if args.dry_run:
            logger.info("DRY RUN MODE - no data will be written")
            validate_only(args.date)
        else:
            run_pipeline(args.date)
        
        logger.info("Pipeline completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.exception("Pipeline failed")
        sys.exit(1)  # Non-zero exit code

if __name__ == "__main__":
    main()
```

---

## 🔧 5. Migration Guide

### **Step 1: Replace Unsafe Overwrites**

**Files to Check:**
- `src/pyspark_interview_project/ingestion_pipeline.py`
- `src/pyspark_interview_project/incremental_loading.py`
- `src/pyspark_interview_project/jobs/*.py`
- `src/pyspark_interview_project/pipeline_core.py`

**Search Pattern:**
```bash
grep -r "\.mode(\"overwrite\")" src/
```

**Replace With:**
```python
# For upserts:
from pyspark_interview_project.utils.safe_writer import SafeDeltaWriter
writer = SafeDeltaWriter(spark)
writer.write_with_merge(df, path, merge_keys=["id"])

# For partition overwrites:
writer.write_partition_overwrite(df, path, "date", "2025-10-18")
```

### **Step 2: Add Schema Validation**

**Before Every Write:**
```python
from pyspark_interview_project.schemas.production_schemas import (
    validate_dataframe_schema
)

# Validate schema before writing
validate_dataframe_schema(
    df=df,
    expected_table="bronze.customers",
    fail_on_mismatch=True
)

# Then write
writer.write_with_merge(df, path, ["customer_id"])
```

### **Step 3: Add DQ Gates**

**After Critical Transforms:**
```python
from pyspark_interview_project.dq.great_expectations_runner import (
    run_dq_checkpoint
)

# Transform data
silver_df = transform_bronze_to_silver(bronze_df)

# DQ gate (fails job if validation fails)
run_dq_checkpoint(
    checkpoint_name="silver_customers_checkpoint",
    fail_on_error=True
)

# Write if DQ passes
writer.write_with_merge(silver_df, silver_path, ["customer_id"])
```

### **Step 4: Replace Print Statements**

**Search:**
```bash
grep -r "print(" src/ --include="*.py"
```

**Replace:**
```python
# BEFORE:
print(f"Processing {count} records")

# AFTER:
import logging
logger = logging.getLogger(__name__)
logger.info(f"Processing {count} records")
```

### **Step 5: Fix Exception Handling**

**Search:**
```bash
grep -r "except:" src/ --include="*.py"
```

**Replace:**
```python
# BEFORE:
try:
    process()
except:  # ❌ Catches everything including KeyboardInterrupt
    logger.error("Failed")

# AFTER:
try:
    process()
except (ValueError, IOError) as e:  # ✅ Specific exceptions
    logger.error(f"Failed: {e}", exc_info=True)
    raise  # Re-raise to fail fast
```

---

## 🧱 6. Delta & Spark Hygiene

### **Partition Strategy**
```python
# ✅ GOOD: Partition by low-cardinality columns
df.write.partitionBy("ingest_date", "region")  # Date + region

# ❌ BAD: Partition by high-cardinality columns
df.write.partitionBy("customer_id")  # Millions of partitions!
```

### **File Size Management**
```python
# Before small writes, coalesce
if df.count() < 10000:
    df = df.coalesce(1)

# For large writes, repartition
if df.count() > 1000000:
    df = df.repartition(100, "customer_id")

df.write.format("delta").save(path)
```

### **OPTIMIZE & VACUUM Schedule**
```python
from delta.tables import DeltaTable

# Schedule OPTIMIZE daily
def optimize_table(spark, path, zorder_cols):
    spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({','.join(zorder_cols)})")
    logger.info(f"Optimized {path}")

# Schedule VACUUM weekly
def vacuum_table(spark, path, retention_hours=168):
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")
    logger.info(f"Vacuumed {path}")

# In Airflow DAG:
optimize_task = PythonOperator(
    task_id="optimize_tables",
    python_callable=optimize_table,
    op_kwargs={"path": "data/lakehouse_delta/gold/customers", "zorder_cols": ["customer_id", "region"]},
    schedule_interval="@daily"
)
```

### **Row Count Validation**
```python
# Always validate row counts
before_count = df.count()
logger.info(f"Before transform: {before_count} rows")

result_df = transform(df)

after_count = result_df.count()
logger.info(f"After transform: {after_count} rows")

if after_count < before_count * 0.5:  # Lost more than 50%
    raise RuntimeError(f"Suspicious row count drop: {before_count} → {after_count}")
```

---

## 📋 7. Implementation Checklist

### **High Priority (Do First)**
- [ ] Replace all `.mode("overwrite")` with safe patterns
- [ ] Add fail-fast DQ gates to critical transforms
- [ ] Add explicit schemas to all source reads
- [ ] Validate schemas before writes
- [ ] Replace `print()` with `logging`

### **Medium Priority (Do Next)**
- [ ] Fix bare `except:` clauses
- [ ] Remove hardcoded paths to config
- [ ] Add row count validation to all writes
- [ ] Implement deduplication by business keys

### **Low Priority (Nice to Have)**
- [ ] Add OPTIMIZE/VACUUM scheduling
- [ ] Implement partition pruning
- [ ] Add file size management
- [ ] Create data quality dashboards

---

## 🚀 8. Quick Start

### **1. Update an Existing Pipeline**
```python
# File: src/pyspark_interview_project/jobs/example_pipeline.py

import logging
from pyspark_interview_project.utils.safe_writer import SafeDeltaWriter
from pyspark_interview_project.schemas.production_schemas import (
    get_schema, validate_dataframe_schema
)
from pyspark_interview_project.dq.great_expectations_runner import (
    run_dq_checkpoint
)

logger = logging.getLogger(__name__)

def run_pipeline(spark, config):
    """Safe pipeline implementation."""
    try:
        # 1. Read with explicit schema
        schema = get_schema("bronze.customers")
        df = spark.read.schema(schema).csv(config["input_path"])
        logger.info(f"Read {df.count()} records")
        
        # 2. Transform
        transformed_df = transform_data(df)
        
        # 3. Validate schema
        validate_dataframe_schema(
            transformed_df,
            "silver.customers",
            fail_on_mismatch=True
        )
        
        # 4. DQ gate
        run_dq_checkpoint(
            "customers_checkpoint",
            fail_on_error=True
        )
        
        # 5. Safe write with MERGE
        writer = SafeDeltaWriter(spark)
        metrics = writer.write_with_merge(
            df=transformed_df,
            target_path=config["silver_path"],
            merge_keys=["customer_id"],
            partition_cols=["ingest_date"]
        )
        
        logger.info(f"Pipeline completed: {metrics}")
        return metrics
        
    except Exception as e:
        logger.exception("Pipeline failed")
        raise RuntimeError(f"Pipeline execution failed: {e}") from e
```

---

## 📖 9. Resources

### **Documentation**
- Safe Writer: `src/pyspark_interview_project/utils/safe_writer.py`
- GE Runner: `src/pyspark_interview_project/dq/great_expectations_runner.py`
- Schemas: `src/pyspark_interview_project/schemas/production_schemas.py`

### **Examples**
- Example patterns in this document
- Test files in `tests/`

### **Support**
- Review code with team before deploying
- Test thoroughly in dev environment
- Monitor row counts and DQ metrics in production

---

## ✅ Success Criteria

### **Before Deployment:**
- [ ] All `.mode("overwrite")` replaced
- [ ] All source reads use explicit schemas
- [ ] DQ gates added to critical transforms
- [ ] All `print()` replaced with `logging`
- [ ] Exception handling improved
- [ ] Hardcoded paths removed
- [ ] Row count validation added
- [ ] OPTIMIZE/VACUUM scheduled

### **After Deployment:**
- [ ] Monitor DQ metrics
- [ ] Check for schema drift alerts
- [ ] Verify row counts
- [ ] Review error logs
- [ ] Validate data correctness

---

**Last Updated**: 2025-10-18  
**Status**: Ready for Implementation  
**Priority**: CRITICAL - Implement ASAP

