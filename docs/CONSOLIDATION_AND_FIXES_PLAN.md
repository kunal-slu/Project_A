# Project Consolidation and Fixes Plan

## Overview
This document tracks the systematic cleanup, consolidation, and fixes for the AWS data platform project.

## 1. Terraform Fixes

### Issues Found:
- ✅ Only one `data "aws_caller_identity" "current"` (in lake_formation.tf) - GOOD
- ✅ Glue DBs defined only in glue_catalog.tf - GOOD
- ⚠️ Missing S3 buckets: `my-etl-logs-demo-424570854632` and `my-etl-code-demo-424570854632`
- ⚠️ CloudWatch log groups: Need to verify no duplicates
- ⚠️ Secrets: Need to verify no duplicates between main.tf and secrets.tf
- ⚠️ Outputs: Need to add missing bucket outputs

### Actions:
1. Add missing S3 buckets (logs, code)
2. Verify CloudWatch log groups are not duplicated
3. Verify secrets are only in secrets.tf
4. Add missing outputs
5. Run terraform fmt and validate

## 2. Schema Alignment

### FX Schema Issues:
- **Raw JSON uses**: `base_ccy`, `quote_ccy`, `rate`
- **Schema expects**: `base_currency`, `target_currency`, `exchange_rate`
- **Action**: Fix `read_fx_json` to rename columns to match schema

### Snowflake Orders:
- ✅ Schema matches CSV headers - GOOD

## 3. PySpark ETL Schema Validation

### Actions:
1. Enhance `src/project_a/utils/contracts.py` with StructType validation
2. Add schema validation in Silver writers
3. Add metrics for defaulted rows in Gold

## 4. Airflow DAGs

### Issues:
- Hard-coded EMR app ID: `00g0tm6kccmdcf09`
- Hard-coded role ARN: `arn:aws:iam::424570854632:role/project-a-dev-emr-exec`
- Hard-coded bucket: `my-etl-artifacts-demo-424570854632`

### Actions:
1. Use Airflow Variables with proper error handling
2. Add docstrings explaining dependencies
3. Verify entryPoint paths

## 5. File Consolidation

### Duplicate Files to Merge:
1. **ETL Runners**: 3 files → 1
   - `local/scripts/run_etl_local.py` (old)
   - `local/jobs/run_etl_pipeline.py` (current)
   - `local/scripts/run_local_etl_safe.py` (enhanced)
   - **Action**: Keep `run_local_etl_safe.py`, delete others

2. **Bronze to Silver**: 9 files → 2 (local + aws)
   - Keep: `local/jobs/transform/bronze_to_silver.py`
   - Keep: `aws/jobs/transform/bronze_to_silver.py`
   - Delete: Others in `jobs/`, `src/jobs/`, `src/project_a/jobs/`, etc.

3. **Silver to Gold**: 8 files → 2 (local + aws)
   - Keep: `local/jobs/transform/silver_to_gold.py`
   - Keep: `aws/jobs/transform/silver_to_gold.py`
   - Delete: Others

4. **DQ Scripts**: Consolidate into single module

## 6. Code Reuse

### Opportunities:
- Extract common ETL runner logic to shared utility
- Consolidate schema validation into single module
- Merge duplicate transformation functions

