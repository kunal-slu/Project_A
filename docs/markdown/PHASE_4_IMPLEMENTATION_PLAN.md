# Phase 4 Implementation Plan - Production-Ready Data Lake

## Overview

This document outlines the implementation plan for making the data lake production-ready with:
- Config-driven architecture (single source of truth)
- All 5 bronze sources properly ingested
- Silver layer transformations
- Gold layer analytics tables
- EMR dependencies management
- Glue catalog registration

## Status

✅ **Completed:**
1. Config structure expanded in `config/dev.yaml`
2. Config loader updated with variable substitution support
3. Bronze schemas defined in `src/pyspark_interview_project/schemas/bronze_schemas.py`

🔄 **In Progress:**
- Bronze ingest jobs (5 sources)
- Silver transformation refactoring
- Gold layer implementation

⏳ **Pending:**
- EMR dependencies packaging
- Glue catalog registration

## Implementation Checklist

### 1️⃣ Config (✅ DONE)
- [x] Expand `config/dev.yaml` with `sources` section
- [x] Add `tables` section for silver/gold table names
- [x] Update config loader to support `${paths.bronze_root}` style references

### 2️⃣ Bronze Ingest Jobs
- [ ] `crm_to_bronze.py` - Read from config, use schemas, normalize
- [ ] `redshift_to_bronze.py` - Already exists, update to use config
- [ ] `snowflake_to_bronze.py` - Already exists, update to use config
- [ ] `fx_rates_to_bronze.py` - Create new job
- [ ] `kafka_orders_to_bronze.py` - Create new job

### 3️⃣ Silver Transform
- [ ] Refactor `bronze_to_silver.py` with loader functions
- [ ] Implement builder functions for each silver table
- [ ] Add joins, null handling, aggregations

### 4️⃣ Gold Layer
- [ ] Create `silver_to_gold.py`
- [ ] Implement `fact_orders`
- [ ] Implement `dim_customer` (SCD-lite)
- [ ] Implement `dim_product`
- [ ] Implement `fact_customer_24m`

### 5️⃣ EMR Dependencies
- [ ] Create `build_emr_deps.sh` script
- [ ] Build `emr_deps.zip` with pyyaml, requests, etc.
- [ ] Update all job submission scripts to use `emr_deps.zip`

### 6️⃣ Glue Catalog
- [ ] Create `register_glue_tables.py` script
- [ ] Register bronze tables
- [ ] Register silver tables
- [ ] Register gold tables

## Next Steps

1. Complete bronze ingest jobs (use existing as templates)
2. Refactor silver transformation
3. Create gold layer job
4. Package EMR dependencies
5. Register Glue tables

## Files Created/Modified

### New Files
- `src/pyspark_interview_project/schemas/bronze_schemas.py` - All bronze schemas
- `docs/markdown/PHASE_4_IMPLEMENTATION_PLAN.md` - This document

### Modified Files
- `config/dev.yaml` - Expanded with sources and tables
- `src/pyspark_interview_project/config_loader.py` - Variable substitution support

