# Code Quality Refactoring Plan: Industry-Level Standards

## Executive Summary

**Goal:** Achieve industry-level code quality with zero duplicates, no patches, and consistent local/AWS code paths.

**Current State:**
- ❌ 7 duplicate `bronze_to_silver*.py` files
- ❌ 6 duplicate `silver_to_gold*.py` files  
- ❌ 89 files with `print()` statements
- ❌ 18 files with bare `except:` blocks
- ❌ Scattered environment checks (`if local/emr`)
- ❌ Inconsistent error handling
- ❌ Hardcoded paths in code

**Target State:**
- ✅ Single canonical implementation per transformation
- ✅ Zero code duplication
- ✅ Consistent logging (no print statements)
- ✅ Proper error handling (no bare except)
- ✅ Configuration-driven (no hardcoded paths)
- ✅ Clean separation: local vs AWS handled via config

## Critical Duplicates to Remove

### 1. Bronze→Silver Transformations

**KEEP (Canonical):**
- ✅ `jobs/transform/bronze_to_silver.py` (888 lines, 15 functions)
  - Complete implementation
  - All 5 sources (CRM, Redshift, Snowflake, FX, Kafka)
  - Proper error handling
  - Lineage tracking

**DELETE (Duplicates):**
- ❌ `jobs/bronze_to_silver_behavior.py` (217 lines, 2 functions)
  - Only handles behavior, incomplete
  - Logic should be in canonical file
  
- ❌ `src/jobs/bronze_to_silver.py` (legacy)
- ❌ `src/project_a/jobs/bronze_to_silver.py` (legacy)
- ❌ `src/project_a/pyspark_interview_project/transform/bronze_to_silver.py` (legacy)
- ❌ `src/project_a/pyspark_interview_project/transform/bronze_to_silver_multi_source.py` (legacy)
- ❌ `src/project_a/pyspark_interview_project/pipeline/bronze_to_silver.py` (legacy)

### 2. Silver→Gold Transformations

**KEEP (Canonical):**
- ✅ `jobs/transform/silver_to_gold.py` (1081 lines, 9 functions)
  - Complete star schema
  - All dimensions and facts
  - Customer 360 and product performance

**CONSOLIDATE:**
- ⚠️ `jobs/silver_build_customer_360.py` (236 lines, 2 functions)
  - Should be merged into canonical file
  
- ⚠️ `jobs/silver_build_product_perf.py` (154 lines, 2 functions)
  - Should be merged into canonical file

**DELETE (Duplicates):**
- ❌ `jobs/gold/silver_to_gold.py` (543 lines, 9 functions)
  - Alternative implementation, not used
  
- ❌ `src/jobs/silver_to_gold.py` (legacy)
- ❌ `src/project_a/jobs/silver_to_gold.py` (legacy)
- ❌ `src/project_a/pyspark_interview_project/transform/silver_to_gold.py` (legacy)
- ❌ `src/project_a/pyspark_interview_project/pipeline/silver_to_gold.py` (legacy)

## Code Quality Issues

### 1. Print Statements (89 files)
**Issue:** Using `print()` instead of `logging`
**Impact:** No structured logging, harder to debug in production
**Fix:** Replace all `print()` with `logger.info/warning/error`

### 2. Bare Except Blocks (18 files)
**Issue:** `except:` catches all exceptions including system exits
**Impact:** Hides critical errors, makes debugging difficult
**Fix:** Use `except Exception as e:` with proper logging

### 3. Hardcoded Paths
**Issue:** S3 paths and local paths hardcoded in code
**Impact:** Not portable, hard to test
**Fix:** Use configuration-driven paths only

### 4. Environment Checks Scattered
**Issue:** `if environment == "local"` checks throughout code
**Impact:** Code duplication, hard to maintain
**Fix:** Use dependency injection and configuration

## Refactoring Plan

### Phase 1: Remove Duplicates (Immediate)

1. **Delete duplicate transformation files:**
   ```bash
   rm jobs/bronze_to_silver_behavior.py
   rm jobs/gold/silver_to_gold.py
   rm -rf src/jobs/
   rm -rf src/project_a/jobs/
   rm -rf src/project_a/pyspark_interview_project/transform/
   rm -rf src/project_a/pyspark_interview_project/pipeline/
   ```

2. **Consolidate partial implementations:**
   - Merge `jobs/silver_build_customer_360.py` into `jobs/transform/silver_to_gold.py`
   - Merge `jobs/silver_build_product_perf.py` into `jobs/transform/silver_to_gold.py`

### Phase 2: Code Quality Fixes

1. **Replace print statements:**
   - Find all `print()` calls
   - Replace with `logger.info/warning/error`
   - Ensure proper logging setup

2. **Fix error handling:**
   - Replace bare `except:` with `except Exception as e:`
   - Add proper error logging
   - Re-raise critical errors

3. **Remove hardcoded paths:**
   - Extract all hardcoded paths to config
   - Use path abstraction layer
   - Support both `file://` and `s3://` transparently

### Phase 3: Architecture Improvements

1. **Unified code path:**
   - Remove environment-specific conditionals
   - Use dependency injection for format handlers
   - Configuration-driven behavior

2. **Extract common logic:**
   - Create `src/project_a/transform/common.py`
   - Create `src/project_a/transform/joins.py`
   - Create `src/project_a/transform/aggregations.py`

3. **Standardize interfaces:**
   - Consistent function signatures
   - Type hints everywhere
   - Docstrings for all public functions

## File Structure (Target)

```
jobs/
  transform/
    bronze_to_silver.py    ✅ CANONICAL
    silver_to_gold.py      ✅ CANONICAL
    raw_to_bronze.py        ✅ (if needed)
  
  ingest/
    snowflake_to_bronze.py ✅
    fx_json_to_bronze.py    ✅
    # ... other ingest jobs
  
  publish/
    gold_to_snowflake.py    ✅
    # ... other publish jobs

src/project_a/
  transform/                ✅ NEW - Common transformations
    common.py
    joins.py
    aggregations.py
  
  extract/                  ✅
  dq/                       ✅
  monitoring/               ✅
  utils/                    ✅
  schemas/                  ✅
```

## Industry Standards Checklist

- [ ] Single source of truth for each transformation
- [ ] No duplicate files
- [ ] No code duplication (DRY principle)
- [ ] Consistent error handling
- [ ] All functions have type hints
- [ ] All functions have docstrings
- [ ] No hardcoded values
- [ ] Configuration-driven
- [ ] Proper logging (no print statements)
- [ ] No bare except blocks
- [ ] Unit tests for all transformations
- [ ] Integration tests for full pipeline
- [ ] Clear separation of concerns
- [ ] Dependency injection where appropriate

