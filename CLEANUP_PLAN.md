# Project_A Cleanup and Refactoring Plan

## Current State Analysis

- **Total Python files**: 300
- **CORE_PIPELINE**: 106 files (needs consolidation)
- **EXPERIMENT**: 86 files (move to archive/)
- **UNUSED**: 20 files (delete)
- **TEST**: 58 files (review and keep working)
- **SUPPORTING**: 28 files (keep)

## Key Issues Identified

1. **Duplicate code**:
   - `jobs/transform/` vs `aws/jobs/transform/` (duplicate ETL jobs)
   - Multiple config loaders (`src/project_a/config_loader.py`, `src/project_a/utils/config.py`, `src/project_a/pyspark_interview_project/config_loader.py`)
   - Multiple Spark session builders
   - Multiple path resolvers

2. **Large monolithic files**:
   - `bronze_to_silver.py`: 1043 lines (should be ~300-400)
   - `silver_to_gold.py`: 1127 lines (should be ~300-400)

3. **Legacy compatibility shim**:
   - `src/project_a/pyspark_interview_project/` - 86 files, mostly unused experiments

4. **Inconsistent structure**:
   - Some jobs in `jobs/`, some in `aws/jobs/`
   - Unclear which is canonical

## Cleanup Strategy

### Phase 1: Consolidate Core Utilities

**Goal**: Single source of truth for common utilities

1. **Config Loader**:
   - Keep: `src/project_a/pyspark_interview_project/utils/config_loader.py` (unified version)
   - Delete: `src/project_a/config_loader.py`, `src/project_a/utils/config.py`
   - Update all imports to use unified version

2. **Spark Session**:
   - Keep: `src/project_a/utils/spark_session.py` (already good)
   - Delete: `src/project_a/delta_utils.py` (spark_session function)
   - Update all jobs to use `build_spark()`

3. **Path Resolver**:
   - Keep: `src/project_a/utils/path_resolver.py` (already good)
   - Delete: `src/project_a/pyspark_interview_project/io/path_resolver.py`
   - Update all imports

### Phase 2: Refactor ETL Jobs

**Goal**: Extract reusable functions, reduce file sizes

1. **Bronze → Silver**:
   - Extract load functions to: `src/project_a/pyspark_interview_project/extract/`
   - Extract transform functions to: `src/project_a/pyspark_interview_project/transform/`
   - Keep `jobs/transform/bronze_to_silver.py` as thin orchestration (~200-300 lines)

2. **Silver → Gold**:
   - Extract dimension builders to: `src/project_a/pyspark_interview_project/transform/dimensions.py`
   - Extract fact builders to: `src/project_a/pyspark_interview_project/transform/facts.py`
   - Extract aggregations to: `src/project_a/pyspark_interview_project/transform/aggregations.py`
   - Keep `jobs/transform/silver_to_gold.py` as thin orchestration (~200-300 lines)

### Phase 3: Consolidate Job Locations

**Goal**: Single canonical location for jobs

1. **Keep**: `jobs/transform/` (canonical)
2. **Delete**: `aws/jobs/transform/` (duplicate)
3. **Keep**: `jobs/ingest/` (canonical)
4. **Review**: `aws/jobs/ingest/` - keep only if different/needed

### Phase 4: Archive Experiments

**Goal**: Move non-essential code out of core

1. Create `archive/` directory
2. Move `src/project_a/pyspark_interview_project/` (except utils, transform, extract, monitoring)
3. Move `notebooks/` to `archive/notebooks/`
4. Move experimental scripts to `archive/scripts/`

### Phase 5: Clean Up Tests

**Goal**: Keep only working, meaningful tests

1. Review all tests in `tests/` and `local/tests/`
2. Keep: Unit tests for utilities, integration tests for ETL
3. Archive: Broken tests, experimental tests
4. Fix: Tests that reference deleted code

### Phase 6: Documentation

**Goal**: Clear project structure documentation

1. Create `PROJECT_INDEX.md` - explains final structure
2. Create `CLEANUP_REPORT.md` - documents what was removed/archived
3. Update `README.md` - reflect clean structure

## Execution Order

1. ✅ Create cleanup plan (this document)
2. ⏳ Consolidate utilities (Phase 1)
3. ⏳ Refactor ETL jobs (Phase 2)
4. ⏳ Consolidate job locations (Phase 3)
5. ⏳ Archive experiments (Phase 4)
6. ⏳ Clean up tests (Phase 5)
7. ⏳ Create documentation (Phase 6)
8. ⏳ Verify pipeline runs end-to-end

