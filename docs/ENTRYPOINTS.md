# Entrypoints and Ownership

This file defines which modules are supported entrypoints, which are helper modules, and which are legacy compatibility paths.

## Active Entrypoints

- Local unified job runner: `jobs/run_pipeline.py`
- Local safe full run: `local/scripts/run_local_etl_safe.py`
- Local transform jobs:
  - `jobs/transform/bronze_to_silver.py`
  - `jobs/transform/silver_to_gold.py`
- AWS orchestration:
  - `aws/dags/daily_batch_pipeline_dag.py`
  - `aws/dags/production_etl_dag.py`

## Primary Helpers

- Core transform functions: `src/project_a/transforms/bronze_to_silver.py`
- Schema registry (single source): `src/project_a/schema/registry.py`
- Schema validation: `src/project_a/utils/schema_validator.py`
- Data contract helpers: `src/project_a/utils/contracts.py`

## Compatibility Wrappers

- `src/project_a/jobs/*.py` wrappers that call class-based jobs first, then legacy fallbacks.
- `src/project_a/transform/__init__.py` compatibility exports for tests and historical imports.

## Legacy (Not Primary)

- `src/project_a/legacy/` and `jobs_legacy/` are legacy implementations retained for fallback and reference.
- New features and bug fixes should target active entrypoints and shared helpers first.
