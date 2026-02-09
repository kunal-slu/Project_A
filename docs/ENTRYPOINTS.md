# Entrypoints and Ownership

This file defines which modules are supported entrypoints, which are helper modules, and which are legacy compatibility paths.

## Active Entrypoints

- Unified runner: `python -m project_a.pipeline.run_pipeline`
- Local full run: `run_complete_etl.py`
- Airflow (local): `airflow/dags/project_a_local_etl_dag.py`

## Primary Helpers

- Jobs: `jobs/ingest/*`, `jobs/transform/*`, `jobs/dq/*`
- Schema validation: `src/project_a/utils/schema_validator.py`
- Contracts: `src/project_a/contracts/runtime_contracts.py`
- DQ: `src/project_a/dq/*`

## Archived Legacy

Legacy code is archived under `archive/` and is not part of the active runtime.
