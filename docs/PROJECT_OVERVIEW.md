# Project Overview

This interview‑ready PySpark project demonstrates a complete lakehouse pipeline with **Iceberg** as the primary table format (Bronze → Silver → Gold), local Airflow orchestration, and production‑style data quality gates.

## Highlights

- Multi‑source ingestion (CRM, Snowflake, Redshift, Kafka, FX)
- Bronze → Silver cleaning and contract validation
- Gold dimensional models + analytics views
- DQ gates + Gold Truth Tests (PK/joins/reconciliation)
- Iceberg tables for ACID, time travel, and schema evolution
- Optional dbt for SQL governance and tests

## Flow Diagram

```mermaid
flowchart TD
    A[Sources] --> B[Bronze (Parquet)]
    B --> C[Silver (Iceberg)]
    C --> D[Gold (Iceberg)]
    D --> E[BI / Analytics]
```

## Running the Project (Local)

1. Create a virtual environment and install dependencies:
   ```bash
   python3 -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt && pip install -e .
   ```
2. Run tests:
   ```bash
   pytest -q
   ```
3. Run the full pipeline:
   ```bash
   python run_complete_etl.py --config local/config/local.yaml --env local --with-validation
   ```
4. Or run individual stages:
   ```bash
   python -m project_a.pipeline.run_pipeline --job bronze_to_silver --env local --config local/config/local.yaml
   python -m project_a.pipeline.run_pipeline --job silver_to_gold --env local --config local/config/local.yaml
   python -m project_a.pipeline.run_pipeline --job gold_truth_tests --env local --config local/config/local.yaml
   ```

## Interview Canonical Stack

For interviews (especially regulated companies like TransUnion), keep the narrative tight:

- **Spark** for compute
- **Iceberg** for Silver/Gold
- **Airflow** for orchestration
- **Contracts + DQ + Truth Tests** for governance

See `docs/interview/TRANSUNION_INTERVIEW_BRIEF.md` for the full interview framing.
