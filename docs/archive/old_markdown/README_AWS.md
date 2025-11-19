# AWS Deployment - Senior-Level Data Platform

## Overview

This is a production-ready, end-to-end data platform deployed on AWS with:
- Multi-source ingestion (5+ sources)
- Bronze → Silver → Gold lakehouse architecture
- Schema contract enforcement
- Data quality gating
- Lineage tracking
- Least-privilege IAM
- Operational runbooks

## Critical Senior Signals

### 1. Schema Contracts Enforced at Ingestion ✅

Every Bronze ingestion job validates incoming data against schema contracts defined in `aws/config/schema_definitions/`:
- Required columns enforced
- Type validation
- Null constraints
- Refuse to write bad data

**Example:**
```python
# In hubspot_to_bronze.py
schema_contract = load_schema_contract("hubspot_contacts_bronze.json")
validate_schema(df, schema_contract)  # Fails if contract violated
```

### 2. DQ Gates Promotion Between Layers ✅

The `daily_batch_pipeline_dag.py` enforces:
```
[ingest_*_to_bronze] 
    -> [dq_check_bronze]      # FAIL HERE = STOPS
        -> [bronze_to_silver]
            -> [dq_check_silver]  # FAIL HERE = STOPS
                -> [silver_to_gold]
                    -> [register_glue_catalog]
                        -> [emit_lineage_and_metrics]
```

**If DQ fails: Gold never updates.** This protects trusted data.

### 3. Least-Privilege IAM ✅

See `aws/terraform/iam.tf`:
- EMR role has access to:
  - Data lake bucket (specific prefix)
  - Code bucket (read-only)
  - Glue DB (specific databases)
  - Secrets Manager (specific ARNs)
  - CloudWatch logs (specific log groups)
- **No wildcards** (`Resource: "*"` avoided)

### 4. Lineage + Metrics Emission ✅

Every job:
- Generates unique `run_id` (timestamp + UUID)
- Logs row counts
- Emits lineage events
- Pushes metrics to CloudWatch

**Example:**
```python
run_id = generate_run_id()
emit_lineage_event(source="hubspot", target="bronze.contacts", run_id=run_id)
emit_metric("rows_ingested", count, run_id)
```

### 5. CI Prevents Broken DAGs ✅

`tests/test_dag_imports.py`:
- Imports all DAGs in `dags/`
- Catches syntax errors before reaching MWAA
- **Senior engineers add this** to prevent production issues

### 6. Complete Runbooks ✅

`RUNBOOK_AWS_2025.md` includes:
- "How to rerun failed Bronze ingestion"
- "How to clean stuck streaming checkpoints"
- "How to restore Silver from Bronze after DQ fix"
- Incident response procedures

## Project Structure

```
aws/
├── README_AWS.md                    # This file
├── RUNBOOK_AWS_2025.md             # Operational runbook
├── requirements.txt                 # Dependencies
│
├── terraform/                       # Infrastructure as Code
│   ├── main.tf                      # S3, EMR Serverless, MWAA
│   ├── iam.tf                       # Least-privilege IAM
│   ├── secrets.tf                   # Secrets Manager
│   ├── cloudwatch.tf                # Log groups, alarms
│   └── outputs.tf                   # Terraform outputs
│
├── config/                          # Runtime configuration
│   ├── prod.yaml                    # Production config
│   ├── dev.yaml                     # Development config
│   ├── dq_thresholds.yaml           # Global DQ expectations
│   └── schema_definitions/          # Schema contracts
│       ├── hubspot_contacts_bronze.json
│       ├── snowflake_orders_bronze.json
│       └── fx_rates_bronze.json
│
├── emr_configs/                     # EMR configurations
│   ├── spark-defaults.conf           # Spark tuning
│   ├── delta-core.conf              # Delta Lake settings
│   └── logging.yaml                 # CloudWatch logging
│
├── jobs/                             # Spark entrypoints
│   # Ingestion
│   ├── hubspot_to_bronze.py
│   ├── snowflake_to_bronze.py
│   ├── vendor_to_bronze.py
│   └── kafka_orders_to_bronze.py
│   # Promotion
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
│   # DQ & Governance
│   ├── dq_check_bronze.py           # Validates Bronze layer
│   ├── dq_check_silver.py           # Validates Silver layer
│   └── emit_lineage_and_metrics.py  # Lineage emission
│
├── dags/                             # Airflow/MWAA orchestration
│   ├── daily_batch_pipeline_dag.py   # Main batch DAG with DQ gates
│   ├── streaming_ingest_dag.py       # Streaming pipeline
│   └── dq_watchdog_dag.py            # Independent DQ checks
│
├── scripts/                          # Deployment tooling
│   ├── build_zip.sh                 # Bundle code for EMR
│   ├── emr_submit.sh                # Manual job submission
│   └── run_ge_checks.py             # Great Expectations runner
│
├── tests/                            # CI/CD tests
│   ├── test_dag_imports.py          # Prevents broken DAGs
│   ├── test_schema_contracts.py     # Validates schema JSONs
│   └── test_prod_config.py          # Validates prod.yaml
│
└── data_fixed/                       # Sample data for demos
    ├── hubspot_contacts_25000.csv
    └── snowflake_orders_100000.csv
```

## Deployment

See `docs/guides/AWS_COMPLETE_DEPLOYMENT.md` for step-by-step instructions.

## What This Demonstrates

✅ **Multi-source ingestion** - 5+ sources with schema contracts  
✅ **Lakehouse modeling** - Bronze → Silver → Gold  
✅ **Governance** - DQ checks, lineage, audit trails  
✅ **Orchestration** - Airflow DAGs with proper gating  
✅ **Operations** - Runbooks, observability, failure handling  

**This is what senior-level data engineering looks like.**
