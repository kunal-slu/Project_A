# 📍 All Code Locations - Complete Reference

## 🎯 Core Package: `src/project_a/`

### Utilities (`src/project_a/utils/`)

| Module | File | Purpose |
|--------|------|---------|
| Spark Session | `utils/spark_session.py` | Build SparkSession with Delta Lake |
| Config Loader | `utils/config.py` | Load config from S3/local, resolve secrets |
| Logging | `utils/logging.py` | Structured JSON logging with trace IDs |
| Run Audit | `utils/run_audit.py` | Write run metadata to S3 |
| CloudWatch Metrics | `utils/cloudwatch_metrics.py` | Emit custom metrics to CloudWatch |
| Error Lanes | `utils/error_lanes.py` | Quarantine bad rows to error lanes |
| Contracts | `utils/contracts.py` | Load and validate schema contracts |

### Extract (`src/project_a/extract/`)

| Module | File | Purpose |
|--------|------|---------|
| FX JSON Reader | `extract/fx_json_reader.py` | Read and normalize FX JSON data |

### Monitoring (`src/project_a/monitoring/`)

| Module | File | Purpose |
|--------|------|---------|
| Lineage Emitter | `monitoring/lineage_emitter.py` | Emit OpenLineage events |

### Schemas (`src/project_a/schemas/`)

| Module | File | Purpose |
|--------|------|---------|
| Bronze Schemas | `schemas/bronze_schemas.py` | PySpark StructType schemas for bronze layer |

### DQ (`src/project_a/dq/`)

| Module | File | Purpose |
|--------|------|---------|
| DQ Gate | `dq/gate.py` | Run DQ checks (not-null, uniqueness, ranges) |

### Jobs (`src/project_a/jobs/`)

| Job | File | Status |
|-----|------|--------|
| FX JSON to Bronze | `jobs/fx_json_to_bronze.py` | ✅ Complete |
| Bronze to Silver | `jobs/bronze_to_silver.py` | ✅ Complete |
| Silver to Gold | `jobs/silver_to_gold.py` | ✅ Complete |
| Publish to Snowflake | `jobs/publish_gold_to_snowflake.py` | ✅ Complete |
| Snowflake to Bronze | `jobs/snowflake_to_bronze.py` | ❌ Missing |
| CRM to Bronze | `jobs/crm_to_bronze.py` | ❌ Missing |
| Redshift to Bronze | `jobs/redshift_to_bronze.py` | ❌ Missing |
| Kafka CSV to Bronze | `jobs/kafka_csv_to_bronze.py` | ❌ Missing |
| DQ Silver Gate | `jobs/dq_silver_gate.py` | ❌ Missing |
| DQ Gold Gate | `jobs/dq_gold_gate.py` | ❌ Missing |
| Publish to Redshift | `jobs/publish_gold_to_redshift.py` | ❌ Missing |

### Pipeline (`src/project_a/pipeline/`)

| Module | File | Purpose |
|--------|------|---------|
| Unified Entrypoint | `pipeline/run_pipeline.py` | Route jobs by name (needs updates) |

---

## 📦 Legacy Jobs (Reference Only)

These exist but use old `pyspark_interview_project` imports. Use as reference when creating new jobs.

| Job | Location |
|-----|----------|
| Snowflake to Bronze | `jobs/ingest/snowflake_to_bronze.py` |
| Redshift to Bronze | `jobs/redshift_to_bronze.py` |
| Kafka Orders to Bronze | `jobs/kafka_orders_to_bronze.py` |
| Publish to Redshift | `jobs/publish/publish_gold_to_redshift.py` |
| DQ Gate | `jobs/dq/dq_gate.py` |

---

## 📋 Configuration Files

| Config | Location | Purpose |
|--------|----------|---------|
| Dev Config | `config/dev.yaml` | Development environment config |
| Schema Contracts | `config/schema_definitions/bronze/*.json` | 9 schema contract files |
| Lineage Config | `config/lineage.yaml` | Lineage emitter configuration |

---

## 🧪 Tests (`tests/`)

| Test | File | Purpose |
|------|------|---------|
| Contract Tests | `tests/test_contracts_customers.py` | Test schema contract validation |
| DQ Tests | `tests/test_bronze_to_silver_orders.py` | Test DQ gate functionality |

---

## ☁️ AWS Infrastructure

| Component | Location | Purpose |
|-----------|----------|---------|
| Airflow DAG | `aws/dags/project_a_daily_pipeline.py` | Orchestrates full pipeline |
| Terraform | `aws/terraform/*.tf` | Infrastructure as code |
| EMR Configs | `aws/emr_configs/` | EMR Serverless configurations |

---

## 📚 Documentation (`docs/`)

| Doc | Location | Purpose |
|-----|----------|---------|
| Quick Start | `docs/QUICK_START.md` | Get started guide |
| Commands | `docs/COMMANDS_REFERENCE.md` | Command reference |
| AWS Deployment | `docs/AWS_DEPLOYMENT_CHECKLIST.md` | AWS deployment guide |
| Refactoring Review | `docs/REFACTORING_REVIEW.md` | Refactoring status |
| Implementation Status | `docs/COMPLETE_IMPLEMENTATION_STATUS.md` | This file |

---

## 🔍 Key Import Patterns

### ✅ Correct (project_a)
```python
from project_a.utils.spark_session import build_spark
from project_a.utils.config import load_config_resolved
from project_a.utils.contracts import load_contract, validate_schema
from project_a.dq.gate import run_dq_gate
from project_a.monitoring.lineage_emitter import LineageEmitter
```

### ❌ Wrong (old package)
```python
from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.config_loader import load_config_resolved
```

---

## 📊 File Count Summary

- **Total files in `src/project_a/`**: ~20 Python files
- **Jobs implemented**: 4/11 (36%)
- **Jobs missing**: 7
- **Schema contracts**: 9/9 (100%)
- **Tests**: 2 files
- **Documentation**: 100+ markdown files

---

## 🎯 Next Steps

1. **Create missing bronze jobs** (4 files)
2. **Create DQ gate jobs** (2 files)
3. **Create Redshift publish job** (1 file)
4. **Update unified entrypoint** (1 file)

Total: 8 files to create/update

