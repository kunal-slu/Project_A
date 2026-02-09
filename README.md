# Project_A: Production Data Engineering Platform

**Enterprise-grade data lakehouse with medallion architecture, incremental processing, and data quality automation**

## 🎯 What This Project Solves

This is not just another ETL pipeline—it's a **production-ready data platform** that solves real engineering challenges:

- **Late-Arriving Data**: Configurable rolling window + CDC-aware merge logic (orders) when enabled
- **Schema Evolution**: Schema baselines + runtime validation, with storage format configurable (Iceberg/Delta/Parquet)
- **Data Quality**: Automated validation gates + Gold Truth Tests, with strictness configurable
- **Scalability**: Incremental processing handles growing data volumes efficiently
- **Observability**: Audit logs + metrics, optional lineage tracking for compliance

## 🧭 Interview Canonical Stack (TransUnion‑Ready)

If you want a **clean, consistent story** for interviews (especially in a regulated company like TransUnion), use this stack:

- **Compute**: Spark (PySpark)
- **Storage**: **Iceberg** (primary table format)
- **Orchestration**: Airflow (Docker for local)
- **Governance**: Data contracts + DQ gates + Gold Truth Tests
- **dbt**: Optional (SQL governance/tests if enabled)

Everything else (Delta, EMR, Step Functions) is **optional/legacy** and should only be mentioned if asked directly.
For the exact interview narrative, see `docs/interview/TRANSUNION_INTERVIEW_BRIEF.md`.
For canonical vs legacy scope, see `docs/INTERVIEW_SCOPE.md`.

## 🏗️ Architecture Overview

```
Sources (CRM, Snowflake, Redshift, Kafka, FX)
         ↓
    Bronze Layer (Raw, Append-Only)
    - Format: Parquet
    - Strategy: Full ingestion + optional incremental dirs / CDC fields
    - Purpose: Immutable source of truth
         ↓
    Silver Layer (Cleaned, Validated)
    - Format: Iceberg (primary) / Parquet (fallback)
    - Optional: Delta (legacy compatibility)
    - Strategy: Cleaning + CDC if present + contract validation
    - Purpose: Single source of truth for analytics
         ↓
    Gold Layer (Business Logic)
    - Format: Iceberg (primary) / Parquet (fallback)
    - Optional: Delta (legacy compatibility)
    - Strategy: Spark-built dims/facts; dbt models optional
    - Purpose: Analytics-ready dimensional models
         ↓
    BI Tools / ML / Data Products
```

### Why This Architecture?

**Bronze = Raw Files (Parquet)**
- Simple, fast ingestion
- No schema enforcement needed
- Cheap storage for compliance/auditing

**Silver = Iceberg (primary)**  
✅ **This is the senior-level signal**
- ACID guarantees for data consistency
- Schema evolution without breaking consumers
- Time travel for debugging and recovery
- Merge operations for handling late data
- Interview line: *"We keep Bronze as raw files, but Silver uses Iceberg for ACID guarantees, schema evolution, and late-arriving data handling."*

**Gold = Spark-built dimensional models (dbt optional)**
- Business logic is implemented in Spark jobs (dims/facts/analytics)
- Contracts and schema baselines enforced at runtime
- dbt project exists for SQL-based modeling/testing if enabled
- Interview line: *"Gold models are built in Spark, with optional dbt for SQL governance and tests."*

## 🔑 Key Technical Decisions

### 1. Incremental Processing + Late Data Handling

**Problem**: Orders can be updated after initial load (refunds, corrections)

**Solution**: Rolling window + merge/dedupe logic in Spark jobs (orders)
```python
cutoff_date = current_date - lookback_days
reprocess_existing = existing_orders.filter(order_date >= cutoff_date)
reprocess_incoming = incoming_orders.filter(order_date >= cutoff_date)
merge_candidates = reprocess_existing.unionByName(reprocess_incoming)
dedupe_latest = row_number(partitionBy=order_id, orderBy=updated_at desc)
```

**Why This Works:**
- Reprocesses last 3 days on every run
- Merge updates existing records
- Safe for late corrections without full reload
- Interview line: *"We reprocess a rolling window to safely handle late events."*

### 2. Iceberg (Primary) vs Delta (Optional)

**Decision**: Iceberg is the canonical storage format. Delta is supported as an optional/legacy path.

**Why:**
- Spark-native integration
- Mature ACID support
- Meets needs: schema evolution, time travel, merges
- Configurable per environment

**When to use Iceberg:**
- Multi-engine access (Trino + Flink + Spark)
- Hidden partitioning requirements
- Petabyte-scale metadata management

**Current Local Config**: Iceberg is enabled (`local/config/local.yaml`)

**Interview guidance**: Lead with Iceberg. Mention Delta only if asked about alternatives.

### 3. dbt for Gold Layer (Optional)

**Why dbt:**
- Business logic as version-controlled SQL
- Built-in testing framework
- Documentation generation
- Lineage DAG visualization
- Industry standard for analytics engineering

**What We Built:**
- ✅ Dimension tables with SCD2 fields (effective_start/end, is_current)
- ✅ Fact tables with FX normalization and partitioning
- ✅ Runtime schema baselines and contract checks
- ✅ Gold Truth Tests (PKs, joins, reconciliation)

### 4. Data Quality Strategy

**Layered Approach:**

1. **Spark ETL Layer**: Basic validation (not_null, type checking)
2. **DQ Gates + Gold Truth Tests**: Join checks + reconciliation
3. **dbt Tests (optional)**: Business rule validation if dbt is enabled
   - Uniqueness constraints
   - Referential integrity
   - Value range checks
   - Custom business logic tests

**Fail-Fast Principle:**
```
Pipelines can be configured to fail if:
- Primary keys are not unique
- Foreign keys reference non-existent records  
- Required fields are null
- Data quality score < threshold
```

Interview line: *"Pipelines fail fast if data quality checks don't pass."*

### 5. Orchestration

**Current**: Local Airflow (Docker) orchestrating Spark jobs  
**Optional**: AWS Spark runtime (e.g., EMR Serverless) for production scale

**Interview line**: *"Airflow orchestrates Spark jobs; the same pipeline runs locally or in AWS."*

### 6. Observability

**What We Track:**
- ✅ Row counts at each stage
- ✅ Job duration and resource usage
- ✅ Data quality scores
- ✅ Lineage information
- ✅ Schema changes

**Mindset**: *"Pipelines are products, not scripts."*

### 7. Operational Ownership

- Runbook: `RUNBOOK.md`
- Data SLAs: `docs/runbooks/DATA_SLA.md`
- DQ failover: `docs/runbooks/RUNBOOK_DQ_FAILOVER.md`
- Entrypoints: `docs/ENTRYPOINTS.md`

### 8. Explicit Contracts and Pipeline Gates

- Machine-readable contracts: `config/contracts/silver_contracts.yaml`
- Runtime contract validation: `src/project_a/contracts/runtime_contracts.py`
- dbt contract/test definitions:
  - `dbt/models/schema.yml`
  - `dbt/models/sources.yml`

Pipeline is expected to stop on contract violations (null PKs, duplicates, RI breaks).

## 📁 Project Structure

```
pyspark_data_engineer_project/
├── config/                      # Configuration files
│   ├── dev.yaml                # Dev environment
│   ├── prod.yaml               # Prod environment
│   └── dq/                     # Data quality config
│
├── local/                       # Local configs/scripts
│   └── config/local.yaml       # Local development config
│
├── src/project_a/               # Main pipeline package
│   ├── jobs/                    # Job entrypoints
│   ├── dq/                      # Data quality
│   ├── utils/                   # Spark + helpers
│   └── observability/           # Audit + alerts
│
├── jobs/                        # Job wrappers (local + AWS optional)
├── airflow/                     # Local Airflow DAGs/configs
├── aws/
│   ├── infra/terraform/        # Infrastructure as code (optional)
│   ├── scripts/                 # Deployment scripts (optional)
│   └── emr_configs/            # EMR configuration (optional)
│
├── tests/                       # Test suite
├── notebooks/                   # Jupyter notebooks
└── docs/                        # Documentation
```

## 🚀 Quick Start

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
pytest tests/

# Run ETL pipeline locally
python -m project_a.pipeline.run_pipeline --job bronze_to_silver --env local --config local/config/local.yaml
python -m project_a.pipeline.run_pipeline --job silver_to_gold --env local --config local/config/local.yaml

# Or run the full pipeline
python run_complete_etl.py --config local/config/local.yaml --env local --with-validation

# Validate ETL output
python tools/validate_local_etl.py --env local --config local/config/local.yaml
```

See [Local ETL Validation Guide](local/docs/LOCAL_ETL_VALIDATION.md) for detailed validation instructions.

### Apache Iceberg (via Spark packages)

Iceberg is supported via Spark packages (no separate install required). Configure it in `local/config/local.yaml`:

```yaml
iceberg:
  enabled: true
  catalog_name: "local"
  catalog_type: "hadoop"
  warehouse: "file:///Users/kunal/IdeaProjects/Project_A/data/iceberg"
  packages: "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0"
```

### Apache Airflow (Optional, Docker on Mac)

Use the Docker-based setup:

```bash
docker compose -f docker-compose-airflow.yml up -d --build
```

Then open the UI at `http://localhost:8080` and trigger the DAG:

- `project_a_local_etl` (runs Bronze → Silver → Gold + DQ + Gold Truth Tests)

Airflow uses a container-friendly config at `airflow/config/local_airflow.yaml`.

### dbt (Local, Spark/Iceberg)

Use the helper script:

```bash
scripts/run_dbt_local.sh
```

This runs `dbt build` using the project’s Spark/Iceberg profile in `dbt/profiles.yml`.

### AWS Deployment

1. **Deploy artifacts to S3:**
   ```bash
   export ARTIFACTS_BUCKET="my-etl-artifacts-demo-424570854632"
   export AWS_REGION="us-east-1"
   bash aws/scripts/deploy_to_aws.sh
   ```

2. **Run ETL jobs on EMR:**
   ```bash
   # Submit Bronze → Silver job
   aws emr-serverless start-job-run \
     --application-id <APP_ID> \
     --execution-role-arn <ROLE_ARN> \
     --job-driver '{"sparkSubmit": {"entryPoint": "s3://.../jobs/transform/bronze_to_silver.py", ...}}'
   
   # Submit Silver → Gold job
   aws emr-serverless start-job-run \
     --application-id <APP_ID> \
     --execution-role-arn <ROLE_ARN> \
     --job-driver '{"sparkSubmit": {"entryPoint": "s3://.../jobs/transform/silver_to_gold.py", ...}}'
   ```

3. **Validate AWS pipeline:**
   ```bash
   # ⚠️ IMPORTANT: This script MUST run on EMR Serverless, not locally
   # Local execution will fail because S3 filesystem support is not available
   
   # Run on EMR Serverless (via Airflow DAG or EMR job):
   python tools/validate_aws_etl.py --config s3://my-etl-artifacts-demo-424570854632/config/aws/config/dev.yaml
   
   # For local testing, use validate_local_etl.py instead:
   python tools/validate_local_etl.py --env local --config local/config/local.yaml
   ```

See [AWS Validation Guide](aws/docs/AWS_VALIDATION.md) for detailed validation instructions and troubleshooting.
See [AWS Validation Local Execution](docs/AWS_VALIDATION_LOCAL_EXECUTION.md) for why local execution fails and how to work around it.

## Phase 4 — EMR Spark Execution (Bronze → Silver → Gold)

This phase runs the ETL pipeline on AWS EMR on EC2 using spark-submit.

### Prerequisites

- EMR cluster running (e.g., `j-3N2JXYADSENNU`)
- AWS CLI configured with appropriate profile
- S3 bucket for artifacts: `my-etl-artifacts-demo-424570854632`
- Delta Lake JARs uploaded to S3

### Step 1: Build Wheel

```bash
# Clean and build
rm -rf build dist src/*.egg-info
python3 -m build

# Verify wheel created
ls -lh dist/project_a-0.1.0-py3-none-any.whl
```

### Step 2: Upload Artifacts to S3

**Option A: Use sync script (recommended)**
```bash
chmod +x sync_artifacts_to_s3.sh
./sync_artifacts_to_s3.sh
```

**Option B: Manual upload**
```bash
# Upload wheel
aws s3 cp dist/project_a-0.1.0-py3-none-any.whl \
  s3://my-etl-artifacts-demo-424570854632/packages/ \
  --profile kunal21 --region us-east-1

# Upload job scripts
aws s3 sync jobs/transform \
  s3://my-etl-artifacts-demo-424570854632/jobs/transform/ \
  --exclude "*" --include "*.py" \
  --profile kunal21 --region us-east-1
```

**Verify Delta JARs exist:**
```bash
aws s3 ls s3://my-etl-artifacts-demo-424570854632/packages/delta-*.jar \
  --profile kunal21 --region us-east-1
```

If missing, download and upload:
- `delta-spark_2.12-3.2.0.jar`: https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/
- `delta-storage-3.2.0.jar`: https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/

### Step 3: Submit EMR Steps

**Option A: Use submission script (recommended)**
```bash
chmod +x run_emr_steps.sh
export EMR_CLUSTER_ID=j-3N2JXYADSENNU
./run_emr_steps.sh both  # or 'bronze' or 'silver'
```

**Option B: Manual submission**

Bronze→Silver:
```bash
aws emr add-steps \
  --cluster-id j-3N2JXYADSENNU \
  --steps file://steps_bronze_to_silver.json \
  --profile kunal21 \
  --region us-east-1
```

Silver→Gold:
```bash
aws emr add-steps \
  --cluster-id j-3N2JXYADSENNU \
  --steps file://steps_silver_to_gold.json \
  --profile kunal21 \
  --region us-east-1
```

### Step 4: Monitor Steps

```bash
# List all steps
aws emr list-steps \
  --cluster-id j-3N2JXYADSENNU \
  --region us-east-1 \
  --profile kunal21 \
  --output table

# Check specific step status
aws emr describe-step \
  --cluster-id j-3N2JXYADSENNU \
  --step-id s-XXXXXXXXXXXXX \
  --profile kunal21 \
  --region us-east-1
```

### Step 5: Verify Output in S3

```bash
export LAKE_BUCKET=my-etl-lake-demo-424570854632

# Check Silver layer
aws s3 ls "s3://${LAKE_BUCKET}/silver/" --recursive \
  --profile kunal21 --region us-east-1 | head -20

# Check Gold layer
aws s3 ls "s3://${LAKE_BUCKET}/gold/" --recursive \
  --profile kunal21 --region us-east-1 | head -20
```

### Step 6: Terminate EMR Cluster (when done)

```bash
aws emr terminate-clusters \
  --cluster-ids j-3N2JXYADSENNU \
  --profile kunal21 \
  --region us-east-1
```

### Troubleshooting

**Import errors:**
- Verify wheel is uploaded: `aws s3 ls s3://my-etl-artifacts-demo-424570854632/packages/project_a-0.1.0-py3-none-any.whl`
- Check job scripts use canonical imports: `from project_a.*`

**Delta Lake errors:**
- Verify JARs are uploaded and paths match in step JSON
- Check Spark config includes Delta extensions

**Step failures:**
- Check CloudWatch logs: EMR cluster logs in S3
- Review step stderr/stdout in EMR console

## 📊 Data Sources

1. **HubSpot CRM** - Contacts and deals
2. **Snowflake** - Orders and customers
3. **Redshift** - Customer behavior analytics
4. **Kafka** - Real-time event streaming
5. **FX Rates** - Exchange rates from vendors

## 🏗️ Architecture

- **Bronze Layer**: Raw data ingestion with schema validation
- **Silver Layer**: Cleaned, conformed data with SCD2 support
- **Gold Layer**: Business-ready dimensional models

## 🔧 Key Features

- ✅ **Incremental loading strategies** with watermark-based CDC
- ✅ **SCD2 support** for slowly changing dimensions
- ✅ **Data quality gates** with Great Expectations (critical failure handling)
- ✅ **Multi-format support**: Delta Lake, Apache Iceberg, Parquet
- ✅ **Dual destinations**: S3 (data lake) + Snowflake (analytics)
- ✅ **Real lineage tracking** via OpenLineage
- ✅ **AWS EMR Serverless deployment**
- ✅ **Monitoring and alerting** with CloudWatch

## 📖 Documentation

- **[Getting Started Guide](README_GETTING_STARTED.md)** 🌟 - **START HERE! Your next steps**
- **[Beginners AWS Guide](BEGINNERS_AWS_DEPLOYMENT_GUIDE.md)** ⭐ - Step-by-step AWS deployment for novices
- [AWS Deployment Guide](AWS_COMPLETE_DEPLOYMENT_GUIDE.md) - Complete end-to-end AWS deployment
- [Data Sources & Architecture](DATA_SOURCES_AND_ARCHITECTURE.md) - All 6 data sources and architecture
- [P0-P6 Implementation Plan](P0_P6_IMPLEMENTATION_PLAN.md) - Production-ready roadmap
- [AWS Runbook](RUNBOOK_AWS_2025.md) - Operational procedures

## 🧪 Testing

```bash
# Run all tests
pytest tests/

# Run specific test suite
pytest tests/test_contracts.py
```

## 📝 Requirements

- Python 3.10+
- PySpark 3.5+
- Delta Lake
- AWS CLI configured
- Terraform 1.0+

## 📄 License

MIT License

## 🎉 Recent Updates (2025)

### Production-Grade Enhancements Completed

- ✅ **Real Lineage Tracking** - OpenLineage integration with automatic metadata capture
- ✅ **Data Quality Gates** - Great Expectations with critical failure handling
- ✅ **Snowflake Target** - Dual destination loading with MERGE operations
- ✅ **AWS Deployment** - Complete end-to-end deployment guide (see [AWS_COMPLETE_DEPLOYMENT_GUIDE.md](AWS_COMPLETE_DEPLOYMENT_GUIDE.md))
- ✅ **Multi-Source Architecture** - 6 data sources documented (see [DATA_SOURCES_AND_ARCHITECTURE.md](DATA_SOURCES_AND_ARCHITECTURE.md))

**Additional Production Features**
- ✅ Dual destination: S3 (data lake) + Snowflake (analytics)
- ✅ `write_df_to_snowflake()` with MERGE support
- ✅ Idempotent upserts with composite primary keys

**D. Iceberg Toggle**
- ✅ Format flexibility: Delta/Iceberg/Parquet via `config/storage.yaml`
- ✅ Glue catalog integration for Iceberg
- ✅ Transparent to application code

### Quick Start
```bash
# Run complete pipeline locally
python run_complete_etl.py --config local/config/local.yaml --env local --with-validation

# Deploy to AWS
# See AWS_COMPLETE_DEPLOYMENT_GUIDE.md for step-by-step instructions
```
