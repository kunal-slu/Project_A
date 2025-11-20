# PySpark Data Engineering Project

Comprehensive AWS Production ETL Pipeline with Delta Lake

## 🎯 Project Overview

This is a production-ready data engineering project that demonstrates best practices for:
- Multi-source data ingestion (HubSpot, Snowflake, Redshift, Kafka, FX Rates)
- Bronze → Silver → Gold data lakehouse architecture
- Incremental loading with SCD2 support
- Data quality validation
- AWS EMR Serverless deployment
- Delta Lake for ACID transactions

## 📁 Project Structure

```
pyspark_data_engineer_project/
├── config/                      # Configuration files
│   ├── local.yaml              # Local development
│   ├── config-dev.yaml         # Dev environment
│   ├── aws.yaml                # AWS production
│   └── dq.yaml                 # Data quality config
│
├── src/pyspark_interview_project/
│   ├── utils/                   # Core utilities
│   ├── extract.py               # Data extraction
│   ├── transform.py             # Data transformation
│   ├── load.py                  # Data loading
│   ├── incremental_loading.py   # SCD2 & CDC
│   ├── jobs/                    # EMR job implementations
│   ├── dq/                      # Data quality
│   └── monitoring/              # Monitoring
│
├── jobs/                        # EMR job wrappers
├── aws/
│   ├── infra/terraform/        # Infrastructure as code
│   ├── scripts/                 # Deployment scripts
│   └── emr_configs/            # EMR configuration
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
python jobs/transform/bronze_to_silver.py --env local --config local/config/local.yaml
python jobs/transform/silver_to_gold.py --env local --config local/config/local.yaml

# Validate ETL output
python tools/validate_local_etl.py --env local --config local/config/local.yaml
```

See [Local ETL Validation Guide](local/docs/LOCAL_ETL_VALIDATION.md) for detailed validation instructions.

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
- Check job scripts use canonical imports: `from project_a.pyspark_interview_project.*`

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
python -m pyspark_interview_project.cli \
  --config config/local.yaml \
  --env local \
  --cmd full

# Deploy to AWS
# See AWS_COMPLETE_DEPLOYMENT_GUIDE.md for step-by-step instructions
```
