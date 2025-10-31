# AWS Scripts Directory

Essential deployment, maintenance, and utility scripts for the data pipeline.

## 📁 Script Organization

### 🚀 Deployment & Infrastructure
- **`aws_production_deploy.sh`** - Complete production deployment (Terraform + EMR + MWAA + jobs)
- **`teardown.sh`** - Clean up AWS resources (use with caution)

### 💻 Job Submission
- **`emr_submit.sh`** - Submit Spark jobs to EMR Serverless with Delta Lake config

### 🔄 Data Operations
- **`backfill_bronze_for_date.sh`** - Backfill bronze data for a specific date
- **`dr_snapshot_export.py`** - Export data lake snapshots to DR region for disaster recovery

### 📊 Data Catalog & Governance
- **`register_glue_tables.py`** - Register Delta tables in AWS Glue Catalog
- **`lf_tags_seed.py`** - Seed Lake Formation tags for tables/columns (access control)

### ✅ Data Quality
- **`run_ge_checks.py`** - Run Great Expectations data quality checks

### 🛠️ Utilities
- **`source_terraform_outputs.sh`** - Source Terraform output variables as environment variables

---

## 📋 Script Details

### Deployment Script
**`aws_production_deploy.sh`**
- Creates S3 buckets (data lake, backups, logs, artifacts)
- Deploys Terraform infrastructure (EMR Serverless, MWAA, Glue, IAM)
- Uploads job code to S3
- Configures Airflow DAGs
- Sets up Secrets Manager
- Registers Glue tables

### EMR Job Submission
**`emr_submit.sh`**
```bash
./emr_submit.sh \
  --app-id emr-app-id \
  --role-arn arn:aws:iam::ACCOUNT:role/EmrServerlessJobExecutionRole \
  --code-bucket s3://company-artifacts-ACCOUNT \
  --entry-point jobs/crm_accounts_ingest.py \
  --config config/prod.yaml
```

### Backfill Operations
**`backfill_bronze_for_date.sh`**
```bash
./backfill_bronze_for_date.sh 2024-01-15
# Triggers re-ingestion of bronze data for specified date
```

### Disaster Recovery
**`dr_snapshot_export.py`**
```bash
python dr_snapshot_export.py \
  --config config/prod.yaml \
  --layers silver gold \
  --days-back 7
# Exports latest Silver/Gold data to DR region (us-west-2)
```

### Glue Catalog
**`register_glue_tables.py`**
```bash
python register_glue_tables.py \
  --db silver \
  --root s3://company-data-lake-ACCOUNT/silver \
  --region us-east-1
```

### Lake Formation Tags
**`lf_tags_seed.py`**
```bash
python lf_tags_seed.py \
  --env prod \
  --tag-layer bronze \
  --tag-classification confidential \
  --tag-contains-pii true
```

---

## 🗑️ Removed Scripts

The following duplicate/redundant scripts were removed for better organization:

**ETL Scripts** (replaced by `aws/jobs/`):
- ❌ `enterprise_etl.py` → Use `aws/jobs/*.py`
- ❌ `enterprise_internal_etl.py` → Use `aws/jobs/*.py`
- ❌ `enterprise_simple_etl.py` → Use `scripts/local/run_pipeline.py`
- ❌ `production_etl.py` → Use `aws/jobs/*.py`
- ❌ `real_world_etl.py` → Use `aws/jobs/*.py`

**Deployment Scripts** (consolidated):
- ❌ `aws_enterprise_deploy.sh` → Merged into `aws_production_deploy.sh`
- ❌ `aws_real_world_deploy.sh` → Merged into `aws_production_deploy.sh`

**Misplaced Files** (moved to proper location):
- ❌ `delta_optimize_vacuum.py` → Moved to `aws/jobs/delta_optimize_vacuum.py`

---

## 📁 Directory Structure

```
aws/scripts/
├── README.md                          # This file
├── aws_production_deploy.sh          # Main deployment script
├── emr_submit.sh                      # EMR job submission
├── teardown.sh                        # Cleanup utility
├── backfill_bronze_for_date.sh        # Backfill operations
├── dr_snapshot_export.py              # Disaster recovery
├── register_glue_tables.py            # Glue catalog
├── lf_tags_seed.py                    # Lake Formation tags
├── run_ge_checks.py                   # Data quality checks
└── source_terraform_outputs.sh         # Terraform utilities
```

**Total**: 9 essential scripts (down from 17) ✅

---

## 🔗 Related Directories

- **`aws/jobs/`** - All production ETL jobs (ingest, transform, analytics)
- **`aws/dags/`** - Airflow DAGs for orchestration
- **`aws/terraform/`** - Infrastructure as Code
- **`scripts/local/`** - Local development and testing scripts

---

**Last Updated**: 2024-01-15  
**Maintained By**: Data Engineering Team

---

## 📋 Usage Examples

### Deploy to Production
```bash
./aws_production_deploy.sh --region us-east-1
```

### Submit EMR Job
```bash
./emr_submit.sh \
  --app-id emr-app-id \
  --role-arn arn:aws:iam::ACCOUNT:role/EmrServerlessJobExecutionRole \
  --code-bucket s3://company-artifacts-ACCOUNT \
  --entry-point jobs/crm_accounts_ingest.py
```

### Backfill Data
```bash
./backfill_bronze_for_date.sh 2024-01-15
```

### Register Glue Tables
```bash
python register_glue_tables.py --db silver --root s3://company-data-lake-ACCOUNT/silver
```

### Export DR Snapshot
```bash
python dr_snapshot_export.py --config config/prod.yaml --layers silver gold
```

---

**Note**: All production ETL jobs are located in `aws/jobs/` directory.

