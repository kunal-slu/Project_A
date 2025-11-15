# Phase 5: Airflow Orchestration Setup Guide

Complete guide for setting up Airflow orchestration (Local Docker + MWAA).

## 📋 Table of Contents

1. [Local Airflow (Docker)](#local-airflow-docker)
2. [MWAA Setup (Optional)](#mwaa-setup-optional)
3. [DAG Configuration](#dag-configuration)
4. [Troubleshooting](#troubleshooting)

---

## 🐳 Local Airflow (Docker)

### Prerequisites

- Docker Desktop installed and running
- `docker compose` available (version 2.0+)
- AWS credentials configured (`~/.aws/credentials`)

### Step 1: Create Airflow Directories

```bash
cd ~/IdeaProjects/pyspark_data_engineer_project

# Create required directories
mkdir -p airflow/{dags,logs,plugins,config}
```

### Step 2: Sync DAGs to Airflow

```bash
# Sync DAGs from aws/dags/ to airflow/dags/
./scripts/sync_dags_to_airflow.sh
```

This copies all DAGs from `aws/dags/` to `airflow/dags/` so Airflow can see them.

### Step 3: Start Airflow

```bash
# Start Airflow services
docker compose -f docker-compose-airflow.yml up -d

# Check containers are running
docker ps | grep airflow
```

You should see:
- `airflow-webserver`
- `airflow-scheduler`
- `airflow-triggerer`
- `postgres`

### Step 4: Access Airflow UI

Open browser: **http://localhost:8080**

**Default credentials:**
- Username: `airflow`
- Password: `airflow`

### Step 5: Configure Airflow Variables and Connections

Run the setup script:

```bash
./scripts/setup_airflow_local.sh
```

This sets:
- **Variables:**
  - `PROJECT_A_ENV` = `dev`
  - `PROJECT_A_CONFIG_URI` = `s3://my-etl-artifacts-demo-424570854632/config/dev.yaml`
  - `PROJECT_A_EMR_APP_ID` = `00g0tm6kccmdcf09`
  - `PROJECT_A_EXEC_ROLE_ARN` = `arn:aws:iam::424570854632:role/project-a-dev-emr-exec`
  - `PROJECT_A_LAKE_BUCKET` = `my-etl-lake-demo-424570854632`
  - `PROJECT_A_ARTIFACTS_BUCKET` = `my-etl-artifacts-demo-424570854632`

- **Connection:**
  - `aws_default` (AWS connection using host's credentials)

### Step 6: Verify DAGs

In Airflow UI:
1. Go to **DAGs** page
2. You should see:
   - `project_a_daily_pipeline` ✅
   - Other DAGs from `aws/dags/`

### Step 7: Trigger a DAG

1. Toggle the DAG **ON** (switch)
2. Click **Trigger DAG** (play button)
3. Monitor in **Graph** view

### Step 8: Monitor EMR Jobs

Check EMR job runs:

```bash
export EMR_APP_ID=00g0tm6kccmdcf09
export AWS_PROFILE=kunal21
export AWS_REGION=us-east-1

aws emr-serverless list-job-runs \
  --application-id "$EMR_APP_ID" \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --max-results 10 \
  --query 'jobRuns[*].[jobRunId,state,createdAt]' \
  --output table
```

### Step 9: Verify Data

Check S3 layers:

```bash
export LAKE_BUCKET=my-etl-lake-demo-424570854632

# Bronze
aws s3 ls "s3://${LAKE_BUCKET}/bronze/" --recursive --profile kunal21 | head

# Silver
aws s3 ls "s3://${LAKE_BUCKET}/silver/" --recursive --profile kunal21 | head

# Gold
aws s3 ls "s3://${LAKE_BUCKET}/gold/" --recursive --profile kunal21 | head
```

---

## ☁️ MWAA Setup (Optional)

### Prerequisites

- Terraform installed
- AWS credentials configured
- VPC with private subnets (MWAA requirement)

### Step 1: Enable MWAA in Terraform

Edit `aws/terraform/env/dev.tfvars`:

```hcl
enable_mwaa = true
mwaa_environment_name = "project-a-airflow-dev"
```

### Step 2: Deploy MWAA Infrastructure

```bash
cd aws/terraform

# Initialize (if not done)
terraform init

# Plan
terraform plan -var-file=env/dev.tfvars

# Apply
terraform apply -var-file=env/dev.tfvars
```

This creates:
- MWAA S3 bucket for DAGs
- MWAA IAM role with EMR permissions
- MWAA environment
- CloudWatch log groups

**Note:** MWAA creation takes 20-30 minutes.

### Step 3: Get MWAA Bucket Name

```bash
cd aws/terraform
terraform output mwaa_s3_bucket
```

### Step 4: Upload DAGs to MWAA

```bash
# Deploy DAGs
./scripts/deploy_mwaa_dags.sh

# Or manually specify bucket
./scripts/deploy_mwaa_dags.sh s3://your-mwaa-bucket-name
```

This uploads:
- All DAGs from `aws/dags/` to `s3://bucket/dags/`
- `requirements.txt` to `s3://bucket/requirements.txt`

### Step 5: Configure MWAA Environment Variables

In AWS Console:
1. Go to **MWAA** → Your environment
2. Click **Edit**
3. Under **Environment variables**, add:

| Key | Value |
|-----|-------|
| `PROJECT_A_ENV` | `dev` |
| `PROJECT_A_CONFIG_URI` | `s3://my-etl-artifacts-demo-424570854632/config/dev.yaml` |
| `PROJECT_A_EMR_APP_ID` | `00g0tm6kccmdcf09` |
| `PROJECT_A_EXEC_ROLE_ARN` | `arn:aws:iam::424570854632:role/project-a-dev-emr-exec` |
| `PROJECT_A_LAKE_BUCKET` | `my-etl-lake-demo-424570854632` |
| `PROJECT_A_ARTIFACTS_BUCKET` | `my-etl-artifacts-demo-424570854632` |

4. Click **Save** (MWAA will restart, takes ~5 minutes)

### Step 6: Configure MWAA Connection

In MWAA Airflow UI:
1. Go to **Admin** → **Connections**
2. Edit `aws_default`:
   - **Conn Type:** `Amazon Web Services`
   - **Extra:** `{"region_name": "us-east-1"}`
   - Leave login/password empty (uses MWAA role)

### Step 7: Access MWAA UI

Get MWAA webserver URL:

```bash
cd aws/terraform
terraform output mwaa_webserver_url
```

Or in AWS Console:
- **MWAA** → Your environment → **Open Airflow UI**

**Login:** Same as local (airflow/airflow) or as configured

### Step 8: Verify DAGs in MWAA

1. Go to **DAGs** page
2. You should see `project_a_daily_pipeline`
3. Toggle it **ON**
4. Trigger manually to test

---

## 📝 DAG Configuration

### Unified Entrypoint Pattern

The DAG (`project_a_daily_pipeline.py`) uses the unified entrypoint:

```python
job_driver = {
    "sparkSubmit": {
        "entryPoint": "s3://bucket/packages/project_a-0.1.0-py3-none-any.whl",
        "entryPointArguments": [
            "--job", "bronze_to_silver",
            "--env", "dev",
            "--config", "s3://bucket/config/dev.yaml",
            "--run-date", "2025-01-15"
        ],
        "sparkSubmitParameters": "..."
    }
}
```

### Available Jobs

- `fx_json_to_bronze` - Ingest FX JSON to Bronze
- `bronze_to_silver` - Transform Bronze → Silver
- `silver_to_gold` - Transform Silver → Gold
- `publish_gold_to_snowflake` - Publish Gold to Snowflake

### Schedule

Default schedule: **Daily at 2:00 AM UTC**

To change, edit DAG:

```python
schedule_interval="0 2 * * *",  # 2 AM UTC daily
```

---

## 🔧 Troubleshooting

### Local Airflow Issues

**DAGs not showing:**
```bash
# Restart scheduler
docker compose -f docker-compose-airflow.yml restart airflow-scheduler

# Check logs
docker logs airflow-scheduler
```

**Variables not set:**
```bash
# Re-run setup
./scripts/setup_airflow_local.sh
```

**AWS connection errors:**
- Ensure `~/.aws/credentials` exists
- Check Docker mounts AWS credentials: `- ${HOME}/.aws:/home/airflow/.aws:ro`

### MWAA Issues

**DAGs not syncing:**
- Wait 2-3 minutes after upload
- Check S3 bucket path: `s3://bucket/dags/`
- Verify DAG files are `.py` files

**EMR job failures:**
- Check MWAA execution role has EMR permissions
- Verify EMR app ID and role ARN in variables
- Check CloudWatch logs: `airflow-{env-name}-*`

**MWAA not starting:**
- Check VPC/subnet configuration
- Verify security groups allow outbound traffic
- Check CloudWatch logs for errors

### Common Errors

**"EMR app not found":**
- Verify `PROJECT_A_EMR_APP_ID` variable
- Check EMR app exists: `aws emr-serverless get-application --application-id ...`

**"Wheel not found":**
- Ensure wheel uploaded to S3: `s3://bucket/packages/project_a-0.1.0-py3-none-any.whl`
- Check `PROJECT_A_ARTIFACTS_BUCKET` variable

**"Config file not found":**
- Verify config uploaded: `s3://bucket/config/dev.yaml`
- Check `PROJECT_A_CONFIG_URI` variable

---

## ✅ Phase 5 Completion Checklist

### Local Airflow
- [ ] Docker containers running
- [ ] DAGs visible in UI
- [ ] Variables set
- [ ] AWS connection configured
- [ ] DAG runs successfully

### MWAA (Optional)
- [ ] `enable_mwaa=true` in Terraform
- [ ] MWAA environment created
- [ ] DAGs uploaded to S3
- [ ] Environment variables set
- [ ] Connection configured
- [ ] DAG runs successfully

---

## 📚 Additional Resources

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [MWAA Documentation](https://docs.aws.amazon.com/mwaa/)
- [EMR Serverless Documentation](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/)

---

**Status:** ✅ Phase 5 Complete

