# Phase 6: Observe & Govern - Complete Setup Guide

Complete guide for implementing observability and governance (CloudWatch, Lineage, Lake Formation).

## 📋 Table of Contents

1. [CloudWatch + SNS Alerts](#cloudwatch--sns-alerts)
2. [Data Lineage](#data-lineage)
3. [Lake Formation](#lake-formation)
4. [Integration](#integration)
5. [Verification](#verification)

---

## 📊 Part 1: CloudWatch + SNS Alerts

### Goal
- Email alerts when EMR jobs fail or run too long
- Dashboards showing EMR job health & data SLAs

### Step 1: Configure Terraform Variables

Edit `aws/terraform/env/dev.tfvars`:

```hcl
alarm_email = "kunal.ks5064@gmail.com"
enable_cloudwatch_dashboards = true
enable_emr_alarms = true
```

Or run the setup script:

```bash
./scripts/setup_phase6.sh
```

### Step 2: Apply Terraform

```bash
cd aws/terraform
terraform init
terraform plan -var-file=env/dev.tfvars
terraform apply -var-file=env/dev.tfvars
```

This creates:
- ✅ SNS topic: `project-a-dev-alerts`
- ✅ Email subscription
- ✅ CloudWatch alarms for EMR failures
- ✅ CloudWatch dashboard for monitoring

### Step 3: Confirm Email Subscription

1. Check your inbox: `kunal.ks5064@gmail.com`
2. Look for: **"AWS Notification – Subscription Confirmation"**
3. Click **"Confirm subscription"**

Verify:

```bash
aws sns list-topics --profile kunal21 --region us-east-1 | grep alerts
```

### Step 4: Verify CloudWatch Dashboard

1. Go to **CloudWatch Console** → **Dashboards**
2. Open: `project-a-dev-emr-monitoring`
3. You should see:
   - EMR Serverless job status (Failed/Succeeded/Running)
   - Custom EMR metrics (Success/Failures/Duration)
   - Job duration by job name

### Step 5: Test Alarms

**Option A: Force a failure (test):**

Temporarily add to a job:
```python
raise Exception("Test failure for alarm")
```

**Option B: Check existing failures:**

```bash
aws cloudwatch describe-alarms \
  --alarm-names "project-a-dev-emr-job-failures" \
  --profile kunal21 --region us-east-1 \
  --query 'MetricAlarms[0].[AlarmName,StateValue,StateReason]' \
  --output table
```

---

## 🔗 Part 2: Data Lineage

### Goal
Track data flow: "For any Gold table, trace back to Bronze/Silver tables and jobs that produced it."

### Step 1: Create Lineage Config

Config already exists: `config/lineage.yaml`

Upload to S3:

```bash
aws s3 cp config/lineage.yaml \
  s3://my-etl-artifacts-demo-424570854632/config/lineage.yaml \
  --profile kunal21 --region us-east-1
```

### Step 2: Configure Lineage Backend

Edit `config/lineage.yaml`:

```yaml
enabled: true
backend: "openlineage"  # or "marquez"
url: "http://localhost:5000"  # Replace with your Marquez/OpenLineage URL
namespace: "project_a_dev"
api_key: ""  # Optional if secured
```

**For local testing (Marquez):**

```bash
# Run Marquez locally (Docker)
docker run -d \
  -p 5000:5000 \
  -e POSTGRES_HOST=postgres \
  -e POSTGRES_PORT=5432 \
  -e POSTGRES_DB=marquez \
  -e POSTGRES_USER=marquez \
  -e POSTGRES_PASSWORD=marquez \
  marquezproject/marquez:latest
```

Then set in config:
```yaml
url: "http://localhost:5000"
```

### Step 3: Lineage Integration

Lineage is already integrated into jobs:

- ✅ `fx_json_to_bronze` - Emits lineage events
- ✅ `bronze_to_silver` - Emits lineage events
- ✅ `silver_to_gold` - Emits lineage events

**How it works:**

1. Job loads lineage config from S3
2. Creates `LineageEmitter` instance
3. Emits START event at job start
4. Emits COMPLETE/FAIL event at job end
5. Includes inputs/outputs datasets

**Example lineage event:**

```json
{
  "eventType": "COMPLETE",
  "job": {
    "namespace": "project_a_dev",
    "name": "bronze_to_silver"
  },
  "inputs": [
    {"namespace": "project_a_dev", "name": "bronze.crm.accounts"},
    {"namespace": "project_a_dev", "name": "bronze.snowflake.orders"}
  ],
  "outputs": [
    {"namespace": "project_a_dev", "name": "silver.customers"},
    {"namespace": "project_a_dev", "name": "silver.orders"}
  ]
}
```

### Step 4: Verify Lineage

**In Marquez/OpenLineage UI:**

1. Go to your Marquez UI (e.g., `http://localhost:5000`)
2. Navigate to **Jobs**
3. Find: `bronze_to_silver`, `silver_to_gold`
4. Check lineage graph shows:
   - Input datasets (Bronze)
   - Output datasets (Silver/Gold)
   - Run history with timestamps

**Check logs:**

```bash
# Check EMR logs for lineage emission
aws s3 ls "s3://my-etl-artifacts-demo-424570854632/emr-logs/" \
  --recursive --profile kunal21 | tail -5
```

Look for: `"✅ Emitted lineage event"` in logs

---

## 🔒 Part 3: Lake Formation

### Goal
Fine-grained access control: column/row-level permissions, audit trails, PII classification.

### Step 1: Enable Lake Formation in Terraform

Edit `aws/terraform/env/dev.tfvars`:

```hcl
enable_lake_formation = true
```

### Step 2: Apply Terraform

```bash
cd aws/terraform
terraform apply -var-file=env/dev.tfvars
```

This creates:
- ✅ Data lake settings with admins
- ✅ S3 resource registrations (bronze/silver/gold)
- ✅ Database permissions for EMR role

### Step 3: Configure Lake Formation (Console)

**3.1 Set Data Lake Admins:**

1. Go to **Lake Formation Console**
2. **Administrative roles and tasks**
3. Add your IAM user/role as **Data lake administrator**

**3.2 Register Data Locations:**

Terraform already registered:
- `s3://my-etl-lake-demo-424570854632/bronze`
- `s3://my-etl-lake-demo-424570854632/silver`
- `s3://my-etl-lake-demo-424570854632/gold`

Verify in console:
- **Data locations** → Should see all three

**3.3 Grant Table Permissions:**

1. Go to **Data permissions** → **Grant**
2. **Principals:** `arn:aws:iam::424570854632:role/project-a-bi-analyst` (if exists)
3. **Database:** `dev_gold`
4. **Tables:** Select all or specific tables
5. **Permissions:** `SELECT`, `DESCRIBE`
6. Click **Grant**

Now BI users can read Gold tables via Athena/Redshift Spectrum.

### Step 4: Optional - LF Tags for PII

**4.1 Create LF Tags:**

1. **LF-tags** → **Create LF-tag**
2. **Tag key:** `Classification`
3. **Tag values:** `PII`, `Confidential`, `Public`
4. Click **Create**

**4.2 Apply Tags to Columns:**

1. Go to **Tables** → Select table (e.g., `silver.customers`)
2. **Actions** → **Edit schema**
3. Select column (e.g., `email`)
4. **LF-tags** → Add `Classification=PII`
5. Save

**4.3 Grant Tag-Based Access:**

1. **Data permissions** → **Grant**
2. **Principals:** Privileged role
3. **LF-tags or catalog resources:** Select `Classification=PII`
4. **Permissions:** `SELECT`
5. Click **Grant**

Now only privileged roles can access PII-tagged columns.

---

## 🔧 Integration

### Metrics Emission

All jobs now emit CloudWatch metrics:

```python
# Success
emit_job_success(
    job_name="bronze_to_silver",
    duration_seconds=120.5,
    env="dev",
    rows_processed=50000
)

# Failure
emit_job_failure(
    job_name="bronze_to_silver",
    env="dev",
    error_type="ValueError"
)
```

**Metrics emitted:**
- `ProjectA/EMR.EMRJobSuccess` (Count)
- `ProjectA/EMR.EMRJobFailures` (Count)
- `ProjectA/EMR.EMRJobDurationSeconds` (Seconds)
- `ProjectA/EMR.EMRJobRowsProcessed` (Count)

### Lineage Emission

All jobs emit lineage events:

```python
emitter = LineageEmitter(lineage_config)
emitter.emit_job(
    job_name="bronze_to_silver",
    run_id=run_id,
    inputs=["bronze.crm.accounts", "bronze.snowflake.orders"],
    outputs=["silver.customers", "silver.orders"],
    status="SUCCESS"
)
```

### Run Audit Trail

All jobs write audit logs to S3:

```
s3://bucket/_audit/{env}/{job_name}/{date}/{run_id}.json
```

---

## ✅ Verification

### CloudWatch & SNS

```bash
# List SNS topics
aws sns list-topics --profile kunal21 --region us-east-1

# Check alarm state
aws cloudwatch describe-alarms \
  --alarm-names "project-a-dev-emr-job-failures" \
  --profile kunal21 --region us-east-1

# View dashboard
# Go to CloudWatch Console → Dashboards → project-a-dev-emr-monitoring
```

**Expected:**
- ✅ SNS topic exists
- ✅ Email subscription confirmed
- ✅ Alarms in OK/ALARM state
- ✅ Dashboard visible

### Lineage

```bash
# Check lineage config in S3
aws s3 ls "s3://my-etl-artifacts-demo-424570854632/config/lineage.yaml" \
  --profile kunal21 --region us-east-1

# Check Marquez/OpenLineage UI
# Navigate to: http://your-marquez-url
# Look for jobs: bronze_to_silver, silver_to_gold
```

**Expected:**
- ✅ Lineage config in S3
- ✅ Lineage events in Marquez UI
- ✅ Lineage graph shows data flow

### Lake Formation

```bash
# Check data lake settings
aws lakeformation get-data-lake-settings \
  --profile kunal21 --region us-east-1

# List registered resources
aws lakeformation list-resources \
  --profile kunal21 --region us-east-1
```

**Expected:**
- ✅ Data lake admins configured
- ✅ S3 locations registered
- ✅ Permissions granted

---

## 🎯 Phase 6 Completion Checklist

### CloudWatch & SNS
- [ ] `aws_sns_topic.project_a_alerts` exists
- [ ] Email subscription confirmed
- [ ] EMR job metrics (EMRJobSuccess, EMRJobFailures) are pushed
- [ ] CloudWatch alarm fires on failure and sends email
- [ ] Dashboard visible in CloudWatch

### Lineage
- [ ] `config/lineage.yaml` exists & uploaded to S3
- [ ] `LineageEmitter` wired into all jobs
- [ ] Lineage events appear in Marquez/OpenLineage UI
- [ ] Lineage graph shows correct inputs/outputs

### Lake Formation
- [ ] `enable_lake_formation=true` applied via Terraform
- [ ] Data lake admins configured
- [ ] S3 data locations (bronze/silver/gold) registered
- [ ] Grants in Lake Formation for BI/DS roles on gold
- [ ] Optional LF tags for PII columns

---

## 📝 Files Created/Updated

1. ✅ `aws/terraform/cloudwatch.tf` - Enhanced with dashboards and alarms
2. ✅ `src/pyspark_interview_project/utils/cloudwatch_metrics.py` - Metrics emission
3. ✅ `src/pyspark_interview_project/monitoring/lineage_emitter.py` - Lineage emitter
4. ✅ `config/lineage.yaml` - Lineage configuration
5. ✅ `aws/terraform/lake_formation.tf` - Enhanced with resource registration
6. ✅ `src/project_a/jobs/*.py` - All jobs integrated with metrics & lineage
7. ✅ `scripts/setup_phase6.sh` - Setup script
8. ✅ `docs/PHASE_6_OBSERVE_GOVERN.md` - Complete documentation

---

## 🚀 Quick Start

```bash
# 1. Setup Phase 6
./scripts/setup_phase6.sh

# 2. Apply Terraform
cd aws/terraform
terraform apply -var-file=env/dev.tfvars

# 3. Confirm SNS email subscription (check inbox)

# 4. Run a job to test metrics/lineage
# (via Airflow or EMR directly)

# 5. Verify in CloudWatch dashboard
# (Console → Dashboards → project-a-dev-emr-monitoring)
```

---

**Status:** ✅ Phase 6 Complete

All observability and governance features are implemented and ready to use!

