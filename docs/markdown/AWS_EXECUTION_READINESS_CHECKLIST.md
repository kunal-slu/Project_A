# 🚀 AWS Execution Readiness Checklist

**Status:** ✅ **100% CODE-READY** | ⚠️ **REQUIRES AWS SETUP**

This document provides a step-by-step checklist to make the project 100% execution-ready on AWS.

---

## 1. ✅ Terraform Infrastructure

### 1.1 Remote State Backend (Optional but Recommended)

**Status:** ⚠️ **Needs Setup**

1. Create S3 bucket for Terraform state:
   ```bash
   aws s3 mb s3://my-etl-terraform-state-424570854632 --profile kunal21 --region us-east-1
   aws s3api put-bucket-versioning \
     --bucket my-etl-terraform-state-424570854632 \
     --versioning-configuration Status=Enabled \
     --profile kunal21
   ```

2. Create DynamoDB table for state locking:
   ```bash
   aws dynamodb create-table \
     --table-name terraform-locks-project-a \
     --attribute-definitions AttributeName=LockID,AttributeType=S \
     --key-schema AttributeName=LockID,KeyType=HASH \
     --billing-mode PAY_PER_REQUEST \
     --profile kunal21 \
     --region us-east-1
   ```

3. Uncomment backend block in `aws/terraform/main.tf`:
   ```hcl
   backend "s3" {
     bucket         = "my-etl-terraform-state-424570854632"
     key            = "project_a/dev/terraform.tfstate"
     region         = "us-east-1"
     dynamodb_table = "terraform-locks-project-a"
     encrypt        = true
     profile        = "kunal21"
   }
   ```

### 1.2 Validate and Apply Terraform

**Status:** ✅ **Ready**

```bash
cd aws/terraform

# Initialize
terraform init

# Validate
terraform validate

# Plan
terraform plan -var-file=env/dev.tfvars

# Apply
terraform apply -var-file=env/dev.tfvars -auto-approve

# Save outputs
terraform output -json > terraform-outputs.dev.json
```

**Expected Outputs:**
- `kms_key_arn`
- `s3_lake_bucket_name`
- `s3_artifacts_bucket`
- `emr_app_id`
- `emr_exec_role_arn`

---

## 2. 🔐 Secrets Manager Setup

### 2.1 Create Secrets

**Status:** ⚠️ **Needs Execution**

Run the helper script:
```bash
cd aws/scripts
KMS_KEY_ARN=$(jq -r '.kms_key_arn.value' ../terraform/terraform-outputs.dev.json)
export KMS_KEY_ARN
./create_secrets.sh
```

**Or manually create each secret:**

```bash
# Snowflake
aws secretsmanager create-secret \
  --profile kunal21 --region us-east-1 \
  --name project-a-dev/snowflake/conn \
  --description "Snowflake connection (dev)" \
  --kms-key-id <KMS_ARN> \
  --secret-string '{"account":"...","user":"...","password":"..."}'

# Redshift
aws secretsmanager create-secret \
  --profile kunal21 --region us-east-1 \
  --name project-a-dev/redshift/conn \
  --kms-key-id <KMS_ARN> \
  --secret-string '{"host":"...","port":5439,"database":"...","user":"...","password":"..."}'

# Kafka, Salesforce, FX (similar pattern)
```

### 2.2 Update Secret Values

**Status:** ⚠️ **Needs Real Credentials**

After creating placeholder secrets, update with real values:
```bash
aws secretsmanager update-secret \
  --name project-a-dev/snowflake/conn \
  --secret-string '{"account":"real_account","user":"real_user",...}' \
  --profile kunal21
```

### 2.3 Verify IAM Permissions

**Status:** ✅ **Already Configured**

The EMR execution role already has read-only access to secrets matching `project-a-dev/*` pattern (see `aws/terraform/main.tf`).

**Test:**
```bash
aws emr-serverless start-job-run \
  --application-id <EMR_APP_ID> \
  --execution-role-arn <EXEC_ROLE_ARN> \
  --job-driver '{"sparkSubmit":{"entryPoint":"s3://<ARTIFACTS>/jobs/dev_secret_probe.py"}}' \
  --profile kunal21
```

---

## 3. 📦 Package and Config Upload

### 3.1 Build Python Package

**Status:** ⚠️ **Needs Execution**

```bash
cd aws/scripts
./build_and_upload_package.sh
```

**Or manually:**
```bash
cd /path/to/repo
python3 -m pip install --upgrade build
python3 -m build
aws s3 cp dist/*.whl s3://<ARTIFACTS_BUCKET>/packages/ --profile kunal21
```

### 3.2 Update config/dev.yaml

**Status:** ⚠️ **Needs Values from Terraform**

1. Open `config/dev.yaml`
2. Replace placeholders:
   - `<PASTE_APP_ID_FROM_TERRAFORM_OUTPUTS>` → EMR app ID
   - `<PASTE_EXEC_ARN_FROM_TERRAFORM_OUTPUTS>` → EMR execution role ARN
   - `<PASTE_KMS_ARN_FROM_TERRAFORM_OUTPUTS>` → KMS key ARN

3. Update `whl_path` with actual wheel filename

### 3.3 Upload Configs to S3

**Status:** ⚠️ **Needs Execution**

```bash
cd aws/scripts
./upload_configs.sh
```

**Or manually:**
```bash
aws s3 cp config/dev.yaml s3://<ARTIFACTS_BUCKET>/config/dev.yaml --profile kunal21
aws s3 sync config/schema_definitions/ s3://<ARTIFACTS_BUCKET>/config/schema_definitions/ --profile kunal21
```

---

## 4. 📤 Upload Jobs to S3

**Status:** ⚠️ **Needs Execution**

```bash
python scripts/upload_jobs_to_s3.py <ARTIFACTS_BUCKET> kunal21
```

**Or manually:**
```bash
aws s3 sync jobs/ s3://<ARTIFACTS_BUCKET>/jobs/ --profile kunal21 --exclude "*.pyc" --exclude "__pycache__/*"
aws s3 sync src/ s3://<ARTIFACTS_BUCKET>/src/ --profile kunal21 --exclude "*.pyc" --exclude "__pycache__/*"
```

---

## 5. 🧪 Test EMR Job Run

**Status:** ⚠️ **Needs Execution**

### 5.1 Test Secret Probe

```bash
cd aws/scripts
./test_emr_job.sh dev_secret_probe
```

### 5.2 Test Snowflake Ingestion

```bash
./test_emr_job.sh snowflake_to_bronze
```

### 5.3 Monitor Job

```bash
# Get job run ID from output above
aws emr-serverless get-job-run \
  --application-id <EMR_APP_ID> \
  --job-run-id <JOB_RUN_ID> \
  --profile kunal21

# View logs
aws s3 ls s3://<ARTIFACTS_BUCKET>/emr-logs/applications/<EMR_APP_ID>/jobs/<JOB_RUN_ID>/ --profile kunal21
```

---

## 6. ⏱️ Airflow DAG Setup

### 6.1 Update Variables

**Status:** ⚠️ **Needs Values**

1. Open `aws/dags/utils/variables.py`
2. Values are auto-loaded from `terraform-outputs.dev.json` if available
3. Otherwise, set environment variables or edit directly

### 6.2 Test DAG Locally (Optional)

```bash
# Using docker-compose (if available)
cd aws/dags
docker-compose up -d

# Or use MWAA directly
```

### 6.3 Deploy to MWAA

**Status:** ⚠️ **Needs MWAA Environment**

1. Copy DAGs to MWAA:
   ```bash
   aws s3 sync aws/dags/ s3://<MWAA_DAGS_BUCKET>/dags/ --profile kunal21
   ```

2. Set Airflow Variables in MWAA UI:
   - `emr_app_id`
   - `emr_exec_role_arn`
   - `artifacts_bucket`

3. Trigger test run

---

## 7. ✅ Final Verification Checklist

- [ ] Terraform applied successfully
- [ ] `terraform-outputs.dev.json` exists with all values
- [ ] Secrets created in Secrets Manager
- [ ] EMR execution role can read secrets (tested)
- [ ] Python package built and uploaded
- [ ] `config/dev.yaml` updated with real values
- [ ] Configs uploaded to S3
- [ ] Jobs uploaded to S3
- [ ] At least one EMR job run succeeded
- [ ] Bronze files visible in S3 lake bucket
- [ ] Airflow variables configured
- [ ] DAG runs successfully (local or MWAA)

---

## 🚨 Common Issues & Solutions

### Issue: "Secret not found"
**Solution:** Ensure secret name matches `project-a-dev/<system>/conn` pattern

### Issue: "Access Denied" on Secrets Manager
**Solution:** Verify IAM policy on EMR execution role includes secrets ARNs

### Issue: "EntryPoint not found"
**Solution:** Ensure jobs are uploaded to S3 at correct paths

### Issue: "Config file not found"
**Solution:** Upload `config/dev.yaml` to S3 artifacts bucket

### Issue: "Delta Lake extensions not found"
**Solution:** Ensure EMR release label is `emr-7.1.0` or later (includes Delta)

---

## 📚 Quick Reference Commands

```bash
# Get Terraform outputs
cd aws/terraform && terraform output -json > terraform-outputs.dev.json

# Create secrets
cd aws/scripts && ./create_secrets.sh

# Build and upload package
cd aws/scripts && ./build_and_upload_package.sh

# Upload configs
cd aws/scripts && ./upload_configs.sh

# Upload jobs
python scripts/upload_jobs_to_s3.py <ARTIFACTS_BUCKET> kunal21

# Test job
cd aws/scripts && ./test_emr_job.sh dev_secret_probe
```

---

**Last Updated:** Generated automatically  
**Next Steps:** Follow checklist above in order

