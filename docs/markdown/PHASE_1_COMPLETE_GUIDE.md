# 🏗️ Phase 1: Complete Step-by-Step Guide

**Goal:** Deploy all AWS infrastructure using Terraform  
**Time:** 15-20 minutes  
**Prerequisites:** AWS CLI configured with profile `kunal21`

---

## ✅ Pre-Flight Checklist

Before starting, verify:
- [ ] AWS CLI installed: `aws --version`
- [ ] AWS credentials configured: `aws sts get-caller-identity --profile kunal21`
- [ ] Terraform installed: `terraform version` (needs >= 1.5.0)
- [ ] You're in the project root directory

---

## 📋 Step-by-Step Instructions

### Step 1: Navigate to Terraform Directory

```bash
cd ~/IdeaProjects/pyspark_data_engineer_project/aws/terraform
pwd
# Should show: .../pyspark_data_engineer_project/aws/terraform
```

**Verify files exist:**
```bash
ls -la
# Should see: main.tf, iam.tf, glue_catalog.tf, cloudwatch.tf, secrets.tf, etc.
```

---

### Step 2: Verify/Create env/dev.tfvars

**Current status:** ✅ File exists at `env/dev.tfvars`

**Verify content:**
```bash
cat env/dev.tfvars
```

**Expected content:**
```hcl
project_name = "project-a"
environment  = "dev"
aws_region   = "us-east-1"
aws_profile  = "kunal21"
account_id   = "424570854632"

buckets = {
  lake      = "my-etl-lake-demo-424570854632"
  code      = "my-etl-code-demo-424570854632"
  logs      = "my-etl-logs-demo-424570854632"
  artifacts = "my-etl-artifacts-demo-424570854632"
}

alarm_email = "kunal.ks5064@gmail.com"
```

**✅ Your file already has this!** No changes needed.

---

### Step 3: Check Terraform Backend (Optional)

**Current status:** ✅ Backend is commented out (using local state)

The backend block in `main.tf` is commented out, so Terraform will use local `terraform.tfstate` file. This is fine for dev.

**If you want remote state later:**
1. Create S3 bucket: `my-etl-terraform-state-424570854632`
2. Create DynamoDB table: `terraform-locks-project-a`
3. Uncomment backend block in `main.tf`

**For now:** ✅ Skip this step, use local state.

---

### Step 4: Initialize Terraform

```bash
cd ~/IdeaProjects/pyspark_data_engineer_project/aws/terraform
terraform init
```

**Expected output:**
```
Initializing the backend...
Initializing provider plugins...
- Finding hashicorp/aws versions matching ">= 5.0"...
- Installing hashicorp/aws 5.x.x...
Terraform has been successfully initialized!
```

**If you see errors:**
- Backend errors → Check S3 bucket exists (if using remote state)
- Provider errors → Check internet connection

---

### Step 5: Validate Configuration

```bash
terraform validate
```

**Expected output:**
```
Success! The configuration is valid.
```

**If validation fails:**
- Check error message for line numbers
- Common issues: missing braces, duplicate resources, undefined variables

---

### Step 6: Format Terraform Files (Optional but Recommended)

```bash
terraform fmt
```

This auto-formats all `.tf` files for consistency.

---

### Step 7: Plan (Preview Changes)

```bash
terraform plan -var-file=env/dev.tfvars
```

**What to look for:**
- `+` lines = resources to be CREATED
- `~` lines = resources to be MODIFIED
- `-` lines = resources to be DESTROYED (should be minimal)

**Expected resources to create:**
- `aws_s3_bucket.data_lake` (+)
- `aws_s3_bucket.artifacts` (+)
- `aws_s3_bucket_versioning.*` (+)
- `aws_s3_bucket_server_side_encryption_configuration.*` (+)
- `aws_s3_bucket_public_access_block.*` (+)
- `aws_s3_bucket_lifecycle_configuration.*` (+)
- `aws_kms_key.cmk` (+)
- `aws_kms_alias.cmk_alias` (+)
- `aws_iam_role.emr_exec` (+)
- `aws_iam_role_policy.emr_exec` (+)
- `aws_iam_role.glue` (+)
- `aws_emrserverless_application.spark` (+)
- `aws_glue_catalog_database.bronze` (+)
- `aws_glue_catalog_database.silver` (+)
- `aws_glue_catalog_database.gold` (+)
- `aws_cloudwatch_log_group.emr_serverless` (+)
- `aws_cloudwatch_log_group.application` (+)
- `aws_sns_topic.data_platform_alerts` (+)

**Review the plan carefully!** Make sure it's creating what you expect.

---

### Step 8: Apply (Create Resources)

```bash
terraform apply -var-file=env/dev.tfvars -auto-approve
```

**This will:**
1. Create all AWS resources
2. Take 3-5 minutes
3. Show progress for each resource
4. Save state to `terraform.tfstate`

**Expected output:**
```
Apply complete! Resources: X added, Y changed, Z destroyed.
```

**If apply fails:**
- Check error message
- Common issues:
  - IAM permissions (need admin/terraform permissions)
  - Bucket name conflicts (if buckets already exist)
  - Rate limiting (wait and retry)

---

### Step 9: Export Outputs

```bash
terraform output -json > terraform-outputs.dev.json
cat terraform-outputs.dev.json
```

**Expected outputs:**
```json
{
  "emr_app_id": {
    "value": "00f0v0example123456"
  },
  "emr_exec_role_arn": {
    "value": "arn:aws:iam::424570854632:role/project-a-dev-emr-exec"
  },
  "kms_key_arn": {
    "value": "arn:aws:kms:us-east-1:424570854632:key/12345678-1234-1234-1234-123456789abc"
  },
  "s3_artifacts_bucket": {
    "value": "my-etl-artifacts-demo-424570854632"
  },
  "s3_lake_bucket_name": {
    "value": "my-etl-lake-demo-424570854632"
  }
}
```

**✅ Save this file!** You'll need these values in Phase 2.

---

### Step 10: Verify in AWS

#### 10.1 S3 Buckets
```bash
aws s3 ls --profile kunal21 --region us-east-1 | grep "my-etl-"
```

**Expected:**
```
my-etl-artifacts-demo-424570854632
my-etl-lake-demo-424570854632
```

**Check bucket settings:**
```bash
# Versioning
aws s3api get-bucket-versioning --bucket my-etl-lake-demo-424570854632 --profile kunal21
# Should show: Status=Enabled

# Encryption
aws s3api get-bucket-encryption --bucket my-etl-lake-demo-424570854632 --profile kunal21
# Should show: SSE-KMS with your CMK ARN
```

#### 10.2 EMR Serverless
```bash
aws emr-serverless list-applications --profile kunal21 --region us-east-1 \
  --query 'applications[*].[name,state,id]' --output table
```

**Expected:**
```
|  project-a-dev-spark  |  CREATED  |  00f0v0example123456  |
```

#### 10.3 Glue Databases
```bash
aws glue get-databases --profile kunal21 --region us-east-1 \
  --query 'DatabaseList[*].Name' --output table
```

**Expected:**
```
|  project-a_bronze_dev  |
|  project-a_silver_dev   |
|  project-a_gold_dev     |
```

#### 10.4 IAM Roles
```bash
aws iam list-roles --profile kunal21 \
  --query 'Roles[?contains(RoleName, `project-a-dev`)].RoleName' --output table
```

**Expected:**
```
|  project-a-dev-emr-exec          |
|  project-a-dev-emr-service-role  |
|  project-a-dev-glue              |
```

#### 10.5 CloudWatch Log Groups
```bash
aws logs describe-log-groups --log-group-name-prefix "/aws/emr-serverless" \
  --profile kunal21 --region us-east-1 \
  --query 'logGroups[*].logGroupName' --output table
```

**Expected:**
```
|  /aws/emr-serverless/spark/project-a-dev  |
```

---

## 🚀 Quick Deployment Script

**Or use the automated script:**

```bash
cd ~/IdeaProjects/pyspark_data_engineer_project
./aws/scripts/deploy_phase1.sh
```

This script will:
1. ✅ Check all prerequisites
2. ✅ Initialize Terraform
3. ✅ Validate configuration
4. ✅ Run plan (with confirmation)
5. ✅ Apply changes (with confirmation)
6. ✅ Export outputs
7. ✅ Run verification checks

---

## ✅ Phase 1 Completion Checklist

Use this checklist to verify completion:

- [ ] `terraform init` completed without errors
- [ ] `terraform validate` → Success!
- [ ] `terraform plan` shows expected resources
- [ ] `terraform apply` completed successfully
- [ ] `terraform-outputs.dev.json` exists and has values
- [ ] S3 buckets exist (lake, artifacts)
- [ ] S3 buckets have versioning enabled
- [ ] S3 buckets have encryption enabled (SSE-KMS)
- [ ] EMR Serverless app exists and is CREATED/STARTED
- [ ] Glue databases exist (bronze, silver, gold)
- [ ] IAM roles exist (emr-exec, glue, emr-service-role)
- [ ] CloudWatch log groups exist
- [ ] KMS key exists with alias
- [ ] SNS topic exists

---

## 🐛 Troubleshooting

### Error: "InvalidClientTokenId"
**Fix:** Configure AWS credentials
```bash
aws configure --profile kunal21
# Enter: Access Key ID, Secret Access Key, Region (us-east-1)
```

### Error: "BucketAlreadyExists"
**Fix:** Buckets already exist. Either:
1. Import existing buckets: `terraform import aws_s3_bucket.data_lake my-etl-lake-demo-424570854632`
2. Or use different bucket names in `dev.tfvars`

### Error: "AccessDenied"
**Fix:** Your IAM user needs permissions:
- `s3:*`
- `iam:*`
- `emr-serverless:*`
- `glue:*`
- `kms:*`
- `logs:*`
- `sns:*`

### Error: "ResourceConflictException" (EMR)
**Fix:** EMR app might already exist. Check:
```bash
aws emr-serverless list-applications --profile kunal21 --region us-east-1
```

---

## 📝 Next Steps After Phase 1

Once Phase 1 is complete:

1. **Update config/dev.yaml** with values from `terraform-outputs.dev.json`
2. **Phase 2:** Create Secrets Manager entries
3. **Phase 3:** Upload jobs and configs to S3
4. **Phase 4:** Test EMR job run

---

## 🎯 Quick Command Reference

```bash
# Navigate
cd ~/IdeaProjects/pyspark_data_engineer_project/aws/terraform

# Initialize
terraform init

# Validate
terraform validate

# Format
terraform fmt

# Plan
terraform plan -var-file=env/dev.tfvars

# Apply
terraform apply -var-file=env/dev.tfvars -auto-approve

# Export outputs
terraform output -json > terraform-outputs.dev.json

# Verify
cd ../..
./aws/scripts/verify_phase1.sh
```

---

**Ready to start?** Run the commands above in order, or use the automated script!

