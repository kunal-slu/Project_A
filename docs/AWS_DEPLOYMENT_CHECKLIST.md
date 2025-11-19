# AWS Deployment Checklist

**Status:** Infrastructure defined in Terraform, needs to be applied to AWS

---

## 📋 What's Defined vs. What's Deployed

### ✅ Defined in Terraform (Ready to Deploy)

| Resource | Terraform File | Status |
|----------|---------------|--------|
| **S3 Buckets** | `main.tf` | ✅ Defined |
| **IAM Roles** | `iam.tf` | ✅ Defined |
| **EMR Serverless** | `main.tf` | ✅ Defined |
| **CloudWatch Logs** | `cloudwatch.tf` | ✅ Defined |
| **SNS Topic** | `cloudwatch.tf` | ✅ Defined |
| **CloudWatch Alarms** | `cloudwatch.tf` | ✅ Defined |
| **CloudWatch Dashboard** | `cloudwatch.tf` | ✅ Defined |
| **Lake Formation** | `lake_formation.tf` | ✅ Defined |
| **Glue Databases** | `glue.tf` | ✅ Defined |
| **KMS Keys** | `main.tf` | ✅ Defined |
| **MWAA** | `mwaa.tf` | ✅ Defined (optional) |

### ⚠️ Not Yet Deployed (Needs Action)

All resources above need to be **applied via Terraform**.

---

## 🚀 Step-by-Step Deployment Guide

### Step 1: Apply Terraform Infrastructure

```bash
cd aws/terraform

# Initialize Terraform
terraform init

# Review plan
terraform plan -var-file=env/dev.tfvars

# Apply infrastructure
terraform apply -var-file=env/dev.tfvars
```

**This creates:**
- ✅ S3 buckets (data lake, artifacts, logs)
- ✅ IAM roles (EMR execution role)
- ✅ EMR Serverless application
- ✅ CloudWatch log groups
- ✅ SNS topic (`project-a-dev-alerts`)
- ✅ CloudWatch alarms
- ✅ CloudWatch dashboard
- ✅ Lake Formation settings
- ✅ Lake Formation resource registrations
- ✅ Glue databases (bronze, silver, gold)
- ✅ KMS keys for encryption
- ✅ MWAA environment (if `enable_mwaa = true`)

**Expected Output:**
```
Apply complete! Resources: XX added, 0 changed, 0 destroyed.
```

---

### Step 2: Confirm SNS Email Subscription

**After Terraform apply:**

1. Check your email inbox: `kunal.ks5064@gmail.com`
2. Look for: **"AWS Notification – Subscription Confirmation"**
3. Click **"Confirm subscription"**

**Verify:**
```bash
aws sns list-subscriptions-by-topic \
  --topic-arn arn:aws:sns:us-east-1:424570854632:project-a-dev-alerts \
  --profile kunal21 --region us-east-1
```

**Expected:** Subscription status = `Confirmed`

---

### Step 3: Upload Artifacts to S3

**Upload required files to artifacts bucket:**

```bash
export AWS_PROFILE=kunal21
export AWS_REGION=us-east-1
export ARTIFACTS_BUCKET=my-etl-artifacts-demo-424570854632

# 1. Build and upload wheel
cd ~/IdeaProjects/pyspark_data_engineer_project
python -m build
WHEEL_PATH=$(ls dist/project_a-*.whl | tail -n 1)
aws s3 cp "$WHEEL_PATH" \
  "s3://${ARTIFACTS_BUCKET}/packages/" \
  --profile "$AWS_PROFILE" --region "$AWS_REGION"

# 2. Upload dependencies
aws s3 cp dist/emr_deps_pyyaml.zip \
  "s3://${ARTIFACTS_BUCKET}/packages/" \
  --profile "$AWS_PROFILE" --region "$AWS_REGION"

# 3. Upload config files
aws s3 cp config/dev.yaml \
  "s3://${ARTIFACTS_BUCKET}/config/dev.yaml" \
  --profile "$AWS_PROFILE" --region "$AWS_REGION"

aws s3 cp config/lineage.yaml \
  "s3://${ARTIFACTS_BUCKET}/config/lineage.yaml" \
  --profile "$AWS_PROFILE" --region "$AWS_REGION"
```

**Verify:**
```bash
aws s3 ls "s3://${ARTIFACTS_BUCKET}/packages/" --profile "$AWS_PROFILE"
aws s3 ls "s3://${ARTIFACTS_BUCKET}/config/" --profile "$AWS_PROFILE"
```

---

### Step 4: Configure Lake Formation

**After Terraform apply:**

1. **Go to Lake Formation Console:**
   - https://console.aws.amazon.com/lakeformation/

2. **Set Data Lake Administrators:**
   - Administrative roles and tasks
   - Add your IAM user/role as Data lake administrator

3. **Verify Resource Registrations:**
   - Data locations
   - Should see:
     - `s3://my-etl-lake-demo-424570854632/bronze`
     - `s3://my-etl-lake-demo-424570854632/silver`
     - `s3://my-etl-lake-demo-424570854632/gold`

4. **Grant Table Permissions (Optional):**
   - Data permissions → Grant
   - Grant SELECT on gold tables to BI roles
   - Grant ALL on bronze/silver to data engineer roles

5. **Create LF Tags (Optional):**
   - LF-tags → Create LF-tag
   - Tag key: `Classification`
   - Values: `PII`, `Confidential`, `Public`
   - Apply to sensitive columns

---

### Step 5: Test EMR Jobs

**Run test jobs to verify everything works:**

```bash
export EMR_APP_ID=$(aws emr-serverless list-applications \
  --profile kunal21 --region us-east-1 \
  --query 'applications[0].applicationId' --output text)

export EMR_ROLE_ARN=arn:aws:iam::424570854632:role/project-a-dev-emr-exec
export ARTIFACTS_BUCKET=my-etl-artifacts-demo-424570854632

# Test 1: FX JSON to Bronze
aws emr-serverless start-job-run \
  --application-id "$EMR_APP_ID" \
  --execution-role-arn "$EMR_ROLE_ARN" \
  --profile kunal21 --region us-east-1 \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://'${ARTIFACTS_BUCKET}'/packages/project_a-0.1.0-py3-none-any.whl",
      "entryPointArguments": [
        "--job", "fx_json_to_bronze",
        "--env", "dev",
        "--config", "s3://'${ARTIFACTS_BUCKET}'/config/dev.yaml"
      ],
      "sparkSubmitParameters": "--py-files s3://'${ARTIFACTS_BUCKET}'/packages/project_a-0.1.0-py3-none-any.whl,s3://'${ARTIFACTS_BUCKET}'/packages/emr_deps_pyyaml.zip --packages io.delta:delta-core_2.12:2.4.0"
    }
  }'

# Check job status
aws emr-serverless list-job-runs \
  --application-id "$EMR_APP_ID" \
  --profile kunal21 --region us-east-1 \
  --max-results 5 \
  --query 'jobRuns[*].[jobRunId,state]' \
  --output table
```

**Verify outputs:**
```bash
export LAKE_BUCKET=my-etl-lake-demo-424570854632

# Check bronze layer
aws s3 ls "s3://${LAKE_BUCKET}/bronze/" --recursive --profile kunal21 | head -20

# Check silver layer (after bronze_to_silver job)
aws s3 ls "s3://${LAKE_BUCKET}/silver/" --recursive --profile kunal21 | head -20

# Check gold layer (after silver_to_gold job)
aws s3 ls "s3://${LAKE_BUCKET}/gold/" --recursive --profile kunal21 | head -20
```

---

### Step 6: Verify Monitoring

**Check CloudWatch Dashboard:**
1. Go to CloudWatch Console → Dashboards
2. Open: `project-a-dev-emr-monitoring`
3. Verify metrics are showing

**Check SNS Topic:**
```bash
aws sns list-topics --profile kunal21 --region us-east-1 | grep alerts
```

**Check CloudWatch Alarms:**
```bash
aws cloudwatch describe-alarms \
  --alarm-names "project-a-dev-emr-job-failures" \
  --profile kunal21 --region us-east-1 \
  --query 'MetricAlarms[0].[AlarmName,StateValue]' \
  --output table
```

**Check Metrics (after running a job):**
```bash
aws cloudwatch get-metric-statistics \
  --namespace "ProjectA/EMR" \
  --metric-name "EMRJobSuccess" \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Sum \
  --profile kunal21 --region us-east-1
```

---

### Step 7: MWAA Setup (Optional)

**If `enable_mwaa = true` in Terraform:**

1. **Upload DAGs:**
   ```bash
   ./scripts/deploy_mwaa_dags.sh
   ```

2. **Configure MWAA Environment Variables:**
   - Go to MWAA Console
   - Edit environment
   - Add variables:
     - `PROJECT_A_ENV=dev`
     - `PROJECT_A_EMR_APP_ID=<app-id>`
     - `PROJECT_A_EXEC_ROLE_ARN=<role-arn>`
     - `PROJECT_A_ARTIFACTS_BUCKET=<bucket>`
     - `PROJECT_A_CONFIG_URI=s3://<bucket>/config/dev.yaml`

3. **Configure MWAA Connections:**
   - Connections → Add
   - Connection Id: `aws_default`
   - Connection Type: `Amazon Web Services`
   - Use MWAA's IAM role

4. **Trigger DAG:**
   - Go to MWAA UI
   - Find `project_a_daily_pipeline`
   - Click "Trigger DAG"

---

## 📊 Deployment Status Tracking

### ✅ Completed
- [ ] Terraform infrastructure applied
- [ ] SNS email subscription confirmed
- [ ] Artifacts uploaded to S3
- [ ] Lake Formation configured
- [ ] EMR jobs tested
- [ ] Monitoring verified
- [ ] MWAA setup (if enabled)

### ⚠️ Pending
- [ ] All items above need to be completed

---

## 🔍 Quick Verification Commands

```bash
# 1. Check Terraform state
cd aws/terraform
terraform show

# 2. Check S3 buckets
aws s3 ls --profile kunal21 | grep etl

# 3. Check EMR application
aws emr-serverless list-applications --profile kunal21 --region us-east-1

# 4. Check SNS topic
aws sns list-topics --profile kunal21 --region us-east-1 | grep alerts

# 5. Check CloudWatch dashboard
aws cloudwatch list-dashboards --profile kunal21 --region us-east-1 | grep emr-monitoring

# 6. Check Lake Formation
aws lakeformation get-data-lake-settings --profile kunal21 --region us-east-1
```

---

## 🎯 Next Steps

1. **Run Step 1:** Apply Terraform infrastructure
2. **Run Step 2:** Confirm SNS email subscription
3. **Run Step 3:** Upload artifacts to S3
4. **Run Step 4:** Configure Lake Formation
5. **Run Step 5:** Test EMR jobs
6. **Run Step 6:** Verify monitoring
7. **Run Step 7:** Setup MWAA (optional)

---

**Status:** Infrastructure defined, ready for deployment

