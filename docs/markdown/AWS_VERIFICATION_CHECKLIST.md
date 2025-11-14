# AWS Console & CLI Verification Checklist - Phase 1

**Purpose:** Verify all Terraform resources are correctly deployed  
**Profile:** kunal21  
**Region:** us-east-1

---

## 🔍 Pre-Flight Checks

### 1. Verify AWS Credentials
```bash
aws sts get-caller-identity --profile kunal21
```
**Expected:** Returns your account ID (424570854632) and user/role ARN

---

## 📦 S3 Buckets Verification

### 2.1 List All Buckets
```bash
aws s3 ls --profile kunal21 --region us-east-1
```
**Expected:** See these buckets:
- `my-etl-lake-demo-424570854632`
- `my-etl-artifacts-demo-424570854632`
- `my-etl-code-demo-424570854632` (if created)
- `my-etl-logs-demo-424570854632` (if created)

### 2.2 Check Bucket Versioning
```bash
aws s3api get-bucket-versioning --bucket my-etl-lake-demo-424570854632 --profile kunal21
aws s3api get-bucket-versioning --bucket my-etl-artifacts-demo-424570854632 --profile kunal21
```
**Expected:** `Status: Enabled`

### 2.3 Check Bucket Encryption
```bash
aws s3api get-bucket-encryption --bucket my-etl-lake-demo-424570854632 --profile kunal21
aws s3api get-bucket-encryption --bucket my-etl-artifacts-demo-424570854632 --profile kunal21
```
**Expected:** SSE-KMS with your CMK ARN

### 2.4 Check Public Access Block
```bash
aws s3api get-public-access-block --bucket my-etl-lake-demo-424570854632 --profile kunal21
aws s3api get-public-access-block --bucket my-etl-artifacts-demo-424570854632 --profile kunal21
```
**Expected:** All 4 flags should be `true`:
- `BlockPublicAcls: true`
- `BlockPublicPolicy: true`
- `IgnorePublicAcls: true`
- `RestrictPublicBuckets: true`

### 2.5 Check Lifecycle Configuration
```bash
aws s3api get-bucket-lifecycle-configuration --bucket my-etl-lake-demo-424570854632 --profile kunal21
aws s3api get-bucket-lifecycle-configuration --bucket my-etl-artifacts-demo-424570854632 --profile kunal21
```
**Expected:** Lifecycle rules for STANDARD_IA and GLACIER transitions

---

## 🔐 KMS Key Verification

### 3.1 Get KMS Key ARN (from Terraform outputs)
```bash
cd aws/terraform
terraform output kms_key_arn
```
**Or manually:**
```bash
aws kms list-aliases --profile kunal21 --region us-east-1 | grep project-a-dev-cmk
```

### 3.2 Describe KMS Key
```bash
KMS_ARN=$(cd aws/terraform && terraform output -raw kms_key_arn 2>/dev/null || echo "arn:aws:kms:us-east-1:424570854632:key/...")
aws kms describe-key --key-id "$KMS_ARN" --profile kunal21 --region us-east-1
```
**Expected:**
- `KeyState: Enabled`
- `KeyRotationEnabled: true`

---

## 🚀 EMR Serverless Verification

### 4.1 List EMR Serverless Applications
```bash
aws emr-serverless list-applications --profile kunal21 --region us-east-1
```
**Expected:** See application with name `project-a-dev-spark`

### 4.2 Get Application Details
```bash
APP_ID=$(cd aws/terraform && terraform output -raw emr_app_id 2>/dev/null || aws emr-serverless list-applications --profile kunal21 --region us-east-1 --query 'applications[?name==`project-a-dev-spark`].id' --output text)
aws emr-serverless get-application --application-id "$APP_ID" --profile kunal21 --region us-east-1
```
**Expected:**
- `state: CREATED` or `STARTED`
- `releaseLabel: emr-7.1.0`
- `type: SPARK`
- `autoStartConfiguration.enabled: true`
- `autoStopConfiguration.enabled: true`

---

## 👤 IAM Roles Verification

### 5.1 EMR Execution Role
```bash
aws iam get-role --role-name project-a-dev-emr-exec --profile kunal21
```
**Expected:**
- Role exists
- Trust policy allows `emr-serverless.amazonaws.com` to assume role
- Has inline policy with S3, KMS, Glue, Secrets Manager permissions

### 5.2 EMR Service Role
```bash
aws iam get-role --role-name project-a-dev-emr-service-role --profile kunal21
```
**Expected:** Role exists with trust policy for `emr-serverless.amazonaws.com`

### 5.3 Glue Role
```bash
aws iam get-role --role-name project-a-dev-glue --profile kunal21
```
**Expected:** Role exists with trust policy for `glue.amazonaws.com`

### 5.4 Check EMR Execution Role Policies
```bash
aws iam list-role-policies --role-name project-a-dev-emr-exec --profile kunal21
aws iam get-role-policy --role-name project-a-dev-emr-exec --policy-name project-a-dev-emr-exec-policy --profile kunal21
```
**Expected:** Policy allows:
- S3: GetObject, PutObject, ListBucket, DeleteObject (on lake/artifacts buckets)
- KMS: Encrypt, Decrypt, GenerateDataKey, DescribeKey
- Glue: GetDatabase, CreateTable, UpdateTable, etc.
- Secrets Manager: GetSecretValue, DescribeSecret (for project-a-dev/* secrets)
- CloudWatch Logs: CreateLogStream, PutLogEvents

---

## 📊 Glue Catalog Verification

### 6.1 List Databases
```bash
aws glue get-databases --profile kunal21 --region us-east-1
```
**Expected:** See these databases:
- `project-a_bronze_dev`
- `project-a_silver_dev`
- `project-a_gold_dev`

### 6.2 Get Database Details
```bash
aws glue get-database --name project-a_bronze_dev --profile kunal21 --region us-east-1
aws glue get-database --name project-a_silver_dev --profile kunal21 --region us-east-1
aws glue get-database --name project-a_gold_dev --profile kunal21 --region us-east-1
```
**Expected:** Each database exists with correct description and tags

---

## 📝 CloudWatch Logs Verification

### 7.1 List Log Groups
```bash
aws logs describe-log-groups --log-group-name-prefix /aws/emr-serverless/ --profile kunal21 --region us-east-1
aws logs describe-log-groups --log-group-name-prefix /aws/data-platform/ --profile kunal21 --region us-east-1
```
**Expected:** See:
- `/aws/emr-serverless/spark/project-a-dev`
- `/aws/data-platform/project-a-dev`

### 7.2 Check Log Group Retention
```bash
aws logs describe-log-groups --log-group-name-prefix /aws/emr-serverless/ --profile kunal21 --region us-east-1 --query 'logGroups[*].[logGroupName,retentionInDays]' --output table
```
**Expected:** Retention set to 30 days

---

## 🔔 SNS Topic Verification

### 8.1 List SNS Topics
```bash
aws sns list-topics --profile kunal21 --region us-east-1
```
**Expected:** See topic: `project-a-dev-data-platform-alerts`

### 8.2 Get Topic Attributes
```bash
TOPIC_ARN=$(aws sns list-topics --profile kunal21 --region us-east-1 --query 'Topics[?contains(TopicArn, `project-a-dev-data-platform-alerts`)].TopicArn' --output text)
aws sns get-topic-attributes --topic-arn "$TOPIC_ARN" --profile kunal21 --region us-east-1
```

### 8.3 Check Subscriptions
```bash
aws sns list-subscriptions-by-topic --topic-arn "$TOPIC_ARN" --profile kunal21 --region us-east-1
```
**Expected:** Email subscription to `kunal.ks5064@gmail.com` (may show as PendingConfirmation)

---

## 🏗️ Lake Formation Verification (if enabled)

### 9.1 Check Data Lake Settings
```bash
aws lakeformation get-data-lake-settings --profile kunal21 --region us-east-1
```
**Expected:** EMR execution role listed as admin

### 9.2 Check Permissions
```bash
aws lakeformation list-permissions --profile kunal21 --region us-east-1
```
**Expected:** Permissions for EMR role on bronze/silver/gold databases

---

## 🔐 Secrets Manager Verification (Phase 2, but good to check)

### 10.1 List Secrets
```bash
aws secretsmanager list-secrets --profile kunal21 --region us-east-1 --query "SecretList[?starts_with(Name, 'project-a-dev/')].Name" --output table
```
**Expected:** (Phase 2) Secrets for:
- `project-a-dev/snowflake/conn`
- `project-a-dev/redshift/conn`
- `project-a-dev/kafka/conn`
- etc.

**Note:** These may not exist yet if Phase 2 hasn't been completed.

---

## ✅ Quick Verification Script

Run this script to check everything at once:

```bash
#!/bin/bash
# Quick Phase 1 Verification Script

PROFILE="kunal21"
REGION="us-east-1"

echo "🔍 Phase 1 Verification Checklist"
echo "=================================="
echo ""

# 1. AWS Identity
echo "1. AWS Identity:"
aws sts get-caller-identity --profile $PROFILE --region $REGION
echo ""

# 2. S3 Buckets
echo "2. S3 Buckets:"
aws s3 ls --profile $PROFILE --region $REGION | grep "my-etl-"
echo ""

# 3. EMR Serverless
echo "3. EMR Serverless Applications:"
aws emr-serverless list-applications --profile $PROFILE --region $REGION --query 'applications[*].[name,state,id]' --output table
echo ""

# 4. Glue Databases
echo "4. Glue Databases:"
aws glue get-databases --profile $PROFILE --region $REGION --query 'DatabaseList[*].Name' --output table
echo ""

# 5. IAM Roles
echo "5. IAM Roles:"
aws iam list-roles --profile $PROFILE --query 'Roles[?contains(RoleName, `project-a-dev`)].RoleName' --output table
echo ""

# 6. CloudWatch Log Groups
echo "6. CloudWatch Log Groups:"
aws logs describe-log-groups --log-group-name-prefix /aws/emr-serverless/ --profile $PROFILE --region $REGION --query 'logGroups[*].logGroupName' --output table
echo ""

# 7. SNS Topics
echo "7. SNS Topics:"
aws sns list-topics --profile $PROFILE --region $REGION --query 'Topics[?contains(TopicArn, `project-a-dev`)].TopicArn' --output table
echo ""

# 8. KMS Keys
echo "8. KMS Keys:"
aws kms list-aliases --profile $PROFILE --region $REGION --query 'Aliases[?contains(AliasName, `project-a-dev`)].AliasName' --output table
echo ""

echo "✅ Verification complete!"
```

---

## 📋 What to Check in AWS Console

### S3 Console
1. Navigate to: **S3 → Buckets**
2. Verify buckets exist and show:
   - ✅ Versioning: Enabled
   - ✅ Encryption: SSE-KMS
   - ✅ Public access: Blocked

### EMR Serverless Console
1. Navigate to: **EMR → Serverless applications**
2. Verify:
   - ✅ Application `project-a-dev-spark` exists
   - ✅ State: CREATED or STARTED
   - ✅ Release: emr-7.1.0

### Glue Console
1. Navigate to: **Glue → Databases**
2. Verify databases:
   - ✅ `project-a_bronze_dev`
   - ✅ `project-a_silver_dev`
   - ✅ `project-a_gold_dev`

### IAM Console
1. Navigate to: **IAM → Roles**
2. Verify roles:
   - ✅ `project-a-dev-emr-exec`
   - ✅ `project-a-dev-emr-service-role`
   - ✅ `project-a-dev-glue`
3. Check policies on `project-a-dev-emr-exec`:
   - ✅ S3 permissions (lake/artifacts buckets)
   - ✅ KMS permissions
   - ✅ Glue permissions
   - ✅ Secrets Manager permissions

### CloudWatch Console
1. Navigate to: **CloudWatch → Log groups**
2. Verify log groups:
   - ✅ `/aws/emr-serverless/spark/project-a-dev`
   - ✅ `/aws/data-platform/project-a-dev`

### KMS Console
1. Navigate to: **KMS → Customer managed keys**
2. Verify:
   - ✅ Key with alias `alias/project-a-dev-cmk`
   - ✅ Key rotation: Enabled

---

## 🚨 Common Issues to Check

### Issue 1: Buckets Not Found
**Check:** Did Terraform apply complete successfully?
```bash
cd aws/terraform
terraform show
```

### Issue 2: EMR App Not Created
**Check:** Verify EMR Serverless is available in your region
```bash
aws emr-serverless list-applications --profile $PROFILE --region $REGION
```

### Issue 3: IAM Role Permissions
**Check:** Verify role has correct trust policy
```bash
aws iam get-role --role-name project-a-dev-emr-exec --profile $PROFILE --query 'Role.AssumeRolePolicyDocument'
```

### Issue 4: KMS Key Access
**Check:** Verify EMR role can use KMS key
```bash
aws kms get-key-policy --key-id <KMS_ARN> --policy-name default --profile $PROFILE
```

---

## 📤 Share Results

After running the verification commands, you can share:
1. Output of the quick verification script
2. Any errors you encounter
3. Screenshots from AWS Console (optional)

I can help troubleshoot any issues found!

