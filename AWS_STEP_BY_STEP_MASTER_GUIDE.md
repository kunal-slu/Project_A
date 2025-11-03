# 🚀 AWS Deployment: Step-by-Step Master Guide

**Complete guide to deploy your PySpark Data Engineering project on AWS**

---

## 📋 Table of Contents

1. [Prerequisites & Setup](#prerequisites--setup)
2. [AWS Account Configuration](#aws-account-configuration)
3. [Terraform Infrastructure](#terraform-infrastructure)
4. [Data Storage Setup](#data-storage-setup)
5. [Compute Resources](#compute-resources)
6. [Code Deployment](#code-deployment)
7. [Configuration Management](#configuration-management)
8. [Secret Management](#secret-management)
9. [Orchestration Setup](#orchestration-setup)
10. [Testing & Validation](#testing--validation)
11. [Monitoring & Maintenance](#monitoring--maintenance)
12. [Troubleshooting](#troubleshooting)

---

## 1. Prerequisites & Setup

### 1.1 Required Tools

```bash
# Install these tools on your local machine
# AWS CLI
brew install awscli  # macOS
# or
sudo apt-get install awscli  # Ubuntu

# Terraform
brew install terraform  # macOS
# or
wget https://releases.hashicorp.com/terraform/1.6.0/terraform_1.6.0_linux_amd64.zip
unzip terraform_1.6.0_linux_amd64.zip
sudo mv terraform /usr/local/bin/

# Python 3.9+
python3 --version  # Should be 3.9 or higher

# Git
git --version

# Verify all tools
aws --version
terraform --version
python3 --version
git --version
```

### 1.2 AWS CLI Configuration

```bash
# Configure AWS CLI
aws configure

# Enter when prompted:
AWS Access Key ID: YOUR_ACCESS_KEY
AWS Secret Access Key: YOUR_SECRET_KEY
Default region: us-east-1
Default output format: json

# Test configuration
aws sts get-caller-identity
```

**Expected Output:**
```json
{
    "UserId": "AIDA...",
    "Account": "123456789012",
    "Arn": "arn:aws:iam::123456789012:user/yourname"
}
```

### 1.3 Clone Repository

```bash
# Clone the project
cd ~/projects
git clone https://github.com/kunal-slu/Project_A.git pyspark-etl-project
cd pyspark-etl-project

# Checkout the AWS production branch
git checkout feature/aws-production

# Verify you have all files
ls -la
```

### 1.4 Project Structure Verification

**Critical directories you MUST have:**

```
pyspark-etl-project/
├── infra/                    # ⭐ Terraform infrastructure
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   ├── modules/
│   └── ...
├── aws/                      # ⭐ AWS-specific code
│   ├── terraform/            # Terraform files
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── terraform.tfvars.example
│   ├── dags/                 # Airflow DAGs
│   │   ├── daily_batch_pipeline_dag.py
│   │   └── daily_pipeline_dag_complete.py
│   ├── jobs/                 # EMR Serverless jobs
│   │   └── ingest/
│   ├── scripts/              # Utility scripts
│   │   └── create_cloudwatch_alarms.py
│   ├── emr_configs/          # EMR configurations
│   │   ├── delta-core.conf
│   │   └── logging.yaml
│   └── docs/
├── config/                   # ⭐ Configuration files
│   ├── prod.yaml
│   ├── local.yaml
│   ├── dq.yaml
│   ├── lineage.yaml
│   └── schema_definitions/
├── src/                      # ⭐ Source code
│   └── pyspark_interview_project/
│       ├── utils/
│       ├── extract/
│       ├── transform/
│       ├── dq/
│       └── monitoring/
├── jobs/                     # ⭐ Main jobs
│   └── ingest/
│       ├── snowflake_to_bronze.py
│       └── _lib/
├── scripts/                  # Utility scripts
│   └── maintenance/
│       ├── backfill_range.py
│       └── optimize_tables.py
├── runbooks/                 # ⭐ Operational runbooks
│   ├── RUNBOOK_DQ_FAILOVER.md
│   ├── RUNBOOK_STREAMING_RESTART.md
│   └── RUNBOOK_BACKFILL.md
├── terraform/                # Alternative Terraform location
└── BEGINNERS_AWS_DEPLOYMENT_GUIDE.md
```

**Verify critical files exist:**

```bash
# Infrastructure
ls -la infra/main.tf
ls -la aws/terraform/main.tf

# Jobs
ls -la jobs/ingest/snowflake_to_bronze.py
ls -la src/pyspark_interview_project/jobs/gold_star_schema.py

# DAGs
ls -la aws/dags/daily_pipeline_dag_complete.py

# Configs
ls -la config/prod.yaml
ls -la config/dq.yaml
ls -la config/local.yaml

# Runbooks
ls -la runbooks/RUNBOOK_*.md
```

---

## 2. AWS Account Configuration

### 2.1 Create IAM User for Terraform

**Console Steps:**

1. Go to **IAM Console** → **Users** → **Add users**
2. Username: `terraform-admin`
3. Access type: **Programmatic access**
4. Attach policies:
   - `AdministratorAccess` (for initial setup)
5. Download credentials CSV
6. Configure locally:

```bash
# Set environment variables
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1

# Verify
aws sts get-caller-identity
```

### 2.2 Create S3 Buckets

```bash
# Set your account ID and region
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export AWS_REGION=us-east-1

# Create buckets
aws s3 mb s3://my-etl-lake-demo-${AWS_ACCOUNT_ID}
aws s3 mb s3://my-etl-code-demo-${AWS_ACCOUNT_ID}
aws s3 mb s3://my-etl-logs-demo-${AWS_ACCOUNT_ID}
aws s3 mb s3://my-etl-artifacts-demo-${AWS_ACCOUNT_ID}

# Verify
aws s3 ls | grep my-etl
```

**Expected Output:**
```
my-etl-artifacts-demo-123456789012
my-etl-code-demo-123456789012
my-etl-logs-demo-123456789012
my-etl-lake-demo-123456789012
```

### 2.3 Enable Versioning

```bash
# Enable versioning on all buckets (required for Delta Lake)
for bucket in my-etl-lake-demo my-etl-code-demo my-etl-logs-demo my-etl-artifacts-demo; do
  aws s3api put-bucket-versioning \
    --bucket ${bucket}-${AWS_ACCOUNT_ID} \
    --versioning-configuration Status=Enabled
done

# Verify
aws s3api get-bucket-versioning --bucket my-etl-lake-demo-${AWS_ACCOUNT_ID}
```

---

## 3. Terraform Infrastructure

### 3.1 Review Terraform Configuration

**Check your Terraform files:**

```bash
# Navigate to Terraform directory
cd aws/terraform  # or cd terraform/ if that's your location

# List files
ls -la

# You should see:
# - main.tf           # Main resources
# - variables.tf      # Variable definitions
# - outputs.tf        # Output values
# - terraform.tfvars  # Your specific values
```

### 3.2 Create terraform.tfvars

```bash
# Create your configuration file
cat > terraform.tfvars << EOF
# AWS Configuration
aws_region              = "us-east-1"
aws_account_id          = "${AWS_ACCOUNT_ID}"
project_name            = "pyspark-etl-project"
environment             = "prod"

# S3 Buckets
lake_bucket_name        = "my-etl-lake-demo-${AWS_ACCOUNT_ID}"
code_bucket_name        = "my-etl-code-demo-${AWS_ACCOUNT_ID}"
logs_bucket_name        = "my-etl-logs-demo-${AWS_ACCOUNT_ID}"

# EMR Serverless
emr_serverless_app_name = "pyspark-etl-app"
emr_job_driver_size     = "4GB"
emr_job_executor_size   = "8GB"
emr_job_executor_count  = 5

# Redshift Serverless (if using)
redshift_namespace_name = "etl-namespace"
redshift_db_name        = "analytics"

# MWAA Airflow (if using)
mwaa_environment_name   = "pyspark-etl-airflow"
mwaa_dag_s3_path        = "dags/"
mwaa_requirements_path  = "requirements.txt"
mwaa_max_workers        = 2

# Tags
tags = {
  Project     = "PySpark ETL"
  Environment = "Production"
  Owner       = "Data Engineering"
  ManagedBy   = "Terraform"
}
EOF

# Review the file
cat terraform.tfvars
```

### 3.3 Initialize Terraform

```bash
# Initialize Terraform
terraform init

# Expected output:
# Initializing the backend...
# Initializing provider plugins...
# Terraform has been successfully initialized!
```

### 3.4 Review Terraform Plan

```bash
# Create an execution plan
terraform plan -out=tfplan

# Review the plan carefully
# Look for:
# - Resources that will be created
# - Resources that will be modified
# - Any destroy operations
# - Cost implications
```

**Key Resources to Verify in Plan:**

- ✅ S3 buckets (lake, code, logs)
- ✅ IAM roles (EMR, MWAA, Glue)
- ✅ EMR Serverless application
- ✅ MWAA environment (if using Airflow)
- ✅ Glue Catalog database
- ✅ Secrets Manager secrets
- ✅ CloudWatch log groups
- ✅ VPC/subnets (if needed)

### 3.5 Apply Terraform

```bash
# Apply the plan
terraform apply tfplan

# Or use interactive mode
terraform apply

# Confirm when prompted: yes

# This will take 15-30 minutes
# Watch for:
# ✅ apply complete!
# Resources: 20 added, 0 changed, 0 destroyed
```

### 3.6 Save Terraform Outputs

```bash
# Get outputs
terraform output > terraform-outputs.json

# View outputs
cat terraform-outputs.json

# Important outputs:
# - emr_application_id
# - emr_execution_role_arn
# - glue_database_name
# - mwaa_webserver_url (if using MWAA)
```

**Expected Outputs:**
```json
{
  "emr_application_id": {
    "value": "00f0dabc1234567890ab"
  },
  "emr_execution_role_arn": {
    "value": "arn:aws:iam::123456789012:role/pyspark-etl-emr-execution-role"
  },
  "glue_database_name": {
    "value": "pyspark_etl_db"
  },
  "lake_bucket_name": {
    "value": "my-etl-lake-demo-123456789012"
  }
}
```

---

## 4. Data Storage Setup

### 4.1 Create Lake Structure

```bash
# Set variables from Terraform outputs
export LAKE_BUCKET=$(terraform output -raw lake_bucket_name)
export CODE_BUCKET=$(terraform output -raw code_bucket_name)

# Create Bronze layer structure
aws s3api put-object \
  --bucket ${LAKE_BUCKET} \
  --key bronze/.gitkeep

# Create Silver layer structure
aws s3api put-object \
  --bucket ${LAKE_BUCKET} \
  --key silver/.gitkeep

# Create Gold layer structure
aws s3api put-object \
  --bucket ${LAKE_BUCKET} \
  --key gold/.gitkeep

# Create source-specific directories
for source in snowflake redshift kafka; do
  aws s3api put-object --bucket ${LAKE_BUCKET} --key bronze/${source}/.gitkeep
done

# Create error lanes
aws s3api put-object --bucket ${LAKE_BUCKET} --key _errors/.gitkeep
aws s3api put-object --bucket ${LAKE_BUCKET} --key _checkpoints/.gitkeep
aws s3api put-object --bucket ${LAKE_BUCKET} --key _metrics/.gitkeep

# Verify structure
aws s3 ls s3://${LAKE_BUCKET}/ --recursive
```

### 4.2 Setup Delta Lake Format

```bash
# Delta Lake requires Spark with Delta extensions
# This is configured in EMR Serverless config

# Verify EMR configuration
cat aws/emr_configs/delta-core.conf

# You should see:
# spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
# spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

---

## 5. Compute Resources

### 5.1 Verify EMR Serverless Application

```bash
# Get application ID
export EMR_APP_ID=$(terraform output -raw emr_application_id)

# Check application status
aws emr-serverless get-application \
  --application-id ${EMR_APP_ID}

# Expected: "RUNNING" or "CREATED"
# If not running, start it:
aws emr-serverless start-application \
  --application-id ${EMR_APP_ID}

# Wait for it to start (2-3 minutes)
aws emr-serverless wait application-running \
  --application-id ${EMR_APP_ID}
```

### 5.2 Configure EMR Execution Role

```bash
# Get the execution role ARN
export EMR_ROLE_ARN=$(terraform output -raw emr_execution_role_arn)

# Verify the role exists
aws iam get-role --role-name $(echo ${EMR_ROLE_ARN} | cut -d'/' -f2)

# Verify role trust policy (allows EMR to assume it)
aws iam get-role --role-name $(echo ${EMR_ROLE_ARN} | cut -d'/' -f2) \
  --query 'Role.AssumeRolePolicyDocument'
```

---

## 6. Code Deployment

### 6.1 Package Python Code

```bash
# Navigate to project root
cd /Users/kunal/IdeaProjects/pyspark_data_engineer_project

# Create a ZIP of source code
zip -r pyspark-etl-source.zip src/ \
  -x "*.pyc" "*__pycache__*" "*.pytest_cache*" ".git/*"

# Create deployment package
mkdir -p deployment
cp -r jobs deployment/
cp -r config deployment/
cp pyspark-etl-source.zip deployment/
cp requirements.txt deployment/
```

### 6.2 Upload Jobs to S3

```bash
# Upload main jobs
aws s3 cp deployment/jobs/ s3://${CODE_BUCKET}/jobs/ \
  --recursive --exclude "*.pyc"

# Upload source code
aws s3 cp pyspark-etl-source.zip s3://${CODE_BUCKET}/src/

# Upload configs
aws s3 cp config/prod.yaml s3://${CODE_BUCKET}/config/
aws s3 cp config/dq.yaml s3://${CODE_BUCKET}/config/
aws s3 cp config/lineage.yaml s3://${CODE_BUCKET}/config/

# Upload requirements
aws s3 cp requirements.txt s3://${CODE_BUCKET}/requirements.txt

# Verify upload
aws s3 ls s3://${CODE_BUCKET}/ --recursive
```

### 6.3 Prepare EMR Configuration

```bash
# Create EMR configuration JSON
cat > emr-config.json << EOF
{
  "monitoringConfiguration": {
    "s3MonitoringConfiguration": {
      "logUri": "s3://${LAKE_BUCKET}/logs/emr-serverless/"
    }
  },
  "applicationConfiguration": [
    {
      "classification": "delta-defaults",
      "properties": {}
    },
    {
      "classification": "spark-defaults",
      "properties": {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.shuffle.partitions": "200",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.adaptive.enabled": "true"
      }
    }
  ]
}
EOF

# Upload configuration
aws s3 cp emr-config.json s3://${CODE_BUCKET}/emr-config.json
```

---

## 7. Configuration Management

### 7.1 Update Production Configuration

```bash
# Edit config/prod.yaml with your AWS resources
cat > config/prod.yaml << 'EOF'
# Production Configuration - AWS
env: prod
cloud: aws

# AWS Configuration
aws:
  region: us-east-1
  lake_bucket: my-etl-lake-demo-123456789012  # ⚠️ UPDATE WITH YOUR BUCKET
  code_bucket: my-etl-code-demo-123456789012  # ⚠️ UPDATE WITH YOUR BUCKET
  glue_catalog_db: silver

# Redshift Configuration
redshift:
  enabled: true
  database: dev
  schema: raw
  iam_role: arn:aws:iam::123456789012:role/redshift-role  # ⚠️ UPDATE

# Snowflake Configuration
snowflake:
  enabled: true
  warehouse: COMPUTE_WH
  database: ETL_PROJECT_DB
  schema: RAW

# Unified Paths
paths:
  bronze: s3a://my-etl-lake-demo-123456789012/bronze  # ⚠️ UPDATE
  silver: s3a://my-etl-lake-demo-123456789012/silver
  gold: s3a://my-etl-lake-demo-123456789012/gold

# DQ Configuration
dq:
  config_file: config/dq.yaml
  enabled: true
  strict_mode: true

# Lineage Configuration
lineage:
  config_file: config/lineage.yaml
  enabled: true
  url: http://marquez:5000
  namespace: company-data-platform

# Spark Runtime
spark:
  app_name: company_production_etl
  shuffle_partitions: 200
  enable_aqe: true

# Storage Configuration
storage:
  warehouse: s3://my-etl-lake-demo-123456789012  # ⚠️ UPDATE
  format: delta
  catalog: glue_catalog

# Source-specific configuration (P0)
sources:
  snowflake_orders:
    load_type: incremental
    watermark_column: updated_at
    watermark_state_key: snowflake_orders_max_ts
    schema_contract: config/schema_definitions/snowflake_orders_bronze.json
  
  redshift_behavior:
    load_type: incremental
    watermark_column: event_ts
    watermark_state_key: redshift_behavior_max_ts
    schema_contract: config/schema_definitions/redshift_behavior_bronze.json

# EMR Serverless Configuration
emr:
  mode: serverless
  application_id: 00f0dabc1234567890ab  # ⚠️ UPDATE WITH YOUR EMR APP ID
  execution_role_arn: arn:aws:iam::123456789012:role/emr-role  # ⚠️ UPDATE

# Secrets Configuration
secrets:
  snowflake: snowflake/etl/prod
  redshift: redshift/etl_user/prod

# Monitoring Configuration
monitoring:
  enabled: true
  metrics_export: cloudwatch
  log_level: INFO

# SLA Configuration
sla:
  bronze_ingestion_hours: 2
  silver_transformation_hours: 2.5
  gold_business_logic_hours: 3
EOF

# Upload updated config
aws s3 cp config/prod.yaml s3://${CODE_BUCKET}/config/prod.yaml
```

### 7.2 Upload Schema Definitions

```bash
# Upload schema contracts
aws s3 cp config/schema_definitions/ s3://${CODE_BUCKET}/config/schema_definitions/ \
  --recursive

# Verify
aws s3 ls s3://${CODE_BUCKET}/config/schema_definitions/
```

---

## 8. Secret Management

### 8.1 Store Secrets in AWS Secrets Manager

```bash
# Store Snowflake credentials
aws secretsmanager create-secret \
  --name snowflake/etl/prod \
  --secret-string '{
    "account": "your-account.snowflakecomputing.com",
    "user": "etl_user",
    "password": "your-password",
    "warehouse": "COMPUTE_WH",
    "database": "ETL_PROJECT_DB",
    "schema": "RAW"
  }'

# Store Redshift credentials
aws secretsmanager create-secret \
  --name redshift/etl_user/prod \
  --secret-string '{
    "host": "redshift-cluster.region.redshift.amazonaws.com",
    "port": 5439,
    "database": "analytics",
    "user": "etl_user",
    "password": "your-password"
  }'

# Verify secrets exist
aws secretsmanager list-secrets
```

### 8.2 Grant EMR Role Access to Secrets

```bash
# Get the EMR role name
EMR_ROLE_NAME=$(echo ${EMR_ROLE_ARN} | cut -d'/' -f2)

# Create policy for secrets access
cat > emr-secrets-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue",
        "secretsmanager:DescribeSecret"
      ],
      "Resource": [
        "arn:aws:secretsmanager:us-east-1:${AWS_ACCOUNT_ID}:secret:snowflake/*",
        "arn:aws:secretsmanager:us-east-1:${AWS_ACCOUNT_ID}:secret:redshift/*"
      ]
    }
  ]
}
EOF

# Attach policy to role
aws iam put-role-policy \
  --role-name ${EMR_ROLE_NAME} \
  --policy-name SecretsAccessPolicy \
  --policy-document file://emr-secrets-policy.json

# Verify policy
aws iam get-role-policy \
  --role-name ${EMR_ROLE_NAME} \
  --policy-name SecretsAccessPolicy
```

---

## 9. Orchestration Setup

### 9.1 Upload Airflow DAGs

```bash
# Upload DAGs to MWAA (or local Airflow)
aws s3 cp aws/dags/daily_pipeline_dag_complete.py \
  s3://${CODE_BUCKET}/dags/

aws s3 cp aws/dags/daily_batch_pipeline_dag.py \
  s3://${CODE_BUCKET}/dags/

# If using MWAA, DAGs go to MWAA bucket
# Get MWAA bucket name
MWAA_BUCKET="pyspark-etl-airflow-bucket-${AWS_ACCOUNT_ID}"
aws s3 cp aws/dags/ s3://${MWAA_BUCKET}/dags/ --recursive

# Verify
aws s3 ls s3://${CODE_BUCKET}/dags/
```

### 9.2 Upload Airflow Requirements (if using MWAA)

```bash
# Create requirements.txt for MWAA
cat > mwaa-requirements.txt << EOF
apache-airflow==2.7.3
apache-airflow-providers-amazon
apache-airflow-providers-postgres
boto3
requests
EOF

# Upload to MWAA
aws s3 cp mwaa-requirements.txt s3://${MWAA_BUCKET}/requirements.txt
```

### 9.3 Access MWAA Airflow UI

```bash
# Get MWAA web server URL
MWAA_URL=$(terraform output -raw mwaa_webserver_url)

echo "Access Airflow UI at: ${MWAA_URL}"
echo "Username: admin"
echo "Password: Use AWS Secrets Manager"

# Get password from Secrets Manager
MWAA_SECRET=$(aws secretsmanager get-secret-value \
  --secret-id airflow/mwaa/pyspark-etl-airflow/credentials \
  --query SecretString --output text | jq -r .password)

echo "Password: ${MWAA_SECRET}"
```

---

## 10. Testing & Validation

### 10.1 Test EMR Serverless Job

```bash
# Run a test job
aws emr-serverless start-job-run \
  --application-id ${EMR_APP_ID} \
  --execution-role-arn ${EMR_ROLE_ARN} \
  --name "test-snowflake-ingestion" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://'${CODE_BUCKET}'/jobs/ingest/snowflake_to_bronze.py",
      "entryPointArguments": ["--config", "config/prod.yaml"],
      "sparkSubmitParameters": "--conf spark.sql.shuffle.partitions=200"
    }
  }' \
  --configuration-overrides file://emr-config.json

# Get job run ID
JOB_RUN_ID=$(aws emr-serverless list-job-runs \
  --application-id ${EMR_APP_ID} \
  --max-results 1 \
  --query 'jobRuns[0].id' --output text)

echo "Job Run ID: ${JOB_RUN_ID}"

# Monitor job
aws emr-serverless get-job-run \
  --application-id ${EMR_APP_ID} \
  --job-run-id ${JOB_RUN_ID} \
  --query 'jobRun.state' --output text
```

**Expected States:**
- `SUBMIITED` → `RUNNING` → `SUCCESS` (good!)
- `SUBMITTED` → `FAILED` (check logs)

### 10.2 View Job Logs

```bash
# Get logs
aws logs tail /aws/emr-serverless/application-logs/${EMR_APP_ID}/${JOB_RUN_ID}/ \
  --follow

# Or download logs
aws s3 sync s3://${LAKE_BUCKET}/logs/emr-serverless/${EMR_APP_ID}/${JOB_RUN_ID}/ \
  ./logs/
```

### 10.3 Verify Data in Bronze

```bash
# Check Bronze layer
aws s3 ls s3://${LAKE_BUCKET}/bronze/snowflake/orders/ --recursive

# Query with Athena (if Glue tables registered)
aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM bronze.snowflake_orders" \
  --result-configuration "OutputLocation=s3://${LAKE_BUCKET}/athena-results/"
```

### 10.4 Run Full Pipeline Test

```bash
# Manually trigger Airflow DAG (if MWAA)
# Or run jobs sequentially

# 1. Extract
aws emr-serverless start-job-run \
  --application-id ${EMR_APP_ID} \
  --execution-role-arn ${EMR_ROLE_ARN} \
  --name "extract-snowflake" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://'${CODE_BUCKET}'/jobs/ingest/snowflake_to_bronze.py"
    }
  }' \
  --configuration-overrides file://emr-config.json

# Wait for completion...

# 2. Transform to Silver
aws emr-serverless start-job-run \
  --application-id ${EMR_APP_ID} \
  --execution-role-arn ${EMR_ROLE_ARN} \
  --name "bronze-to-silver" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://'${CODE_BUCKET}'/jobs/bronze_to_silver.py"
    }
  }' \
  --configuration-overrides file://emr-config.json

# 3. Build Gold
aws emr-serverless start-job-run \
  --application-id ${EMR_APP_ID} \
  --execution-role-arn ${EMR_ROLE_ARN} \
  --name "build-gold" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://'${CODE_BUCKET}'/jobs/gold_star_schema.py"
    }
  }' \
  --configuration-overrides file://emr-config.json
```

---

## 11. Monitoring & Maintenance

### 11.1 Setup CloudWatch Alarms

```bash
# Create CloudWatch alarms
python3 aws/scripts/create_cloudwatch_alarms.py \
  --region us-east-1 \
  --sns-topic arn:aws:sns:us-east-1:${AWS_ACCOUNT_ID}:data-engineering-alerts

# Verify alarms
aws cloudwatch describe-alarms \
  --alarm-name-prefix "etl-"
```

### 11.2 Create CloudWatch Dashboard

```bash
# Create dashboard JSON
cat > dashboard.json << 'EOF'
{
  "widgets": [
    {
      "type": "metric",
      "properties": {
        "metrics": [
          ["ETLPipelineMetrics", "records_processed_total", {"stat": "Sum"}]
        ],
        "period": 300,
        "stat": "Sum",
        "region": "us-east-1",
        "title": "Records Processed"
      }
    }
  ]
}
EOF

# Create dashboard
aws cloudwatch put-dashboard \
  --dashboard-name "PySpark-ETL-Pipeline" \
  --dashboard-body file://dashboard.json

# View dashboard
echo "View dashboard at: https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=PySpark-ETL-Pipeline"
```

### 11.3 Schedule Daily Pipeline

```bash
# In Airflow UI:
# 1. Go to your DAG: daily_pipeline_complete
# 2. Toggle ON to enable scheduling
# 3. Verify schedule: daily at 2 AM UTC

# Or use AWS EventBridge
aws events put-rule \
  --name daily-etl-pipeline \
  --schedule-expression "cron(0 2 * * ? *)" \
  --description "Trigger ETL pipeline daily at 2 AM UTC"

# Add target (your DAG or Lambda)
# ... (configure based on your setup)
```

---

## 12. Troubleshooting

### 12.1 Common Issues & Solutions

#### Issue 1: EMR Job Fails - "Cannot find module"

```bash
# Problem: Python dependencies not available
# Solution: Update requirements.txt and redeploy

# Add missing dependencies to requirements.txt
echo "pyspark==3.5.0" >> requirements.txt
echo "delta-spark==3.0.0" >> requirements.txt
echo "boto3>=1.28.0" >> requirements.txt

# Upload to S3
aws s3 cp requirements.txt s3://${CODE_BUCKET}/requirements.txt

# Update EMR configuration to install dependencies
# See aws/emr_configs/logging.yaml
```

#### Issue 2: Permission Denied - S3 Access

```bash
# Problem: EMR role cannot access S3
# Solution: Verify IAM role policies

# Check S3 access
aws iam get-role-policy \
  --role-name ${EMR_ROLE_NAME} \
  --policy-name S3Access

# If missing, add policy:
cat > s3-access-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-etl-lake-demo-*",
        "arn:aws:s3:::my-etl-lake-demo-*/*",
        "arn:aws:s3:::my-etl-code-demo-*",
        "arn:aws:s3:::my-etl-code-demo-*/*"
      ]
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name ${EMR_ROLE_NAME} \
  --policy-name S3Access \
  --policy-document file://s3-access-policy.json
```

#### Issue 3: Secrets Manager Access Denied

```bash
# Problem: Cannot read secrets
# Solution: Add Secrets Manager policy

# Check existing policy
aws iam get-role-policy \
  --role-name ${EMR_ROLE_NAME} \
  --policy-name SecretsAccessPolicy

# If missing, add from step 8.2
```

#### Issue 4: Delta Lake Errors

```bash
# Problem: Delta table operations failing
# Solution: Verify Delta Spark extensions

# Check EMR configuration
cat emr-config.json | grep -A 5 "spark.sql.extensions"

# Should see:
# "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"

# Re-upload configuration
aws s3 cp emr-config.json s3://${CODE_BUCKET}/emr-config.json
```

#### Issue 5: Airflow DAG Not Appearing

```bash
# Problem: DAG not showing in Airflow UI
# Solution: Check DAG syntax and location

# Validate DAG syntax
python3 aws/dags/daily_pipeline_dag_complete.py

# Check DAG folder
aws s3 ls s3://${MWAA_BUCKET}/dags/

# Verify DAG has proper permissions
aws s3api put-object-acl \
  --bucket ${MWAA_BUCKET} \
  --key dags/daily_pipeline_dag_complete.py \
  --acl public-read
```

### 12.2 View Logs

```bash
# EMR Serverless logs
aws logs tail /aws/emr-serverless/application-logs/ --follow

# CloudWatch logs
aws logs tail /aws/emr-serverless/jobs/ --follow

# Airflow logs (if MWAA)
aws logs tail /aws/mwaa/pyspark-etl-airflow/ --follow
```

### 12.3 Debug Job Failures

```bash
# Get failed job details
aws emr-serverless get-job-run \
  --application-id ${EMR_APP_ID} \
  --job-run-id ${JOB_RUN_ID} \
  --query 'jobRun.totalResourceUtilization'

# Get error message
aws emr-serverless get-job-run \
  --application-id ${EMR_APP_ID} \
  --job-run-id ${JOB_RUN_ID} \
  --query 'jobRun.stateDetails'
```

---

## 🎯 Quick Reference Checklist

### Pre-Deployment
- [ ] AWS CLI configured and tested
- [ ] Terraform installed
- [ ] Repository cloned
- [ ] AWS account credentials ready
- [ ] Budget alerts configured

### Infrastructure
- [ ] S3 buckets created with versioning
- [ ] Terraform initialized and planned
- [ ] IAM roles created
- [ ] EMR Serverless app running
- [ ] Glue Catalog database created

### Code Deployment
- [ ] Source code packaged and uploaded
- [ ] Jobs uploaded to S3
- [ ] Config files uploaded
- [ ] Requirements.txt uploaded
- [ ] EMR configuration uploaded

### Secrets & Security
- [ ] Secrets stored in Secrets Manager
- [ ] IAM roles have correct permissions
- [ ] S3 bucket policies configured
- [ ] VPC/security groups setup (if needed)

### Orchestration
- [ ] Airflow DAGs uploaded
- [ ] DAGs visible in UI
- [ ] Schedule configured
- [ ] Dataset dependencies set

### Testing
- [ ] Test job runs successfully
- [ ] Bronze data landing correctly
- [ ] Silver transformations working
- [ ] Gold tables populated
- [ ] Lineage tracking enabled

### Monitoring
- [ ] CloudWatch alarms created
- [ ] Dashboard configured
- [ ] SNS notifications setup
- [ ] Logs accessible

### Maintenance
- [ ] Runbooks accessible
- [ ] Backfill scripts tested
- [ ] Optimization scripts ready
- [ ] Documented procedures

---

## 📚 Important Files Reference

### Infrastructure
- `aws/terraform/main.tf` - Main Terraform resources
- `aws/terraform/variables.tf` - Variable definitions
- `aws/terraform/outputs.tf` - Output values
- `infra/main.tf` - Alternative Terraform location

### Configuration
- `config/prod.yaml` - **Production configuration**
- `config/local.yaml` - Local development config
- `config/dq.yaml` - Data quality suites
- `config/lineage.yaml` - Lineage configuration
- `config/schema_definitions/*.json` - Schema contracts

### Jobs
- `jobs/ingest/snowflake_to_bronze.py` - **Main ingestion job** (P0 features)
- `src/pyspark_interview_project/jobs/dim_customer_scd2.py` - SCD2 dimension
- `src/pyspark_interview_project/jobs/gold_star_schema.py` - Star schema builder
- `src/pyspark_interview_project/transform/bronze_to_silver_multi_source.py` - Multi-source transformation

### Utilities
- `src/pyspark_interview_project/utils/contracts.py` - Schema validation
- `src/pyspark_interview_project/utils/error_lanes.py` - Error quarantine
- `src/pyspark_interview_project/utils/watermark_utils.py` - Incremental loads
- `src/pyspark_interview_project/dq/gate.py` - DQ gate enforcement

### Orchestration
- `aws/dags/daily_pipeline_dag_complete.py` - **Complete ETL DAG**
- `aws/dags/daily_batch_pipeline_dag.py` - Alternative DAG

### Scripts
- `scripts/maintenance/backfill_range.py` - Historical reprocessing
- `scripts/maintenance/optimize_tables.py` - Delta optimization
- `aws/scripts/create_cloudwatch_alarms.py` - Monitoring setup

### Runbooks
- `runbooks/RUNBOOK_DQ_FAILOVER.md` - DQ bypass procedure
- `runbooks/RUNBOOK_STREAMING_RESTART.md` - Checkpoint reset
- `runbooks/RUNBOOK_BACKFILL.md` - Backfill procedures

### Documentation
- `AWS_COMPLETE_DEPLOYMENT_GUIDE.md` - Detailed deployment guide
- `BEGINNERS_AWS_DEPLOYMENT_GUIDE.md` - Beginner-friendly guide
- `VERIFICATION_COMPLETE.md` - Verification report
- `PROJECT_COMPLETE.md` - Project completion status

---

## 🚀 Next Steps After Deployment

1. **Validate Pipeline**: Run full ETL and verify data in all layers
2. **Setup Monitoring**: Create CloudWatch dashboards and alarms
3. **Document Access**: Share credentials securely with team
4. **Train Users**: Walk team through Airflow UI and data access
5. **Setup Backups**: Configure daily backups of critical data
6. **Cost Monitoring**: Setup billing alerts and cost optimization

---

## 📞 Support & Resources

- **Documentation**: See `BEGINNERS_AWS_DEPLOYMENT_GUIDE.md`
- **Runbooks**: Check `runbooks/` directory
- **AWS Console**: https://console.aws.amazon.com
- **CloudWatch Logs**: https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:
- **Terraform Docs**: https://registry.terraform.io/providers/hashicorp/aws/latest/docs

---

**Last Updated:** 2025-01-15  
**Version:** 1.0  
**Status:** Production-Ready

