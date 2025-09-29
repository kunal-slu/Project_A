# AWS Data Platform - PySpark Data Engineer Project

This directory contains AWS-specific implementations for the PySpark data engineering project, including EMR Serverless, MWAA, Glue/Athena, and S3 data lake infrastructure.

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Ingestion     │    │   AWS Data Lake │
│                 │    │                 │    │                 │
│ • REST APIs     │───▶│ • Lambda        │───▶│ • S3 Bronze     │
│ • RDS (Postgres)│    │ • SQS           │    │ • S3 Silver     │
│ • Salesforce    │    │ • DMS           │    │ • S3 Gold       │
│ • Snowflake     │    │ • AppFlow       │    │                 │
│ • Kafka         │    │ • MSK           │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
                       ┌─────────────────┐            │
                       │   Processing    │◀───────────┘
                       │                 │
                       │ • EMR Serverless│
                       │ • PySpark       │
                       │ • Delta Lake    │
                       └─────────────────┘
                                │
                       ┌─────────────────┐
                       │   Orchestration │
                       │                 │
                       │ • MWAA (Airflow)│
                       │ • DAGs          │
                       └─────────────────┘
                                │
                       ┌─────────────────┐
                       │   Data Quality  │
                       │                 │
                       │ • Great Expect. │
                       │ • Data Docs     │
                       └─────────────────┘
```

## 🚀 Golden Path (Copy-Paste Runnable)

### Prerequisites

```bash
# Set environment variables
export AWS_REGION="us-east-1"
export PROJECT="pyspark-de-project"
export ENVIRONMENT="dev"
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Verify AWS credentials
aws sts get-caller-identity
```

### Step 1: Deploy Infrastructure

```bash
# Navigate to AWS infrastructure
cd aws/infra/terraform

# Initialize Terraform
terraform init

# Review the plan
terraform plan -var="project=$PROJECT" -var="environment=$ENVIRONMENT"

# Deploy infrastructure
terraform apply -var="project=$PROJECT" -var="environment=$ENVIRONMENT"

# Save outputs
export DATA_LAKE_BUCKET=$(terraform output -raw data_lake_bucket_name)
export ARTIFACTS_BUCKET=$(terraform output -raw artifacts_bucket_name)
export EMR_APP_ID=$(terraform output -raw emr_serverless_application_id)
export EMR_ROLE_ARN=$(terraform output -raw emr_serverless_job_role_arn)
```

### Step 2: Build and Upload Code

```bash
# Return to project root
cd ../../..

# Build Python wheel
python -m build

# Upload wheel and jobs to S3
aws s3 cp dist/*.whl s3://$ARTIFACTS_BUCKET/dist/
aws s3 sync src/pyspark_interview_project/jobs/ s3://$ARTIFACTS_BUCKET/jobs/
aws s3 sync aws/scripts/ s3://$ARTIFACTS_BUCKET/scripts/
aws s3 sync config/ s3://$ARTIFACTS_BUCKET/config/
aws s3 sync dq/ s3://$ARTIFACTS_BUCKET/dq/
```

### Step 3: Run FX Bronze → Silver Processing

```bash
# Submit FX to Bronze job
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $ARTIFACTS_BUCKET \
  --lake-bucket $DATA_LAKE_BUCKET \
  --entry-point s3://$ARTIFACTS_BUCKET/jobs/fx_to_bronze.py

# Submit FX Bronze to Silver job
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $ARTIFACTS_BUCKET \
  --lake-bucket $DATA_LAKE_BUCKET \
  --entry-point s3://$ARTIFACTS_BUCKET/jobs/fx_bronze_to_silver.py
```

### Step 4: Run Salesforce Incremental (with Secrets Manager)

```bash
# Update Salesforce credentials in Secrets Manager
aws secretsmanager update-secret \
  --secret-id $PROJECT-$ENVIRONMENT-salesforce-credentials \
  --secret-string '{"username":"your-sf-user","password":"your-sf-pass","security_token":"your-sf-token","domain":"login"}'

# Submit Salesforce incremental job
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $ARTIFACTS_BUCKET \
  --lake-bucket $DATA_LAKE_BUCKET \
  --entry-point s3://$ARTIFACTS_BUCKET/jobs/salesforce_to_bronze.py \
  --extra-args "--env SF_SECRET_NAME=$PROJECT-$ENVIRONMENT-salesforce-credentials"
```

### Step 5: Start Kafka Stream Job

```bash
# Set Kafka environment variables
export KAFKA_BOOTSTRAP="your-confluent-bootstrap-servers"
export KAFKA_API_KEY="your-confluent-api-key"
export KAFKA_API_SECRET="your-confluent-api-secret"
export KAFKA_TOPIC="orders_events"

# Submit Kafka streaming job
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $ARTIFACTS_BUCKET \
  --lake-bucket $DATA_LAKE_BUCKET \
  --entry-point s3://$ARTIFACTS_BUCKET/jobs/kafka_orders_stream.py \
  --extra-args "--env KAFKA_BOOTSTRAP=$KAFKA_BOOTSTRAP --env KAFKA_API_KEY=$KAFKA_API_KEY --env KAFKA_API_SECRET=$KAFKA_API_SECRET --env KAFKA_TOPIC=$KAFKA_TOPIC"
```

### Step 6: Run Snowflake Backfill + MERGE

```bash
# Set Snowflake environment variables
export SF_URL="your-snowflake-account.snowflakecomputing.com"
export SF_USER="your-snowflake-user"
export SF_PASS="your-snowflake-password"
export SF_DB="SNOWFLAKE_SAMPLE_DATA"
export SF_SCHEMA="TPCH_SF1"
export SF_WH="COMPUTE_WH"

# Submit Snowflake backfill job
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $ARTIFACTS_BUCKET \
  --lake-bucket $DATA_LAKE_BUCKET \
  --entry-point s3://$ARTIFACTS_BUCKET/jobs/snowflake_to_bronze.py

# Submit Snowflake merge job
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $ARTIFACTS_BUCKET \
  --lake-bucket $DATA_LAKE_BUCKET \
  --entry-point s3://$ARTIFACTS_BUCKET/jobs/snowflake_bronze_to_silver_merge.py
```

### Step 7: Register Tables in Glue Catalog

```bash
# Register silver tables
python aws/scripts/register_glue_tables.py \
  --lake-root s3a://$DATA_LAKE_BUCKET/lake \
  --database pyspark_de_project_silver \
  --layer silver

# Register gold tables
python aws/scripts/register_glue_tables.py \
  --lake-root s3a://$DATA_LAKE_BUCKET/lake \
  --database pyspark_de_project_gold \
  --layer gold
```

### Step 8: Query in Athena

```sql
-- Sample query in Athena
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(total_amount) as total_spent
FROM pyspark_de_project_silver.orders
WHERE order_date >= current_date - interval '30' day
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 10;
```

### Step 9: Run Data Quality Checks

```bash
# Run DQ checks on silver fx_rates
python aws/scripts/run_ge_checks.py \
  --lake-root s3a://$DATA_LAKE_BUCKET/lake \
  --lake-bucket $DATA_LAKE_BUCKET \
  --suite dq/suites/silver_fx_rates.yml \
  --table fx_rates \
  --layer silver

# Run DQ checks on silver orders
python aws/scripts/run_ge_checks.py \
  --lake-root s3a://$DATA_LAKE_BUCKET/lake \
  --lake-bucket $DATA_LAKE_BUCKET \
  --suite dq/suites/silver_orders.yml \
  --table orders \
  --layer silver
```

### Step 10: Setup Lake Formation Governance (Optional)

```bash
# Setup Lake Formation tags and policies
python aws/scripts/lf_tags_seed.py --database pyspark_de_project_silver
```

## 📁 Directory Structure

```
aws/
├── scripts/                    # AWS-specific scripts
│   ├── emr_submit.sh          # EMR Serverless job submission
│   ├── register_glue_tables.py # Glue table registration
│   ├── run_ge_checks.py       # Data quality checks
│   └── lf_tags_seed.py        # Lake Formation setup
├── dags/                      # Airflow DAGs
│   └── daily_pipeline.py      # Main ETL pipeline DAG
├── jobs/                      # ETL job scripts
│   ├── fx_to_bronze.py
│   ├── fx_bronze_to_silver.py
│   ├── salesforce_to_bronze.py
│   ├── kafka_orders_stream.py
│   ├── snowflake_to_bronze.py
│   └── snowflake_bronze_to_silver_merge.py
├── infra/                     # Infrastructure as Code
│   └── terraform/             # Terraform modules
├── .github/workflows/         # CI/CD pipelines
│   └── ci.yml
└── README_AWS.md             # This file
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `LAKE_ROOT` | S3 root path of the data lake | `s3a://pyspark-de-project-dev-data-lake/lake` |
| `CODE_BUCKET` | S3 bucket for code artifacts | `pyspark-de-project-dev-artifacts` |
| `LAKE_BUCKET` | S3 bucket for data lake | `pyspark-de-project-dev-data-lake` |
| `AWS_REGION` | AWS region | `us-east-1` |
| `EMR_APP_ID` | EMR Serverless application ID | `00f7vjq1q0t0u000` |
| `EMR_ROLE_ARN` | IAM role ARN for EMR jobs | `arn:aws:iam::123456789012:role/...` |

### Terraform Variables

```hcl
variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project name"
  type        = string
  default     = "pyspark-de-project"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}
```

## 🚀 Usage Examples

### EMR Serverless Job Submission

```bash
# Basic job submission
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $ARTIFACTS_BUCKET \
  --lake-bucket $DATA_LAKE_BUCKET \
  --entry-point s3://$ARTIFACTS_BUCKET/jobs/fx_to_bronze.py

# With EMR 6.8 compatibility
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $ARTIFACTS_BUCKET \
  --lake-bucket $DATA_LAKE_BUCKET \
  --entry-point s3://$ARTIFACTS_BUCKET/jobs/fx_to_bronze.py \
  --emr6_8_compat
```

### Glue Table Registration

```bash
# Register all tables in a layer
python aws/scripts/register_glue_tables.py \
  --lake-root s3a://$DATA_LAKE_BUCKET/lake \
  --database pyspark_de_project_silver \
  --layer silver

# Register specific table
python aws/scripts/register_glue_tables.py \
  --lake-root s3a://$DATA_LAKE_BUCKET/lake \
  --database pyspark_de_project_silver \
  --layer silver \
  --table orders
```

### Data Quality Checks

```bash
# Run DQ checks on a table
python aws/scripts/run_ge_checks.py \
  --lake-root s3a://$DATA_LAKE_BUCKET/lake \
  --lake-bucket $DATA_LAKE_BUCKET \
  --suite dq/suites/silver_orders.yml \
  --table orders \
  --layer silver
```

## 🔍 Monitoring

### CloudWatch Logs

- EMR Serverless job logs: `/aws/emr-serverless/{application-id}`
- Application logs: Custom log groups for each service

### Data Quality Reports

- Great Expectations Data Docs: `s3://{lake-bucket}/gold/quality/`
- Automated quality alerts via CloudWatch

### Cost Monitoring

- EMR Serverless costs via CloudWatch metrics
- S3 storage costs via billing alerts
- Budget alerts for overall project costs

## 🛠️ Development

### Local Testing

```bash
# Test EMR submit script
./aws/scripts/emr_submit.sh --help

# Test Glue registration locally
python aws/scripts/register_glue_tables.py --help

# Test DQ checks locally
python aws/scripts/run_ge_checks.py --help
```

### CI/CD Pipeline

The GitHub Actions workflow automatically:
1. Lints and tests code
2. Builds Python wheel
3. Uploads artifacts to S3
4. Deploys to EMR Serverless

## 📚 Additional Resources

- [AWS EMR Serverless Documentation](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/)
- [Delta Lake on AWS](https://docs.delta.io/latest/aws.html)
- [Great Expectations on AWS](https://docs.greatexpectations.io/docs/deployment_patterns/how_to_use_great_expectations_in_aws/)
- [Apache Airflow on MWAA](https://docs.aws.amazon.com/mwaa/latest/userguide/)
