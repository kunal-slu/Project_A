# AWS Data Platform Runbook 2025
**Updated: 2025-01-27**

## 🎯 Overview

This runbook provides step-by-step instructions for deploying and operating the PySpark data engineering platform on AWS. The platform supports multiple data sources, automated ETL pipelines, data quality gates, and governance features.

## 📋 Prerequisites

### Required Tools & Versions
- **AWS CLI**: v2.15.0+
- **Terraform**: v1.6.0+
- **Python**: 3.10+
- **Docker**: 20.10+ (for local testing)
- **jq**: 1.6+ (for JSON processing)

### AWS Account Setup
- AWS Account with appropriate permissions
- IAM user/role with administrative access
- AWS region: `us-east-1` (recommended)

### Environment Variables
```bash
export AWS_REGION="us-east-1"
export PROJECT="pyspark-de-project"
export ENVIRONMENT="dev"
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output tsv)
```

## 🏗️ Infrastructure Deployment

### Step 1: Deploy Core Infrastructure

```bash
# Navigate to AWS infrastructure
cd aws/infra/terraform

# Initialize Terraform
terraform init

# Review the plan
terraform plan -var="project=$PROJECT" -var="environment=$ENVIRONMENT"

# Deploy infrastructure
terraform apply -var="project=$PROJECT" -var="environment=$ENVIRONMENT"

# Save critical outputs
export DATA_LAKE_BUCKET=$(terraform output -raw data_lake_bucket_name)
export ARTIFACTS_BUCKET=$(terraform output -raw artifacts_bucket_name)
export EMR_APP_ID=$(terraform output -raw emr_serverless_application_id)
export EMR_ROLE_ARN=$(terraform output -raw emr_serverless_job_role_arn)
export GLUE_DB_SILVER=$(terraform output -raw glue_database_silver)
export GLUE_DB_GOLD=$(terraform output -raw glue_database_gold)
export KMS_KEY_ID=$(terraform output -raw kms_key_id)
```

### Step 2: Verify Infrastructure

```bash
# Verify S3 buckets
aws s3 ls s3://$DATA_LAKE_BUCKET
aws s3 ls s3://$ARTIFACTS_BUCKET

# Verify EMR Serverless app
aws emr-serverless get-application --application-id $EMR_APP_ID

# Verify Glue databases
aws glue get-database --name $GLUE_DB_SILVER
aws glue get-database --name $GLUE_DB_GOLD
```

## 📦 Code Deployment

### Step 3: Build and Upload Artifacts

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

# Verify uploads
aws s3 ls s3://$ARTIFACTS_BUCKET/dist/
aws s3 ls s3://$ARTIFACTS_BUCKET/jobs/
```

## 🔄 Data Pipeline Execution

### Step 4: FX Data Pipeline (Bronze → Silver)

```bash
# Submit FX to Bronze job
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $ARTIFACTS_BUCKET \
  --lake-bucket $DATA_LAKE_BUCKET \
  --entry-point s3://$ARTIFACTS_BUCKET/jobs/fx_to_bronze.py \
  --extra-args "--config s3://$ARTIFACTS_BUCKET/config/application-aws.yaml"

# Wait for completion, then submit Bronze to Silver
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $ARTIFACTS_BUCKET \
  --lake-bucket $DATA_LAKE_BUCKET \
  --entry-point s3://$ARTIFACTS_BUCKET/jobs/fx_bronze_to_silver.py \
  --extra-args "--config s3://$ARTIFACTS_BUCKET/config/application-aws.yaml"
```

### Step 5: Salesforce Incremental Pipeline

```bash
# Create Salesforce credentials in Secrets Manager
aws secretsmanager create-secret \
  --name "$PROJECT-$ENVIRONMENT-salesforce-credentials" \
  --secret-string '{
    "username": "your-salesforce-username",
    "password": "your-salesforce-password", 
    "security_token": "your-salesforce-token",
    "domain": "login"
  }'

# Submit Salesforce incremental job
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $ARTIFACTS_BUCKET \
  --lake-bucket $DATA_LAKE_BUCKET \
  --entry-point s3://$ARTIFACTS_BUCKET/jobs/salesforce_to_bronze.py \
  --extra-args "--config s3://$ARTIFACTS_BUCKET/config/application-aws.yaml --env SF_SECRET_NAME=$PROJECT-$ENVIRONMENT-salesforce-credentials"
```

### Step 6: Kafka Streaming Pipeline

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
  --extra-args "--config s3://$ARTIFACTS_BUCKET/config/application-aws.yaml --env KAFKA_BOOTSTRAP=$KAFKA_BOOTSTRAP --env KAFKA_API_KEY=$KAFKA_API_KEY --env KAFKA_API_SECRET=$KAFKA_API_SECRET --env KAFKA_TOPIC=$KAFKA_TOPIC"

# Produce sample events (in separate terminal)
python scripts/kafka_producer.py \
  --bootstrap-servers $KAFKA_BOOTSTRAP \
  --api-key $KAFKA_API_KEY \
  --api-secret $KAFKA_API_SECRET \
  --topic $KAFKA_TOPIC \
  --num-orders 50
```

### Step 7: Snowflake Backfill Pipeline

```bash
# Set Snowflake environment variables
export SF_URL="your-snowflake-account.snowflakecomputing.com"
export SF_USER="your-snowflake-username"
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
  --entry-point s3://$ARTIFACTS_BUCKET/jobs/snowflake_to_bronze.py \
  --extra-args "--config s3://$ARTIFACTS_BUCKET/config/application-aws.yaml"

# Submit Snowflake merge job
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $ARTIFACTS_BUCKET \
  --lake-bucket $DATA_LAKE_BUCKET \
  --entry-point s3://$ARTIFACTS_BUCKET/jobs/snowflake_bronze_to_silver_merge.py \
  --extra-args "--config s3://$ARTIFACTS_BUCKET/config/application-aws.yaml"
```

### Step 8: Optional DMS RDS CDC Pipeline

```bash
# Create DMS replication instance (if RDS source available)
aws dms create-replication-instance \
  --replication-instance-identifier "$PROJECT-$ENVIRONMENT-dms-instance" \
  --replication-instance-class "dms.t3.micro" \
  --allocated-storage 20

# Submit DMS to Bronze job
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $ARTIFACTS_BUCKET \
  --lake-bucket $DATA_LAKE_BUCKET \
  --entry-point s3://$ARTIFACTS_BUCKET/jobs/dms_rds_to_bronze.py \
  --extra-args "--config s3://$ARTIFACTS_BUCKET/config/application-aws.yaml"
```

## 📊 Data Catalog & Querying

### Step 9: Register Tables in Glue Catalog

```bash
# Register silver tables
python aws/scripts/register_glue_tables.py \
  --lake-root s3a://$DATA_LAKE_BUCKET/lake \
  --database $GLUE_DB_SILVER \
  --layer silver

# Register gold tables
python aws/scripts/register_glue_tables.py \
  --lake-root s3a://$DATA_LAKE_BUCKET/lake \
  --database $GLUE_DB_GOLD \
  --layer gold

# Verify table registration
aws glue get-tables --database-name $GLUE_DB_SILVER
aws glue get-tables --database-name $GLUE_DB_GOLD
```

### Step 10: Query Data in Athena

```sql
-- Sample queries in Athena
-- FX rates analysis
SELECT 
    ccy,
    rate_to_base,
    as_of_date,
    rate_category
FROM pyspark_de_project_silver.fx_rates
WHERE as_of_date >= current_date - interval '7' day
ORDER BY as_of_date DESC, ccy;

-- Orders analysis
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(total_amount) as total_spent,
    AVG(total_amount) as avg_order_value
FROM pyspark_de_project_silver.orders
WHERE order_date >= current_date - interval '30' day
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 10;

-- Cross-layer analysis
SELECT 
    o.customer_id,
    o.order_date,
    o.total_amount,
    f.rate_to_base as usd_rate
FROM pyspark_de_project_silver.orders o
LEFT JOIN pyspark_de_project_silver.fx_rates f
  ON o.currency = f.ccy
  AND o.order_date = f.as_of_date
WHERE o.order_date >= current_date - interval '7' day;
```

## 🔍 Data Quality Gates

### Step 11: Run Data Quality Checks

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

# Check DQ results
aws s3 ls s3://$DATA_LAKE_BUCKET/lake/gold/quality/ --recursive
```

### Step 12: Demonstrate DQ Failure Behavior

```bash
# Introduce data quality issue (for demo)
aws s3 cp s3://$DATA_LAKE_BUCKET/lake/silver/fx_rates/ s3://$DATA_LAKE_BUCKET/lake/silver/fx_rates_backup/ --recursive

# Create test data with quality issues
echo '{"ccy": null, "rate_to_base": -1.0, "as_of_date": "2025-01-27"}' | aws s3 cp - s3://$DATA_LAKE_BUCKET/lake/silver/fx_rates/test_bad_data.json

# Run DQ checks (should fail)
python aws/scripts/run_ge_checks.py \
  --lake-root s3a://$DATA_LAKE_BUCKET/lake \
  --lake-bucket $DATA_LAKE_BUCKET \
  --suite dq/suites/silver_fx_rates.yml \
  --table fx_rates \
  --layer silver

# Restore clean data
aws s3 rm s3://$DATA_LAKE_BUCKET/lake/silver/fx_rates/ --recursive
aws s3 cp s3://$DATA_LAKE_BUCKET/lake/silver/fx_rates_backup/ s3://$DATA_LAKE_BUCKET/lake/silver/fx_rates/ --recursive
```

## 🔗 Data Lineage

### Step 13: Configure OpenLineage

```bash
# Set OpenLineage environment variables
export OPENLINEAGE_URL="http://your-openlineage-server:8080"
export OPENLINEAGE_NAMESPACE="pyspark-de-project-aws"

# Deploy MWAA environment with OpenLineage
# (This would be done via Terraform or AWS Console)
# The DAG will automatically use these environment variables
```

## 🛡️ Data Governance

### Step 14: Setup Lake Formation Governance

```bash
# Run Lake Formation tags seeding
python aws/scripts/lf_tags_seed.py \
  --database $GLUE_DB_SILVER \
  --principal-arn "arn:aws:iam::$AWS_ACCOUNT_ID:role/DataAnalystRole"

# Verify LF-Tags
aws lakeformation list-lf-tags

# Test governance by attempting to query PII data
# (This should be restricted based on LF-Tag policies)
```

## 💰 Cost & File Health

### Step 15: Run Delta Optimization

```bash
# Run Delta table optimization and vacuum
python aws/scripts/delta_optimize_vacuum.py \
  --lake-root s3a://$DATA_LAKE_BUCKET/lake \
  --layers silver gold \
  --retention-hours 168

# Schedule this to run weekly via EventBridge
aws events put-rule \
  --name "$PROJECT-$ENVIRONMENT-delta-optimization" \
  --schedule-expression "rate(7 days)"

aws events put-targets \
  --rule "$PROJECT-$ENVIRONMENT-delta-optimization" \
  --targets "Id"="1","Arn"="arn:aws:lambda:us-east-1:$AWS_ACCOUNT_ID:function:delta-optimization"
```

## 📊 Monitoring & Alerting

### Step 16: Setup CloudWatch Monitoring

```bash
# Create CloudWatch dashboard
aws cloudwatch put-dashboard \
  --dashboard-name "$PROJECT-$ENVIRONMENT-dashboard" \
  --dashboard-body file://aws/monitoring/dashboard.json

# Create SNS topic for alerts
aws sns create-topic --name "$PROJECT-$ENVIRONMENT-alerts"

# Create CloudWatch alarms
aws cloudwatch put-metric-alarm \
  --alarm-name "$PROJECT-$ENVIRONMENT-emr-failures" \
  --alarm-description "Alert when EMR job failure rate exceeds 10%" \
  --metric-name JobRunFailureCount \
  --namespace AWS/EMRServerless \
  --statistic Sum \
  --period 300 \
  --threshold 5 \
  --comparison-operator GreaterThanThreshold
```

## 🔄 Disaster Recovery

### Step 17: DR & Rollback Procedures

```bash
# Enable S3 Cross-Region Replication
aws s3api put-bucket-replication \
  --bucket $DATA_LAKE_BUCKET \
  --replication-configuration file://aws/infra/s3-replication.json

# Reprocess by partition (for data recovery)
aws s3 ls s3://$DATA_LAKE_BUCKET/lake/silver/fx_rates/ --recursive | grep "proc_date=2025-01-27"

# Re-run specific partition
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $ARTIFACTS_BUCKET \
  --lake-bucket $DATA_LAKE_BUCKET \
  --entry-point s3://$ARTIFACTS_BUCKET/jobs/fx_bronze_to_silver.py \
  --extra-args "--config s3://$ARTIFACTS_BUCKET/config/application-aws.yaml --partition-date 2025-01-27"
```

## 🚨 Troubleshooting

### Common Issues & Solutions

#### EMR Serverless Job Failures
```bash
# Check job status
aws emr-serverless get-job-run --application-id $EMR_APP_ID --job-run-id <JOB_ID>

# Check logs
aws logs get-log-events --log-group-name "/aws/emr-serverless/$EMR_APP_ID" --log-stream-name "<JOB_ID>"

# Common fixes:
# - Increase driver/executor memory
# - Check IAM permissions
# - Verify Delta JARs are included
```

#### Delta Lake Issues
```bash
# Check Delta table history
aws s3 ls s3://$DATA_LAKE_BUCKET/lake/silver/fx_rates/_delta_log/

# Repair Delta table
python -c "
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, 's3a://$DATA_LAKE_BUCKET/lake/silver/fx_rates')
dt.repair()
"
```

#### Athena Query Issues
```bash
# Refresh table metadata
aws glue start-crawler --name "$PROJECT-$ENVIRONMENT-crawler"

# Check table schema
aws glue get-table --database-name $GLUE_DB_SILVER --name fx_rates
```

#### Kafka Streaming Issues
```bash
# Check DLQ contents
aws s3 ls s3://$DATA_LAKE_BUCKET/lake/_errors/kafka/orders_events/ --recursive

# Clear checkpoint and restart
aws s3 rm s3://$DATA_LAKE_BUCKET/lake/_checkpoints/orders_events/ --recursive
```

#### Salesforce Connection Issues
```bash
# Check Secrets Manager
aws secretsmanager get-secret-value --secret-id "$PROJECT-$ENVIRONMENT-salesforce-credentials"

# Test connection
python -c "
import boto3
import json
client = boto3.client('secretsmanager')
secret = client.get_secret_value(SecretId='$PROJECT-$ENVIRONMENT-salesforce-credentials')
creds = json.loads(secret['SecretString'])
print('Credentials retrieved successfully')
"
```

## 📈 Performance Optimization

### Step 18: Performance Tuning

```bash
# Optimize S3 storage class
aws s3api put-bucket-lifecycle-configuration \
  --bucket $DATA_LAKE_BUCKET \
  --lifecycle-configuration file://aws/infra/s3-lifecycle.json

# Enable S3 Transfer Acceleration
aws s3api put-bucket-accelerate-configuration \
  --bucket $DATA_LAKE_BUCKET \
  --accelerate-configuration Status=Enabled
```

## 🔐 Security Best Practices

### Step 19: Security Hardening

```bash
# Enable S3 bucket versioning
aws s3api put-bucket-versioning \
  --bucket $DATA_LAKE_BUCKET \
  --versioning-configuration Status=Enabled

# Enable S3 bucket encryption
aws s3api put-bucket-encryption \
  --bucket $DATA_LAKE_BUCKET \
  --server-side-encryption-configuration file://aws/infra/s3-encryption.json

# Rotate KMS keys
aws kms schedule-key-deletion --key-id $KMS_KEY_ID --pending-window-in-days 7
```

## 📋 Maintenance Checklist

### Daily
- [ ] Check EMR Serverless job status
- [ ] Review data quality reports
- [ ] Monitor S3 storage usage
- [ ] Check CloudWatch logs for errors

### Weekly
- [ ] Run Delta table optimization
- [ ] Review and clean up old logs
- [ ] Check cost reports
- [ ] Update documentation if needed

### Monthly
- [ ] Review and update IAM permissions
- [ ] Check for security vulnerabilities
- [ ] Review and optimize costs
- [ ] Update dependencies

## 📞 Support Contacts

- **Primary On-Call**: data-engineering@yourcompany.com
- **Secondary On-Call**: platform-engineering@yourcompany.com
- **Escalation**: engineering-manager@yourcompany.com
- **AWS Support**: [Support Case URL]

---

**Last Updated**: 2025-01-27  
**Version**: 1.0  
**Maintainer**: Data Engineering Team
