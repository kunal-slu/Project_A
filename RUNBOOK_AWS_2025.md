# AWS Production Runbook 2025

**Date**: 2025-01-27  
**Version**: 1.0  
**Environment**: Production-ready AWS deployment

## 🎯 Overview

This runbook provides a complete, step-by-step guide to deploy and run the PySpark Data Engineering project on AWS. All steps are copy-paste ready and require no code modifications.

## 📋 Prerequisites

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

# Data Lake Configuration
export LAKE_BUCKET="pyspark-de-project-dev-data-lake"
export CODE_BUCKET="pyspark-de-project-dev-artifacts"

# EMR Serverless Configuration
export EMR_APP_ID="your-emr-app-id"
export EMR_ROLE_ARN="arn:aws:iam::123456789012:role/your-role"

# Salesforce Configuration
export SF_SECRET_NAME="pyspark-de-project-dev-salesforce-credentials"

# Kafka Configuration
export KAFKA_BOOTSTRAP="your-confluent-bootstrap-servers"
export KAFKA_API_KEY="your-confluent-api-key"
export KAFKA_API_SECRET="your-confluent-api-secret"

# Snowflake Configuration
export SNOWFLAKE_URL="your-account.snowflakecomputing.com"
export SNOWFLAKE_USER="your-username"
export SNOWFLAKE_PASSWORD="your-password"
export SNOWFLAKE_DATABASE="SNOWFLAKE_SAMPLE_DATA"
export SNOWFLAKE_SCHEMA="TPCH_SF1"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"

# Logging Configuration
export LOG_LEVEL="INFO"
```

## 🏗️ Infrastructure Deployment

### Step 1: Deploy Core Infrastructure
```bash
# Initialize Terraform
cd aws/infra/terraform
terraform init

# Review the plan
terraform plan -var="project=$PROJECT" -var="environment=$ENVIRONMENT"

# Deploy infrastructure
terraform apply -var="project=$PROJECT" -var="environment=$ENVIRONMENT"

# Save outputs
terraform output -json > terraform-outputs.json
```

### Step 2: Extract Infrastructure Outputs
```bash
# Extract key values from Terraform outputs
export EMR_APP_ID=$(terraform output -raw emr_serverless_application_id)
export EMR_ROLE_ARN=$(terraform output -raw emr_serverless_job_role_arn)
export LAKE_BUCKET=$(terraform output -raw data_lake_bucket)
export CODE_BUCKET=$(terraform output -raw artifacts_bucket)

echo "EMR App ID: $EMR_APP_ID"
echo "EMR Role ARN: $EMR_ROLE_ARN"
echo "Lake Bucket: $LAKE_BUCKET"
echo "Code Bucket: $CODE_BUCKET"
```

## 📦 Code Deployment

### Step 3: Build and Upload Code
```bash
# Build Python wheel
cd ../../
python -m build

# Upload wheel and jobs to S3
aws s3 cp dist/*.whl s3://$CODE_BUCKET/dist/
aws s3 sync src/pyspark_interview_project/jobs/ s3://$CODE_BUCKET/jobs/
aws s3 sync aws/scripts/ s3://$CODE_BUCKET/scripts/
aws s3 sync config/ s3://$CODE_BUCKET/config/
aws s3 sync dq/ s3://$CODE_BUCKET/dq/
aws s3 sync kafka/ s3://$CODE_BUCKET/kafka/
```

## 🔄 Data Pipeline Execution

### Step 4: FX Rates Bronze to Silver
```bash
# Submit FX to Bronze job
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $CODE_BUCKET \
  --lake-bucket $LAKE_BUCKET \
  --entry-point s3://$CODE_BUCKET/jobs/fx_to_bronze.py \
  --extra-args "--lake-root s3://$LAKE_BUCKET --days-back 7"

# Submit FX Bronze to Silver job
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $CODE_BUCKET \
  --lake-bucket $LAKE_BUCKET \
  --entry-point s3://$CODE_BUCKET/jobs/fx_bronze_to_silver.py \
  --extra-args "--lake-root s3://$LAKE_BUCKET"
```

### Step 5: Salesforce Incremental Load
```bash
# Store Salesforce credentials in Secrets Manager
aws secretsmanager create-secret \
  --name "$SF_SECRET_NAME" \
  --secret-string '{"username":"your-sf-user","password":"your-sf-pass","security_token":"your-sf-token","domain":"login"}' \
  --region $AWS_REGION

# Submit Salesforce incremental job
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $CODE_BUCKET \
  --lake-bucket $LAKE_BUCKET \
  --entry-point s3://$CODE_BUCKET/jobs/salesforce_to_bronze.py \
  --extra-args "--lake-root s3://$LAKE_BUCKET --env SF_SECRET_NAME=$SF_SECRET_NAME"
```

### Step 6: Snowflake Backfill and Merge
```bash
# Submit Snowflake backfill job
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $CODE_BUCKET \
  --lake-bucket $LAKE_BUCKET \
  --entry-point s3://$CODE_BUCKET/jobs/snowflake_to_bronze.py \
  --extra-args "--lake-root s3://$LAKE_BUCKET"

# Submit Snowflake merge job
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $CODE_BUCKET \
  --lake-bucket $LAKE_BUCKET \
  --entry-point s3://$CODE_BUCKET/jobs/snowflake_bronze_to_silver_merge.py \
  --extra-args "--lake-root s3://$LAKE_BUCKET"
```

### Step 7: Kafka Streaming (Optional)
```bash
# Submit Kafka streaming job
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $CODE_BUCKET \
  --lake-bucket $LAKE_BUCKET \
  --entry-point s3://$CODE_BUCKET/jobs/kafka_orders_stream.py \
  --extra-args "--lake-root s3://$LAKE_BUCKET --env KAFKA_BOOTSTRAP=$KAFKA_BOOTSTRAP --env KAFKA_API_KEY=$KAFKA_API_KEY --env KAFKA_API_SECRET=$KAFKA_API_SECRET"
```

## 🗄️ Data Catalog and Quality

### Step 8: Register Glue Tables
```bash
# Register silver tables
python aws/scripts/register_glue_tables.py \
  --lake-root s3://$LAKE_BUCKET \
  --database ${PROJECT}_${ENVIRONMENT}_silver \
  --layer silver

# Register gold tables
python aws/scripts/register_glue_tables.py \
  --lake-root s3://$LAKE_BUCKET \
  --database ${PROJECT}_${ENVIRONMENT}_gold \
  --layer gold
```

### Step 9: Run Data Quality Checks
```bash
# Run DQ checks on silver FX rates
python aws/scripts/run_ge_checks.py \
  --lake-root s3://$LAKE_BUCKET \
  --lake-bucket $LAKE_BUCKET \
  --suite dq/suites/silver_fx_rates.yml \
  --table fx_rates \
  --layer silver

# Run DQ checks on silver orders
python aws/scripts/run_ge_checks.py \
  --lake-root s3://$LAKE_BUCKET \
  --lake-bucket $LAKE_BUCKET \
  --suite dq/suites/silver_orders.yml \
  --table orders \
  --layer silver
```

### Step 10: Build Gold Tables
```bash
# Submit Silver to Gold transformation
./aws/scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $CODE_BUCKET \
  --lake-bucket $LAKE_BUCKET \
  --entry-point s3://$CODE_BUCKET/jobs/silver_to_gold.py \
  --extra-args "--lake-root s3://$LAKE_BUCKET"
```

## 🔍 Query with Athena

### Step 11: Query Gold Tables
```sql
-- Sample queries in Athena
-- Check customer dimension
SELECT * FROM pyspark_de_project_dev_gold.dim_customers LIMIT 10;

-- Check FX rates
SELECT * FROM pyspark_de_project_dev_gold.dim_fx 
WHERE as_of_date >= current_date - interval '7' day
ORDER BY as_of_date DESC, ccy;

-- Check orders fact table
SELECT 
    o.order_id,
    c.name as customer_name,
    o.order_date,
    o.currency,
    o.amount_native,
    o.amount_usd,
    o.status
FROM pyspark_de_project_dev_gold.fact_orders o
JOIN pyspark_de_project_dev_gold.dim_customers c ON o.customer_id = c.id
WHERE o.order_date >= current_date - interval '30' day
ORDER BY o.order_date DESC
LIMIT 10;
```

## 🛠️ Maintenance and Optimization

### Step 12: Delta Table Optimization
```bash
# Run weekly optimization
python aws/scripts/delta_optimize_vacuum.py \
  --lake-root s3://$LAKE_BUCKET \
  --layers silver gold
```

### Step 13: Lake Formation Setup (Optional)
```bash
# Setup Lake Formation tags and policies
python aws/scripts/lf_tags_seed.py \
  --database ${PROJECT}_${ENVIRONMENT}_silver
```

## 🚨 Common Gotchas and Troubleshooting

### Permissions Issues
- **Problem**: Access denied errors when reading/writing S3
- **Solution**: Ensure EMR role has proper S3 permissions and KMS key access
- **Check**: `aws iam get-role-policy --role-name your-role --policy-name your-policy`

### Delta Lake on S3
- **Problem**: Delta operations failing on S3
- **Solution**: Ensure `spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore`
- **Check**: Verify Delta packages are included in EMR job

### Salesforce Authentication
- **Problem**: Salesforce API authentication failures
- **Solution**: Check security token and domain settings
- **Check**: Verify credentials in Secrets Manager: `aws secretsmanager get-secret-value --secret-id $SF_SECRET_NAME`

### Kafka Authentication
- **Problem**: Kafka connection failures
- **Solution**: Verify Confluent Cloud credentials and bootstrap servers
- **Check**: Test connection with Kafka tools

### Glue Table Registration
- **Problem**: Tables not appearing in Athena
- **Solution**: Ensure Glue databases exist and have proper permissions
- **Check**: `aws glue get-databases` and `aws glue get-tables --database-name your-db`

## 📊 Monitoring and Observability

### CloudWatch Logs
- EMR Serverless job logs: `/aws/emr-serverless/$PROJECT-$ENVIRONMENT`
- Application logs: `s3://$LAKE_BUCKET/logs/`

### Data Quality Reports
- Great Expectations results: `s3://$LAKE_BUCKET/gold/quality/`
- Data Docs: HTML reports in S3

### Cost Monitoring
- EMR Serverless costs in CloudWatch
- S3 storage costs and lifecycle policies
- Data transfer costs

## 🔄 Automated Pipeline

### Airflow DAGs
The project includes Airflow DAGs for automated execution:
- `returns_pipeline_dag.py`: Main ETL pipeline
- `catalog_and_dq_dag.py`: Catalog registration and DQ checks

### CI/CD Pipeline
GitHub Actions workflow includes:
- Code quality checks (black, isort, flake8)
- Unit tests (pytest)
- Build and deployment to S3

## 📈 Performance Optimization

### Delta Lake Best Practices
- Regular OPTIMIZE operations for small files
- VACUUM old files to reduce storage costs
- Z-ordering for query performance

### EMR Serverless Optimization
- Right-size driver and executor memory
- Use adaptive query execution
- Enable dynamic allocation

## 🎯 Success Criteria

After following this runbook, you should have:
- ✅ All data sources ingested to Bronze layer
- ✅ Clean, validated data in Silver layer
- ✅ Business-ready Gold tables
- ✅ Glue tables registered for Athena queries
- ✅ Data quality checks passing
- ✅ Monitoring and alerting configured

## 📞 Support

For issues or questions:
1. Check CloudWatch logs for error details
2. Verify environment variables and permissions
3. Review the troubleshooting section above
4. Check project documentation in `docs/`

---

**Last Updated**: 2025-01-27  
**Maintained by**: Data Engineering Team
