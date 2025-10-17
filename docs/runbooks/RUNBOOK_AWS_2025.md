# 🚀 AWS Production Runbook 2025

## 📋 Overview

This runbook provides step-by-step instructions for operating the AWS Production ETL Pipeline in 2025. It covers deployment, monitoring, troubleshooting, and maintenance procedures.

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    AWS Data Lakehouse Architecture              │
└─────────────────────────────────────────────────────────────────┘

┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Bronze    │    │   Silver    │    │    Gold     │
│             │    │             │    │             │
│ • Raw Data  │───▶│ • Conformed │───▶│ • Analytics │
│ • Quarantine│    │ • SCD-2     │    │ • Aggregates│
│ • Validation│    │ • Incremental│   │ • As-of Joins│
└─────────────┘    └─────────────┘    └─────────────┘
       │                   │                   │
       ▼                   ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   S3 Raw    │    │  S3 Silver  │    │  S3 Gold    │
│   Storage   │    │   Storage    │    │   Storage   │
└─────────────┘    └─────────────┘    └─────────────┘
```

---

## 🚀 Deployment Procedures

### Prerequisites
```bash
# Required tools
aws --version          # AWS CLI v2
terraform --version    # Terraform v1.5+
python --version       # Python 3.11+
docker --version       # Docker (for local testing)
```

### Step 1: Infrastructure Deployment
```bash
# 1. Navigate to Terraform directory
cd infra/terraform

# 2. Initialize Terraform
terraform init

# 3. Plan deployment
terraform plan -var-file="variables.tfvars"

# 4. Apply infrastructure
terraform apply -var-file="variables.tfvars"

# 5. Source outputs for scripts
source ../../aws/scripts/source_terraform_outputs.sh
```

### Step 2: Airflow Variables Setup
```bash
# Set required Airflow Variables in MWAA UI or via CLI
aws mwaa create-environment-variable \
  --name "EMR_APP_ID" \
  --value "$EMR_APP_ID"

aws mwaa create-environment-variable \
  --name "EMR_JOB_ROLE_ARN" \
  --value "$EMR_JOB_ROLE_ARN"

aws mwaa create-environment-variable \
  --name "GLUE_DB_SILVER" \
  --value "silver_db"

aws mwaa create-environment-variable \
  --name "GLUE_DB_GOLD" \
  --value "gold_db"

aws mwaa create-environment-variable \
  --name "S3_LAKE_BUCKET" \
  --value "$S3_LAKE_BUCKET"

aws mwaa create-environment-variable \
  --name "S3_CHECKPOINT_PREFIX" \
  --value "checkpoints"
```

### Step 3: Pipeline Deployment
```bash
# 1. Deploy code and scripts
./aws/scripts/aws_production_deploy.sh

# 2. Verify deployment
aws s3 ls s3://$S3_LAKE_BUCKET/
aws s3 ls s3://$CODE_BUCKET/

# 3. Check MWAA DAGs
# Navigate to MWAA webserver URL and verify DAGs are loaded
```

---

## 📊 Monitoring Procedures

### Daily Health Checks
```bash
# 1. Check pipeline execution status
aws emr-serverless list-job-runs --application-id $EMR_APP_ID --max-items 10

# 2. Check data freshness
aws s3 ls s3://$S3_LAKE_BUCKET/bronze/ --recursive | tail -10
aws s3 ls s3://$S3_LAKE_BUCKET/silver/ --recursive | tail -10
aws s3 ls s3://$S3_LAKE_BUCKET/gold/ --recursive | tail -10

# 3. Check data quality
python aws/scripts/run_ge_checks.py --table customers
python aws/scripts/run_ge_checks.py --table orders

# 4. Check quarantine
aws s3 ls s3://$S3_LAKE_BUCKET/quarantine/ --recursive | wc -l
```

### Weekly Maintenance
```bash
# 1. Delta table optimization
aws emr-serverless start-job-run \
  --application-id $EMR_APP_ID \
  --execution-role-arn $EMR_JOB_ROLE_ARN \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://'$CODE_BUCKET'/scripts/optimize_delta_tables.py"
    }
  }'

# 2. S3 lifecycle policy check
aws s3api get-bucket-lifecycle-configuration --bucket $S3_LAKE_BUCKET

# 3. Glue table registration update
python aws/scripts/register_glue_tables.py

# 4. Performance metrics review
# Check CloudWatch dashboards for trends
```

---

## 🔧 Troubleshooting Guide

### Common Issues and Solutions

#### 1. EMR Serverless Job Failures
**Symptoms**: Jobs failing to start or complete
```bash
# Check job logs
aws logs describe-log-groups --log-group-name-prefix /aws/emr-serverless
aws logs get-log-events --log-group-name /aws/emr-serverless/jobs --log-stream-name <stream-name>

# Common fixes:
# - Verify IAM roles exist and have correct permissions
# - Check S3 bucket policies
# - Verify application is in RUNNING state
```

#### 2. Data Quality Failures
**Symptoms**: DQ checks failing, data in quarantine
```bash
# Check quarantine data
aws s3 ls s3://$S3_LAKE_BUCKET/quarantine/ --recursive

# Investigate specific failures
python aws/scripts/run_ge_checks.py --table <table_name> --verbose

# Common fixes:
# - Check source data format changes
# - Verify schema evolution
# - Update DQ policies if needed
```

#### 3. Airflow DAG Issues
**Symptoms**: DAGs not loading or tasks failing
```bash
# Check MWAA logs
aws logs describe-log-groups --log-group-name-prefix /aws/mwaa
aws logs get-log-events --log-group-name /aws/mwaa/<env-name> --log-stream-name <stream-name>

# Common fixes:
# - Verify Airflow Variables are set correctly
# - Check Python dependencies in requirements.txt
# - Verify DAG syntax with airflow dags list
```

#### 4. S3 Access Issues
**Symptoms**: Permission denied errors
```bash
# Check bucket policies
aws s3api get-bucket-policy --bucket $S3_LAKE_BUCKET

# Check IAM policies
aws iam get-role-policy --role-name EMR_DefaultRole --policy-name DataLakeAccess

# Common fixes:
# - Update bucket policies for new resources
# - Verify IAM role permissions
# - Check KMS key permissions
```

---

## 📈 Performance Optimization

### Spark Configuration Tuning
```bash
# Key Spark configurations for production
spark.sql.adaptive.enabled=true
spark.sql.adaptive.skewJoin.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.shuffle.partitions=400
spark.sql.autoBroadcastJoinThreshold=64MB
```

### Delta Lake Optimization
```bash
# Regular maintenance commands
# Compaction (run weekly)
OPTIMIZE delta.`s3://$S3_LAKE_BUCKET/silver/customers_conformed`

# Vacuum old versions (run monthly)
VACUUM delta.`s3://$S3_LAKE_BUCKET/silver/customers_conformed` RETAIN 168 HOURS

# Z-ordering (run after major data loads)
OPTIMIZE delta.`s3://$S3_LAKE_BUCKET/gold/customer_analytics` ZORDER BY customer_id, date
```

### Cost Optimization
```bash
# S3 lifecycle policies
# - Move old data to IA after 30 days
# - Move to Glacier after 90 days
# - Delete after 7 years

# EMR Serverless optimization
# - Use appropriate worker types
# - Enable auto-scaling
# - Monitor idle time
```

---

## 🔒 Security Procedures

### Access Management
```bash
# 1. Regular access reviews
aws iam list-users
aws iam list-roles --query 'Roles[?contains(RoleName, `ETL`)]'

# 2. Rotate access keys quarterly
aws iam create-access-key --user-name <username>
aws iam delete-access-key --user-name <username> --access-key-id <old-key>

# 3. Update IAM policies as needed
aws iam put-role-policy --role-name EMR_DefaultRole --policy-name DataLakeAccess --policy-document file://policy.json
```

### Data Encryption
```bash
# Verify encryption status
aws s3api get-bucket-encryption --bucket $S3_LAKE_BUCKET
aws kms describe-key --key-id <key-id>

# Enable encryption if not present
aws s3api put-bucket-encryption --bucket $S3_LAKE_BUCKET --server-side-encryption-configuration file://encryption.json
```

### Compliance Monitoring
```bash
# 1. Audit log review
aws logs describe-log-groups --log-group-name-prefix /aws/cloudtrail
aws logs get-log-events --log-group-name /aws/cloudtrail --log-stream-name <stream-name>

# 2. Data lineage verification
# Use Marquez UI to verify data flow
# Check OpenLineage events in CloudWatch

# 3. Backup verification
aws s3 ls s3://$S3_LAKE_BUCKET/backups/ --recursive
```

---

## 📋 Emergency Procedures

### Data Recovery
```bash
# 1. Restore from backup
aws s3 cp s3://$S3_LAKE_BUCKET/backups/bronze/2024-01-01/ s3://$S3_LAKE_BUCKET/bronze/ --recursive

# 2. Delta time travel
# Restore to specific version
RESTORE TABLE delta.`s3://$S3_LAKE_BUCKET/silver/customers_conformed` TO VERSION AS OF 10

# 3. Re-run affected pipelines
# Trigger specific DAG runs in Airflow UI
```

### Service Outage Response
```bash
# 1. Check service status
aws health describe-events --filter services=EMR,S3,GLUE

# 2. Switch to backup region (if configured)
export AWS_DEFAULT_REGION=us-west-2

# 3. Notify stakeholders
# Use configured alerting channels

# 4. Document incident
# Update incident log with details
```

### Performance Degradation
```bash
# 1. Check CloudWatch metrics
aws cloudwatch get-metric-statistics --namespace AWS/EMR-Serverless --metric-name JobRunDuration

# 2. Scale resources
aws emr-serverless update-application --application-id $EMR_APP_ID --initial-capacity '{"Worker": {"value": 10}}'

# 3. Optimize queries
# Review slow-running queries in Athena
# Check Spark UI for bottlenecks
```

---

## 📞 Contact Information

### On-Call Rotation
- **Primary**: Data Engineering Team Lead
- **Secondary**: Senior Data Engineer
- **Escalation**: Platform Engineering Manager

### Communication Channels
- **Slack**: #data-platform-alerts
- **Email**: data-engineering@company.com
- **PagerDuty**: Data Platform On-Call

### External Vendors
- **AWS Support**: Enterprise Support Plan
- **HubSpot**: Technical Account Manager
- **Snowflake**: Customer Success Manager

---

## 📚 Additional Resources

### Documentation
- [Project Explanation](project_explain.md)
- [Improvement Plan](IMPROVEMENT_PLAN.md)
- [Data Quality Guide](DQ_GUIDE.md)
- [Security Guide](SECURITY_GUIDE.md)

### Tools and Dashboards
- **Airflow UI**: MWAA Webserver URL
- **Marquez**: Data Lineage UI
- **CloudWatch**: AWS Monitoring
- **Athena**: Ad-hoc Queries

### Training Materials
- [PySpark Best Practices](PYSPARK_GUIDE.md)
- [Delta Lake Documentation](DELTA_GUIDE.md)
- [AWS EMR Serverless Guide](EMR_GUIDE.md)
- [Data Quality Framework](DQ_FRAMEWORK.md)

---

**🎯 This runbook is your go-to resource for operating the AWS Production ETL Pipeline. Keep it updated and accessible to all team members.**
