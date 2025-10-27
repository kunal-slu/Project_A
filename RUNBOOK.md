# PySpark Data Engineer Project - Runbook

## 🚨 Emergency Contacts

- **Primary On-Call**: [Your Name] - [Phone] - [Email]
- **Secondary On-Call**: [Backup Name] - [Phone] - [Email]
- **Escalation**: [Manager Name] - [Phone] - [Email]

## 📋 Quick Reference

### Key AWS Resources

| Resource | Name | Region | Purpose |
|----------|------|--------|---------|
| S3 Data Lake | `pyspark-de-project-dev-data-lake` | us-east-1 | Raw and processed data |
| S3 Artifacts | `pyspark-de-project-dev-artifacts` | us-east-1 | Code and configurations |
| EMR Serverless | `pyspark-de-project-dev-emr-serverless` | us-east-1 | Spark processing |
| MWAA | `pyspark-de-project-dev-mwaa` | us-east-1 | Airflow orchestration |
| Glue DB Silver | `pyspark_de_project_silver` | us-east-1 | Silver layer tables |
| Glue DB Gold | `pyspark_de_project_gold` | us-east-1 | Gold layer tables |

### Key Commands

```bash
# Check EMR Serverless job status
aws emr-serverless get-job-run --application-id <APP_ID> --job-run-id <JOB_ID>

# Check MWAA DAG status
aws mwaa get-environment --name pyspark-de-project-dev-mwaa

# Check S3 data lake contents
aws s3 ls s3://pyspark-de-project-dev-data-lake/lake/ --recursive

# Check CloudWatch logs
aws logs describe-log-groups --log-group-name-prefix "/aws/emr-serverless"
```

## 🔧 Common Issues & Solutions

### 1. EMR Serverless Job Failures

**Symptoms**: Jobs failing in EMR Serverless, error logs in CloudWatch

**Diagnosis**:
```bash
# Get job run details
aws emr-serverless get-job-run --application-id <APP_ID> --job-run-id <JOB_ID>

# Check CloudWatch logs
aws logs get-log-events --log-group-name "/aws/emr-serverless/<APP_ID>" --log-stream-name "<JOB_ID>"
```

**Common Causes & Solutions**:
- **Out of Memory**: Increase driver/executor memory in job configuration
- **Delta Lake Issues**: Ensure Delta JARs are included in packages
- **S3 Access**: Check IAM permissions for EMR job role
- **Code Issues**: Verify job code is uploaded to S3 artifacts bucket

**Resolution**:
```bash
# Restart job with increased memory
./scripts/emr_submit.sh \
  --app-id <APP_ID> \
  --role-arn <ROLE_ARN> \
  --code-bucket <CODE_BUCKET> \
  --lake-bucket <LAKE_BUCKET> \
  --entry-point s3://<CODE_BUCKET>/jobs/<JOB_NAME>.py \
  --extra-args "--conf spark.driver.memory=4g --conf spark.executor.memory=4g"
```

### 2. Data Quality Failures

**Symptoms**: DQ checks failing, critical failures blocking pipeline

**Diagnosis**:
```bash
# Check DQ results
aws s3 ls s3://pyspark-de-project-dev-data-lake/gold/quality/ --recursive

# View latest DQ results
aws s3 cp s3://pyspark-de-project-dev-data-lake/gold/quality/silver/orders/dq_results_orders_YYYYMMDD_HHMMSS.json - | jq .
```

**Common Causes & Solutions**:
- **Data Schema Changes**: Update expectation suites
- **Data Quality Issues**: Fix source data or adjust validation rules
- **Missing Columns**: Check if new columns were added to source

**Resolution**:
```bash
# Update expectation suite
vim dq/suites/silver_orders.yml

# Re-run DQ checks
python aws/scripts/run_ge_checks.py \
  --lake-root s3://pyspark-de-project-dev-data-lake \
  --lake-bucket pyspark-de-project-dev-data-lake \
  --suite dq/suites/silver_orders.yml \
  --table orders \
  --layer silver
```

### 3. Glue Table Registration Issues

**Symptoms**: Tables not appearing in Athena, Glue catalog errors

**Diagnosis**:
```bash
# Check Glue tables
aws glue get-tables --database-name pyspark_de_project_silver
aws glue get-tables --database-name pyspark_de_project_gold

# Check table details
aws glue get-table --database-name pyspark_de_project_silver --name orders
```

**Common Causes & Solutions**:
- **Table Not Found**: Re-run registration script
- **Permission Issues**: Check IAM permissions for Glue access
- **Delta Table Issues**: Ensure Delta tables are valid

**Resolution**:
```bash
# Re-register tables
python aws/scripts/register_glue_tables.py \
  --lake-root s3://pyspark-de-project-dev-data-lake \
  --database pyspark_de_project_silver \
  --layer silver
```

### 4. Kafka Streaming Issues

**Symptoms**: No data flowing through Kafka stream, DLQ filling up

**Diagnosis**:
```bash
# Check Kafka stream job status
aws emr-serverless get-job-run --application-id <APP_ID> --job-run-id <JOB_ID>

# Check DLQ contents
aws s3 ls s3://pyspark-de-project-dev-data-lake/_errors/kafka/orders_events/ --recursive
```

**Common Causes & Solutions**:
- **Kafka Connection Issues**: Check Confluent Cloud credentials
- **Schema Validation Failures**: Check DLQ for invalid messages
- **Checkpoint Issues**: Clear checkpoint and restart stream

**Resolution**:
```bash
# Clear checkpoint and restart
aws s3 rm s3://pyspark-de-project-dev-data-lake/_checkpoints/orders_events/ --recursive

# Restart Kafka stream job
./scripts/emr_submit.sh \
  --app-id <APP_ID> \
  --role-arn <ROLE_ARN> \
  --code-bucket <CODE_BUCKET> \
  --lake-bucket <LAKE_BUCKET> \
  --entry-point s3://<CODE_BUCKET>/jobs/kafka_orders_stream.py
```

### 5. Salesforce/Snowflake Connection Issues

**Symptoms**: Data extraction failures, authentication errors

**Diagnosis**:
```bash
# Check Secrets Manager
aws secretsmanager get-secret-value --secret-id pyspark-de-project-dev-salesforce-credentials

# Check job logs
aws logs get-log-events --log-group-name "/aws/emr-serverless/<APP_ID>" --log-stream-name "<JOB_ID>"
```

**Common Causes & Solutions**:
- **Credential Issues**: Update secrets in AWS Secrets Manager
- **Network Issues**: Check VPC/security group configurations
- **Rate Limiting**: Implement backoff and retry logic

**Resolution**:
```bash
# Update Salesforce credentials
aws secretsmanager update-secret \
  --secret-id pyspark-de-project-dev-salesforce-credentials \
  --secret-string '{"username":"new-user","password":"new-pass","security_token":"new-token","domain":"login"}'

# Re-run extraction job
./scripts/emr_submit.sh \
  --app-id <APP_ID> \
  --role-arn <ROLE_ARN> \
  --code-bucket <CODE_BUCKET> \
  --lake-bucket <LAKE_BUCKET> \
  --entry-point s3://<CODE_BUCKET>/jobs/salesforce_to_bronze.py
```

## 📊 Monitoring & Alerting

### CloudWatch Dashboards

- **EMR Serverless Performance**: Job duration, success/failure rates
- **Data Quality Metrics**: DQ check results, failure counts
- **S3 Storage**: Bucket sizes, object counts
- **Lambda Performance**: Duration, errors, invocations

### Key Metrics to Monitor

1. **EMR Serverless Jobs**:
   - Job success rate > 95%
   - Average job duration < 30 minutes
   - No consecutive failures

2. **Data Quality**:
   - Critical failures = 0
   - Warning failures < 5%
   - DQ checks running on schedule

3. **S3 Storage**:
   - Data lake growth rate
   - Artifacts bucket size
   - Log retention compliance

4. **Costs**:
   - EMR Serverless costs < $100/day
   - S3 storage costs < $50/day
   - Overall project costs < $200/day

### Alerting Rules

```bash
# Set up CloudWatch alarms
aws cloudwatch put-metric-alarm \
  --alarm-name "EMR-Job-Failure-Rate" \
  --alarm-description "Alert when EMR job failure rate exceeds 10%" \
  --metric-name JobRunFailureCount \
  --namespace AWS/EMRServerless \
  --statistic Sum \
  --period 300 \
  --threshold 5 \
  --comparison-operator GreaterThanThreshold

aws cloudwatch put-metric-alarm \
  --alarm-name "DQ-Critical-Failures" \
  --alarm-description "Alert when critical DQ failures occur" \
  --metric-name CriticalFailures \
  --namespace Custom/DataQuality \
  --statistic Sum \
  --period 300 \
  --threshold 0 \
  --comparison-operator GreaterThanThreshold
```

## 🔄 Maintenance Tasks

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

## 🚨 Escalation Procedures

### Level 1: On-Call Engineer
- Monitor alerts and logs
- Attempt basic troubleshooting
- Escalate if issue persists > 30 minutes

### Level 2: Senior Engineer
- Complex troubleshooting
- Infrastructure changes
- Escalate if issue persists > 2 hours

### Level 3: Engineering Manager
- Business impact assessment
- Resource allocation
- External vendor coordination

## 📞 Contact Information

### Internal Teams
- **Data Engineering**: [Team Email]
- **Platform Engineering**: [Team Email]
- **Security Team**: [Team Email]
- **Cost Management**: [Team Email]

### External Vendors
- **AWS Support**: [Support Case URL]
- **Confluent Support**: [Support Case URL]
- **Salesforce Support**: [Support Case URL]
- **Snowflake Support**: [Support Case URL]

## 📚 Additional Resources

- [AWS EMR Serverless Documentation](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Great Expectations Documentation](https://docs.greatexpectations.io/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Project GitHub Repository](https://github.com/your-org/pyspark-data-engineer-project)
new