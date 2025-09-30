# 🚀 AWS Production Runbook 2025

## 📋 **GOLDEN PATH CHECKLIST**

### **Prerequisites Setup**
- [ ] AWS CLI configured with appropriate permissions
- [ ] Terraform installed (v1.5.0+)
- [ ] Python 3.10+ with virtual environment
- [ ] EMR Serverless application created
- [ ] MWAA environment configured
- [ ] S3 buckets created with proper policies
- [ ] Secrets stored in AWS Secrets Manager

### **Infrastructure Deployment**
```bash
# 1. Deploy infrastructure
cd aws/infra/terraform
terraform init
terraform plan
terraform apply

# 2. Verify resources
aws s3 ls s3://your-lake-bucket/
aws emr-serverless list-applications
aws mwaa list-environments
```

### **Pipeline Execution Order**
1. **FX Bronze → Silver** (`01_fx_bronze` → `02_fx_silver`)
2. **Salesforce Bronze** (`03_salesforce_bronze`) - incremental
3. **Snowflake Bronze → MERGE** (`05_snowflake_bronze` → `06_merge_to_silver_orders`)
4. **Kafka Streaming** (`04_kafka_stream`) - continuous
5. **Data Quality Checks** (`run_ge_checks`) - **HARD GATE**
6. **Silver → Gold Build** (`07_silver_to_gold`)
7. **Glue/Athena Registration** (`register_silver` → `register_gold`)
8. **Athena Queries** - validation and analytics

---

## 🏗️ **INFRASTRUCTURE SETUP**

### **1. S3 Buckets**
```bash
# Create data lake bucket
aws s3 mb s3://your-company-data-lake-prod

# Create code bucket
aws s3 mb s3://your-company-code-bucket-prod

# Enable versioning and encryption
aws s3api put-bucket-versioning --bucket your-company-data-lake-prod --versioning-configuration Status=Enabled
aws s3api put-bucket-encryption --bucket your-company-data-lake-prod --server-side-encryption-configuration '{
  "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
}'
```

### **2. EMR Serverless Application**
```bash
# Create EMR Serverless application
aws emr-serverless create-application \
  --name "pyspark-data-pipeline" \
  --release-label "emr-6.7.0" \
  --type "SPARK" \
  --initial-capacity '{
    "DRIVER": {"workerCount": 2, "workerConfiguration": {"cpu": "2 vCPU", "memory": "4 GB"}},
    "EXECUTOR": {"workerCount": 4, "workerConfiguration": {"cpu": "4 vCPU", "memory": "8 GB"}}
  }'
```

### **3. MWAA Environment**
```bash
# Create MWAA environment
aws mwaa create-environment \
  --name "data-pipeline-airflow" \
  --execution-role-arn "arn:aws:iam::ACCOUNT:role/MWAAExecutionRole" \
  --source-bucket-arn "arn:aws:s3:::your-company-code-bucket-prod" \
  --dag-s3-path "dags/" \
  --requirements-s3-path "requirements.txt" \
  --max-workers 10
```

### **4. Secrets Management**
```bash
# Store Salesforce credentials
aws secretsmanager create-secret \
  --name "pyspark-interview-project/salesforce/credentials" \
  --description "Salesforce API credentials" \
  --secret-string '{
    "username": "your-username@company.com",
    "password": "your-password",
    "security_token": "your-security-token",
    "instance_url": "https://your-instance.salesforce.com"
  }'

# Store Kafka credentials
aws secretsmanager create-secret \
  --name "pyspark-interview-project/kafka/credentials" \
  --description "Confluent Cloud Kafka credentials" \
  --secret-string '{
    "bootstrap_servers": "your-kafka-bootstrap-servers",
    "api_key": "your-api-key",
    "api_secret": "your-api-secret",
    "group_id": "orders-stream-processor"
  }'

# Store Snowflake credentials
aws secretsmanager create-secret \
  --name "pyspark-interview-project/snowflake/credentials" \
  --description "Snowflake credentials" \
  --secret-string '{
    "account": "your-account",
    "username": "your-username",
    "password": "your-password",
    "warehouse": "your-warehouse",
    "database": "your-database",
    "schema": "your-schema"
  }'
```

---

## 🔄 **PIPELINE EXECUTION**

### **Daily Batch Pipeline**
```bash
# 1. FX Bronze to Silver
aws emr-serverless start-job-run \
  --application-id "your-emr-app-id" \
  --execution-role-arn "arn:aws:iam::ACCOUNT:role/EMRServerlessExecutionRole" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://your-code-bucket/jobs/fx_to_bronze.py",
      "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4"
    }
  }'

# 2. Salesforce Bronze (incremental)
aws emr-serverless start-job-run \
  --application-id "your-emr-app-id" \
  --execution-role-arn "arn:aws:iam::ACCOUNT:role/EMRServerlessExecutionRole" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://your-code-bucket/jobs/salesforce_to_bronze.py",
      "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4"
    }
  }'

# 3. Data Quality Checks (HARD GATE)
aws emr-serverless start-job-run \
  --application-id "your-emr-app-id" \
  --execution-role-arn "arn:aws:iam::ACCOUNT:role/EMRServerlessExecutionRole" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://your-code-bucket/scripts/run_ge_checks.py",
      "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4"
    }
  }'
```

### **Kafka Streaming Pipeline**
```bash
# Start Kafka streaming job
aws emr-serverless start-job-run \
  --application-id "your-emr-app-id" \
  --execution-role-arn "arn:aws:iam::ACCOUNT:role/EMRServerlessExecutionRole" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://your-code-bucket/jobs/kafka_orders_stream_hardened.py",
      "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
    }
  }'
```

---

## 📊 **QUERY IN ATHENA**

### **Basic Data Exploration**
```sql
-- Count records in each table
SELECT 'fx_rates' as table_name, COUNT(*) as record_count FROM silver.fx_rates
UNION ALL
SELECT 'orders' as table_name, COUNT(*) as record_count FROM silver.orders
UNION ALL
SELECT 'salesforce_accounts' as table_name, COUNT(*) as record_count FROM silver.salesforce_accounts;

-- Check data freshness
SELECT 
    'fx_rates' as table_name,
    MAX(as_of_date) as latest_date
FROM silver.fx_rates
UNION ALL
SELECT 
    'orders' as table_name,
    MAX(order_date) as latest_date
FROM silver.orders;
```

### **Business Intelligence Queries**
```sql
-- Orders by Status
SELECT 
    status,
    COUNT(*) as order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM silver.orders
GROUP BY status
ORDER BY order_count DESC;

-- Revenue Analysis with USD Normalization
SELECT 
    f.order_id,
    c.name AS customer_name,
    f.order_date,
    f.currency,
    f.amount_native,
    ROUND(f.amount_usd, 2) AS amount_usd,
    f.status
FROM gold.fact_orders f
JOIN gold.dim_customers c ON f.customer_id = c.id
ORDER BY f.order_date DESC
LIMIT 50;

-- Top 10 Customers by Revenue
SELECT 
    c.name AS customer_name,
    c.type AS customer_type,
    ROUND(SUM(f.amount_usd), 2) as total_revenue_usd,
    COUNT(f.order_id) as total_orders
FROM gold.fact_orders f
JOIN gold.dim_customers c ON f.customer_id = c.id
WHERE f.amount_usd IS NOT NULL
GROUP BY c.name, c.type
ORDER BY total_revenue_usd DESC
LIMIT 10;
```

### **Data Quality Validation**
```sql
-- Check for NULL values in critical fields
SELECT 
    'fx_rates' as table_name,
    'ccy' as column_name,
    COUNT(*) as total_records,
    COUNT(ccy) as non_null_records,
    COUNT(*) - COUNT(ccy) as null_records
FROM silver.fx_rates
UNION ALL
SELECT 
    'orders' as table_name,
    'order_id' as column_name,
    COUNT(*) as total_records,
    COUNT(order_id) as non_null_records,
    COUNT(*) - COUNT(order_id) as null_records
FROM silver.orders;

-- Check for duplicate order IDs
SELECT 
    order_id,
    COUNT(*) as duplicate_count
FROM silver.orders
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;
```

---

## 🔧 **OPERATIONS**

### **Weekly Delta Maintenance**
```bash
# Run weekly housekeeping
aws emr-serverless start-job-run \
  --application-id "your-emr-app-id" \
  --execution-role-arn "arn:aws:iam::ACCOUNT:role/EMRServerlessExecutionRole" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://your-code-bucket/scripts/delta_optimize_vacuum.py",
      "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4"
    }
  }'
```

### **Glue/Athena Table Registration**
```bash
# Register Silver tables
python aws/scripts/register_glue_tables.py \
  --db silver \
  --lake-bucket your-company-data-lake-prod \
  --region us-east-1

# Register Gold tables
python aws/scripts/register_glue_tables.py \
  --db gold \
  --lake-bucket your-company-data-lake-prod \
  --region us-east-1
```

### **Kafka Event Production (Testing)**
```bash
# Produce test events to Kafka
python -c "
import json
from confluent_kafka import Producer

# Kafka configuration
config = {
    'bootstrap.servers': 'your-kafka-bootstrap-servers',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'your-api-key',
    'sasl.password': 'your-api-secret'
}

producer = Producer(config)

# Produce 2000 test events
for i in range(2000):
    event = {
        'order_id': f'TEST_ORD_{i:04d}',
        'customer_id': f'CUST_{i % 100:03d}',
        'order_date': '2024-01-27',
        'currency': ['USD', 'EUR', 'GBP', 'JPY'][i % 4],
        'amount': round(100 + (i % 1000), 2),
        'status': ['PLACED', 'PAID', 'SHIPPED', 'CANCELLED'][i % 4],
        'event_timestamp': '2024-01-27T10:00:00Z'
    }
    
    producer.produce('orders', value=json.dumps(event))
    
    if i % 100 == 0:
        print(f'Produced {i} events...')

producer.flush()
print('All 2000 events produced!')
"
```

---

## 🚨 **TROUBLESHOOTING**

### **Common Issues and Solutions**

#### **1. S3 Permissions Issues**
```bash
# Check S3 bucket policies
aws s3api get-bucket-policy --bucket your-company-data-lake-prod

# Verify IAM role permissions
aws iam get-role-policy --role-name EMRServerlessExecutionRole --policy-name EMRServerlessExecutionPolicy
```

#### **2. Delta LogStore Issues**
```bash
# Check Delta table metadata
aws s3 ls s3://your-company-data-lake-prod/silver/fx_rates/_delta_log/

# Verify Delta configuration
spark.conf.get("spark.delta.logStore.class")
```

#### **3. Salesforce Token Issues**
```bash
# Check Salesforce credentials
aws secretsmanager get-secret-value --secret-id pyspark-interview-project/salesforce/credentials

# Test Salesforce connection
python -c "
from simple_salesforce import Salesforce
import json
import os

# Load credentials from AWS Secrets Manager
# (Implementation depends on your AWS SDK setup)
print('Testing Salesforce connection...')
"
```

#### **4. Kafka SASL Configuration**
```bash
# Check Kafka credentials
aws secretsmanager get-secret-value --secret-id pyspark-interview-project/kafka/credentials

# Test Kafka connection
python -c "
from confluent_kafka import Consumer
import json

config = {
    'bootstrap.servers': 'your-kafka-bootstrap-servers',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'your-api-key',
    'sasl.password': 'your-api-secret',
    'group.id': 'test-consumer',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(config)
print('Kafka connection successful!')
"
```

#### **5. Snowflake Connector Version Mismatch**
```bash
# Check Snowflake connector version
pip show snowflake-connector-python

# Update if needed
pip install snowflake-connector-python==3.0.0
```

#### **6. Incremental Watermark Issues**
```bash
# Check checkpoint data
aws s3 ls s3://your-company-data-lake-prod/_checkpoints/salesforce/

# Reset checkpoint if needed
aws s3 rm s3://your-company-data-lake-prod/_checkpoints/salesforce/accounts/ --recursive
```

### **Data Quality Failures**
```bash
# Check DQ results
aws s3 ls s3://your-company-data-lake-prod/gold/quality/

# View specific DQ failure
aws s3 cp s3://your-company-data-lake-prod/gold/quality/fx_rates/20240127_143000.json - | jq .
```

### **Delta Time Travel and Rollback**
```sql
-- View table history
DESCRIBE HISTORY delta.`s3://your-company-data-lake-prod/silver/fx_rates`

-- Rollback to previous version
RESTORE TABLE delta.`s3://your-company-data-lake-prod/silver/fx_rates` TO VERSION AS OF 0

-- Rollback to specific timestamp
RESTORE TABLE delta.`s3://your-company-data-lake-prod/silver/fx_rates` TO TIMESTAMP AS OF '2024-01-27 10:00:00'
```

---

## 📈 **MONITORING AND ALERTING**

### **CloudWatch Metrics**
- EMR Serverless job success/failure rates
- S3 object counts and sizes
- Lambda function invocations and errors
- MWAA task success/failure rates

### **Data Quality Monitoring**
- Daily DQ check results stored in S3
- Critical failures trigger pipeline stops
- Weekly DQ trend analysis

### **Cost Optimization**
- EMR Serverless auto-scaling
- S3 lifecycle policies for old data
- Weekly Delta maintenance for performance

---

## 🔐 **SECURITY**

### **IAM Roles and Policies**
- EMR Serverless execution role with minimal S3 permissions
- MWAA execution role for orchestration
- S3 bucket policies blocking public access
- Secrets Manager for credential storage

### **Data Encryption**
- S3 server-side encryption (AES-256)
- Secrets Manager encryption at rest
- TLS for all API communications

### **Network Security**
- VPC endpoints for S3 and Secrets Manager
- Security groups restricting access
- Private subnets for EMR Serverless

---

## 📚 **ADDITIONAL RESOURCES**

### **EMR vs Glue ETL vs Lambda Comparison**

| Service | Use Case | Pros | Cons |
|---------|----------|------|------|
| **EMR Serverless** | Complex Spark jobs, ML workloads | Auto-scaling, cost-effective, full Spark features | Cold start time, complex setup |
| **Glue ETL** | Simple ETL jobs, data cataloging | Serverless, built-in catalog integration | Limited Spark features, vendor lock-in |
| **Lambda** | Lightweight processing, event-driven | Fast, simple, pay-per-use | 15-minute limit, limited memory |

### **When to Use Each:**
- **EMR Serverless**: Complex data transformations, ML pipelines, large datasets
- **Glue ETL**: Simple ETL jobs, data discovery, catalog management
- **Lambda**: Event processing, API integrations, small data transformations

---

## ✅ **ACCEPTANCE CRITERIA**

- [ ] All CI checks pass (black, isort, flake8, mypy, pytest)
- [ ] Airflow daily DAG shows GE task fails on critical DQ errors
- [ ] Glue registration creates tables accessible in Athena
- [ ] Sample Athena queries return expected results
- [ ] Kafka job writes invalid events to DLQ path
- [ ] Weekly housekeeping DAG exists and targets Silver/Gold paths
- [ ] No plaintext secrets in repository
- [ ] README documents Secrets Manager usage
- [ ] All Delta tables have proper ZORDER optimization
- [ ] MERGE operations are idempotent and tested

---

**🎉 This runbook provides everything needed to run a production-ready data pipeline on AWS!**