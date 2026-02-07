# 🚀 AWS Production ETL Pipeline - Quick Start Guide

## 📋 One-Page Navigation

### 🏃‍♂️ **Quick Start**
- **Run Locally**: See [Local Development](#local-development)
- **Run on MWAA/EMR**: See [AWS Production](#aws-production)
- **Data Sources**: See [Data Sources](#data-sources)
- **Data Quality**: See [Data Quality](#data-quality)

---

## 🖥️ Local Development

### Prerequisites
```bash
# Install dependencies
pip install -r requirements.txt

# Set up environment
cp .env.example .env
# Edit .env with your AWS credentials
```

### Run Pipeline
```bash
# Run complete pipeline
python -m pyspark_interview_project.pipeline --env local

# Run specific jobs
python src/pyspark_interview_project/jobs/hubspot_to_bronze.py
python src/pyspark_interview_project/jobs/fx_bronze_to_silver.py
```

### Local Testing
```bash
# Run tests
pytest tests/ -v

# Test DAG imports
pytest tests/test_dag_import.py -v

# Test data quality
pytest tests/test_dq_policies.py -v
```

---

## ☁️ AWS Production

### Prerequisites
```bash
# Set Airflow Variables
aws configure  # AWS CLI setup
```

### Deploy Infrastructure
```bash
# Deploy with Terraform
cd infra/terraform
terraform init
terraform plan
terraform apply

# Source outputs
source ../../aws/scripts/source_terraform_outputs.sh
```

### Run on MWAA/EMR
```bash
# Deploy to AWS
./aws/scripts/aws_production_deploy.sh

# Check Airflow UI
# Navigate to MWAA webserver URL from terraform outputs
```

### Monitor Pipeline
```bash
# Check EMR Serverless jobs
aws emr-serverless list-job-runs --application-id $EMR_APP_ID

# Check S3 data
aws s3 ls s3://$S3_LAKE_BUCKET/bronze/
aws s3 ls s3://$S3_LAKE_BUCKET/silver/
aws s3 ls s3://$S3_LAKE_BUCKET/gold/
```

---

## 📊 Data Sources

### 5 Production Data Sources
1. **HubSpot CRM** - Contacts, Deals, Companies
2. **Snowflake DWH** - Customers, Orders, Products  
3. **Redshift Analytics** - Customer Behavior
4. **Kafka Streams** - Real-time Events
5. **FX Rates API** - Currency Exchange Rates

### Data Flow
```
Bronze (Raw) → Silver (Conformed) → Gold (Analytics)
     ↓              ↓                   ↓
   S3 Raw       S3 Conformed      S3 Analytics
   Quarantine   SCD-2 History     Aggregations
```

### Key Metrics
- **Bronze**: ~500GB daily raw data
- **Silver**: ~300GB daily conformed data  
- **Gold**: ~100GB daily analytics
- **Processing**: 2-3 hours total runtime

---

## 🔍 Data Quality

### DQ Framework
- **Great Expectations** - Policy-driven validation
- **Custom Rules** - Business-specific checks
- **Quarantine System** - Bad data isolation
- **Monitoring** - Real-time alerts

### Run DQ Checks
```bash
# Run all DQ checks
python aws/scripts/run_ge_checks.py

# Run specific table checks
python aws/scripts/run_ge_checks.py --table customers

# Check quarantine
aws s3 ls s3://$S3_LAKE_BUCKET/quarantine/
```

### DQ Policies
- **Location**: `dq/suites/*.yml`
- **Coverage**: All bronze/silver/gold tables
- **Types**: Completeness, accuracy, freshness, integrity

---

## 🛠️ Troubleshooting

### Common Issues
1. **EMR Role Missing**: Check IAM roles exist
2. **S3 Access Denied**: Verify bucket policies
3. **DAG Import Error**: Check Python paths
4. **Data Stale**: Verify source connectivity

### Debug Commands
```bash
# Check Airflow DAGs
airflow dags list

# Check EMR logs
aws logs describe-log-groups --log-group-name-prefix /aws/emr-serverless

# Check data freshness
python src/pyspark_interview_project/utils/freshness_guards.py
```

### Support
- **Documentation**: [Full Documentation](README.md)
- **Runbook**: [AWS Runbook](RUNBOOK_AWS_2025.md)
- **Issues**: GitHub Issues
- **Monitoring**: CloudWatch Dashboards

---

## 📈 Performance

### Optimization Features
- **Spark Optimizations**: Caching, partitioning, broadcast joins
- **Delta Lake**: ACID transactions, time travel, compaction
- **S3 Optimizations**: Lifecycle policies, storage classes
- **EMR Serverless**: Auto-scaling, cost optimization

### Monitoring
- **OpenLineage**: Data lineage tracking
- **Marquez**: Lineage visualization  
- **CloudWatch**: Performance metrics
- **Custom Dashboards**: Business KPIs

---

## 🔒 Security & Compliance

### Security Features
- **Encryption**: At rest and in transit
- **Access Control**: IAM roles and policies
- **Network Security**: VPC and security groups
- **Audit Logging**: Comprehensive trails

### Compliance
- **GDPR**: Data privacy protection
- **SOX**: Financial data compliance
- **HIPAA**: Healthcare data protection
- **PCI DSS**: Payment card security

---

**🎯 Ready to get started? Choose your path above and dive in!**
