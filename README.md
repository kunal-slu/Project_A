# PySpark Data Engineer Project

A comprehensive data engineering project built with PySpark, Delta Lake, and cloud services. This project demonstrates modern data lakehouse architecture with automated ETL pipelines, data quality checks, and infrastructure as code.

## 🌐 Multi-Cloud Support

This project supports both **AWS** and **Azure** cloud platforms:

- **AWS**: EMR Serverless + S3 + MWAA + Glue/Athena
- **Azure**: Databricks + ADLS Gen2 + Data Factory + Key Vault

Choose your cloud platform and follow the respective runbook:

- 📘 **[AWS Production Runbook](aws/RUNBOOK_AWS_2025.md)** - Complete step-by-step AWS deployment guide
- 📗 **[Azure Production Runbook](azure/RUNBOOK_AZURE_2025.md)** - Complete step-by-step Azure deployment guide

## 🏗️ Architecture

### High-Level Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Ingestion     │    │   Data Lake     │
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

### Data Flow

1. **Ingestion**: Data flows from various sources (REST APIs, RDS, Salesforce, Snowflake, Kafka) into S3 Bronze layer
2. **Processing**: EMR Serverless processes data through Bronze → Silver → Gold transformations
3. **Orchestration**: MWAA (Airflow) orchestrates the entire pipeline
4. **Quality**: Great Expectations ensures data quality at each layer
5. **Storage**: Glue Data Catalog provides metadata management
6. **Governance**: Lake Formation provides row-level access control

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- AWS CLI configured
- Terraform 1.0+
- Docker (optional, for local development)

### Local Development

1. **Clone and setup**:
   ```bash
   git clone <repository-url>
   cd pyspark_data_engineer_project
   make venv
   ```

2. **Run local ETL**:
   ```bash
   make run-local
   ```

3. **Run tests**:
   ```bash
   make test
   ```

## 🌟 Run on AWS (Golden Path)

This section provides a complete, copy-paste runnable guide for deploying and running the entire pipeline on AWS.

### Step 1: Prerequisites Setup

```bash
# Set environment variables
export AWS_REGION="us-east-1"
export PROJECT="pyspark-de-project"
export ENVIRONMENT="dev"
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Verify AWS credentials
aws sts get-caller-identity
```

### Step 2: Deploy Infrastructure

```bash
# Initialize Terraform
make infra-init

# Review the plan
make infra-plan

# Deploy infrastructure
make infra-apply

# Note the outputs (save these for later steps)
terraform -chdir=infra/terraform output
```

### Step 3: Build and Upload Code

```bash
# Build Python wheel
make dist

# Upload wheel and jobs to S3
aws s3 cp dist/*.whl s3://$PROJECT-$ENVIRONMENT-artifacts/dist/
aws s3 sync src/pyspark_interview_project/jobs/ s3://$PROJECT-$ENVIRONMENT-artifacts/jobs/
aws s3 sync aws/scripts/ s3://$PROJECT-$ENVIRONMENT-artifacts/scripts/
aws s3 sync conf/ s3://$PROJECT-$ENVIRONMENT-artifacts/config/
aws s3 sync dq/ s3://$PROJECT-$ENVIRONMENT-artifacts/dq/
```

### Step 4: Run Bronze to Silver Processing

```bash
# Get EMR application ID and role ARN from Terraform outputs
EMR_APP_ID=$(terraform -chdir=infra/terraform output -raw emr_serverless_application_id)
EMR_ROLE_ARN=$(terraform -chdir=infra/terraform output -raw emr_serverless_job_role_arn)
CODE_BUCKET="$PROJECT-$ENVIRONMENT-artifacts"

# Submit bronze to silver job
./scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $CODE_BUCKET \
  --lake-bucket $PROJECT-$ENVIRONMENT-data-lake \
  --entry-point s3://$CODE_BUCKET/jobs/bronze_to_silver.py
```

### Step 5: Run Silver to Gold Processing

```bash
# Submit silver to gold job
./scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $CODE_BUCKET \
  --lake-bucket $PROJECT-$ENVIRONMENT-data-lake \
  --entry-point s3://$CODE_BUCKET/jobs/silver_to_gold.py
```

### Step 6: Run Salesforce Incremental (with Secrets Manager)

```bash
# Store Salesforce credentials in Secrets Manager
aws secretsmanager create-secret \
  --name "$PROJECT-$ENVIRONMENT-salesforce-credentials" \
  --secret-string '{"username":"your-sf-user","password":"your-sf-pass","security_token":"your-sf-token","domain":"login"}'

# Submit Salesforce incremental job
./scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $CODE_BUCKET \
  --lake-bucket $PROJECT-$ENVIRONMENT-data-lake \
  --entry-point s3://$CODE_BUCKET/jobs/salesforce_to_bronze.py \
  --extra-args "--env SF_SECRET_NAME=$PROJECT-$ENVIRONMENT-salesforce-credentials"
```

### Step 7: Start Kafka Stream Job

```bash
# Set Kafka environment variables
export KAFKA_BOOTSTRAP="your-confluent-bootstrap-servers"
export KAFKA_API_KEY="your-confluent-api-key"
export KAFKA_API_SECRET="your-confluent-api-secret"
export KAFKA_TOPIC="orders_events"

# Submit Kafka streaming job
./scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $CODE_BUCKET \
  --lake-bucket $PROJECT-$ENVIRONMENT-data-lake \
  --entry-point s3://$CODE_BUCKET/jobs/kafka_orders_stream.py \
  --extra-args "--env KAFKA_BOOTSTRAP=$KAFKA_BOOTSTRAP --env KAFKA_API_KEY=$KAFKA_API_KEY --env KAFKA_API_SECRET=$KAFKA_API_SECRET --env KAFKA_TOPIC=$KAFKA_TOPIC"
```

### Step 8: Run Snowflake Backfill and Merge

```bash
# Set Snowflake environment variables
export SF_URL="your-snowflake-account.snowflakecomputing.com"
export SF_USER="your-snowflake-user"
export SF_PASS="your-snowflake-password"
export SF_DB="SNOWFLAKE_SAMPLE_DATA"
export SF_SCHEMA="TPCH_SF1"
export SF_WH="COMPUTE_WH"

# Submit Snowflake backfill job
./scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $CODE_BUCKET \
  --lake-bucket $PROJECT-$ENVIRONMENT-data-lake \
  --entry-point s3://$CODE_BUCKET/jobs/snowflake_to_bronze.py

# Submit Snowflake merge job
./scripts/emr_submit.sh \
  --app-id $EMR_APP_ID \
  --role-arn $EMR_ROLE_ARN \
  --code-bucket $CODE_BUCKET \
  --lake-bucket $PROJECT-$ENVIRONMENT-data-lake \
  --entry-point s3://$CODE_BUCKET/jobs/snowflake_bronze_to_silver_merge.py
```

### Step 9: Register Tables in Glue Catalog

```bash
# Register silver tables
python aws/scripts/register_glue_tables.py \
  --lake-root s3://$PROJECT-$ENVIRONMENT-data-lake \
  --database $PROJECT\_silver \
  --layer silver

# Register gold tables
python aws/scripts/register_glue_tables.py \
  --lake-root s3://$PROJECT-$ENVIRONMENT-data-lake \
  --database $PROJECT\_gold \
  --layer gold
```

### Step 10: Query in Athena

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

### Step 11: Run Data Quality Checks

```bash
# Run DQ checks on silver orders
python aws/scripts/run_ge_checks.py \
  --lake-root s3://$PROJECT-$ENVIRONMENT-data-lake \
  --lake-bucket $PROJECT-$ENVIRONMENT-data-lake \
  --suite dq/suites/silver_orders.yml \
  --table orders \
  --layer silver

# Run DQ checks on silver fx_rates
python aws/scripts/run_ge_checks.py \
  --lake-root s3://$PROJECT-$ENVIRONMENT-data-lake \
  --lake-bucket $PROJECT-$ENVIRONMENT-data-lake \
  --suite dq/suites/silver_fx_rates.yml \
  --table fx_rates \
  --layer silver
```

### Step 12: Setup Lake Formation Governance

```bash
# Setup Lake Formation tags and policies
python aws/scripts/lf_tags_seed.py --database pyspark_de_project_silver
```

### Step 13: Produce Sample Kafka Events

```bash
# Produce sample events to Kafka
make kafka-produce
```

### Step 14: View Results

```bash
# Check data lake contents
aws s3 ls s3://$PROJECT-$ENVIRONMENT-data-lake/lake/ --recursive

# View DQ results
aws s3 ls s3://$PROJECT-$ENVIRONMENT-data-lake/gold/quality/

# Check Glue tables
aws glue get-tables --database-name $PROJECT\_silver
aws glue get-tables --database-name $PROJECT\_gold
```

### Environment Variables Reference

| Variable | Description | Example |
|----------|-------------|---------|
| `LAKE_ROOT` | S3 root path of the data lake | `s3://pyspark-de-project-dev-data-lake` |
| `CODE_BUCKET` | S3 bucket for code artifacts | `pyspark-de-project-dev-artifacts` |
| `LAKE_BUCKET` | S3 bucket for data lake | `pyspark-de-project-dev-data-lake` |
| `AWS_REGION` | AWS region | `us-east-1` |
| `KAFKA_BOOTSTRAP` | Confluent Cloud bootstrap servers | `pkc-xxxxx.us-east-1.aws.confluent.cloud:9092` |
| `KAFKA_API_KEY` | Confluent Cloud API key | `your-api-key` |
| `KAFKA_API_SECRET` | Confluent Cloud API secret | `your-api-secret` |
| `SF_URL` | Snowflake account URL | `your-account.snowflakecomputing.com` |
| `SF_USER` | Snowflake username | `your-username` |
| `SF_PASS` | Snowflake password | `your-password` |
| `SF_DB` | Snowflake database | `SNOWFLAKE_SAMPLE_DATA` |
| `SF_SCHEMA` | Snowflake schema | `TPCH_SF1` |
| `SF_WH` | Snowflake warehouse | `COMPUTE_WH` |

## 📁 Project Structure

```
pyspark_data_engineer_project/
├── src/                          # Source code
│   ├── pyspark_interview_project/
│   │   ├── utils/                # Utility functions
│   │   ├── pipeline/             # Data transformations
│   │   ├── jobs/                 # ETL jobs
│   │   └── dq/                   # Data quality
│   └── common/                   # Common modules
├── config/                       # Configuration files
│   └── aws/                      # AWS-specific configs
├── infra/                        # Infrastructure as Code
│   └── terraform/                # Terraform modules
├── dags/                         # Airflow DAGs
├── plugins/                      # Airflow plugins
├── ingest/                       # Data ingestion
│   ├── rest/                     # REST API ingestion
│   └── rds/                      # RDS CDC
├── ge/                           # Great Expectations
├── dq/                           # Data quality suites
├── kafka/                        # Kafka schemas
├── aws/                          # AWS scripts
│   ├── scripts/                  # Utility scripts
│   └── monitoring/               # CloudWatch dashboards
├── tests/                        # Test files
├── scripts/                      # Utility scripts
└── docs/                         # Documentation
```

## 🔧 Configuration

### Environment Variables

Create a `.env` file based on `.env.example`:

```bash
# AWS Configuration
AWS_REGION=us-east-1
AWS_PROFILE=default

# Project Configuration
PROJECT=pyspark-de-project
ENVIRONMENT=dev

# S3 Buckets
S3_DATA_LAKE_BUCKET=pyspark-de-project-dev-data-lake
S3_ARTIFACTS_BUCKET=pyspark-de-project-dev-artifacts
S3_LOGS_BUCKET=pyspark-de-project-dev-logs

# EMR Serverless
EMR_APPLICATION_ID=your-emr-app-id
EMR_JOB_ROLE_ARN=arn:aws:iam::123456789012:role/your-role

# MWAA
MWAA_ENVIRONMENT_NAME=pyspark-de-project-dev-mwaa
MWAA_DAGS_BUCKET=pyspark-de-project-dev-mwaa-bucket

# Kafka (Confluent Cloud)
KAFKA_BOOTSTRAP=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
KAFKA_API_KEY=your-api-key
KAFKA_API_SECRET=your-api-secret
KAFKA_TOPIC=orders_events

# Snowflake
SF_URL=your-account.snowflakecomputing.com
SF_USER=your-username
SF_PASS=your-password
SF_DB=SNOWFLAKE_SAMPLE_DATA
SF_SCHEMA=TPCH_SF1
SF_WH=COMPUTE_WH

# Salesforce
SF_USER=your-sf-user
SF_PASS=your-sf-pass
SF_TOKEN=your-sf-token
SF_DOMAIN=login
```

### Configuration Files

- `config/aws/config-simple.yaml`: Main configuration
- `config/aws/connections.yaml`: AWS resource ARNs
- `ge/great_expectations.yml`: Data quality configuration

## 🏗️ Infrastructure

### Terraform Modules

- **S3**: Data lake, artifacts, and logs buckets
- **KMS**: Encryption keys for data at rest
- **IAM**: Roles and policies for least privilege access
- **Glue**: Data catalog databases
- **EMR Serverless**: Spark processing
- **MWAA**: Airflow orchestration
- **Lambda**: Serverless ingestion
- **SQS**: Message queuing
- **Lake Formation**: Data governance

### Cost Optimization

- EMR Serverless auto-scaling
- S3 lifecycle policies
- CloudWatch log retention
- Budget alerts
- Delta table optimization and vacuum

## 📊 Data Quality

### Great Expectations

- **Expectations**: Data validation rules
- **Checkpoints**: Automated validation runs
- **Data Docs**: HTML reports in S3

### Example Expectations

```yaml
expect_column_to_exist:
  column: customer_id
expect_column_values_to_not_be_null:
  column: customer_id
  meta:
    severity: error
expect_column_values_to_be_unique:
  column: customer_id
  meta:
    severity: error
```

## 🔄 CI/CD Pipeline

### GitHub Actions

- **Test**: Unit tests, linting, type checking
- **Build**: Package artifacts for deployment
- **Deploy**: Deploy to S3 and update MWAA

### Workflows

- `ci.yml`: Main CI/CD pipeline
- `infra-plan.yml`: Infrastructure planning

## 🚀 Usage Examples

### Local Development

```bash
# Setup environment
make venv

# Run local ETL
make run-local

# Run individual layers
make run-bronze
make run-silver
make run-gold

# Run tests
make test

# Start Docker services
make up
make down
```

### AWS Operations

```bash
# Deploy infrastructure
make infra-init
make infra-plan
make infra-apply

# Run ETL job
make aws

# Data quality
make dq-run
make dq-docs

# Delta optimization
make optimize-vacuum

# Lake Formation setup
make lf-setup

# Kafka producer
make kafka-produce

# Cleanup
make infra-destroy
```

### Airflow DAGs

The main ETL DAG (`daily_pipeline.py`) includes:

1. **Data Availability Check**: Verify source data exists
2. **Bronze to Silver**: Clean and standardize data
3. **Silver to Gold**: Create business-ready datasets
4. **Data Quality**: Validate data quality
5. **Data Docs**: Generate quality reports

## 🔍 Monitoring

### CloudWatch Logs

- EMR Serverless job logs
- Lambda function logs
- MWAA execution logs

### Data Quality Reports

- Great Expectations Data Docs
- S3-hosted HTML reports
- Automated quality alerts

### Cost Monitoring

- CloudWatch dashboards
- Budget alerts
- Delta table optimization

## 🛠️ Development

### Code Quality

- **Black**: Code formatting
- **isort**: Import sorting
- **flake8**: Linting
- **mypy**: Type checking
- **pytest**: Testing
- **detect-secrets**: Security scanning

### Pre-commit Hooks

```bash
pip install pre-commit
pre-commit install
```

## 📚 Documentation

- [Architecture Overview](docs/arch/)
- [API Reference](docs/api/)
- [Deployment Guide](docs/deployment/)
- [Troubleshooting](docs/troubleshooting/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Issues**: [GitHub Issues](https://github.com/your-org/pyspark-data-engineer-project/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-org/pyspark-data-engineer-project/discussions)
- **Documentation**: [Project Wiki](https://github.com/your-org/pyspark-data-engineer-project/wiki)

## 🏆 Features

- ✅ **Modern Data Stack**: PySpark, Delta Lake, AWS services
- ✅ **Infrastructure as Code**: Terraform modules
- ✅ **Data Quality**: Great Expectations integration
- ✅ **Orchestration**: MWAA (Airflow) DAGs
- ✅ **CI/CD**: GitHub Actions workflows
- ✅ **Cost Optimization**: Auto-scaling, lifecycle policies
- ✅ **Security**: IAM least privilege, KMS encryption
- ✅ **Monitoring**: CloudWatch logs and metrics
- ✅ **Testing**: Comprehensive test suite
- ✅ **Documentation**: Detailed README and docs
- ✅ **Governance**: Lake Formation row-level access control
- ✅ **Streaming**: Kafka integration with DLQ
- ✅ **Multi-source**: Salesforce, Snowflake, RDS, REST APIs

## 🔮 Roadmap

- [ ] **ML Pipeline**: MLOps with SageMaker
- [ ] **Data Lineage**: Apache Atlas integration
- [ ] **Real-time Analytics**: Kinesis Analytics
- [ ] **Data Mesh**: Multi-account architecture