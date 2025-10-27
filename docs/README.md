# PySpark Data Engineering Project

Comprehensive AWS Production ETL Pipeline with Delta Lake

## 🎯 Project Overview

This is a production-ready data engineering project that demonstrates best practices for:
- Multi-source data ingestion (HubSpot, Snowflake, Redshift, Kafka, FX Rates)
- Bronze → Silver → Gold data lakehouse architecture
- Incremental loading with SCD2 support
- Data quality validation
- AWS EMR Serverless deployment
- Delta Lake for ACID transactions

## 📁 Project Structure

```
pyspark_data_engineer_project/
├── config/                      # Configuration files
│   ├── local.yaml              # Local development
│   ├── config-dev.yaml         # Dev environment
│   ├── aws.yaml                # AWS production
│   └── dq.yaml                 # Data quality config
│
├── src/pyspark_interview_project/
│   ├── utils/                   # Core utilities
│   ├── extract.py               # Data extraction
│   ├── transform.py             # Data transformation
│   ├── load.py                  # Data loading
│   ├── incremental_loading.py   # SCD2 & CDC
│   ├── jobs/                    # EMR job implementations
│   ├── dq/                      # Data quality
│   └── monitoring/              # Monitoring
│
├── jobs/                        # EMR job wrappers
├── aws/
│   ├── infra/terraform/        # Infrastructure as code
│   ├── scripts/                 # Deployment scripts
│   └── emr_configs/            # EMR configuration
│
├── tests/                       # Test suite
├── notebooks/                   # Jupyter notebooks
└── docs/                        # Documentation
```

## 🚀 Quick Start

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
pytest tests/

# Run pipeline locally
python src/pyspark_interview_project/pipeline_core.py config/config-dev.yaml
```

### AWS Deployment

See [AWS_DEPLOYMENT_GUIDE.md](AWS_DEPLOYMENT_GUIDE.md) for complete deployment instructions.

## 📊 Data Sources

1. **HubSpot CRM** - Contacts and deals
2. **Snowflake** - Orders and customers
3. **Redshift** - Customer behavior analytics
4. **Kafka** - Real-time event streaming
5. **FX Rates** - Exchange rates from vendors

## 🏗️ Architecture

- **Bronze Layer**: Raw data ingestion with schema validation
- **Silver Layer**: Cleaned, conformed data with SCD2 support
- **Gold Layer**: Business-ready dimensional models

## 🔧 Key Features

- ✅ Incremental loading strategies
- ✅ SCD2 support for slowly changing dimensions
- ✅ Data quality checks with Great Expectations
- ✅ Delta Lake for ACID transactions
- ✅ AWS EMR Serverless deployment
- ✅ Monitoring and alerting

## 📖 Documentation

- [AWS Deployment Guide](AWS_DEPLOYMENT_GUIDE.md)
- [Project Structure](PROJECT_FINAL_STRUCTURE.md)
- [AWS Runbook](RUNBOOK_AWS_2025.md)

## 🧪 Testing

```bash
# Run all tests
pytest tests/

# Run specific test suite
pytest tests/test_contracts.py
```

## 📝 Requirements

- Python 3.10+
- PySpark 3.5+
- Delta Lake
- AWS CLI configured
- Terraform 1.0+

## 📄 License

MIT License
