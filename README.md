# PySpark Data Engineering Project

## 🎯 Project Overview

This project provides a comprehensive PySpark data engineering pipeline with 5 essential data sources for learning and practice.

## 📊 Data Sources

### 1️⃣ HubSpot CRM
- **Contacts**: Customer contact information (25K records)
- **Deals**: Sales opportunities and pipeline (30K records)

### 2️⃣ Snowflake Warehouse
- **Customers**: Customer master data (50K records)
- **Orders**: Order transactions (100K records)
- **Products**: Product catalog (10K records)

### 3️⃣ Redshift Analytics
- **Customer Behavior**: User behavior analytics (50K records)

### 4️⃣ Stream Data
- **Kafka Events**: Real-time event streaming (100K records)

### 5️⃣ FX Rates
- **Historical Rates**: Exchange rates (20K records)

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- PySpark 3.5+
- Delta Lake

### Installation
```bash
pip install -r requirements.txt
```

### Running the Pipeline
```bash
python src/pyspark_interview_project/pipeline.py config/config-dev.yaml
```
Project_A/
│
├── README.md                              ← Overview, setup steps, quickstart
├── Makefile                               ← Common shortcuts (run, test, deploy)
├── requirements.txt                       ← Python dependencies for local/dev
├── setup.py                               ← Optional (for packaging jobs)
│
├── config/
│   ├── local.yaml                         ← Local testing configs (S3, paths)
│   ├── prod.yaml                          ← MWAA/EMR production configs
│   ├── dq.yaml                            ← Data quality thresholds / rules
│   └── logging.conf                       ← Logging format and levels
│
├── dags/                                  ← Airflow DAGs (used by MWAA)
│   ├── daily_pipeline_dag.py              ← Master DAG (bronze → silver → gold)
│   ├── returns_pipeline_dag.py            ← Returns + reconciliation pipeline
│   ├── catalog_and_dq_dag.py              ← Glue registration + DQ checks
│   └── utils/                             ← Custom Airflow operators/hooks
│
├── aws/                                   ← AWS deployment scripts
│   ├── scripts/
│   │   ├── aws_production_deploy.sh       ← Bootstraps EMR/roles/buckets
│   │   ├── emr_submit.sh                  ← Submits EMR Serverless jobs
│   │   ├── register_glue_tables.py        ← Glue/Athena table registration
│   │   ├── run_ge_checks.py               ← Great Expectations DQ runner
│   │   ├── delta_optimize_vacuum.py       ← (Optional) Maintenance job
│   │   └── teardown.sh                    ← Destroys AWS resources cleanly
│   │
│   ├── terraform/                         ← Infrastructure as Code (optional)
│   │   ├── main.tf                        ← MWAA, EMR, S3, IAM setup
│   │   ├── variables.tf                   ← Parameter definitions
│   │   └── outputs.tf                     ← Useful resource ARNs/IDs
│   │
│   └── emr_configs/
│       ├── spark-defaults.conf            ← Spark EMR configs
│       ├── delta-core.conf                ← Delta Lake specific configs
│       └── logging.yaml                   ← Spark log4j2 config
│
├── src/                                   ← Core PySpark code
│   └── pyspark_interview_project/
│       ├── __init__.py
│       ├── utils/
│       │   ├── spark_session.py           ← SparkSession builder (Delta-aware)
│       │   ├── io_utils.py                ← S3 / Snowflake read-write helpers
│       │   ├── path_resolver.py           ← Converts lake:// to s3a:// paths
│       │   ├── dq_utils.py                ← Quality check helpers
│       │   └── metrics.py                 ← Logging + OpenLineage metrics
│       │
│       ├── extract/
│       │   ├── hubspot_contacts.py        ← Extracts HubSpot contacts CSV
│       │   ├── hubspot_companies.py       ← Extracts HubSpot companies CSV
│       │   ├── snowflake_orders.py        ← Extract from Snowflake
│       │   ├── redshift_behavior.py       ← Extract from Redshift
│       │   ├── kafka_orders_stream.py     ← Structured streaming reader
│       │   └── fx_rates.py                ← Loads FX rate CSV / API
│       │
│       ├── transform/
│       │   ├── bronze_to_silver.py        ← Cleans & standardizes raw data
│       │   ├── silver_to_gold.py          ← Joins across sources for KPIs
│       │   ├── enrich_with_fx.py          ← Applies FX normalization
│       │   ├── build_customer_segments.py ← Customer segmentation
│       │   └── build_product_perf.py      ← Product analytics
│       │
│       ├── jobs/
│       │   ├── pipeline_driver.py         ← Unified entrypoint for DAGs/jobs
│       │   ├── run_snowflake_pipeline.py  ← Example standalone runner
│       │   └── run_redshift_pipeline.py   ← Example Redshift → Gold runner
│       │
│       ├── dq/
│       │   ├── suites/
│       │   │   ├── silver_orders.yml
│       │   │   ├── silver_fx_rates.yml
│       │   │   └── gold_revenue.yml
│       │   └── dq_runner.py               ← Executes GE suites programmatically
│       │
│       ├── pipeline/
│       │   ├── run_pipeline.py            ← Orchestrates extract → transform
│       │   └── orchestration_utils.py     ← Helper utilities for sequencing
│       │
│       └── monitoring/
│           ├── lineage_emitter.py         ← OpenLineage/Marquez integration
│           ├── alerts.py                  ← Slack/Email alerts for DQ failures
│           └── metrics_collector.py       ← Emits Prometheus/CloudWatch metrics
│
├── notebooks/
│   ├── 00_data_exploration.ipynb          ← Quick validation of sample data
│   ├── 01_customer_kpi_analysis.ipynb     ← Gold-layer analytics (PySpark SQL)
│   ├── 02_delta_table_validation.ipynb    ← Time travel / vacuum demo
│   └── 03_streaming_monitoring.ipynb      ← Kafka → Delta checkpoint validation
│
├── data/                                  ← Local-only test data
│   ├── hubspot_companies_1000.csv
│   ├── hubspot_contacts_1000.csv
│   ├── snowflake_orders_100000.csv
│   ├── snowflake_customers_50000.csv
│   ├── redshift_customer_behavior_50000.csv
│   ├── fx_rates_historical_730_days.csv
│   ├── stream_kafka_events_100000.csv
│   └── ...
│
├── tests/                                 ← Unit/integration tests
│   ├── test_spark_session.py              ← Ensures SparkSession config correct
│   ├── test_dq_suites.py                  ← Validates YAML schema and runs dry-run
│   ├── test_dag_imports.py                ← Confirms all Airflow DAGs parse
│   ├── test_glue_catalog_contract.py      ← Checks S3 paths & DB match config
│   └── conftest.py
│
└── docs/
    ├── guides/
    │   ├── README.md                      ← How to run locally + on AWS
    │   ├── AWS_COMPLETE_DEPLOYMENT.md     ← Step-by-step MWAA + EMR setup
    │   ├── DQ_SETUP.md                    ← GE + expectations instructions
    │   └── OPENLINEAGE_INTEGRATION.md     ← How to integrate lineage tracking
    │
    ├── runbooks/
    │   ├── RUNBOOK_AWS_2025.md            ← Operational runbook
    │   ├── RUNBOOK_DQ_FAILOVER.md         ← How to handle DQ breaches
    │   └── RUNBOOK_STREAMING_RESTART.md   ← Restart/Recover Kafka jobs
    │
    └── architecture/
        ├── diagrams/
        │   ├── aws_data_lake_architecture.png
        │   ├── airflow_mwaa_pipeline.png
        │   └── delta_lake_layers.png
        └── architecture_overview.md
## 📁 Project Structure

```
├── aws/data_fixed/           # Data sources
│   ├── 01_hubspot_crm/       # HubSpot CRM data
│   ├── 02_snowflake_warehouse/ # Snowflake warehouse data
│   ├── 03_redshift_analytics/ # Redshift analytics data
│   ├── 04_stream_data/       # Streaming data
│   └── 05_fx_rates/          # FX rates data
├── config/                   # Configuration files
├── src/pyspark_interview_project/ # Main pipeline code
├── airflow/dags/             # Airflow DAGs
└── docs/                     # Documentation
```

## 🎯 Learning Objectives

- **Data Engineering**: ETL pipelines, data quality, transformations
- **Analytics**: Aggregations, window functions, statistical analysis
- **Performance**: Optimization, partitioning, caching strategies
- **Integration**: Multi-source data integration
- **Real-time Processing**: Streaming data and event processing

## 📚 Documentation

- [Simplified Data Sources](docs/SIMPLIFIED_DATA_SOURCES.md)
- [Data Quality Report](docs/DATA_QUALITY_REPORT.md)

## 🔧 Configuration

All configurations are managed in the `config/` directory:
- `default.yaml` - Base configuration
- `aws.yaml` - AWS-specific settings
- `azure.yaml` - Azure-specific settings
- `local.yaml` - Local development settings

## 🚀 Ready for PySpark Practice!

This project provides realistic, high-quality data for comprehensive PySpark learning and practice.
