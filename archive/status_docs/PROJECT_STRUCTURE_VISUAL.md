# 📁 Project Structure - Visual Overview

```
pyspark_data_engineer_project/
│
├── 📋 Top-Level Configuration
│   ├── README.md
│   ├── Makefile
│   ├── setup.py
│   ├── pyproject.toml
│   ├── requirements.txt
│   ├── requirements-dev.txt
│   ├── pytest.ini
│   ├── .gitignore
│   ├── .env.example
│   └── .pre-commit-config.yaml
│
├── 🔄 .github/
│   └── workflows/
│       └── ci.yml
│
├── ⚙️ config/                          # Centralized Configuration
│   ├── local.yaml                      # Local development
│   ├── dev.yaml                        # Development environment
│   ├── prod.yaml                      # Production (AWS)
│   ├── dq.yaml                        # Data quality thresholds
│   ├── lineage.yaml                   # Lineage configuration
│   ├── logging.conf                   # Logging config
│   ├── schema_definitions/            # JSON schema contracts
│   │   ├── crm_accounts.schema.json
│   │   ├── crm_contacts.schema.json
│   │   ├── snowflake_orders.schema.json
│   │   └── ...
│   └── contracts/                      # Additional contracts
│
├── 🪶 dags/                            # Airflow DAGs (MWAA Ready)
│   ├── daily_batch_pipeline_dag.py    # Main ETL pipeline
│   ├── dq_watchdog_dag.py             # Data quality monitoring
│   ├── salesforce_ingestion_dag.py     # CRM ingestion
│   ├── maintenance_dag.py             # Delta maintenance
│   └── utils/                          # DAG utilities
│       ├── emr_serverless_operator.py  # EMR Serverless operator
│       ├── sensors.py                  # Custom sensors
│       └── airflow_helpers.py          # Helper functions
│
├── 🧠 src/pyspark_interview_project/    # Core PySpark Package
│   │
│   ├── utils/                          # Utility modules
│   │   ├── spark_session.py            # Spark builder
│   │   ├── config.py                   # Config loader
│   │   ├── io.py                       # I/O operations
│   │   ├── schema_validator.py         # Schema validation
│   │   ├── dq_utils.py                 # DQ utilities
│   │   ├── secrets.py                  # Secrets Manager
│   │   ├── watermark.py                # Watermark management
│   │   └── metrics.py                  # Metrics tracking
│   │
│   ├── contracts/                     # Data contracts
│   │   ├── crm_accounts.schema.json
│   │   ├── crm_contacts.schema.json
│   │   └── README.md
│   │
│   ├── extract/                        # Extraction modules
│   │   ├── salesforce_accounts.py
│   │   ├── salesforce_contacts.py
│   │   ├── salesforce_opportunities.py
│   │   ├── snowflake_orders.py
│   │   ├── redshift_behavior.py
│   │   ├── fx_rates.py
│   │   └── kafka_orders_stream.py
│   │
│   ├── transform/                      # Transformation modules
│   │   ├── bronze_to_silver.py
│   │   ├── incremental_upsert.py
│   │   ├── scd2.py
│   │   └── silver_to_gold.py
│   │
│   ├── pipeline/                       # Pipeline orchestration
│   │   ├── run_pipeline.py             # Main orchestrator
│   │   ├── bronze_to_silver.py
│   │   └── silver_to_gold.py
│   │
│   ├── dq/                             # Data quality
│   │   ├── runner.py
│   │   └── suites/                     # DQ suites
│   │
│   ├── monitoring/                     # Observability
│   │   ├── metrics.py                  # Metrics collection
│   │   ├── lineage_emitter.py          # OpenLineage
│   │   ├── alerts.py                   # Alerting (Slack/email)
│   │   └── __init__.py
│   │
│   ├── jobs/                           # Job modules
│   │   └── ...
│   │
│   └── [other modules]/                # Additional modules
│
├── ☁️ aws/                             # AWS Deployment
│   │
│   ├── jobs/                           # Deployment job entrypoints
│   │   ├── ingest/                     # 8 ingestion jobs
│   │   │   ├── crm_accounts_ingest.py
│   │   │   ├── crm_contacts_ingest.py
│   │   │   ├── snowflake_to_bronze.py
│   │   │   ├── redshift_behavior_ingest.py
│   │   │   ├── fx_rates_ingest.py
│   │   │   └── ...
│   │   ├── transform/                  # 3 transformation jobs
│   │   │   ├── snowflake_bronze_to_silver_merge.py
│   │   │   ├── dq_check_bronze.py
│   │   │   └── dq_check_silver.py
│   │   ├── analytics/                  # 4 analytics jobs
│   │   │   ├── build_sales_fact_table.py
│   │   │   ├── build_customer_dimension.py
│   │   │   └── ...
│   │   └── maintenance/                # 2 maintenance jobs
│   │       ├── delta_optimize_vacuum.py
│   │       └── apply_data_masking.py
│   │
│   ├── scripts/                        # Deployment scripts
│   │   ├── deployment/                  # Deployment scripts
│   │   │   ├── aws_production_deploy.sh
│   │   │   └── teardown.sh
│   │   ├── maintenance/                 # Maintenance scripts
│   │   │   ├── backfill_bronze_for_date.sh
│   │   │   └── dr_snapshot_export.py
│   │   └── utilities/                   # Utility scripts
│   │       ├── emr_submit.sh
│   │       ├── register_glue_tables.py
│   │       ├── lf_tags_seed.py
│   │       └── ...
│   │
│   ├── terraform/                      # Infrastructure as Code
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   ├── outputs.tf
│   │   ├── iam.tf
│   │   ├── networking.tf
│   │   ├── glue_catalog.tf
│   │   ├── lake_formation.tf
│   │   ├── secrets.tf
│   │   └── cloudwatch.tf
│   │
│   ├── dags/                           # AWS-specific DAGs (legacy)
│   │   ├── production/
│   │   └── development/
│   │
│   ├── config/                         # AWS-specific configs
│   │   ├── environments/
│   │   ├── schemas/
│   │   └── shared/
│   │
│   ├── emr_configs/                    # EMR configurations
│   │   ├── spark-defaults.conf
│   │   ├── delta-core.conf
│   │   └── logging.yaml
│   │
│   ├── data/                           # Sample data (local)
│   │   └── samples/
│   │       ├── crm/
│   │       ├── snowflake/
│   │       └── ...
│   │
│   ├── tests/                          # AWS-specific tests
│   └── docs/                           # AWS documentation
│
├── 📊 data/                            # Data Directory
│   ├── samples/                        # Sample datasets
│   │   ├── crm/
│   │   ├── snowflake/
│   │   ├── redshift/
│   │   ├── fx/
│   │   └── kafka/
│   ├── lakehouse_delta/                # Delta Lake output
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   └── [other data dirs]/
│
├── 📒 notebooks/                      # Jupyter Notebooks
│   ├── 00_data_exploration.ipynb
│   └── ...
│
├── 🧪 tests/                           # Test Suite
│   ├── conftest.py                    # Pytest fixtures
│   ├── test_spark_session.py
│   ├── test_io_utils.py
│   ├── test_dq_runner.py
│   ├── test_contracts.py
│   └── test_dag_imports.py
│
├── 📚 docs/                           # Documentation
│   ├── guides/                        # Guides
│   │   ├── AWS_COMPLETE_DEPLOYMENT.md
│   │   └── ...
│   ├── runbooks/                      # Runbooks
│   │   ├── RUNBOOK_AWS_2025.md
│   │   └── ...
│   └── architecture/                  # Architecture docs
│       └── diagrams/
│
├── 🔧 scripts/                        # Utility Scripts
│   ├── local/                         # Local execution scripts
│   └── ...
│
└── 🐳 docker/                         # Docker configs
    └── docker-compose.yml
```

## 📊 Key Statistics

| Category | Count | Location |
|----------|-------|----------|
| **Jobs** | 22 | `aws/jobs/` (organized by function) |
| **DAGs** | 7 | `dags/` (root level) |
| **Config Files** | 28 | `config/` (centralized) |
| **Scripts** | 12+ | `aws/scripts/` (organized by purpose) |
| **Source Modules** | 120 | `src/pyspark_interview_project/` (Python files) |
| **Tests** | 27 | `tests/` |
| **Schemas** | 10+ | `config/schema_definitions/` |

## 🎯 Key Directories

### ✅ **src/pyspark_interview_project/** - Core Package
- Reusable PySpark modules
- Importable Python package
- Business logic and transformations

### ✅ **aws/jobs/** - Deployment Entrypoints
- Organized by function (ingest/transform/analytics/maintenance)
- These are what get submitted to EMR Serverless
- Reference modules from `src/`

### ✅ **dags/** - Airflow Orchestration
- Production DAGs at root level
- DAG utilities in `utils/`
- Ready for MWAA deployment

### ✅ **config/** - Centralized Configuration
- Environment-specific configs
- Schema definitions
- Shared configurations

### ✅ **aws/terraform/** - Infrastructure as Code
- All AWS resources defined in Terraform
- EMR, MWAA, S3, IAM, etc.

---

**Last Updated**: 2024-01-15  
**Structure**: Industry Standard ✅

