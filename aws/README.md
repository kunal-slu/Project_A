# AWS Data Engineering Project

Enterprise-grade AWS data engineering platform with industry-standard structure.

## 📁 Project Structure

```
aws/
├── terraform/                    # Infrastructure as Code (Terraform)
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   ├── iam.tf
│   ├── networking.tf
│   ├── glue_catalog.tf
│   ├── lake_formation.tf
│   ├── secrets.tf
│   ├── cloudwatch.tf
│   └── README_TERRAFORM.md
│
├── jobs/                        # ETL Jobs (organized by function)
│   ├── ingest/                  # Data ingestion jobs → Bronze layer
│   │   ├── crm_accounts_ingest.py
│   │   ├── crm_contacts_ingest.py
│   │   ├── crm_opportunities_ingest.py
│   │   ├── snowflake_to_bronze.py
│   │   ├── redshift_behavior_ingest.py
│   │   ├── fx_rates_ingest.py
│   │   ├── salesforce_to_bronze.py
│   │   └── kafka_orders_stream.py
│   ├── transform/               # Transformation jobs
│   │   ├── snowflake_bronze_to_silver_merge.py
│   │   ├── dq_check_bronze.py
│   │   └── dq_check_silver.py
│   ├── analytics/               # Analytics & dimensional modeling
│   │   ├── build_sales_fact_table.py
│   │   ├── build_customer_dimension.py
│   │   ├── build_marketing_attribution.py
│   │   └── update_customer_dimension_scd2.py
│   └── maintenance/             # Maintenance operations
│       ├── delta_optimize_vacuum.py
│       └── apply_data_masking.py
│
├── dags/                        # Airflow DAGs
│   ├── production/              # Production DAGs
│   ├── development/             # Development/test DAGs
│   ├── utils/                   # DAG utilities
│   ├── daily_batch_pipeline_dag.py
│   ├── salesforce_ingestion_dag.py
│   ├── dq_watchdog_dag.py
│   └── maintenance_dag.py
│
├── config/                      # Configuration files
│   ├── environments/            # Environment-specific configs
│   │   ├── dev.yaml
│   │   ├── prod.yaml
│   │   └── local.yaml
│   ├── schemas/                 # Schema definitions
│   │   ├── fx_rates_bronze.json
│   │   └── snowflake_orders_bronze.json
│   └── shared/                  # Shared configurations
│       ├── dq_thresholds.yaml
│       ├── lineage.yaml
│       └── logging.conf
│
├── scripts/                     # Utility scripts
│   ├── deployment/              # Deployment scripts
│   │   ├── aws_production_deploy.sh
│   │   └── teardown.sh
│   ├── maintenance/             # Maintenance scripts
│   │   ├── backfill_bronze_for_date.sh
│   │   └── dr_snapshot_export.py
│   └── utilities/               # Utility scripts
│       ├── emr_submit.sh
│       ├── register_glue_tables.py
│       ├── lf_tags_seed.py
│       ├── run_ge_checks.py
│       ├── source_terraform_outputs.sh
│       ├── emit_lineage_and_metrics.py
│       └── notify_on_sla_breach.py
│
├── data/                        # Sample/test data
│   └── samples/                 # Sample data files
│       ├── crm/
│       ├── snowflake/
│       ├── redshift/
│       ├── fx/
│       └── kafka/
│
├── tests/                       # Tests for AWS components
│   ├── test_dag_imports.py
│   ├── test_schema_contracts.py
│   └── test_prod_config_contract.py
│
├── notebooks/                   # Jupyter notebooks
│
├── docs/                        # AWS-specific documentation
│   ├── AWS_DEPLOYMENT_GUIDE.md
│   └── ...
│
├── emr_configs/                 # EMR configuration files
│   ├── spark-defaults.conf
│   ├── delta-core.conf
│   ├── logging.yaml
│   └── hive-site.xml
│
└── athena_queries/              # Sample Athena queries
    └── sample_queries.sql
```

## 🎯 Directory Purpose

### `infrastructure/terraform/`
Terraform Infrastructure as Code for:
- EMR Serverless applications
- MWAA (Managed Workflows for Apache Airflow)
- Glue Catalog databases
- IAM roles and policies
- S3 buckets
- Secrets Manager
- Lake Formation
- CloudWatch

### `jobs/`
All ETL jobs organized by function:
- **ingest/**: Ingest from sources → Bronze layer
- **transform/**: Transform Bronze → Silver → Gold
- **analytics/**: Build dimensional models and fact tables
- **maintenance/**: Optimization, vacuum, masking operations

### `dags/`
Airflow DAGs for orchestration:
- Production DAGs in `production/`
- Development/test DAGs in `development/`
- Main orchestration DAGs at root level

### `config/`
Configuration management:
- **environments/**: Environment-specific configs (dev/prod/local)
- **schemas/**: JSON schema definitions for data contracts
- **shared/**: Shared configs (DQ thresholds, lineage, logging)

### `scripts/`
Utility scripts organized by purpose:
- **deployment/**: Infrastructure deployment and teardown
- **maintenance/**: Data backfill and DR operations
- **utilities/**: General utilities (EMR submit, Glue registration, etc.)

## 🚀 Quick Start

### Deploy Infrastructure
```bash
cd aws/infrastructure/terraform
terraform init
terraform plan
terraform apply
```

### Run Jobs Locally
```bash
# Ingest job
python aws/jobs/ingest/crm_accounts_ingest.py --env dev

# Transform job
python aws/jobs/transform/snowflake_bronze_to_silver_merge.py --config config/environments/dev.yaml

# Analytics job
python aws/jobs/analytics/build_sales_fact_table.py --env prod
```

### Submit to EMR
```bash
./aws/scripts/utilities/emr_submit.sh \
  --app-id emr-app-id \
  --role-arn arn:aws:iam::ACCOUNT:role/EmrServerlessJobExecutionRole \
  --entry-point jobs/ingest/crm_accounts_ingest.py
```

## 📊 Data Flow

```
External Sources → jobs/ingest/ → Bronze (S3)
                                      ↓
                              jobs/transform/ → Silver (Delta)
                                      ↓
                              jobs/analytics/ → Gold (Delta)
                                      ↓
                              Glue Catalog → Athena/Business Intelligence
```

## 🔗 Related Documentation

- [Deployment Guide](docs/AWS_DEPLOYMENT_GUIDE.md)
- [Runbook](RUNBOOK_AWS_2025.md)
- [Scripts Documentation](scripts/README.md)

---

**Last Updated**: 2024-01-15  
**Maintained By**: Data Engineering Team

