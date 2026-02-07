# AWS Project Structure (Industry Standard)

## 📂 Directory Structure

```
aws/
│
├── 📋 README.md                     # Main documentation
│
├── 🏗️ terraform/                     # Infrastructure as Code
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
├── 💼 jobs/                          # ETL Jobs (17 jobs total)
│   ├── ingest/                       # 8 ingestion jobs
│   │   ├── crm_accounts_ingest.py
│   │   ├── crm_contacts_ingest.py
│   │   ├── crm_opportunities_ingest.py
│   │   ├── snowflake_to_bronze.py
│   │   ├── redshift_behavior_ingest.py
│   │   ├── fx_rates_ingest.py
│   │   ├── salesforce_to_bronze.py
│   │   └── kafka_orders_stream.py
│   ├── transform/                    # 3 transformation jobs
│   │   ├── snowflake_bronze_to_silver_merge.py
│   │   ├── dq_check_bronze.py
│   │   └── dq_check_silver.py
│   ├── analytics/                    # 4 analytics jobs
│   │   ├── build_sales_fact_table.py
│   │   ├── build_customer_dimension.py
│   │   ├── build_marketing_attribution.py
│   │   └── update_customer_dimension_scd2.py
│   └── maintenance/                  # 2 maintenance jobs
│       ├── delta_optimize_vacuum.py
│       └── apply_data_masking.py
│
├── 🔄 dags/                          # Airflow DAGs
│   ├── production/                    # Production DAGs
│   ├── development/                  # Development DAGs
│   │   └── archive/                  # Archived DAGs
│   ├── utils/                        # DAG utilities
│   ├── daily_batch_pipeline_dag.py   # Main production DAG
│   ├── salesforce_ingestion_dag.py   # CRM ingestion DAG
│   ├── dq_watchdog_dag.py            # DQ monitoring DAG
│   ├── maintenance_dag.py             # Maintenance DAG
│   └── README.md                     # DAG documentation
│
├── ⚙️ config/                        # Configuration files
│   ├── environments/                 # Environment configs
│   │   ├── prod.yaml                 # Production config
│   │   ├── dev.yaml                  # Development config
│   │   └── local.yaml                 # Local config
│   ├── schemas/                      # Schema definitions
│   │   ├── fx_rates_bronze.json
│   │   └── snowflake_orders_bronze.json
│   └── shared/                       # Shared configs
│       ├── dq_thresholds.yaml
│       ├── lineage.yaml
│       └── logging.conf
│
├── 🔧 scripts/                       # Utility scripts (9 scripts)
│   ├── deployment/                   # 2 deployment scripts
│   │   ├── aws_production_deploy.sh
│   │   └── teardown.sh
│   ├── maintenance/                  # 2 maintenance scripts
│   │   ├── backfill_bronze_for_date.sh
│   │   └── dr_snapshot_export.py
│   ├── utilities/                    # 7 utility scripts
│   │   ├── emr_submit.sh
│   │   ├── register_glue_tables.py
│   │   ├── lf_tags_seed.py
│   │   ├── run_ge_checks.py
│   │   ├── source_terraform_outputs.sh
│   │   ├── emit_lineage_and_metrics.py
│   │   └── notify_on_sla_breach.py
│   └── README.md                     # Scripts documentation
│
├── 📊 data/                          # Sample/test data
│   └── samples/                      # Sample data files
│       ├── crm/                      # CRM sample data
│       ├── snowflake/                # Snowflake sample data
│       ├── redshift/                 # Redshift sample data
│       ├── fx/                       # FX rates sample data
│       └── kafka/                    # Kafka events sample data
│
├── 🧪 tests/                         # Tests
│   ├── test_dag_imports.py
│   ├── test_schema_contracts.py
│   └── test_prod_config_contract.py
│
├── 📓 notebooks/                     # Jupyter notebooks
│
├── 📚 docs/                          # Documentation
│   ├── AWS_DEPLOYMENT_GUIDE.md
│   └── ...
│
├── ⚡ emr_configs/                   # EMR configurations
│   ├── spark-defaults.conf
│   ├── delta-core.conf
│   ├── logging.yaml
│   └── hive-site.xml
│
└── 🔍 athena_queries/                # Athena query samples
    └── sample_queries.sql
```

## 📊 Statistics

| Category | Count | Description |
|----------|-------|-------------|
| Jobs | 17 | ETL jobs (ingest/transform/analytics/maintenance) |
| DAGs | 4+ | Airflow orchestration |
| Config Files | 7+ | Environment + schema + shared configs |
| Scripts | 9 | Deployment + maintenance + utilities |
| Tests | 3 | Test files |

## 🎯 Key Principles

1. **Functional Organization**: Jobs organized by function (ingest/transform/analytics)
2. **Environment Separation**: Configs separated by environment
3. **Clear Boundaries**: Infrastructure, jobs, configs, scripts clearly separated
4. **Scalability**: Easy to add new components without clutter
5. **Documentation**: Comprehensive README files for each major section

## 🔗 Related Documentation

- [Main AWS README](README.md)
- [Scripts Documentation](scripts/README.md)
- [DAGs Documentation](dags/README.md)
- [Terraform Documentation](terraform/README_TERRAFORM.md)

---

**Last Updated**: 2024-01-15  
**Structure**: Industry Standard ✅

