# Industry-Standard Project Structure

## 📁 Standard Structure

```
pyspark_data_engineer_project/
│
├── src/                                    # Reusable Python package (installed via pip)
│   └── pyspark_interview_project/
│       ├── extract/                       # Extraction modules (reusable classes)
│       │   ├── base_extractor.py          # Base class for all extractors
│       │   ├── snowflake_orders.py        # Snowflake extractor
│       │   ├── redshift_behavior.py       # Redshift extractor
│       │   ├── crm_hubspot.py             # CRM/HubSpot extractor
│       │   ├── kafka_orders.py            # Kafka streaming extractor
│       │   └── rest_fx_rates.py           # FX rates REST extractor
│       │
│       ├── transform/                     # Transformation modules
│       │   ├── base_transformer.py        # Base class
│       │   ├── bronze_to_silver.py        # Bronze → Silver
│       │   └── silver_to_gold.py          # Silver → Gold
│       │
│       ├── io/                            # I/O utilities
│       │   ├── publish.py                 # Publish to warehouses
│       │   └── writer.py                  # S3/Delta writers
│       │
│       ├── utils/                         # Utilities
│       │   ├── spark_session.py
│       │   ├── config.py
│       │   ├── state_store.py             # Watermark management
│       │   └── secrets.py                 # Secrets Manager integration
│       │
│       ├── monitoring/                    # Observability
│       │   ├── metrics_collector.py
│       │   ├── lineage_decorator.py
│       │   └── alerts.py
│       │
│       └── dq/                            # Data Quality
│           ├── runner.py                  # GE runner
│           └── rules.py
│
├── jobs/                                  # Production job entry points (EMR/Spark)
│   ├── ingest/                           # Ingestion jobs → raw/
│   │   ├── ingest_snowflake_to_s3.py     # Snowflake → S3 raw
│   │   ├── ingest_redshift_to_s3.py      # Redshift → S3 raw
│   │   ├── ingest_crm_to_s3.py           # CRM → S3 raw
│   │   ├── ingest_kafka_to_s3.py         # Kafka → S3 raw
│   │   └── ingest_fx_rates_to_s3.py     # FX → S3 raw
│   │
│   ├── transform/                        # Transform jobs
│   │   ├── raw_to_bronze.py              # raw → bronze
│   │   ├── bronze_to_silver.py           # bronze → silver
│   │   └── silver_to_gold.py             # silver → gold
│   │
│   └── publish/                          # Publishing jobs
│       ├── publish_gold_to_snowflake.py  # Gold → Snowflake
│       └── publish_gold_to_redshift.py   # Gold → Redshift
│
├── scripts/                               # Utility scripts (NOT production code)
│   ├── local/                            # Local development
│   ├── generate/                         # Data generation
│   ├── maintenance/                      # Maintenance tasks
│   └── backfill/                         # Backfill scripts
│
├── airflow/dags/                         # Airflow DAGs
│   ├── ingest_daily_sources_dag.py
│   ├── build_analytics_dag.py
│   └── dq_watchdog_dag.py
│
├── config/                               # Configuration
│   ├── schema_definitions/               # Schema contracts
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   ├── local.yaml
│   ├── prod.yaml
│   └── dq.yaml
│
├── aws/                                  # AWS-specific
│   ├── infra/terraform/                 # Infrastructure as Code
│   ├── scripts/                         # Deployment scripts
│   └── ddl/                             # DDL files
│       └── create_tables.sql            # All table definitions
│
├── tests/                                # Tests
│   ├── unit/
│   ├── integration/
│   └── contract/
│
└── docs/                                 # Documentation
    ├── INTERVIEW_STORY.md
    ├── SCHEMA_CATALOG.md
    └── guides/
```

## 🔑 Key Distinctions

### `src/` = Reusable Library Code
- Imported by jobs/scripts
- Never executed directly
- Unit tested
- Installed as package

### `jobs/` = Production Entry Points
- Executed by EMR/Airflow
- Import from `src/`
- Command-line interfaces
- Production-tested

### `scripts/` = Utilities Only
- Data generation
- Testing/validation
- Maintenance tasks
- One-off operations

## 🗂️ Data Lake Zones

```
s3://my-etl-lake-demo/
├── raw/              # Source dumps (1:1 from source, no schema)
│   ├── snowflake/
│   ├── redshift/
│   ├── crm/
│   ├── kafka/
│   └── fx_rates/
│
├── bronze/           # Normalized (canonical columns, ingest_ts)
│   ├── fact_orders/
│   ├── dim_customer/
│   ├── dim_account/
│   └── dim_product/
│
├── silver/           # Business-cleaned (PKs/UKs, dtypes fixed)
│   ├── dim_customer/
│   ├── dim_product/
│   ├── dim_account/
│   └── fact_orders/
│
├── gold/             # Analytics-ready (joined, aggregated)
│   ├── fact_customer_360/
│   ├── fact_orders_daily/
│   └── fact_marketing_events/
│
└── _checkpoints/     # Streaming/DQ checkpoints
```

