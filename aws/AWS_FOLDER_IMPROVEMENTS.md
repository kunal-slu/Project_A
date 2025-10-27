# AWS Folder Improvements - Industry Best Practices

## Current Issues Found

### 1. **Duplicate/Obsolete Config Files** ❌
- Multiple config files with similar names:
  - config-aws-enterprise.yaml
  - config-aws-enterprise-simple.yaml
  - config-aws-enterprise-internal.yaml
  - config-aws-real-world.yaml
  - config-aws-prod.yaml

**Recommendation:** Keep only `config-prod.yaml` and `config-dev.yaml`

### 2. **Missing Terraform Structure** ❌
- Terraform files exist but incomplete
- Missing: networking.tf, cloudwatch.tf, glue_catalog.tf
- Need per-environment variables

### 3. **Jobs Folder Has Non-Job Files** ❌
- jobs/ folder contains .json files (Databricks configs)
- Should move to config/ or databricks/

### 4. **Duplicate Scripts** ❌
- Multiple similar ETL scripts in scripts/
- Should consolidate into one production script

### 5. **DAGs Not Organized** ❌
- Multiple DAG files with similar purposes
- Need clear separation: production vs development

## Recommended Structure

```
aws/
├── README_AWS.md
├── RUNBOOK_AWS_2025.md
├── requirements.txt
│
├── terraform/                    # Complete IaC
│   ├── main.tf                   # Core resources
│   ├── iam.tf                    # IAM roles & policies
│   ├── secrets.tf                # Secrets Manager
│   ├── networking.tf             # VPC, subnets, SG
│   ├── cloudwatch.tf              # Logs, alarms
│   ├── glue_catalog.tf            # Glue databases
│   ├── variables.tf               # Variables
│   ├── outputs.tf                 # Outputs
│   ├── terraform.tfvars           # Default values
│   └── README_TERRAFORM.md       # How to deploy
│
├── config/                       # Runtime configuration
│   ├── prod.yaml                 # Production
│   ├── dev.yaml                  # Development
│   ├── dq_thresholds.yaml         # DQ config
│   └── schema_definitions/        # Schema contracts
│
├── emr_configs/                  # EMR configurations
│   ├── spark-defaults.conf
│   ├── delta-core.conf
│   └── logging.yaml
│
├── jobs/                         # Spark entrypoints ONLY
│   ├── hubspot_to_bronze.py
│   ├── snowflake_to_bronze.py
│   ├── dq_check_bronze.py
│   ├── dq_check_silver.py
│   └── bronze_to_silver.py
│
├── dags/                         # Airflow DAGs
│   ├── daily_batch_pipeline.py    # Main production DAG
│   ├── streaming_ingest.py        # Streaming DAG
│   └── utils/
│       ├── emr_operator.py
│       └── sensors.py
│
├── scripts/                      # Deployment tooling
│   ├── build_zip.sh
│   ├── emr_submit.sh
│   └── run_ge_checks.py
│
└── tests/                        # CI/CD tests
    ├── test_dag_imports.py
    └── test_terraform_outputs.py
```

## Priority Actions

### High Priority
1. ✅ Consolidate config files to prod.yaml and dev.yaml
2. ✅ Complete Terraform infrastructure (add missing files)
3. ✅ Remove duplicate DAGs (keep only production ones)
4. ✅ Organize jobs/ folder (remove non-job files)

### Medium Priority
5. Consolidate ETL scripts in scripts/
6. Add networking.tf for VPC configuration
7. Add cloudwatch.tf for monitoring
8. Add glue_catalog.tf for Glue setup

### Low Priority
9. Create terraform.tfvars per environment
10. Add README_TERRAFORM.md
11. Clean up old doc files in docs/

## Implementation Plan

1. **Consolidate Config Files** (5 min)
2. **Complete Terraform Structure** (15 min)
3. **Organize DAGs** (10 min)
4. **Clean Jobs Folder** (5 min)
5. **Add Missing Infrastructure** (20 min)

Total time: ~55 minutes

