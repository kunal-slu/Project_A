# AWS Project Structure Standardization

## Current Issues

1. ✅ Empty directories: `infra/`, `data_fixed/`
2. ✅ Duplicate terraform locations: `infra/` (empty) vs `terraform/` (used)
3. ✅ Jobs not organized by function (all in one folder)
4. ✅ Config files duplication (`prod.yaml` vs `config-prod.yaml`)
5. ✅ `.github/` in wrong location (should be at project root)
6. ✅ Documentation scattered

## Industry Standard Structure

```
aws/
├── infrastructure/          # Infrastructure as Code
│   └── terraform/          # Terraform configurations
│
├── jobs/                    # ETL jobs organized by function
│   ├── ingest/              # Data ingestion jobs
│   ├── transform/           # Transformation jobs (bronze→silver, silver→gold)
│   ├── analytics/           # Analytics and dimensional modeling
│   └── maintenance/         # Maintenance operations (optimize, vacuum)
│
├── dags/                    # Airflow DAGs
│   ├── production/          # Production DAGs
│   ├── development/         # Development/test DAGs
│   └── utils/               # DAG utilities
│
├── config/                  # Configuration files
│   ├── environments/        # Environment-specific configs
│   ├── schemas/             # Schema definitions
│   └── shared/              # Shared configs (dq, lineage, etc.)
│
├── scripts/                 # Utility scripts
│   ├── deployment/          # Deployment scripts
│   ├── maintenance/         # Maintenance scripts
│   └── utilities/           # Utility scripts
│
├── data/                    # Sample/test data
│   └── samples/             # Sample data files
│
├── tests/                   # Tests for AWS components
│
├── notebooks/               # Jupyter notebooks
│
├── docs/                    # AWS-specific documentation
│
└── emr_configs/             # EMR configuration files
```

## Implementation Plan

### Step 1: Organize Jobs by Function
- Move ingest jobs to `jobs/ingest/`
- Move transform jobs to `jobs/transform/`
- Move analytics jobs to `jobs/analytics/`
- Move maintenance to `jobs/maintenance/`

### Step 2: Consolidate Config
- Keep `config/environments/` for dev/prod/local
- Move schemas to `config/schemas/`
- Move shared configs to `config/shared/`

### Step 3: Clean Up Directories
- Remove empty `infra/` and `data_fixed/`
- Move `.github/` to project root
- Consolidate terraform in one location

### Step 4: Organize Scripts
- Group scripts by purpose (deployment, maintenance, utilities)

---

**Status**: Ready to implement

