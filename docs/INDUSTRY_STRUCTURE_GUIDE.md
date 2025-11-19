# Industry-Standard Data Engineering Project Structure

## Overview

This project follows industry best practices for data engineering platforms, with clear separation between:
- **Shared Library** (`src/`): Reusable, testable business logic
- **AWS/EMR Jobs** (`aws/`): Cloud-specific implementations
- **Local Development** (`local/`): Local testing and development

## Directory Structure

```
Project_A/
│
├── src/                          # 📚 SHARED LIBRARY (No environment-specific code)
│   └── project_a/
│       ├── extract/              # Data extraction utilities
│       ├── transform/            # Core transformation logic
│       │   ├── bronze_to_silver.py
│       │   └── silver_to_gold.py
│       ├── dq/                   # Data quality framework
│       ├── monitoring/            # Monitoring, lineage, metrics
│       ├── utils/                # Utility functions
│       │   ├── spark_session.py
│       │   ├── logging.py
│       │   └── config.py
│       └── schemas/              # Schema definitions
│
├── aws/                          # ☁️ AWS/EMR-SPECIFIC CODE
│   ├── jobs/                     # EMR job entry points
│   │   ├── transform/
│   │   │   ├── bronze_to_silver.py  # EMR entry point
│   │   │   └── silver_to_gold.py    # EMR entry point
│   │   ├── ingest/               # Data ingestion jobs
│   │   └── publish/              # Data publishing jobs
│   ├── scripts/                  # AWS deployment scripts
│   │   ├── build_dependencies_zip.sh
│   │   ├── sync_artifacts_to_s3.sh
│   │   └── run_emr_steps.sh
│   ├── terraform/                # Infrastructure as Code
│   ├── dags/                     # Airflow DAGs
│   └── config/                   # AWS-specific configs
│       └── dev.yaml
│
├── local/                        # 💻 LOCAL DEVELOPMENT
│   ├── jobs/                     # Local job runners
│   │   ├── transform/
│   │   │   ├── bronze_to_silver.py  # Local entry point
│   │   │   └── silver_to_gold.py    # Local entry point
│   │   └── run_etl_pipeline.py   # Main local ETL runner
│   ├── scripts/                  # Local utility scripts
│   └── config/                   # Local configs
│       └── local.yaml
│
├── config/                       # 📋 SHARED CONFIG TEMPLATES
│   ├── dev.yaml                  # Development config template
│   ├── prod.yaml                 # Production config template
│   └── schema_definitions/       # Schema definitions
│
├── tests/                        # 🧪 TESTS
│   ├── unit/                     # Unit tests
│   ├── integration/              # Integration tests
│   └── fixtures/                 # Test fixtures
│
├── data/                         # 💾 LOCAL DATA (gitignored)
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── docs/                         # 📖 DOCUMENTATION
│   ├── PROJECT_STRUCTURE.md
│   └── ...
│
├── scripts/                       # 🔧 PROJECT-LEVEL UTILITIES
│   └── cleanup_unwanted_files.sh
│
├── pyproject.toml                # Python package config
├── requirements.txt              # Production dependencies
├── requirements-dev.txt          # Development dependencies
└── README.md                     # Project documentation
```

## Key Principles

### 1. Single Source of Truth
- **Business Logic**: Only in `src/project_a/transform/`
- **Entry Points**: Separate for local (`local/jobs/`) and AWS (`aws/jobs/`)
- **No Duplication**: Each transformation has ONE implementation

### 2. Clear Separation
- **Shared Library** (`src/`): Environment-agnostic, testable, reusable
- **AWS Jobs** (`aws/jobs/`): EMR-specific entry points, S3 paths, Delta Lake
- **Local Jobs** (`local/jobs/`): Local entry points, file paths, Parquet

### 3. Configuration-Driven
- All paths in config files
- No hardcoded values
- Environment-specific configs in respective folders

### 4. Industry Standards
- **Modular**: Clear module boundaries
- **Testable**: Shared library easily unit tested
- **Scalable**: Easy to add new transformations
- **Maintainable**: Clear structure, no duplication

## Usage

### Local Development

```bash
# Run full ETL pipeline locally
python local/jobs/run_etl_pipeline.py --config local/config/local.yaml

# Run individual steps
python local/jobs/transform/bronze_to_silver.py --config local/config/local.yaml
python local/jobs/transform/silver_to_gold.py --config local/config/local.yaml
```

### AWS/EMR Execution

```bash
# Upload jobs to S3
aws s3 sync aws/jobs/ s3://bucket/jobs/

# Submit EMR job
aws emr-serverless start-job-run \
  --application-id $EMR_APP_ID \
  --execution-role-arn $EMR_ROLE_ARN \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://bucket/jobs/transform/bronze_to_silver.py",
      "entryPointArguments": ["--env", "dev", "--config", "s3://bucket/config/dev.yaml"]
    }
  }'
```

## File Organization Rules

### ✅ DO:
- Put shared business logic in `src/project_a/transform/`
- Put AWS-specific code in `aws/`
- Put local-specific code in `local/`
- Use config files for all paths and settings
- Keep entry points thin (just import and call shared library)

### ❌ DON'T:
- Duplicate transformation logic
- Hardcode paths or environment checks
- Mix AWS and local code in same file
- Put business logic in entry points
- Create multiple implementations of same transformation

## Migration Status

- ✅ Created new directory structure
- ✅ Created local job entry points
- ✅ Created AWS job entry points
- ⏳ Moving shared transformation logic to `src/project_a/transform/`
- ⏳ Updating imports and paths
- ⏳ Removing duplicate files

