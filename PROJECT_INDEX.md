# Project_A - Final Structure Index

## Overview

This document describes the final, cleaned structure of Project_A after comprehensive refactoring. The project is now organized as a minimal, production-grade data platform with clear separation of concerns.

## Core Directory Structure

```
Project_A/
├── jobs/                    # ETL job entry points
│   ├── transform/          # Main transformation jobs
│   │   ├── bronze_to_silver.py    (341 lines - refactored)
│   │   └── silver_to_gold.py      (321 lines - refactored)
│   ├── ingest/             # Data ingestion jobs
│   ├── dq/                 # Data quality jobs
│   └── publish/            # Data publishing jobs
│
├── src/                     # Shared library code
│   └── project_a/
│       ├── utils/           # Core utilities
│       │   ├── spark_session.py      # SparkSession builder
│       │   ├── path_resolver.py       # Path resolution (local/AWS)
│       │   ├── config_loader.py       # Config loading
│       │   ├── logging.py            # Logging utilities
│       │   └── run_audit.py          # Run audit logging
│       │
│       ├── pyspark_interview_project/  # Main library
│       │   ├── transform/            # Transformation modules
│       │   │   ├── bronze_loaders.py    # Bronze data loaders
│       │   │   ├── silver_builders.py   # Silver builders
│       │   │   ├── gold_builders.py     # Gold builders
│       │   │   └── base_transformer.py  # Base transformer class
│       │   │
│       │   ├── io/                   # I/O utilities
│       │   │   └── delta_writer.py     # Delta/Parquet writer
│       │   │
│       │   ├── monitoring/            # Monitoring & lineage
│       │   │   ├── lineage_decorator.py
│       │   │   └── metrics_collector.py
│       │   │
│       │   └── utils/                # Additional utilities
│       │       ├── config_loader.py
│       │       └── contracts.py
│       │
│       ├── extract/                  # Data extraction
│       │   └── fx_json_reader.py
│       │
│       └── schemas/                   # Schema definitions
│           └── bronze_schemas.py
│
├── config/                  # Configuration files
│   ├── dev.yaml            # Development config (AWS)
│   ├── local.yaml          # Local development config
│   └── prod.yaml           # Production config
│
├── aws/                     # AWS-specific code
│   ├── dags/               # Airflow DAGs
│   ├── terraform/          # Infrastructure as Code
│   ├── scripts/            # AWS deployment scripts
│   └── config/            # AWS-specific configs
│
├── tests/                   # Test suite
│   ├── unit/               # Unit tests
│   └── integration/        # Integration tests
│
├── docs/                    # Documentation
│   ├── README.md
│   ├── CLEANUP_REPORT.md
│   └── PROJECT_INDEX.md (this file)
│
└── data/                    # Sample data (not in version control)
    └── samples/            # Local sample data files
```

## Key Files Explained

### ETL Jobs

1. **`jobs/transform/bronze_to_silver.py`** (341 lines)
   - Main entry point for Bronze → Silver transformation
   - Orchestrates loading, transformation, and writing
   - Uses shared loaders, builders, and writers

2. **`jobs/transform/silver_to_gold.py`** (321 lines)
   - Main entry point for Silver → Gold transformation
   - Builds star schema (dimensions, facts, analytics)
   - Uses shared builders and writers

### Shared Libraries

#### Transform Modules

- **`bronze_loaders.py`**: Loads data from Bronze layer with fallback (Delta → Parquet → CSV)
- **`silver_builders.py`**: Builds Silver tables (customers, orders, products, behavior)
- **`gold_builders.py`**: Builds Gold layer (dimensions, facts, analytics)

#### I/O Modules

- **`delta_writer.py`**: Unified writer for Delta/Parquet formats
  - Automatically selects format based on environment
  - Handles partitioning, optimization, and vacuum

#### Utilities

- **`spark_session.py`**: Creates SparkSession with Delta Lake support
- **`path_resolver.py`**: Resolves paths for local vs AWS environments
- **`config_loader.py`**: Loads YAML configs from local files or S3

## Data Flow

```
Bronze (Raw) → Silver (Cleansed) → Gold (Analytics)
     ↓              ↓                    ↓
  CSV/JSON      Delta/Parquet        Delta/Parquet
  Delta         Partitioned          Star Schema
```

## Execution Patterns

### Local Execution

```bash
# Bronze → Silver
python jobs/transform/bronze_to_silver.py \
  --env local \
  --config local/config/local.yaml

# Silver → Gold
python jobs/transform/silver_to_gold.py \
  --env local \
  --config local/config/local.yaml
```

### AWS Execution (EMR)

```bash
# Submit via EMR Serverless or EMR on EC2
# Jobs automatically detect AWS environment from config
```

## Configuration

- **`config/local.yaml`**: Local development config
  - Points to local file paths
  - Uses Parquet format
  - Local Spark (`local[*]`)

- **`config/dev.yaml`**: AWS development config
  - Points to S3 paths
  - Uses Delta format
  - EMR Serverless/EC2

## Key Design Principles

1. **Separation of Concerns**: Load → Transform → Write
2. **Reusability**: Shared utilities across all jobs
3. **Environment Agnostic**: Same code works locally and on AWS
4. **Config-Driven**: No hardcoded paths or settings
5. **Graceful Degradation**: Handles missing data/files gracefully

## Module Dependencies

```
jobs/transform/
  ├── bronze_loaders (loads Bronze data)
  ├── silver_builders (transforms to Silver)
  ├── gold_builders (transforms to Gold)
  ├── delta_writer (writes output)
  ├── config_loader (loads config)
  ├── path_resolver (resolves paths)
  └── spark_session (creates SparkSession)
```

## Testing

- **Unit Tests**: Test individual functions in `tests/unit/`
- **Integration Tests**: Test full pipeline in `tests/integration/`
- **Local Run**: Test with sample data in `data/samples/`

## Next Steps for New Developers

1. **Read**: `README.md` for project overview
2. **Review**: `CLEANUP_REPORT.md` for recent changes
3. **Run Locally**: Execute `bronze_to_silver.py` and `silver_to_gold.py`
4. **Explore**: Check `src/project_a/pyspark_interview_project/transform/` for reusable functions
5. **Configure**: Adjust `config/local.yaml` for your environment

## Archive

Old/experimental code has been moved to:
- `jobs/transform/*_old.py` (backup of original files)
- `archive/` (for future cleanup of experiments)

## Maintenance Notes

- **Adding New Sources**: Extend `bronze_loaders.py`
- **Adding New Transformations**: Extend `silver_builders.py` or `gold_builders.py`
- **Changing Output Format**: Modify `delta_writer.py`
- **Adding Config Options**: Update `config_loader.py` and config YAMLs

