# Project Structure Explanation

## Why Both `src/` and `scripts/`?

### Current Issue

**`src/` folder**: Contains reusable Python packages/modules
- `src/pyspark_interview_project/` - Main package
  - `extract/` - Extraction modules (reusable classes/functions)
  - `transform/` - Transformation modules (reusable classes/functions)
  - `utils/` - Utility functions
  - `monitoring/` - Monitoring utilities

**`scripts/` folder**: Contains standalone executable scripts
- `scripts/local/` - Local development/test scripts
- `scripts/performance/` - Performance testing
- One-off utilities, data generation, maintenance tasks

### Problem

This creates confusion:
- Where should new code go?
- Scripts import from `src/`, but they're separate
- Not clear what's "production code" vs "utility scripts"

## Industry-Standard Structure

### Standard Pattern

```
project_root/
├── src/                      # Production code (installed as package)
│   └── package_name/        # Installable package
│       ├── extract/         # Reusable modules
│       ├── transform/       # Reusable modules
│       └── utils/           # Utilities
│
├── scripts/                  # Executable scripts (entry points)
│   ├── ingest/              # Ingestion scripts
│   ├── transform/          # Transformation scripts
│   └── maintenance/        # Maintenance utilities
│
├── jobs/                     # EMR/Spark job definitions
│   ├── ingest/              # Ingestion jobs
│   ├── transform/           # Transformation jobs
│   └── publish/             # Publishing jobs
│
├── dags/                     # Airflow DAGs (or airflow/dags/)
│
├── config/                   # Configuration files
│
├── tests/                    # Test files
│
└── docs/                     # Documentation
```

### Industry Standard Rules

1. **`src/`**: Reusable code (installed as package)
   - Imported by scripts/jobs
   - No direct execution
   - Unit tested

2. **`scripts/`**: Executable entry points
   - Can be run directly (`python scripts/ingest/ingest_snowflake.py`)
   - Import from `src/`
   - Command-line interfaces

3. **`jobs/`**: EMR/Spark job files
   - Job definitions for EMR Serverless
   - May duplicate some scripts but with EMR config
   - Entry points for Spark applications

## Our Consolidated Structure

After cleanup:

```
project_root/
├── src/pyspark_interview_project/  # Reusable package (production code)
│   ├── extract/                     # Base extractors, reusable classes
│   ├── transform/                   # Base transformers, reusable classes
│   ├── io/                          # I/O utilities (S3, Snowflake, Redshift)
│   ├── utils/                        # Utilities (spark, config, state, secrets)
│   ├── monitoring/                  # Metrics, lineage, DQ
│   └── dq/                          # Data quality modules
│
├── scripts/                          # Executable scripts (kept for utilities)
│   ├── local/                       # Local testing/development
│   ├── generate/                    # Data generation
│   └── maintenance/                 # Maintenance tasks
│
├── jobs/                            # Production job entry points
│   ├── ingest/                      # Ingestion jobs → raw/
│   ├── transform/                   # Transform jobs (bronze→silver→gold)
│   └── publish/                     # Publish jobs (S3→Snowflake/Redshift)
│
├── airflow/dags/                    # Airflow DAGs
│
├── config/                          # Configuration
│   ├── schema_definitions/          # Schema contracts
│   └── *.yaml                       # Environment configs
│
└── tests/                           # Tests
```

## Migration Plan

1. **Keep `src/`**: For reusable, importable code
2. **Keep `scripts/`**: Only for utilities (data generation, testing, maintenance)
3. **Use `jobs/`**: For all production job entry points
4. **Clear separation**: 
   - `src/` = library code
   - `jobs/` = application entry points
   - `scripts/` = one-off utilities

## Summary

- **`src/`**: Reusable modules (BaseExtractor, BaseTransformer, utilities)
- **`jobs/`**: Production job entry points (ingest, transform, publish)
- **`scripts/`**: Utilities only (testing, data generation, maintenance)

This follows Python packaging best practices and industry standards.

