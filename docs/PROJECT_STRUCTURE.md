# Industry-Standard Data Engineering Project Structure

## Target Structure

```
Project_A/
├── src/                          # Shared library code (used by both local and AWS)
│   └── project_a/
│       ├── extract/              # Data extraction utilities
│       ├── transform/            # Common transformation logic
│       ├── dq/                   # Data quality framework
│       ├── monitoring/           # Monitoring and lineage
│       ├── utils/                # Utility functions
│       └── schemas/              # Schema definitions
│
├── aws/                          # AWS/EMR-specific code
│   ├── jobs/                     # EMR job entry points
│   │   ├── transform/
│   │   │   ├── bronze_to_silver.py
│   │   │   └── silver_to_gold.py
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
├── local/                        # Local development and testing
│   ├── jobs/                     # Local job runners
│   │   ├── transform/
│   │   │   ├── bronze_to_silver.py
│   │   │   └── silver_to_gold.py
│   │   └── run_etl_pipeline.py
│   ├── scripts/                  # Local utility scripts
│   └── config/                   # Local configs
│       └── local.yaml
│
├── config/                       # Shared configuration templates
│   ├── dev.yaml                  # Development config template
│   ├── prod.yaml                 # Production config template
│   └── schema_definitions/       # Schema definitions
│
├── tests/                        # Unit and integration tests
│   ├── unit/
│   ├── integration/
│   └── fixtures/
│
├── data/                         # Local data storage (gitignored)
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── docs/                         # Documentation
├── scripts/                      # Project-level utility scripts
│   └── run_etl_local.py          # Main local ETL runner
│
├── pyproject.toml                # Python package configuration
├── requirements.txt              # Production dependencies
├── requirements-dev.txt          # Development dependencies
└── README.md                     # Project documentation
```

## Key Principles

1. **Separation of Concerns:**
   - `src/` = Shared library (no environment-specific code)
   - `aws/` = AWS/EMR-specific implementations
   - `local/` = Local development/testing implementations

2. **Single Source of Truth:**
   - Each transformation has ONE canonical implementation
   - AWS and local jobs use the same shared library code
   - Only entry points differ (local vs EMR)

3. **Configuration-Driven:**
   - All paths and settings in config files
   - No hardcoded values
   - Environment-specific configs in respective folders

4. **Industry Standards:**
   - Clear separation: library vs jobs vs infrastructure
   - Reusable components
   - Testable architecture
   - Scalable structure

## Migration Plan

### Phase 1: Create New Structure
- Create `local/` folder structure
- Reorganize `aws/` folder
- Ensure `src/` contains only shared code

### Phase 2: Move Files
- Move AWS jobs to `aws/jobs/`
- Move local jobs to `local/jobs/`
- Move AWS scripts to `aws/scripts/`
- Move local scripts to `local/scripts/`

### Phase 3: Update Imports
- Update all imports to use new paths
- Ensure shared library imports work from both local and AWS

### Phase 4: Remove Duplicates
- Delete duplicate transformation files
- Consolidate similar implementations
- Keep only canonical versions

### Phase 5: Update Configs
- Move AWS configs to `aws/config/`
- Move local configs to `local/config/`
- Update path references

