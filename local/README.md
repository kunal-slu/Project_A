# Local Development

This folder contains all code, scripts, and configurations for **local development and testing**.

## Structure

```
local/
├── jobs/              # Local job entry points
│   ├── transform/    # Bronze→Silver, Silver→Gold
│   └── run_etl_pipeline.py
├── scripts/          # Local utility scripts
│   └── run_etl_local.py (if exists)
├── tests/            # Local test scripts
├── config/           # Local configuration files
│   └── local.yaml
└── docs/             # Local development documentation
```

## Usage

### Run Full ETL Pipeline Locally

```bash
python local/jobs/run_etl_pipeline.py --config local/config/local.yaml
```

### Run Individual Steps

```bash
# Bronze → Silver
python local/jobs/transform/bronze_to_silver.py --config local/config/local.yaml

# Silver → Gold
python local/jobs/transform/silver_to_gold.py --config local/config/local.yaml
```

## Data Source

**Both local and AWS use the same S3 bronze layer:**
- Source: `s3://my-etl-lake-demo-424570854632/bronze/`
- Requires AWS credentials configured locally
- Outputs: S3 (silver/gold layers)

## Requirements

- AWS credentials (`~/.aws/credentials` or environment variables)
- Spark installed locally
- Python 3.9+
- All dependencies from `requirements.txt`

## Notes

- Local execution uses the **same ETL code** as AWS (shared library in `src/project_a/`)
- Local entry points are thin wrappers that call shared library functions
- Same data source = same results as AWS
