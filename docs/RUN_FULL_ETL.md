# 🚀 Running Full ETL Pipeline

## Quick Start

### Option 1: Run Full Pipeline (All Steps)

```bash
# Run complete pipeline: Bronze → Silver → Gold → Publish
python3 scripts/run_full_etl.py --env dev

# Skip bronze ingestion (if already done)
python3 scripts/run_full_etl.py --env dev --skip-bronze

# Skip publish step (if only testing transformations)
python3 scripts/run_full_etl.py --env dev --skip-publish
```

### Option 2: Run Individual Jobs

```bash
# 1. FX JSON to Bronze
python3 -m project_a.pipeline.run_pipeline \
  --job fx_json_to_bronze \
  --env dev \
  --config config/dev.yaml

# 2. Bronze to Silver
python3 -m project_a.pipeline.run_pipeline \
  --job bronze_to_silver \
  --env dev \
  --config config/dev.yaml

# 3. Silver to Gold
python3 -m project_a.pipeline.run_pipeline \
  --job silver_to_gold \
  --env dev \
  --config config/dev.yaml

# 4. Publish to Snowflake
python3 -m project_a.pipeline.run_pipeline \
  --job publish_gold_to_snowflake \
  --env dev \
  --config config/dev.yaml
```

## Prerequisites

### Local Execution
- Python 3.10+
- PySpark 3.5.1 installed
- Java 8 or 11
- Config file: `config/dev.yaml`
- Sample data files in S3 (or local paths configured)

### EMR Serverless Execution
- AWS credentials configured
- EMR Serverless application created
- Config file uploaded to S3
- Wheel package uploaded to S3

## Pipeline Steps

The full ETL pipeline consists of:

1. **Bronze Ingestion** (`fx_json_to_bronze`)
   - Reads FX JSON from source
   - Validates against schema contract
   - Writes to Bronze layer (Delta format)

2. **Bronze → Silver** (`bronze_to_silver`)
   - Transforms all 5 sources (CRM, Redshift, Snowflake, FX, Kafka)
   - Joins and normalizes data
   - Writes to Silver layer

3. **Silver → Gold** (`silver_to_gold`)
   - Creates star schema (fact_orders, dim_customer, dim_product)
   - Aggregations and business logic
   - Writes to Gold layer

4. **Publish** (`publish_gold_to_snowflake`)
   - Reads Gold tables
   - Publishes to Snowflake (and optionally Redshift)

## Running on EMR Serverless

```bash
# Submit job via AWS CLI
aws emr-serverless start-job-run \
  --application-id <EMR_APP_ID> \
  --execution-role-arn <EXEC_ROLE_ARN> \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://bucket/packages/project_a-0.1.0-py3-none-any.whl",
      "entryPointArguments": [
        "--job", "bronze_to_silver",
        "--env", "dev",
        "--config", "s3://bucket/config/dev.yaml"
      ],
      "sparkSubmitParameters": "--packages io.delta:delta-core_2.12:2.4.0"
    }
  }'
```

## Troubleshooting

### SparkSession Creation Error
If you see SparkSession initialization errors locally:
- Ensure Java 8 or 11 is installed: `java -version`
- Check PySpark version compatibility
- For local testing, use `--skip-bronze` if data already exists

### Config File Not Found
- Ensure `config/dev.yaml` exists
- Or specify custom path: `--config /path/to/config.yaml`

### Import Errors
- Ensure `src/` is in Python path
- Install package: `pip install -e .`

## Expected Output

```
🚀 Starting Full ETL Pipeline
================================================================================

📋 Step: FX JSON → Bronze
--------------------------------------------------------------------------------
🚀 Running job: fx_json_to_bronze
✅ Job fx_json_to_bronze completed successfully

📋 Step: Bronze → Silver
--------------------------------------------------------------------------------
🚀 Running job: bronze_to_silver
✅ Job bronze_to_silver completed successfully

📋 Step: Silver → Gold
--------------------------------------------------------------------------------
🚀 Running job: silver_to_gold
✅ Job silver_to_gold completed successfully

📋 Step: Gold → Snowflake
--------------------------------------------------------------------------------
🚀 Running job: publish_gold_to_snowflake
✅ Job publish_gold_to_snowflake completed successfully

================================================================================
🎉 Full ETL Pipeline Completed Successfully!
================================================================================
```

