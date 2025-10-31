# Quick Start Guide - 5 Minutes to Running Pipeline

Get the PySpark data engineering platform up and running in 5 minutes.

## Prerequisites

- Python 3.10 or 3.11
- 8GB+ RAM
- 10GB+ free disk space

## Step 1: Clone and Setup (2 minutes)

```bash
# Clone repository
git clone <repo-url>
cd pyspark_data_engineer_project

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

## Step 2: Configure Environment (1 minute)

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your settings (or use defaults)
# For local testing, defaults are fine
```

## Step 3: Run Your First Pipeline (1 minute)

```bash
# Run local ETL pipeline
make run-local

# Or directly:
python scripts/local/run_pipeline.py --config config/local.yaml --phase all
```

**Expected Output**:
```
✅ Bronze ingestion complete: 50,000 records
✅ Silver transformation complete: 45,000 records
✅ Gold aggregation complete: 5,000 records
✅ Pipeline completed successfully
```

## Step 4: Verify Results (1 minute)

```bash
# Check Delta Lake outputs
ls -la data/lakehouse_delta/bronze/
ls -la data/lakehouse_delta/silver/
ls -la data/lakehouse_delta/gold/

# Or use Spark to query
python << 'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("QuickCheck").getOrCreate()
df = spark.read.format("delta").load("data/lakehouse_delta/gold/fact_sales")
print(f"Gold records: {df.count():,}")
df.show(5)
PY
```

## Next Steps

### Explore the Platform

1. **View Documentation**:
   ```bash
   open docs/PROJECT_OVERVIEW.md
   ```

2. **Run Tests**:
   ```bash
   make test
   ```

3. **Check Data Quality**:
   ```bash
   make dq-check
   ```

4. **Explore Notebooks**:
   ```bash
   jupyter notebook notebooks/
   ```

### Common Commands

```bash
# Format code
make fmt

# Lint code
make lint

# Run tests
make test

# Run pipeline locally
make run-local

# Check Airflow DAGs
make airflow-check
```

## Troubleshooting

### Issue: "Module not found"

**Solution**: Ensure virtual environment is activated and dependencies installed:
```bash
source venv/bin/activate
pip install -r requirements.txt
```

### Issue: "Out of memory"

**Solution**: Reduce data size or increase Spark memory:
```bash
export SPARK_DRIVER_MEMORY=4g
```

### Issue: "Permission denied"

**Solution**: Ensure data directory is writable:
```bash
chmod -R 755 data/
```

## Need Help?

- **Documentation**: Check `docs/` directory
- **Issues**: Open GitHub issue
- **Questions**: See `docs/BEGINNERS_GUIDE.md`

---

**Time to Pipeline**: ~5 minutes ✅  
**Last Updated**: 2024-01-15

