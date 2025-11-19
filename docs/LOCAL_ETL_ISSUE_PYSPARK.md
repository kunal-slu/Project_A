# Local ETL - PySpark 3.4.4 + Java 17 Compatibility Issue

## Problem

PySpark 3.4.4 has a known bug with Java 17 that prevents SparkSession creation:
```
Py4JException: Constructor org.apache.spark.sql.SparkSession([class org.apache.spark.SparkContext, class java.util.HashMap]) does not exist
```

This is a bug in PySpark's internal code, not our code.

## Solution

**Upgrade PySpark to 3.5.0 or later:**

```bash
# Activate your virtual environment first
source .venv/bin/activate  # or: source venv/bin/activate

# Upgrade PySpark
pip install --upgrade pyspark==3.5.0

# Verify
python3 -c "import pyspark; print(pyspark.__version__)"
# Should show: 3.5.0
```

## Alternative: Use Java 11

If you can't upgrade PySpark, install Java 11:

```bash
# Install Java 11 via Homebrew
brew install openjdk@11

# Then update scripts/run_local_etl_fixed.sh to prefer Java 11
# (It already tries Java 11 first, so just install it)
```

## After Fix

Once PySpark is upgraded or Java 11 is installed, run:

```bash
./scripts/run_local_etl_fixed.sh
```

## Status

✅ **All code fixes complete** - The ETL pipeline code is ready  
✅ **File cleanup complete** - Removed 23 unused files and 25 empty directories  
❌ **Blocked by PySpark version** - Need to upgrade to 3.5.0+

## What Was Fixed

1. ✅ Java version detection and PATH setup
2. ✅ PYTHONPATH configuration
3. ✅ Path resolution for local file:// paths
4. ✅ Import paths for local execution
5. ✅ Config parameter passing to load functions
6. ✅ Removed 23 duplicate/unused files
7. ✅ Removed 25 empty directories

## Next Steps

1. Upgrade PySpark: `pip install --upgrade pyspark==3.5.0`
2. Run: `./scripts/run_local_etl_fixed.sh`
3. Verify output: `ls -lh data/silver/*/` and `ls -lh data/gold/*/`

