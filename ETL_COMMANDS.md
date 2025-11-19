# ETL Run Commands

## 🚀 Quick Start

### Option 1: Run Bronze → Silver (Main ETL)

```bash
cd /Users/kunal/IdeaProjects/Project_A

# Set PYTHONPATH
export PYTHONPATH=./src:$PYTHONPATH

# Run Bronze → Silver transformation
python3 jobs/transform/bronze_to_silver.py \
  --env local \
  --config local/config/local.yaml
```

### Option 2: Run Silver → Gold

```bash
cd /Users/kunal/IdeaProjects/Project_A

# Set PYTHONPATH
export PYTHONPATH=./src:$PYTHONPATH

# Run Silver → Gold transformation
python3 jobs/transform/silver_to_gold.py \
  --env local \
  --config local/config/local.yaml
```

### Option 3: Run Full Pipeline (Bronze → Silver → Gold)

```bash
cd /Users/kunal/IdeaProjects/Project_A

# Use the safe runner script
PYTHONPATH=./src:$PYTHONPATH python3 local/scripts/run_local_etl_safe.py
```

### Option 4: Run Using Unified Entry Point (Future)

```bash
cd /Users/kunal/IdeaProjects/Project_A

# Bronze → Silver
PYTHONPATH=./src:$PYTHONPATH python3 jobs/run_pipeline.py \
  --job bronze_to_silver \
  --env local \
  --config local/config/local.yaml

# Silver → Gold
PYTHONPATH=./src:$PYTHONPATH python3 jobs/run_pipeline.py \
  --job silver_to_gold \
  --env local \
  --config local/config/local.yaml
```

---

## ☁️ AWS EMR Commands

### Run on EMR Serverless

```bash
export AWS_PROFILE=kunal21
export AWS_REGION=us-east-1
export ARTIFACTS_BUCKET=my-etl-artifacts-demo-424570854632
export LAKE_BUCKET=my-etl-lake-demo-424570854632
export EMR_APP_ID=00g0tm6kccmdcf09
export EMR_ROLE_ARN=arn:aws:iam::424570854632:role/project-a-dev-emr-exec

# Bronze → Silver
aws emr-serverless start-job-run \
  --application-id "$EMR_APP_ID" \
  --execution-role-arn "$EMR_ROLE_ARN" \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"s3://${ARTIFACTS_BUCKET}/jobs/transform/bronze_to_silver.py\",
      \"entryPointArguments\": [
        \"--env\", \"dev\",
        \"--config\", \"s3://${ARTIFACTS_BUCKET}/config/dev.yaml\"
      ],
      \"sparkSubmitParameters\": \"--conf spark.executor.instances=1 --conf spark.executor.cores=1 --conf spark.executor.memory=3G --conf spark.driver.memory=2G --conf spark.pyspark.python=python3 --conf spark.pyspark.driver.python=python3 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --py-files s3://${ARTIFACTS_BUCKET}/packages/project_a-0.1.0-py3-none-any.whl,s3://${ARTIFACTS_BUCKET}/packages/emr_deps_pyyaml.zip --jars s3://${ARTIFACTS_BUCKET}/packages/delta-spark_2.12-3.2.0.jar,s3://${ARTIFACTS_BUCKET}/packages/delta-storage-3.2.0.jar\"
    }
  }" \
  --configuration-overrides "{
    \"monitoringConfiguration\": {
      \"s3MonitoringConfiguration\": {
        \"logUri\": \"s3://${ARTIFACTS_BUCKET}/logs/\"
      }
    }
  }"
```

### Run on EMR on EC2

```bash
export EMR_CLUSTER_ID=j-XXXXXXXXXXXXX  # Your cluster ID
export ARTIFACTS_BUCKET=my-etl-artifacts-demo-424570854632

# Bronze → Silver
aws emr add-steps \
  --cluster-id "$EMR_CLUSTER_ID" \
  --steps file://steps_bronze_to_silver.json \
  --profile kunal21 \
  --region us-east-1

# Silver → Gold
aws emr add-steps \
  --cluster-id "$EMR_CLUSTER_ID" \
  --steps file://steps_silver_to_gold.json \
  --profile kunal21 \
  --region us-east-1
```

---

## ⚠️ Local Spark Issues (Java 17 Compatibility)

**Problem**: PySpark has compatibility issues with Java 17. You'll see:
```
Py4JError: Constructor org.apache.spark.sql.SparkSession(...) does not exist
```

### Solution 1: Use Java 11 (Quick Fix)

```bash
# Set Java 11
export JAVA_HOME=$(/usr/libexec/java_home -v 11)

# Verify
java -version  # Should show Java 11

# Run ETL
cd /Users/kunal/IdeaProjects/Project_A
export PYTHONPATH=./src:$PYTHONPATH
python3 jobs/transform/bronze_to_silver.py --env local --config local/config/local.yaml
```

**Or use the helper script:**
```bash
./scripts/run_etl_with_java11.sh
```

### Solution 2: Install Java 11 (if not installed)

```bash
# Install Java 11 via Homebrew
brew install openjdk@11

# Set JAVA_HOME
export JAVA_HOME=$(/usr/libexec/java_home -v 11)

# Add to ~/.zshrc for persistence
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 11)' >> ~/.zshrc
```

### Solution 3: Use AWS EMR (Recommended)

- Both local and AWS use the same S3 data
- EMR has proper Spark/Java compatibility
- No local environment setup needed
- See AWS EMR commands below

### Solution 4: Check S3 Data Directly (No Spark)

```bash
python3 local/scripts/dq/check_s3_data_quality.py --layer all
```

---

## 📊 Verify Results

### Check Local Output

```bash
# Check Silver layer
ls -lh data/silver/*/

# Check Gold layer
ls -lh data/gold/*/
```

### Check S3 Output

```bash
export LAKE_BUCKET=my-etl-lake-demo-424570854632

# List Silver tables
aws s3 ls "s3://${LAKE_BUCKET}/silver/" --recursive --profile kunal21

# List Gold tables
aws s3 ls "s3://${LAKE_BUCKET}/gold/" --recursive --profile kunal21
```

---

## 🔧 Troubleshooting

### ModuleNotFoundError
```bash
export PYTHONPATH=./src:$PYTHONPATH
```

### SparkSession errors
- Use AWS EMR instead
- Or fix Java version: `export JAVA_HOME=$(/usr/libexec/java_home -v 11)`

### Config file not found
- Ensure config path is correct: `local/config/local.yaml`
- Or use S3 path: `s3://bucket/config/dev.yaml`

