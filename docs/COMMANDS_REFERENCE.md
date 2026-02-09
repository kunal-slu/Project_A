# 🚀 Quick Command Reference

## Essential Commands

### **1. Run ETL Pipeline**
```bash
# Full pipeline
python run_complete_etl.py --config local/config/local.yaml --env local --with-validation

# Individual stages
python -m project_a.pipeline.run_pipeline --job fx_json_to_bronze --env local --config local/config/local.yaml
python -m project_a.pipeline.run_pipeline --job bronze_to_silver --env local --config local/config/local.yaml
python -m project_a.pipeline.run_pipeline --job silver_to_gold --env local --config local/config/local.yaml
python -m project_a.pipeline.run_pipeline --job gold_truth_tests --env local --config local/config/local.yaml
```

### **2. Start Monitoring Stack**
```bash
# Start all monitoring services
docker-compose -f docker-compose-monitoring.yml up -d

# Stop services
docker-compose -f docker-compose-monitoring.yml down

# View logs
docker-compose -f docker-compose-monitoring.yml logs -f

# Access UIs:
# Grafana:        http://localhost:3000 (admin/admin)
# Prometheus:     http://localhost:9090
# Alertmanager:   http://localhost:9093
# Pushgateway:    http://localhost:9091
```

### **3. Run Tests**
```bash
# All tests with coverage
pytest tests/ --cov=src/project_a --cov-report=html --cov-report=term-missing -v

# Specific test file
pytest tests/test_safe_writer.py -v
pytest tests/test_schemas.py -v
pytest tests/test_monitoring.py -v

# View coverage report
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

### **4. Verify Setup**
```bash
# Check Delta Lake tables
ls -la data/lakehouse_delta/bronze/
ls -la data/lakehouse_delta/silver/
ls -la data/lakehouse_delta/gold/

# Count versions
find data/lakehouse_delta -name "_delta_log" -type d | wc -l

# Check monitoring stack
docker-compose -f docker-compose-monitoring.yml ps
```

---

## 📊 Monitoring Commands

### **Emit Metrics (Example)**
```python
from project_a.monitoring.metrics_collector import emit_metrics

emit_metrics("my_etl_job", rows_in=100, rows_out=95, duration_seconds=3.2, dq_status="pass", config={})
```

### **Query Prometheus**
```bash
# Via API
curl http://localhost:9090/api/v1/query?query=etl_job_executions_total

# View all metrics
curl http://localhost:9090/api/v1/label/__name__/values | jq .
```

---

## 🧪 Testing Commands

### **Run with different options**
```bash
# Fast (skip slow tests)
pytest tests/ -m "not slow" -v

# Parallel execution
pytest tests/ -n auto -v

# Stop on first failure
pytest tests/ -x -v

# Show local variables on failure
pytest tests/ -l -v

# Specific test
pytest tests/test_safe_writer.py::TestSafeDeltaWriter::test_write_merge_upsert -v
```

---

## 🐳 Docker Commands

### **Production Compose**
```bash
# Start
docker-compose -f docker-compose-production.yml up -d

# Stop
docker-compose -f docker-compose-production.yml down

# Rebuild
docker-compose -f docker-compose-production.yml build --no-cache

# View logs
docker-compose -f docker-compose-production.yml logs -f airflow-scheduler
```

### **Build Custom Image**
```bash
# Build
docker build -t pyspark-etl:latest .

# Run
docker run -it --rm \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/config:/app/config \
  pyspark-etl:latest

# Push to registry
docker tag pyspark-etl:latest yourusername/pyspark-etl:latest
docker push yourusername/pyspark-etl:latest
```

---

## ☸️ Kubernetes Commands

### **Deploy**
```bash
# Apply all manifests
kubectl apply -f k8s/base/

# Check status
kubectl get all -n pyspark-etl

# View logs
kubectl logs -f deployment/airflow-scheduler -n pyspark-etl
kubectl logs -f deployment/airflow-webserver -n pyspark-etl

# Port forward
kubectl port-forward svc/airflow-webserver 8080:8080 -n pyspark-etl
kubectl port-forward svc/grafana 3000:3000 -n pyspark-etl
```

### **Delete**
```bash
# Delete all resources
kubectl delete -f k8s/base/

# Delete namespace
kubectl delete namespace pyspark-etl
```

---

## 📦 Dependency Management

### **Install**
```bash
# Install all dependencies
pip install -r requirements.txt

# Install dev dependencies
pip install -r requirements-dev.txt

# Install with monitoring
pip install -r requirements.txt prometheus-client psutil
```

### **Upgrade**
```bash
# Upgrade specific package
pip install --upgrade pyspark

# Upgrade all
pip install --upgrade -r requirements.txt

# Check outdated
pip list --outdated
```

---

## 🔍 Debugging Commands

### **Check Delta Lake**
```python
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("Debug").getOrCreate()

# Read table
df = spark.read.format("delta").load("data/lakehouse_delta/bronze/customers")
df.show()

# Check history
history = DeltaTable.forPath(spark, "data/lakehouse_delta/bronze/customers").history()
history.show()

# Check version
version_df = spark.read.format("delta").option("versionAsOf", 0).load("data/lakehouse_delta/bronze/customers")
version_df.show()
```

### **Check Metrics**
```bash
# Get current metrics
curl http://localhost:9091/metrics

# Query specific metric
curl "http://localhost:9090/api/v1/query?query=etl_job_executions_total"
```

---

## 🧹 Cleanup Commands

### **Clean Data**
```bash
# Remove all Delta Lake data
rm -rf data/lakehouse_delta/*

# Remove old versions (keep last 30 days)
# Run this in Spark
# VACUUM delta.`data/lakehouse_delta/bronze/customers` RETAIN 720 HOURS
```

### **Clean Docker**
```bash
# Remove all stopped containers
docker container prune -f

# Remove all unused images
docker image prune -a -f

# Remove all volumes
docker volume prune -f

# Complete cleanup
docker system prune -a -f --volumes
```

---

## 📝 Git Commands

### **Commit Changes**
```bash
# Stage all changes
git add .

# Commit
git commit -m "feat: add monitoring and CI/CD"

# Push
git push origin main

# Create feature branch
git checkout -b feature/new-feature
git push -u origin feature/new-feature
```

---

## 🔧 Environment Setup

### **Set Environment Variables**
```bash
# Add to ~/.bashrc or ~/.zshrc
export PYTHONPATH="$(pwd)/src:$PYTHONPATH"
export AIRFLOW_HOME="$HOME/.airflow_local"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# Reload
source ~/.bashrc  # or source ~/.zshrc
```

### **Activate Virtual Environment**
```bash
# Create venv
python3 -m venv venv

# Activate
source venv/bin/activate  # macOS/Linux
venv\Scripts\activate  # Windows

# Deactivate
deactivate
```

---

## 🎯 Quick Verification

### **End-to-End Test**
```bash
# 1. Run ETL (local)
python run_complete_etl.py --config local/config/local.yaml --env local --with-validation

# 2. Verify outputs
ls -la data/bronze/
ls -la data/silver/
ls -la data/gold/

# 3. Check gold truth report
ls -la artifacts/run_audit/
```

---

**Last Updated**: 2025-10-18
