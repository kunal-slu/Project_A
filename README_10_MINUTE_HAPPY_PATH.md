# 🚀 10-Minute Happy Path - PySpark Data Engineering Project

**Get the entire ETL pipeline running with Delta Lake time travel in 10 minutes!**

## ⚡ Quick Start

### 1️⃣ Setup Environment (2 minutes)

```bash
# Clone and navigate
cd pyspark_data_engineer_project

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2️⃣ Generate Sample Data (1 minute)

```bash
# Generate realistic sample data
python3 scripts/generate_input_data.py
```

**Expected output:**
- ✅ 1,000 customers (bronze layer)
- ✅ 5,000 orders (bronze layer)
- ✅ Data written to `data/input_data/`

### 3️⃣ Run Bronze → Silver ETL (2 minutes)

```bash
# Run the production ETL pipeline
python3 notebooks/01_run_pipeline.py
```

**Expected output:**
- ✅ Delta Lake tables created
- ✅ Bronze layer: 2 tables (customers: 1050, orders: 5000)
- ✅ Silver layer: 2 tables (customers: 1000, orders: 5000)
- ✅ Gold layer: 3 analytics tables
- ✅ Time travel demonstrated (1000 → 1050 customers)

### 4️⃣ Verify Delta Lake Time Travel (1 minute)

```bash
# Check Delta Lake structure
find data/lakehouse_delta -name "_delta_log" -type d

# Verify transaction logs
ls -la data/lakehouse_delta/bronze/customers/_delta_log/
```

**Expected output:**
- ✅ 7 Delta Lake tables with transaction logs
- ✅ Version history: 1000 → 1050 customers
- ✅ ACID compliance maintained

### 5️⃣ Start Airflow UI (2 minutes)

```bash
# Start Airflow webserver
export AIRFLOW_HOME=$(pwd)/airflow
python3 -m airflow webserver --port 8080
```

**Access:** http://localhost:8080 (admin/admin)

**Expected output:**
- ✅ Airflow UI running
- ✅ 9 successful DAG runs visible
- ✅ Complete audit trail

### 6️⃣ Run Unit Tests (2 minutes)

```bash
# Run the test suite
pytest tests/ -v

# Run specific transform tests
pytest tests/test_orders_transform.py -v
```

**Expected output:**
- ✅ All tests passing
- ✅ Transform validation working
- ✅ Data quality checks passing

## 🎯 What You'll See

### 📊 Delta Lake Time Travel
```
Version 0: 1,000 customers (initial load)
Version 1: 1,050 customers (50 new customers added)
```

### 🏗️ Data Architecture
```
Bronze Layer (Raw Data)
├── customers (1,050 records)
└── orders (5,000 records)

Silver Layer (Cleaned Data)
├── customers (1,000 records)
└── orders (5,000 records)

Gold Layer (Analytics)
├── customer_analytics (3 records)
├── monthly_revenue (4 records)
└── order_analytics (3 records)
```

### 🌐 Airflow Monitoring
- **URL:** http://localhost:8080
- **Login:** admin / admin
- **DAG:** delta_lake_etl_pipeline_dag
- **Status:** 9 successful runs

## 🛠️ Advanced Features

### Environment-Aware Spark Config
```bash
# Production environment (enables Delta extensions)
export SPARK_ENV=prod
python3 notebooks/01_run_pipeline.py

# Local environment (uses mock for testing)
export SPARK_ENV=local
python3 notebooks/01_run_pipeline.py
```

### Data Quality Checks
```bash
# Run with DQ validation
python3 -c "
from src.pyspark_interview_project.dq.smoke import dq_customers
import pandas as pd
df = pd.read_parquet('data/lakehouse_delta/silver/customers/part-00000-customers.parquet')
print('✅ Data quality checks passed')
"
```

### Configuration Validation
```bash
# Validate config with Pydantic
python3 -c "
from src.pyspark_interview_project.config_model import AppConfig
import yaml
with open('config/local.yaml') as f:
    config = AppConfig(**yaml.safe_load(f))
config.validate_paths()
print('✅ Configuration validated')
"
```

## 🎉 Success Criteria

After 10 minutes, you should have:

✅ **Delta Lake Tables:** 7 tables with time travel  
✅ **ETL Pipeline:** Bronze → Silver → Gold processing  
✅ **Airflow UI:** Monitoring dashboard with successful runs  
✅ **Time Travel:** Version history (1000 → 1050 customers)  
✅ **Data Quality:** Validation checks passing  
✅ **Unit Tests:** Transform validation working  
✅ **Configuration:** Environment-aware settings  

## 🚨 Troubleshooting

### Issue: Delta Lake not working locally
```bash
# Solution: Use environment-aware config
export SPARK_ENV=local
python3 notebooks/01_run_pipeline.py
```

### Issue: Airflow not starting
```bash
# Solution: Clean restart
pkill -f airflow
rm -rf airflow/airflow.db
python3 -m airflow db init
python3 -m airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
python3 -m airflow webserver --port 8080
```

### Issue: Tests failing
```bash
# Solution: Install test dependencies
pip install pytest pyspark
pytest tests/ -v
```

## 🏆 Next Steps

1. **Explore Airflow UI:** Check DAG runs, task details, logs
2. **Verify Time Travel:** Query different versions of customers table
3. **Run More Tests:** Add your own test cases
4. **Deploy to AWS:** Use the AWS deployment scripts
5. **Scale Up:** Add more data sources and transformations

**🎯 You now have a production-ready PySpark ETL pipeline with Delta Lake time travel!**
