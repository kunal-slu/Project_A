# 🛠️ Production Hardening Fixes - Senior to Lead Level

## 🚨 **CRITICAL FIXES (Fix First)**

### 1. **Security - Remove Hard-coded Secrets**

**Problem**: Hard-coded passwords/tokens in config files
**Solution**: Use environment variables and secret management

```yaml
# config/config-dev.yaml (FIXED)
azure:
  storage:
    account_name: "${ENV:AZURE_STORAGE_ACCOUNT}"
    account_key: "${SECRET:azure:storage_key}"
  databricks:
    workspace_url: "${ENV:DATABRICKS_WORKSPACE_URL}"
    token: "${SECRET:databricks:token}"
```

### 2. **Path Externalization**

**Problem**: Hard-coded `/mnt/` paths
**Solution**: Use environment variables

```python
# src/pyspark_interview_project/config/paths.py (FIXED)
DATA_ROOT = Path(os.getenv("DATA_ROOT", "data/lakehouse"))
METRICS_FILE = Path(os.getenv("METRICS_PATH", "data/metrics/pipeline_metrics.json"))
DR_ROOT = Path(os.getenv("DR_ROOT", "data/dr"))
```

### 3. **Replace print() with Structured Logging**

**Problem**: `print()` statements in runtime code
**Solution**: Use structured logging

```python
# FIXED - Replace all print() with logger
import logging
logger = logging.getLogger(__name__)

# Instead of: print("Pipeline completed")
logger.info("Pipeline completed successfully")
```

## 🔧 **CI/CD Pipeline Implementation**

### 1. **GitHub Actions Workflow**

```yaml
# .github/workflows/production-pipeline.yml
name: Production CI/CD Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: |
          pip install -r requirements-dev.txt
      - name: Run linting
        run: |
          flake8 src/ tests/
          black --check src/ tests/
          isort --check-only src/ tests/

  test:
    needs: lint
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: |
          pip install -r requirements-dev.txt
      - name: Run tests
        run: |
          pytest tests/ -v --cov=src --cov-report=xml
      - name: Upload coverage
        uses: codecov/codecov-action@v3

  build:
    needs: test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Build Docker image
        run: |
          docker build -t pyspark-etl:latest .
      - name: Push to registry
        run: |
          echo ${{ secrets.DOCKER_PASSWORD }} | docker login -u ${{ secrets.DOCKER_USERNAME }} --password-stdin
          docker tag pyspark-etl:latest ${{ secrets.DOCKER_REGISTRY }}/pyspark-etl:latest
          docker push ${{ secrets.DOCKER_REGISTRY }}/pyspark-etl:latest
```

## 🎯 **Orchestration - Airflow DAG**

```python
# dags/production_etl_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'production_etl_pipeline',
    default_args=default_args,
    description='Production ETL Pipeline - Bronze to Gold',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['etl', 'production', 'lakehouse'],
)

# Bronze Layer
bronze_task = DatabricksSubmitRunOperator(
    task_id='bronze_layer',
    databricks_conn_id='databricks_default',
    existing_cluster_id='{{ var.value.databricks_cluster_id }}',
    notebook_task={
        'notebook_path': '/Shared/ETL/bronze_layer',
        'base_parameters': {
            'data_date': '{{ ds }}',
            'environment': 'production'
        }
    },
    dag=dag,
)

# Silver Layer
silver_task = DatabricksSubmitRunOperator(
    task_id='silver_layer',
    databricks_conn_id='databricks_default',
    existing_cluster_id='{{ var.value.databricks_cluster_id }}',
    notebook_task={
        'notebook_path': '/Shared/ETL/silver_layer',
        'base_parameters': {
            'data_date': '{{ ds }}',
            'environment': 'production'
        }
    },
    dag=dag,
)

# Gold Layer
gold_task = DatabricksSubmitRunOperator(
    task_id='gold_layer',
    databricks_conn_id='databricks_default',
    existing_cluster_id='{{ var.value.databricks_cluster_id }}',
    notebook_task={
        'notebook_path': '/Shared/ETL/gold_layer',
        'base_parameters': {
            'data_date': '{{ ds }}',
            'environment': 'production'
        }
    },
    dag=dag,
)

# Data Quality Check
dq_task = DatabricksSubmitRunOperator(
    task_id='data_quality_check',
    databricks_conn_id='databricks_default',
    existing_cluster_id='{{ var.value.databricks_cluster_id }}',
    notebook_task={
        'notebook_path': '/Shared/ETL/data_quality_check',
        'base_parameters': {
            'data_date': '{{ ds }}',
            'environment': 'production'
        }
    },
    dag=dag,
)

# Dependencies
bronze_task >> silver_task >> gold_task >> dq_task
```

## 📊 **Observability - Prometheus/OpenTelemetry**

```python
# src/pyspark_interview_project/observability.py
import logging
from prometheus_client import Counter, Histogram, Gauge, generate_latest
from opentelemetry import trace, metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from flask import Flask, Response

logger = logging.getLogger(__name__)

class ObservabilityManager:
    def __init__(self):
        # Prometheus metrics
        self.pipeline_runs = Counter('pipeline_runs_total', 'Total pipeline runs')
        self.pipeline_duration = Histogram('pipeline_duration_seconds', 'Pipeline duration')
        self.records_processed = Counter('records_processed_total', 'Total records processed')
        self.errors_total = Counter('errors_total', 'Total errors')
        
        # OpenTelemetry setup
        self.tracer = trace.get_tracer(__name__)
        self.meter = metrics.get_meter(__name__)
        
        # Flask app for metrics endpoint
        self.app = Flask(__name__)
        FlaskInstrumentor().instrument_app(self.app)
        
        @self.app.route('/metrics')
        def metrics():
            return Response(generate_latest(), mimetype='text/plain')
    
    def record_pipeline_run(self, duration_seconds, records_processed):
        """Record pipeline metrics"""
        self.pipeline_runs.inc()
        self.pipeline_duration.observe(duration_seconds)
        self.records_processed.inc(records_processed)
    
    def record_error(self, error_type):
        """Record error metrics"""
        self.errors_total.labels(error_type=error_type).inc()
    
    def start_span(self, name):
        """Start OpenTelemetry span"""
        return self.tracer.start_span(name)
```

## 🔍 **Data Quality - Great Expectations**

```python
# src/pyspark_interview_project/data_quality_gx.py
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.resource_identifiers import GeCloudIdentifier

class GreatExpectationsManager:
    def __init__(self, context_root_dir="gx"):
        self.context = gx.get_context(context_root_dir=context_root_dir)
    
    def validate_schema(self, df, table_name):
        """Validate DataFrame schema against expectations"""
        batch_request = RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name=table_name,
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": "default_identifier"},
        )
        
        # Define expectations
        expectations = [
            {
                "expectation_type": "expect_table_columns_to_match_set",
                "kwargs": {
                    "column_set": df.columns.tolist()
                }
            },
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {
                    "min_value": 1,
                    "max_value": 1000000
                }
            }
        ]
        
        # Validate
        results = self.context.run_validation_operator(
            "action_list_operator",
            assets_to_validate=[batch_request],
            expectation_suite_name=f"{table_name}_suite"
        )
        
        return results
    
    def validate_data_rules(self, df, table_name):
        """Validate data quality rules"""
        # Customer-specific rules
        if table_name == "customers":
            return self._validate_customers(df)
        elif table_name == "orders":
            return self._validate_orders(df)
        else:
            return {"status": "no_rules_defined"}
    
    def _validate_customers(self, df):
        """Customer-specific validation rules"""
        rules = [
            {
                "name": "email_format",
                "expectation": "expect_column_values_to_match_regex",
                "kwargs": {
                    "column": "email",
                    "regex": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                }
            },
            {
                "name": "customer_id_not_null",
                "expectation": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "customer_id"}
            }
        ]
        
        results = []
        for rule in rules:
            result = self.context.run_validation_operator(
                "action_list_operator",
                assets_to_validate=[df],
                expectation_suite_name=f"customers_{rule['name']}"
            )
            results.append(result)
        
        return results
```

## 🔗 **Lineage - OpenLineage Integration**

```python
# src/pyspark_interview_project/lineage.py
from openlineage.client import OpenLineageClient, RunEvent, RunState, Run
from openlineage.client.facet import DocumentationJobFacet, SourceCodeLocationJobFacet
from openlineage.client.run import Job, RunEventType
import uuid
from datetime import datetime

class LineageTracker:
    def __init__(self, namespace="pyspark-etl", producer="https://github.com/your-org/pyspark-etl"):
        self.client = OpenLineageClient()
        self.namespace = namespace
        self.producer = producer
    
    def track_pipeline_start(self, pipeline_name, inputs=None, outputs=None):
        """Track pipeline start event"""
        run_id = str(uuid.uuid4())
        
        job = Job(
            namespace=self.namespace,
            name=pipeline_name,
            facets={
                "documentation": DocumentationJobFacet(
                    description=f"ETL pipeline for {pipeline_name}"
                ),
                "sourceCodeLocation": SourceCodeLocationJobFacet(
                    type="git",
                    url="https://github.com/your-org/pyspark-etl",
                    path="src/pyspark_interview_project/pipeline.py"
                )
            }
        )
        
        run = Run(
            id=run_id,
            facets={}
        )
        
        event = RunEvent(
            eventType=RunEventType.START,
            eventTime=datetime.utcnow(),
            run=run,
            job=job,
            producer=self.producer,
            inputs=inputs or [],
            outputs=outputs or []
        )
        
        self.client.emit(event)
        return run_id
    
    def track_pipeline_complete(self, run_id, pipeline_name, inputs=None, outputs=None):
        """Track pipeline completion event"""
        job = Job(
            namespace=self.namespace,
            name=pipeline_name
        )
        
        run = Run(
            id=run_id,
            facets={}
        )
        
        event = RunEvent(
            eventType=RunEventType.COMPLETE,
            eventTime=datetime.utcnow(),
            run=run,
            job=job,
            producer=self.producer,
            inputs=inputs or [],
            outputs=outputs or []
        )
        
        self.client.emit(event)
```

## 🏗️ **Infrastructure as Code - Terraform**

```hcl
# infra/main.tf
terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

# Resource Group
resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
}

# Storage Account
resource "azurerm_storage_account" "storage" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  
  blob_properties {
    versioning_enabled = true
    delete_retention_policy {
      days = 30
    }
  }
}

# Data Lake Storage Gen2
resource "azurerm_storage_data_lake_gen2_filesystem" "lakehouse" {
  name               = "lakehouse"
  storage_account_id = azurerm_storage_account.storage.id
}

# Databricks Workspace
resource "azurerm_databricks_workspace" "workspace" {
  name                = var.databricks_workspace_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "standard"
}

# Key Vault
resource "azurerm_key_vault" "vault" {
  name                        = var.key_vault_name
  location                    = azurerm_resource_group.rg.location
  resource_group_name         = azurerm_resource_group.rg.name
  enabled_for_disk_encryption = true
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  soft_delete_retention_days  = 7
  purge_protection_enabled    = false
  sku_name                   = "standard"
}

# Key Vault Access Policy
resource "azurerm_key_vault_access_policy" "terraform" {
  key_vault_id = azurerm_key_vault.vault.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = data.azurerm_client_config.current.object_id

  key_permissions = [
    "Get", "List", "Create", "Delete", "Update"
  ]

  secret_permissions = [
    "Get", "List", "Set", "Delete"
  ]
}
```

## 🐳 **Docker Hardening**

```dockerfile
# Dockerfile (HARDENED)
FROM openjdk:11-jre-slim

# Create non-root user
RUN groupadd -r spark && useradd -r -g spark spark

# Install Python and dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY config/ ./config/

# Create necessary directories
RUN mkdir -p /app/data /app/logs /app/tmp

# Change ownership to non-root user
RUN chown -R spark:spark /app

# Switch to non-root user
USER spark

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python3 -c "import requests; requests.get('http://localhost:8080/health')" || exit 1

# Expose ports
EXPOSE 8080

# Set environment variables
ENV PYTHONPATH=/app/src
ENV SPARK_HOME=/opt/spark
ENV DATA_ROOT=/app/data

# Default command
CMD ["python3", "-m", "pyspark_interview_project"]
```

## 📈 **Performance Benchmarking**

```python
# src/pyspark_interview_project/benchmark.py
import time
import psutil
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

class PerformanceBenchmark:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.results = {}
    
    def benchmark_pipeline(self, pipeline_func, *args, **kwargs) -> Dict[str, Any]:
        """Benchmark a pipeline function"""
        start_time = time.time()
        start_memory = psutil.virtual_memory().used
        
        try:
            result = pipeline_func(*args, **kwargs)
            success = True
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            result = None
            success = False
        
        end_time = time.time()
        end_memory = psutil.virtual_memory().used
        
        duration = end_time - start_time
        memory_used = end_memory - start_memory
        
        benchmark_result = {
            "duration_seconds": duration,
            "memory_used_mb": memory_used / (1024 * 1024),
            "success": success,
            "timestamp": time.time()
        }
        
        self.results[pipeline_func.__name__] = benchmark_result
        return benchmark_result
    
    def benchmark_scale_test(self, data_sizes: list) -> Dict[str, Any]:
        """Benchmark pipeline at different data sizes"""
        scale_results = {}
        
        for size in data_sizes:
            logger.info(f"Benchmarking with {size} records")
            
            # Generate test data
            test_data = self._generate_test_data(size)
            
            # Benchmark
            result = self.benchmark_pipeline(self._test_pipeline, test_data)
            scale_results[size] = result
        
        return scale_results
    
    def generate_report(self) -> str:
        """Generate benchmark report"""
        report = "Performance Benchmark Report\n"
        report += "=" * 40 + "\n\n"
        
        for pipeline_name, result in self.results.items():
            report += f"Pipeline: {pipeline_name}\n"
            report += f"  Duration: {result['duration_seconds']:.2f}s\n"
            report += f"  Memory: {result['memory_used_mb']:.2f}MB\n"
            report += f"  Success: {result['success']}\n\n"
        
        return report
```

## 🚀 **Implementation Priority**

1. **Week 1**: Security fixes (secrets, paths, logging)
2. **Week 2**: CI/CD pipeline implementation
3. **Week 3**: Orchestration (Airflow DAGs)
4. **Week 4**: Observability (Prometheus/OpenTelemetry)
5. **Week 5**: Data Quality (Great Expectations)
6. **Week 6**: Lineage (OpenLineage)
7. **Week 7**: Infrastructure (Terraform)
8. **Week 8**: Docker hardening and performance benchmarking

This roadmap will transform the project from Senior-level to Lead-level production readiness! 🎯
