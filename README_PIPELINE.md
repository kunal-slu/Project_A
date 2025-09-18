# PySpark Data Engineer Project - Pipeline Components

This document describes the new pipeline components that have been integrated into the project.

## Overview

The project now includes a complete ETL pipeline for processing `returns_raw` data from bronze → silver → gold layers, with comprehensive orchestration, monitoring, and infrastructure support.

## New Components

### 1. ETL Pipeline Modules

#### `src/pyspark_interview_project/pipeline/bronze_to_silver.py`
- **Purpose**: Transforms raw returns data from bronze to silver layer
- **Features**: 
  - Schema validation using `returns_raw_schema`
  - Data quality checks (trimming, null/negative value handling)
  - Partitioning by `return_date`
  - Metrics collection and logging

#### `src/pyspark_interview_project/pipeline/silver_to_gold.py`
- **Purpose**: Transforms clean silver data to gold metrics
- **Features**:
  - Aggregation by SKU and return reason
  - Calculation of total amounts and return counts
  - Metrics collection and logging

#### `src/pyspark_interview_project/pipeline/run_pipeline.py`
- **Purpose**: Main orchestrator for the complete pipeline
- **Features**:
  - Runs bronze → silver → gold pipeline
  - Optional disaster recovery execution
  - Optional metrics JSON ingestion
  - Metrics server startup

### 2. Infrastructure and Configuration

#### `src/pyspark_interview_project/config/paths.py`
- **Purpose**: Centralized path configuration
- **Features**:
  - Environment-variable-driven paths
  - Data lakehouse layer paths (Bronze, Silver, Gold)
  - Metrics and DR configuration paths

#### `src/pyspark_interview_project/logging_setup.py`
- **Purpose**: Structured logging with JSON formatting
- **Features**:
  - JSON log formatter for machine readability
  - Correlation ID tracking
  - Environment-aware logging

#### `src/pyspark_interview_project/schemas/returns_raw.py`
- **Purpose**: Explicit PySpark schema definition
- **Features**:
  - Type-safe schema validation
  - Nullability specifications
  - Consistent data structure enforcement

### 3. Monitoring and Metrics

#### `src/pyspark_interview_project/metrics/metrics_exporter.py`
- **Purpose**: Prometheus metrics export
- **Features**:
  - Pipeline execution counters
  - Row processing metrics
  - Stage duration tracking
  - HTTP metrics server

#### `src/pyspark_interview_project/metrics/ingest_pipeline_metrics.py`
- **Purpose**: Parse and ingest pipeline metrics JSON
- **Features**:
  - Reads `pipeline_metrics.json`
  - Updates Prometheus metrics
  - Supports historical metrics ingestion

### 4. Disaster Recovery

#### `src/pyspark_interview_project/dr/dr_runner.py`
- **Purpose**: Execute backup and replication strategies
- **Features**:
  - Reads backup strategy configurations
  - Simulates replication operations
  - Extensible for cloud SDK integration

### 5. Orchestration

#### `dags/returns_pipeline_dag.py`
- **Purpose**: Airflow DAG for pipeline orchestration
- **Features**:
  - Daily scheduled execution
  - Task dependencies (bronze → silver → gold → finalize)
  - Environment variable configuration

#### `databricks/returns_pipeline_job.json`
- **Purpose**: Databricks job definition
- **Features**:
  - Spark Python task definitions
  - Task dependencies
  - Parameter passing

### 6. Infrastructure as Code

#### `infra/terraform/`
- **Purpose**: AWS S3 infrastructure provisioning
- **Features**:
  - S3 bucket creation for data lakehouse
  - Configurable region and bucket names
  - Terraform 1.5+ compatibility

### 7. CI/CD and Development

#### `.github/workflows/ci.yml`
- **Purpose**: GitHub Actions CI pipeline
- **Features**:
  - Linting with flake8
  - Testing with pytest
  - Docker image building

#### `.pre-commit-config.yaml`
- **Purpose**: Pre-commit hooks for code quality
- **Features**:
  - Black code formatting
  - isort import sorting
  - flake8 linting
  - detect-secrets security scanning

#### `Makefile`
- **Purpose**: Development workflow automation
- **Features**:
  - Dependency installation
  - Code formatting
  - Testing and linting
  - Pipeline execution

#### `Dockerfile`
- **Purpose**: Containerized application deployment
- **Features**:
  - Non-root user security
  - Health check endpoint
  - Optimized Python environment

## Usage

### Running the Pipeline Locally

```bash
# Install dependencies
make install

# Run the complete pipeline
make run

# Or run manually
APP_ENV=dev python -m src.pipeline.run_pipeline --ingest-metrics-json --with-dr
```

### Running Individual Stages

```bash
# Bronze to Silver
python -m src.pipeline.bronze_to_silver

# Silver to Gold
python -m src.pipeline.silver_to_gold
```

### Development Workflow

```bash
# Install pre-commit hooks
make hooks

# Format code
make fmt

# Run tests
make test

# Lint code
make lint
```

### Infrastructure Deployment

```bash
# Deploy S3 infrastructure
cd infra/terraform
terraform init
terraform plan
terraform apply
```

## Configuration

### Environment Variables

- `DATA_ROOT`: Base path for data lakehouse (default: `/mnt/data/data_extracted/data/lakehouse`)
- `APP_ENV`: Application environment (dev/test/prod)
- `SPARK_TZ`: Spark timezone (default: UTC)
- `LOG_LEVEL`: Logging level (default: INFO)

### Metrics Endpoint

The pipeline exposes Prometheus metrics at `/metrics` on port 8000 (or next available port).

## Testing

Run the new pipeline component tests:

```bash
pytest tests/test_pipeline_components.py -v
```

## Dependencies

New dependencies added:
- `prometheus-client`: Metrics export
- `black`, `isort`, `flake8`: Code quality
- `detect-secrets`: Security scanning
- `apache-airflow`: Orchestration (optional)

## Next Steps

1. **Integration Testing**: Test the complete pipeline with real data
2. **Cloud Integration**: Replace DR simulation with actual cloud SDK calls
3. **Monitoring Dashboard**: Create Grafana dashboards for metrics visualization
4. **Alerting**: Set up Prometheus alerting rules
5. **Performance Tuning**: Optimize based on actual data volumes and patterns

## Architecture Benefits

- **Modularity**: Each component has a single responsibility
- **Testability**: Comprehensive test coverage for all components
- **Observability**: Structured logging and metrics collection
- **Scalability**: Designed for horizontal scaling
- **Security**: Non-root containers and secret management
- **DevOps**: Automated CI/CD and infrastructure provisioning
