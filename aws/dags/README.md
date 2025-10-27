# Airflow DAGs Documentation

## Production DAGs (`production/`)

### 1. `delta_lake_etl_pipeline_dag.py`
**Main Delta Lake ETL Pipeline**
- **Schedule**: Daily at 2 AM UTC
- **Purpose**: Complete ETL pipeline with Delta Lake features
- **Features**: Bronze/Silver/Gold architecture, data quality, optimization
- **Dependencies**: External data ingestion, Delta Lake maintenance
- **SLA**: 6 hours

### 2. `delta_lake_maintenance_dag.py`
**Delta Lake Maintenance Operations**
- **Schedule**: Daily at 3 AM UTC
- **Purpose**: OPTIMIZE, VACUUM, and health checks
- **Features**: File compaction, Z-ordering, cleanup, monitoring
- **Dependencies**: Main ETL pipeline completion
- **SLA**: 3 hours

### 3. `external_data_ingestion_dag.py`
**External Data Sources Ingestion**
- **Schedule**: Hourly
- **Purpose**: Multi-source data collection
- **Sources**: HubSpot, Snowflake, Redshift, Kafka, FX Rates
- **Features**: Incremental loading, error handling, validation
- **Dependencies**: None (source systems)

## Development DAGs (`development/`)

### Demo and Testing DAGs
- Located in `dags/` directory
- Used for development and testing
- Not deployed to production

## DAG Naming Convention

### Production DAGs
- Format: `{component}_{purpose}_dag.py`
- Examples: `delta_lake_etl_pipeline_dag.py`, `delta_lake_maintenance_dag.py`

### Development DAGs
- Format: `{description}_{type}_dag.py`
- Examples: `demo_pipeline_dag.py`, `test_etl_dag.py`

## Configuration

### Environment Variables
- `PROJECT_HOME`: Project root directory
- `VENV_ACTIVATE`: Virtual environment activation script
- `CONFIG_FILE`: Configuration file path
- `ALERT_EMAIL`: Email for failure notifications

### Airflow Variables
- `EMR_APP_ID`: EMR Serverless application ID
- `EMR_JOB_ROLE_ARN`: EMR job execution role ARN
- `S3_LAKE_BUCKET`: S3 bucket for data lake
- `ENVIRONMENT`: Environment (dev/staging/prod)

## Monitoring and Alerting

### SLA Configuration
- Main ETL: 6 hours
- Maintenance: 3 hours
- Ingestion: 1 hour

### Email Notifications
- Failure notifications sent to data-team@company.com
- Retry notifications for critical failures
- SLA breach alerts

## Security

### Access Control
- DAGs owned by 'data-engineering' role
- Restricted access to production DAGs
- Audit logging for all DAG executions

### Secrets Management
- Database credentials via Airflow Connections
- API keys via Airflow Variables
- S3 access via IAM roles
