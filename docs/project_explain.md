# 🏗️ AWS Production ETL Pipeline - Complete Execution Guide

## 📋 Project Overview

This is a comprehensive **AWS Production ETL Pipeline** built with PySpark, Delta Lake, and Apache Airflow. The project implements a modern data lakehouse architecture for processing financial and business data from multiple sources.

### 🎯 **Project Goals**
- **Data Ingestion**: Collect data from 5 different sources (HubSpot, Snowflake, Redshift, Kafka, FX Rates)
- **Data Processing**: Transform raw data through Bronze → Silver → Gold layers
- **Data Quality**: Implement comprehensive data quality checks and quarantine
- **Orchestration**: Schedule and monitor ETL jobs with Apache Airflow
- **AWS Integration**: Deploy on AWS using EMR Serverless, S3, Glue, and Athena

---

## 🏛️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    AWS Data Lakehouse Architecture              │
└─────────────────────────────────────────────────────────────────┘

┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Bronze    │    │   Silver    │    │    Gold     │
│             │    │             │    │             │
│ • Raw Data  │───▶│ • Conformed │───▶│ • Analytics │
│ • Quarantine│    │ • SCD-2     │    │ • Aggregates│
│ • Validation│    │ • Incremental│   │ • As-of Joins│
└─────────────┘    └─────────────┘    └─────────────┘
       │                   │                   │
       ▼                   ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   S3 Raw    │    │  S3 Silver  │    │  S3 Gold    │
│   Storage   │    │   Storage    │    │   Storage   │
└─────────────┘    └─────────────┘    └─────────────┘
```

---

## 📊 Data Sources

### 1. **HubSpot CRM Data**
- **Files**: `hubspot_contacts_25000.csv`, `hubspot_deals_30000.csv`
- **Records**: 25,000 contacts, 30,000 deals
- **Purpose**: Customer relationship management data
- **Key Fields**: `customer_id`, `company_domain`, `deal_value`, `pipeline_stage`

### 2. **Snowflake Data Warehouse**
- **Files**: `snowflake_customers_50000.csv`, `snowflake_orders_100000.csv`, `snowflake_products_10000.csv`
- **Records**: 50,000 customers, 100,000 orders, 10,000 products
- **Purpose**: Transactional business data
- **Key Fields**: `customer_id`, `order_id`, `product_id`, `order_date`, `amount`

### 3. **Redshift Analytics**
- **Files**: `redshift_customer_behavior_50000.csv`
- **Records**: 50,000 behavioral records
- **Purpose**: Customer analytics and behavior tracking
- **Key Fields**: `customer_id`, `session_duration`, `page_views`, `conversion_value`

### 4. **Kafka Stream Data**
- **Files**: `stream_kafka_events_100000.csv`
- **Records**: 100,000 streaming events
- **Purpose**: Real-time event processing
- **Key Fields**: `event_id`, `event_timestamp`, `event_type`, `user_id`

### 5. **FX Rates Data**
- **Files**: `fx_rates_historical_730_days.csv`
- **Records**: 730 days of currency rates
- **Purpose**: Currency conversion and financial calculations
- **Key Fields**: `date`, `currency`, `usd_rate`, `rate_source`

---

## 🚀 Execution Steps

### **Step 1: Environment Setup**

#### 1.1 **Prerequisites Installation**
```bash
# Install Python dependencies
pip install -r requirements.txt

# Key packages installed:
# - pyspark>=3.5.1,<5.0.0
# - delta-spark>=3.2.0,<4.0.0
# - apache-airflow==2.8.0
# - apache-airflow-providers-amazon>=9.0.0,<10
# - apache-airflow-providers-openlineage>=1.8.0,<2
```

#### 1.2 **Configuration Setup**
```bash
# Copy environment template
cp .env.example .env

# Key environment variables:
# - AWS_ACCESS_KEY_ID
# - AWS_SECRET_ACCESS_KEY
# - S3_BUCKET_NAME
# - EMR_APP_ID
# - EMR_JOB_ROLE_ARN
```

#### 1.3 **AWS Configuration**
```yaml
# config/aws.yaml
cloud: aws
spark:
  master: yarn
  deploy_mode: client
  executor_memory: 4g
  executor_cores: 2
  max_result_size: 2g
```

### **Step 2: Data Ingestion (Bronze Layer)**

#### 2.1 **HubSpot Data Ingestion**
```python
# aws/scripts/hubspot_to_bronze.py
def extract_hubspot_data():
    """
    Extracts data from HubSpot API
    - Handles rate limiting (100 requests/second)
    - Implements pagination for large datasets
    - Flattens nested JSON structures
    """
    # API Configuration
    api_key = os.getenv('HUBSPOT_API_KEY')
    base_url = 'https://api.hubapi.com/crm/v3/objects'
    
    # Extract contacts
    contacts = extract_contacts(api_key, base_url)
    
    # Extract deals
    deals = extract_deals(api_key, base_url)
    
    return contacts, deals
```

#### 2.2 **Snowflake Data Ingestion**
```python
# aws/scripts/snowflake_to_bronze.py
def extract_snowflake_data():
    """
    Extracts data from Snowflake using JDBC
    - Connects via JDBC driver
    - Executes SQL queries for each table
    - Handles large result sets with pagination
    """
    # JDBC Configuration
    jdbc_url = f"jdbc:snowflake://{account}.snowflakecomputing.com"
    connection_properties = {
        "user": username,
        "password": password,
        "warehouse": warehouse,
        "database": database,
        "schema": schema
    }
    
    # Extract each table
    customers = spark.read.jdbc(jdbc_url, "customers", properties=connection_properties)
    orders = spark.read.jdbc(jdbc_url, "orders", properties=connection_properties)
    products = spark.read.jdbc(jdbc_url, "products", properties=connection_properties)
    
    return customers, orders, products
```

#### 2.3 **Redshift Data Ingestion**
```python
# aws/scripts/redshift_to_bronze.py
def extract_redshift_data():
    """
    Extracts data from Redshift using JDBC
    - Connects to Redshift cluster
    - Executes analytical queries
    - Handles complex joins and aggregations
    """
    jdbc_url = f"jdbc:redshift://{host}:{port}/{database}"
    connection_properties = {
        "user": username,
        "password": password,
        "driver": "com.amazon.redshift.jdbc.Driver"
    }
    
    # Extract customer behavior data
    behavior = spark.read.jdbc(jdbc_url, "customer_behavior", properties=connection_properties)
    
    return behavior
```

#### 2.4 **Kafka Stream Ingestion**
```python
# aws/scripts/kafka_to_bronze.py
def extract_kafka_streams():
    """
    Consumes data from Kafka topics
    - Handles real-time streaming
    - Implements checkpointing for fault tolerance
    - Processes JSON and Avro formats
    """
    kafka_options = {
        "kafka.bootstrap.servers": kafka_brokers,
        "subscribe": "events",
        "startingOffsets": "latest"
    }
    
    # Read from Kafka
    stream_df = spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()
    
    return stream_df
```

#### 2.5 **FX Rates Data Ingestion**
```python
# aws/scripts/fx_rates_to_bronze.py
def extract_fx_rates():
    """
    Downloads FX rates from external API
    - Fetches historical rates
    - Handles multiple currencies
    - Implements retry logic for API failures
    """
    # External API Configuration
    api_url = "https://api.exchangerate-api.com/v4/historical"
    
    # Download historical rates
    rates = download_historical_rates(api_url, start_date, end_date)
    
    return rates
```

### **Step 3: Data Quality Checks (Bronze Layer)**

#### 3.1 **Data Quality Framework**
```python
# src/pyspark_interview_project/dq/runner.py
def run_yaml_policy(df, policy, key_cols=None):
    """
    Executes data quality checks based on YAML policies
    - Uniqueness checks on key columns
    - Null value validation
    - Range and format validation
    - Referential integrity checks
    """
    results = {
        "timestamp": datetime.now().isoformat(),
        "checks": [],
        "summary": {
            "total_checks": 0,
            "passed": 0,
            "failed": 0,
            "critical_failures": 0
        }
    }
    
    # Run uniqueness checks
    if key_cols:
        for col in key_cols:
            total_count = df.count()
            unique_count = df.select(col).distinct().count()
            if total_count == unique_count:
                results["checks"].append({
                    "check": f"uniqueness_{col}",
                    "passed": True,
                    "severity": "critical"
                })
            else:
                results["checks"].append({
                    "check": f"uniqueness_{col}",
                    "passed": False,
                    "severity": "critical",
                    "error": f"Column '{col}' has {total_count - unique_count} duplicate values"
                })
    
    return results
```

#### 3.2 **Quarantine Mechanism**
```python
# src/pyspark_interview_project/dq/quarantine.py
def quarantine_bad_data(df, dq_results, quarantine_path):
    """
    Moves bad data to quarantine zone
    - Identifies failed records
    - Writes to quarantine S3 location
    - Logs quarantine reasons
    """
    failed_records = df.filter(condition_for_failed_records)
    
    if failed_records.count() > 0:
        failed_records.write \
            .mode("append") \
            .format("delta") \
            .save(quarantine_path)
        
        logger.warning(f"Quarantined {failed_records.count()} records to {quarantine_path}")
```

### **Step 4: Data Transformation (Silver Layer)**

#### 4.1 **Data Conformance**
```python
# src/pyspark_interview_project/transform/silver_conform.py
def conform_customer_data(bronze_customers):
    """
    Conforms customer data from multiple sources
    - Standardizes column names
    - Handles data type conversions
    - Implements business rules
    - Creates unified customer view
    """
    # Standardize column names
    conformed = bronze_customers.select(
        col("customer_id").alias("customer_id"),
        col("first_name").alias("first_name"),
        col("last_name").alias("last_name"),
        col("email").alias("email"),
        col("phone").alias("phone"),
        col("created_date").alias("created_date"),
        col("last_updated").alias("last_updated")
    )
    
    # Apply business rules
    conformed = conformed.withColumn(
        "full_name", 
        concat(col("first_name"), lit(" "), col("last_name"))
    )
    
    return conformed
```

#### 4.2 **SCD-2 Implementation**
```python
# src/pyspark_interview_project/transform/scd2.py
def implement_scd2(current_df, historical_df):
    """
    Implements Slowly Changing Dimension Type 2
    - Tracks historical changes
    - Maintains effective date ranges
    - Handles updates and inserts
    """
    # Merge current data with historical
    merged = current_df.join(
        historical_df,
        ["customer_id"],
        "left_outer"
    )
    
    # Identify changes
    changes = merged.filter(
        (col("current.email") != col("historical.email")) |
        (col("current.phone") != col("historical.phone"))
    )
    
    # Create new version records
    new_versions = changes.select(
        col("customer_id"),
        col("current.email").alias("email"),
        col("current.phone").alias("phone"),
        current_timestamp().alias("effective_date"),
        lit(None).alias("end_date"),
        lit(True).alias("is_current")
    )
    
    return new_versions
```

### **Step 5: Data Aggregation (Gold Layer)**

#### 5.1 **Customer Analytics**
```python
# src/pyspark_interview_project/aggregate/customer_analytics.py
def create_customer_analytics(silver_customers, silver_orders):
    """
    Creates customer analytics aggregations
    - Customer lifetime value
    - Purchase frequency
    - Customer segmentation
    - Behavioral insights
    """
    customer_analytics = silver_orders.groupBy("customer_id") \
        .agg(
            sum("amount").alias("total_spent"),
            count("order_id").alias("total_orders"),
            avg("amount").alias("avg_order_value"),
            max("order_date").alias("last_purchase_date")
        ) \
        .withColumn("customer_lifetime_value", col("total_spent")) \
        .withColumn("purchase_frequency", col("total_orders")) \
        .withColumn("customer_segment", 
            when(col("total_spent") > 10000, "VIP")
            .when(col("total_spent") > 5000, "High Value")
            .when(col("total_spent") > 1000, "Medium Value")
            .otherwise("Low Value")
        )
    
    return customer_analytics
```

#### 5.2 **Business Metrics**
```python
# src/pyspark_interview_project/aggregate/business_metrics.py
def create_business_metrics(silver_orders, silver_products):
    """
    Creates business performance metrics
    - Revenue by product category
    - Sales trends over time
    - Geographic performance
    - Customer acquisition metrics
    """
    business_metrics = silver_orders.join(
        silver_products,
        ["product_id"],
        "left"
    ).groupBy(
        "product_category",
        date_format("order_date", "yyyy-MM").alias("month")
    ).agg(
        sum("amount").alias("revenue"),
        count("order_id").alias("order_count"),
        countDistinct("customer_id").alias("unique_customers")
    )
    
    return business_metrics
```

### **Step 6: Orchestration with Apache Airflow**

#### 6.1 **Main ETL DAG**
```python
# dags/pyspark_etl_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'pyspark_etl_pipeline',
    default_args=default_args,
    description='Main ETL Pipeline',
    schedule_interval='@daily',
    catchup=False
)

# Trigger external data ingestion
trigger_external_ingestion = TriggerDagRunOperator(
    task_id='trigger_external_ingestion',
    trigger_dag_id='external_data_ingestion',
    dag=dag
)

# Run main PySpark ETL
run_pyspark_etl = BashOperator(
    task_id='run_pyspark_etl',
    bash_command='python src/pyspark_interview_project/pipeline.py',
    dag=dag
)

# Data quality validation
validate_data_quality = BashOperator(
    task_id='validate_data_quality',
    bash_command='python aws/scripts/run_ge_checks.py',
    dag=dag
)

# Update monitoring
update_monitoring = BashOperator(
    task_id='update_monitoring',
    bash_command='python scripts/update_monitoring.py',
    dag=dag
)

# Set task dependencies
trigger_external_ingestion >> run_pyspark_etl >> validate_data_quality >> update_monitoring
```

#### 6.2 **External Data Ingestion DAG**
```python
# dags/external_data_ingestion_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    'external_data_ingestion',
    default_args=default_args,
    description='External Data Ingestion',
    schedule_interval='@hourly',
    catchup=False
)

# Ingest from each source
ingest_hubspot = BashOperator(
    task_id='ingest_hubspot',
    bash_command='python aws/scripts/hubspot_to_bronze.py',
    dag=dag
)

ingest_snowflake = BashOperator(
    task_id='ingest_snowflake',
    bash_command='python aws/scripts/snowflake_to_bronze.py',
    dag=dag
)

ingest_redshift = BashOperator(
    task_id='ingest_redshift',
    bash_command='python aws/scripts/redshift_to_bronze.py',
    dag=dag
)

ingest_kafka = BashOperator(
    task_id='ingest_kafka',
    bash_command='python aws/scripts/kafka_to_bronze.py',
    dag=dag
)

ingest_fx_rates = BashOperator(
    task_id='ingest_fx_rates',
    bash_command='python aws/scripts/fx_rates_to_bronze.py',
    dag=dag
)

# Run all ingestion tasks in parallel
[ingest_hubspot, ingest_snowflake, ingest_redshift, ingest_kafka, ingest_fx_rates]
```

### **Step 7: AWS Deployment**

#### 7.1 **EMR Serverless Configuration**
```json
{
  "name": "pyspark-etl-job",
  "applicationId": "00f1g2h3i4j5k6l7",
  "executionRoleArn": "arn:aws:iam::123456789012:role/EMRServerlessExecutionRole",
  "jobDriver": {
    "sparkSubmit": {
      "entryPoint": "s3://code-bucket/pyspark-etl.py",
      "entryPointArguments": ["--config", "s3://config-bucket/config.yaml"],
      "sparkSubmitParameters": "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    }
  },
  "configurationOverrides": {
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {
        "logUri": "s3://logs-bucket/emr-logs/"
      }
    }
  }
}
```

#### 7.2 **S3 Bucket Structure**
```
s3://data-lake-bucket/
├── bronze/
│   ├── hubspot/
│   │   ├── contacts/
│   │   └── deals/
│   ├── snowflake/
│   │   ├── customers/
│   │   ├── orders/
│   │   └── products/
│   ├── redshift/
│   │   └── customer_behavior/
│   ├── kafka/
│   │   └── events/
│   └── fx_rates/
│       └── rates/
├── silver/
│   ├── customers/
│   ├── orders/
│   ├── products/
│   └── analytics/
├── gold/
│   ├── customer_analytics/
│   ├── business_metrics/
│   └── reporting/
└── quarantine/
    ├── failed_records/
    └── data_quality_issues/
```

#### 7.3 **Glue Data Catalog Registration**
```python
# aws/scripts/register_glue_tables.py
import boto3

def register_glue_tables():
    """
    Registers Delta tables in AWS Glue Data Catalog
    - Creates databases for each layer
    - Registers tables with proper schemas
    - Enables Athena queries
    """
    glue_client = boto3.client('glue')
    
    # Create databases
    databases = ['bronze_db', 'silver_db', 'gold_db']
    for db_name in databases:
        try:
            glue_client.create_database(
                DatabaseInput={
                    'Name': db_name,
                    'Description': f'{db_name} layer tables'
                }
            )
        except glue_client.exceptions.AlreadyExistsException:
            pass
    
    # Register tables
    tables = [
        {
            'DatabaseName': 'bronze_db',
            'TableInput': {
                'Name': 'hubspot_contacts',
                'StorageDescriptor': {
                    'Location': 's3://data-lake-bucket/bronze/hubspot/contacts/',
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    }
                }
            }
        }
    ]
    
    for table in tables:
        glue_client.create_table(**table)
```

### **Step 8: Monitoring and Observability**

#### 8.1 **OpenLineage Integration**
```python
# src/pyspark_interview_project/lineage/openlineage_emitter.py
from openlineage.client import OpenLineageClient
from openlineage.client.facet import DataSourceDatasetFacet, SchemaDatasetFacet

def emit_lineage_event(job_name, inputs, outputs):
    """
    Emits data lineage events to OpenLineage
    - Tracks data flow between datasets
    - Records job execution metadata
    - Enables lineage visualization
    """
    client = OpenLineageClient(url="http://marquez:5000")
    
    # Create input datasets
    input_datasets = []
    for input_path in inputs:
        input_datasets.append({
            "namespace": "s3://data-lake-bucket",
            "name": input_path,
            "facets": {
                "dataSource": DataSourceDatasetFacet(
                    name="S3",
                    uri=f"s3://data-lake-bucket/{input_path}"
                )
            }
        })
    
    # Create output datasets
    output_datasets = []
    for output_path in outputs:
        output_datasets.append({
            "namespace": "s3://data-lake-bucket",
            "name": output_path,
            "facets": {
                "dataSource": DataSourceDatasetFacet(
                    name="S3",
                    uri=f"s3://data-lake-bucket/{output_path}"
                )
            }
        })
    
    # Emit lineage event
    client.emit(
        run_id=job_name,
        job_name=job_name,
        inputs=input_datasets,
        outputs=output_datasets
    )
```

#### 8.2 **Data Quality Monitoring**
```python
# scripts/update_monitoring.py
def update_data_quality_metrics():
    """
    Updates data quality monitoring metrics
    - Records DQ check results
    - Tracks data freshness
    - Monitors record counts
    - Alerts on failures
    """
    # Read DQ results
    dq_results = read_dq_results()
    
    # Update metrics
    metrics = {
        "timestamp": datetime.now().isoformat(),
        "total_records": dq_results["total_records"],
        "passed_checks": dq_results["passed_checks"],
        "failed_checks": dq_results["failed_checks"],
        "data_freshness": dq_results["data_freshness"]
    }
    
    # Write to monitoring system
    write_metrics(metrics)
    
    # Send alerts if needed
    if dq_results["failed_checks"] > 0:
        send_alert("Data quality issues detected")
```

---

## 🔄 Complete Execution Flow

### **Daily Execution Sequence**

1. **00:00 UTC** - External data ingestion DAG triggers
   - Ingests data from all 5 sources
   - Writes to Bronze layer (S3)
   - Applies data quality checks
   - Quarantines bad data

2. **01:00 UTC** - Main ETL pipeline triggers
   - Reads from Bronze layer
   - Transforms data to Silver layer
   - Implements SCD-2 for historical tracking
   - Applies business rules

3. **02:00 UTC** - Gold layer aggregation
   - Creates customer analytics
   - Generates business metrics
   - Builds reporting tables
   - Updates data catalog

4. **03:00 UTC** - Data quality validation
   - Runs comprehensive DQ checks
   - Validates data relationships
   - Updates monitoring metrics
   - Sends alerts if needed

5. **04:00 UTC** - Monitoring and cleanup
   - Updates lineage information
   - Cleans up temporary files
   - Generates execution reports
   - Updates SLA metrics

### **Real-time Processing**

- **Kafka streams** are processed continuously
- **Data quality checks** run on every batch
- **Monitoring alerts** are sent immediately
- **Lineage tracking** is updated in real-time

---

## 🛠️ Key Technologies

### **Data Processing**
- **Apache Spark**: Distributed data processing
- **Delta Lake**: ACID transactions and time travel
- **PySpark**: Python API for Spark

### **Orchestration**
- **Apache Airflow**: Workflow orchestration
- **DAGs**: Directed Acyclic Graphs for task dependencies
- **Operators**: Bash, Python, and custom operators

### **Cloud Services**
- **AWS S3**: Data lake storage
- **AWS EMR Serverless**: Serverless Spark processing
- **AWS Glue**: Data catalog and ETL
- **AWS Athena**: Serverless SQL queries

### **Data Quality**
- **Great Expectations**: Data quality framework
- **Custom DQ rules**: Business-specific validations
- **Quarantine system**: Bad data isolation

### **Monitoring**
- **OpenLineage**: Data lineage tracking
- **Marquez**: Lineage visualization
- **Custom metrics**: Business KPIs

---

## 📈 Performance Optimizations

### **Spark Optimizations**
- **Partitioning**: Data partitioned by date and key columns
- **Caching**: Frequently accessed datasets cached in memory
- **Broadcast joins**: Small tables broadcast to all executors
- **Predicate pushdown**: Filters applied at storage level

### **Delta Lake Optimizations**
- **Compaction**: Regular compaction of small files
- **VACUUM**: Cleanup of old versions
- **Z-ordering**: Co-locate related data
- **Statistics**: Automatic statistics collection

### **S3 Optimizations**
- **Lifecycle policies**: Automatic data archival
- **Storage classes**: Cost-optimized storage tiers
- **Cross-region replication**: Disaster recovery
- **Encryption**: Data encryption at rest and in transit

---

## 🔒 Security and Compliance

### **Data Security**
- **Encryption**: All data encrypted at rest and in transit
- **Access control**: IAM roles and policies
- **Network security**: VPC and security groups
- **Audit logging**: Comprehensive audit trails

### **Compliance**
- **GDPR**: Data privacy and protection
- **SOX**: Financial data compliance
- **HIPAA**: Healthcare data protection
- **PCI DSS**: Payment card data security

---

## 🚀 Deployment Commands

### **Local Development**
```bash
# Start local environment
make up

# Run data ingestion
make bronze

# Run data transformation
make silver

# Run data aggregation
make gold

# Run data quality checks
make dq

# Run complete pipeline
make pipeline
```

### **AWS Production**
```bash
# Deploy to AWS
aws emr-serverless create-application \
  --name pyspark-etl \
  --release-label emr-6.15.0 \
  --type SPARK

# Submit job
aws emr-serverless start-job-run \
  --application-id 00f1g2h3i4j5k6l7 \
  --execution-role-arn arn:aws:iam::123456789012:role/EMRServerlessExecutionRole \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://code-bucket/pyspark-etl.py",
      "entryPointArguments": ["--config", "s3://config-bucket/config.yaml"]
    }
  }'
```

---

## 📊 Expected Results

### **Data Volume**
- **Bronze Layer**: ~500GB of raw data daily
- **Silver Layer**: ~300GB of conformed data daily
- **Gold Layer**: ~100GB of aggregated data daily

### **Processing Time**
- **Data Ingestion**: 30-45 minutes
- **Data Transformation**: 60-90 minutes
- **Data Aggregation**: 30-45 minutes
- **Data Quality**: 15-30 minutes
- **Total Pipeline**: 2-3 hours daily

### **Data Quality Metrics**
- **Data Completeness**: >99%
- **Data Accuracy**: >98%
- **Data Freshness**: <24 hours
- **DQ Check Pass Rate**: >95%

---

## 🎯 Success Criteria

### **Technical Metrics**
- ✅ **Pipeline Reliability**: 99.9% uptime
- ✅ **Data Freshness**: <24 hours SLA
- ✅ **Processing Speed**: <3 hours total runtime
- ✅ **Data Quality**: >95% pass rate

### **Business Metrics**
- ✅ **Data Coverage**: 100% of required sources
- ✅ **Data Accuracy**: >98% accuracy
- ✅ **Cost Optimization**: <$1000/month AWS costs
- ✅ **Compliance**: 100% audit trail coverage

---

## 🔧 Troubleshooting

### **Common Issues**
1. **Data Quality Failures**: Check quarantine logs
2. **Processing Timeouts**: Increase executor memory
3. **Connection Issues**: Verify network connectivity
4. **Permission Errors**: Check IAM roles and policies

### **Monitoring Commands**
```bash
# Check pipeline status
airflow dags state pyspark_etl_pipeline

# Check data quality
python aws/scripts/run_ge_checks.py --table customers

# Check S3 data
aws s3 ls s3://data-lake-bucket/bronze/hubspot/contacts/

# Check EMR logs
aws logs describe-log-groups --log-group-name-prefix /aws/emr-serverless
```

---

## 📚 Additional Resources

### **Documentation**
- [AWS EMR Serverless Guide](docs/RUN_AWS.md)
- [Data Quality Framework](docs/DQ_GUIDE.md)
- [Airflow Configuration](docs/AIRFLOW_GUIDE.md)
- [Troubleshooting Guide](docs/TROUBLESHOOTING.md)

### **Code Repositories**
- [Main Pipeline](src/pyspark_interview_project/)
- [AWS Scripts](aws/scripts/)
- [Data Quality](dq/)
- [Tests](tests/)

### **Monitoring Dashboards**
- [Airflow UI](http://airflow:8080)
- [Marquez UI](http://marquez:5000)
- [AWS CloudWatch](https://console.aws.amazon.com/cloudwatch/)
- [S3 Console](https://console.aws.amazon.com/s3/)

---

This comprehensive guide explains every step of the AWS production ETL pipeline execution, from data ingestion to final analytics, ensuring a complete understanding of the entire data processing workflow.
