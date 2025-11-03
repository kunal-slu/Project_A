# 📊 Complete Data Sources & Architecture Guide

## Overview

This project processes data from **6 distinct sources** into a unified data lakehouse architecture using PySpark, Delta Lake, and AWS services.

---

## 🌐 Data Sources

### 1️⃣ **Snowflake (Data Warehouse)**
**Type**: External data warehouse  
**Connection**: JDBC  
**Frequency**: Daily batch or real-time CDC  
**Authentication**: OAuth2 / Username-Password  

**Tables**:
- `ORDERS` (100K records)
- `CUSTOMERS` (50K records)
- `PRODUCTS` (10K records)

**Schema**:
```python
orders: {
    "order_id": STRING,
    "customer_id": STRING,
    "order_date": TIMESTAMP,
    "product_id": STRING,
    "quantity": INTEGER,
    "amount": DECIMAL(10,2),
    "currency": STRING,
    "status": STRING,
    "last_modified_ts": TIMESTAMP
}
```

**Extractor**: `SnowflakeExtractor`  
**Job**: `aws/jobs/ingest/snowflake_to_bronze.py`  
**Module**: `src/pyspark_interview_project/extract/snowflake_orders.py`

---

### 2️⃣ **Redshift (Analytics Database)**
**Type**: Internal analytics database  
**Connection**: JDBC  
**Frequency**: Hourly incremental  
**Authentication**: IAM Role / Username-Password  

**Tables**:
- `customer_behavior` (50K records)

**Schema**:
```python
behavior: {
    "event_id": STRING,
    "customer_id": STRING,
    "event_name": STRING,
    "event_ts": TIMESTAMP,
    "session_id": STRING,
    "device": STRING,
    "geo": STRING,
    "url_1": STRING,     # First URL
    "url_2": STRING,     # Second URL (variable)
    "metadata": STRING   # JSON with variable fields
}
```

**Extractor**: `RedshiftExtractor`  
**Job**: `aws/jobs/ingest/redshift_behavior_ingest.py`  
**Module**: `src/pyspark_interview_project/extract/redshift_behavior.py`

**Special Features**:
- Variable number of URLs (uses `url_1`, `url_2`)
- Session ID parsing
- Event timestamp normalization
- Geo-location parsing

---

### 3️⃣ **Salesforce (CRM)**
**Type**: Cloud CRM platform  
**Connection**: REST API (SoQL)  
**Frequency**: Daily batch or incremental  
**Authentication**: OAuth2 / API credentials  

**Objects**:
- `Account` (accounts)
- `Contact` (contacts)
- `Opportunity` (opportunities)
- `Lead` (leads)
- `Case` (cases)
- `Task` (tasks)
- `Solution` (solutions)

**Schema**:
```python
accounts: {
    "id": STRING,
    "name": STRING,
    "industry": STRING,
    "annual_revenue": DECIMAL,
    "billing_city": STRING,
    "billing_country": STRING,
    "created_date": TIMESTAMP,
    "last_modified_date": TIMESTAMP
}

opportunities: {
    "id": STRING,
    "account_id": STRING,
    "name": STRING,
    "stage": STRING,
    "amount": DECIMAL,
    "close_date": DATE,
    "probability": INTEGER,
    "created_date": TIMESTAMP,
    "last_modified_date": TIMESTAMP
}
```

**Extractor**: `SalesforceExtractor`  
**Jobs**:
- `aws/jobs/ingest/salesforce_to_bronze.py` (main)
- `aws/jobs/ingest/crm_accounts_ingest.py`
- `aws/jobs/ingest/crm_contacts_ingest.py`
- `aws/jobs/ingest/crm_opportunities_ingest.py`

**Special Features**:
- Incremental loading via `LastModifiedDate`
- PII masking support
- Relationship mapping (accounts ↔ contacts ↔ opportunities)

---

### 4️⃣ **S3 (External File Storage)**
**Type**: External data dumps  
**Connection**: Direct S3 reads  
**Frequency**: Daily / Weekly depending on source  
**Authentication**: IAM role  

**Data Types**:
- ERP exports (inventory, purchasing)
- HR exports (employees, performance reviews)
- Accounting data (transactions, GL accounts)
- Vendor feeds (product catalogs)

**File Formats**: CSV, Parquet, JSON  
**Bronze Path**: `s3://bucket/bronze/external/`

**Example**:
```bash
s3://company-raw-data/
├── erp-export/inventory/YYYY-MM-DD/inventory.csv
├── hr-export/employees/YYYY-MM-DD/employees.csv
└── accounting/transactions/YYYY-MM-DD/transactions.parquet
```

**Extractor**: Generic file readers + AWS Glue Crawlers  
**Strategy**: Scheduled S3 event triggers → Lambda → Glue Crawler → Bronze

---

### 5️⃣ **Kafka (Streaming Events)**
**Type**: Real-time event streaming  
**Connection**: Kafka consumer  
**Frequency**: Continuous / near real-time  
**Authentication**: SASL_SSL  

**Topics**:
- `orders_events` - Order lifecycle events
- `user_events` - User interaction events
- `inventory_updates` - Stock level changes

**Schema**:
```python
order_event: {
    "order_id": STRING,
    "customer_id": STRING,
    "event_type": STRING,  # PLACED, PAID, SHIPPED, CANCELLED
    "event_timestamp": TIMESTAMP,
    "metadata": STRING
}
```

**Extractor**: `KafkaConsumer` (Structured Streaming)  
**Job**: `aws/jobs/ingest/kafka_orders_stream.py`  
**Module**: `src/pyspark_interview_project/extract/kafka_orders_stream.py`

**Special Features**:
- Checkpointed exactly-once processing
- Watermarking for late data
- Dead Letter Queue (DLQ) for failures
- Schema evolution support

---

### 6️⃣ **FX Rates (REST API)**
**Type**: External financial data  
**Connection**: REST API  
**Frequency**: Hourly / Daily  
**Authentication**: API key / Bearer token  

**Endpoint**: Financial data provider API  
**Data**: Currency exchange rates  

**Schema**:
```python
fx_rates: {
    "date": DATE,
    "ccy_pair": STRING,  # USD/EUR, GBP/USD, etc.
    "rate": DECIMAL(10,6),
    "base_currency": STRING,
    "quote_currency": STRING,
    "as_of_time": TIMESTAMP
}
```

**Extractor**: `FXRatesExtractor`  
**Job**: `aws/jobs/ingest/fx_rates_ingest.py`  
**Module**: `src/pyspark_interview_project/extract/base_extractor.py`

**Special Features**:
- REST API with fallback to CSV
- Multi-currency support
- Historical backfill capability

---

## 🏗️ Complete Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                         │
├──────────────┬─────────────┬────────────┬──────────┬──────────┬─────────────┤
│ Snowflake    │ Redshift    │ Salesforce │ S3       │ Kafka    │ FX Rates    │
│ (Warehouse)  │ (Analytics) │ (CRM)      │ (Files)  │ (Stream) │ (API)       │
│              │             │            │          │          │             │
│ • orders     │ • behavior  │ • accounts │ • ERP    │ • order  │ • exchange  │
│ • customers  │ • events    │ • contacts │ • HR     │   events │   rates     │
│ • products   │             │ • opps     │ • acct   │ • user   │             │
└──────┬───────┴─────┬───────┴─────┬──────┴─────┬────┴─────┬────┴──────┬──────┘
       │             │             │            │          │           │
       ▼             ▼             ▼            ▼          ▼           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                       EXTRACT LAYER (Bronze)                                 │
├──────────────────────────────────────────────────────────────────────────────┤
│  EMR Serverless / Spark Cluster                                             │
│  • SnowflakeExtractor   →  s3a://bucket/bronze/snowflake/                   │
│  • RedshiftExtractor    →  s3a://bucket/bronze/redshift/behavior/           │
│  • SalesforceExtractor  →  s3a://bucket/bronze/crm/salesforce/              │
│  • S3FileExtractor      →  s3a://bucket/bronze/external/                    │
│  • KafkaStream          →  s3a://bucket/bronze/kafka/                       │
│  • FXRatesExtractor     →  s3a://bucket/bronze/fx/rates/                    │
│                                                                              │
│  Metadata Columns Added:                                                    │
│  • _ingest_ts          (timestamp)                                          │
│  • _source             (source system)                                      │
│  • _run_id             (ETL run identifier)                                 │
└─────────────────────────────────────┬────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                    TRANSFORM LAYER (Silver)                                  │
├──────────────────────────────────────────────────────────────────────────────┤
│  EMR Serverless / Spark Cluster                                             │
│                                                                              │
│  Transformations:                                                            │
│  • Data cleaning (nulls, duplicates)                                        │
│  • Schema conformance                                                        │
│  • Data enrichment                                                          │
│  • Referential integrity checks                                             │
│  • Great Expectations validation (HARD GATE)                                │
│                                                                              │
│  Outputs: s3a://bucket/silver/                                              │
│  • silver/orders                                                           │
│  • silver/behavior                                                         │
│  • silver/customers                                                        │
│  • silver/accounts                                                         │
│  • silver/fx_rates                                                         │
└─────────────────────────────────────┬────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                    ANALYTICS LAYER (Gold)                                    │
├──────────────────────────────────────────────────────────────────────────────┤
│  EMR Serverless / Spark Cluster                                             │
│                                                                              │
│  Dimensional Models:                                                         │
│  • gold/customer_360        # Multi-source customer view                    │
│  • gold/product_perf_daily  # Daily product metrics                         │
│  • gold/revenue_by_geography # Geographic revenue analysis                  │
│  • gold/revenue_by_industry  # Industry revenue analysis                   │
│                                                                              │
│  Multi-Source Joins:                                                         │
│  orders + behavior + accounts + customers → customer_360                     │
└─────────────────────────────────────┬────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                    ANALYTICS & CONSUMPTION                                   │
├──────────────────────────────────────────────────────────────────────────────┤
│  • Amazon Athena          # SQL queries on Gold layer                       │
│  • Amazon QuickSight      # BI dashboards                                   │
│  • Jupyter Notebooks      # Data science exploration                        │
│  • Snowflake              # Dual destination (some tables)                  │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 🔄 Data Flow Example

### Example: Building customer_360 Gold Table

**Input Sources**:
1. `silver/orders` (from Snowflake)
2. `silver/behavior` (from Redshift)
3. `silver/accounts` (from Salesforce)
4. `silver/customers` (from Snowflake)

**Join Logic**:
```python
customer_360 = (
    customers.alias("c")
    # Enrich with CRM data
    .join(accounts.alias("a"), 
          F.col("c.email") == F.col("a.primary_email"), 
          "left")
    # Add order metrics
    .join(
        orders.groupBy("customer_id")
              .agg(
                  F.count("order_id").alias("total_orders"),
                  F.sum("amount_usd").alias("total_revenue"),
                  F.max("order_date").alias("last_order_date")
              ).alias("o"),
        F.col("c.customer_id") == F.col("o.customer_id"),
        "left"
    )
    # Add behavior metrics
    .join(
        behavior.groupBy("customer_id")
                .agg(
                    F.count("event_id").alias("total_events"),
                    F.count_distinct("session_id").alias("total_sessions"),
                    F.max("event_ts").alias("last_activity_date")
                ).alias("b"),
        F.col("c.customer_id") == F.col("b.customer_id"),
        "left"
    )
    .select(
        # Core customer fields
        F.coalesce(F.col("c.customer_id"), F.col("a.account_id")).alias("customer_key"),
        "c.customer_id",
        "c.email",
        "c.country",
        
        # CRM enrichment
        "a.industry",
        "a.annual_revenue",
        "a.billing_city",
        
        # Order metrics
        "o.total_orders",
        "o.total_revenue",
        "o.last_order_date",
        
        # Behavior metrics
        "b.total_events",
        "b.total_sessions",
        "b.last_activity_date"
    )
    .dropDuplicates(["customer_key"])
)
```

**Output**: `s3://bucket/gold/customer_360/`

---

## 🔐 Authentication & Secrets

### Secrets Manager Structure

All credentials stored in AWS Secrets Manager:

```
secrets/
├── snowflake/etl/prod          # Snowflake credentials
├── redshift/etl_user/prod      # Redshift IAM/user
├── salesforce/api_token/prod   # Salesforce OAuth2
├── kafka/orders_stream/prod    # Kafka SASL credentials
└── fx_vendor/api_key/prod      # FX API key
```

### Environment Configuration

```yaml
# config/prod.yaml
secrets:
  snowflake: snowflake/etl/prod
  redshift: redshift/etl_user/prod
  salesforce: salesforce/api_token/prod
  kafka: kafka/orders_stream/prod
  fx_vendor: fx_vendor/api_key/prod

data_sources:
  snowflake:
    enabled: true
    warehouse: COMPUTE_WH
    database: ETL_PROJECT_DB
    schema: RAW
  
  redshift:
    enabled: true
    database: dev
    schema: raw
    iam_role: arn:aws:iam::123456789012:role/RedshiftLoadRole
```

---

## 📊 Monitoring & Observability

### CloudWatch Metrics

**Extraction Metrics**:
- `records_extracted_{source}` - Record counts per source
- `extraction_duration_{source}` - Duration per source
- `extraction_failures_{source}` - Failure counts

**Transformation Metrics**:
- `records_transformed_{layer}` - Transformed records
- `dq_pass_rate_{suite}` - Data quality pass rate
- `dq_critical_failures` - Critical DQ failures

**Gold Metrics**:
- `customer_360_records` - Total customer records
- `revenue_aggregations` - Revenue calculations

### OpenLineage Tracking

Every extraction, transformation, and analytics job emits lineage events:

```json
{
  "job": "extract_snowflake_orders",
  "inputs": ["snowflake://ORDERS"],
  "outputs": ["s3://bucket/bronze/snowflake/orders"],
  "schema": {...},
  "row_count": 100000,
  "run_time": "2024-11-02T10:00:00Z"
}
```

---

## 🔄 Incremental Loading Strategy

### Watermark-Based CDC

All extractors support incremental loading using watermarks:

```python
# Get last successful extract timestamp
watermark = get_watermark("snowflake_orders", config, spark)

# Extract only new/updated records
df = spark.read.format("snowflake").option("query", 
    f"SELECT * FROM ORDERS WHERE last_modified_ts > '{watermark}'"
)

# Update watermark after successful extract
upsert_watermark("snowflake_orders", latest_ts, config, spark)
```

**Watermark Storage**: DynamoDB or S3 state store

---

## 🎯 Data Quality Gates

### Critical Checks (Fail Pipeline)

1. **NOT NULL checks** on primary keys
2. **Referential integrity** between tables
3. **Freshness** (data not too old)
4. **Volume** (record counts within expected range)

### Warning Checks (Alert Only)

1. Missing values in non-critical fields
2. Outlier detection
3. Duplicate checks (non-key fields)

**DQ Configuration**: `config/dq.yaml`  
**Storage**: `s3://bucket/_dq_results/`

---

## 🚀 Execution Schedule

### Daily Batch (2 AM UTC)

```
00:00 - FX Rates (hourly)
02:00 - Snowflake Extract
02:30 - Redshift Extract
03:00 - Salesforce Extract
03:30 - Bronze → Silver Transform
04:00 - Data Quality Check (HARD GATE)
04:30 - Silver → Gold Analytics
05:00 - Glue Table Registration
05:30 - Athena Query Validation
```

### Streaming (Continuous)

```
Kafka Stream → Bronze (continuous, checkpointed)
```

### Weekly Maintenance

```
Sunday 00:00 - Delta Lake Optimization
Sunday 01:00 - Vacuum old versions
Sunday 02:00 - Compaction small files
```

---

## 🔄 Change Data Capture (CDC) Support

### Supported Methods

1. **Timestamp-based**: Watermark comparison
2. **Delta CDF**: Change Data Feed when enabled
3. **Kafka CDC**: Debezium → Kafka → Bronze
4. **S3 Event Triggers**: File arrival → Lambda → Glue Crawler

### Debezium Configuration

For database CDC (PostgreSQL, MySQL) → Kafka:

```json
{
  "connector.class": "io.debezium.connector.mysql.MySqlConnector",
  "database.hostname": "mysql-server",
  "database.include.list": "crmdb",
  "table.include.list": "crmdb.customers,crmdb.orders",
  "include.schema.changes": "true",
  "topic.prefix": "cdc"
}
```

---

## 📊 Schema Evolution

### Handling Schema Changes

**Bronze**: Store as-is, schema-on-read, MERGE schema  
**Silver**: Validate against contract, quarantine bad data  
**Gold**: Strict schema enforcement

### Configuration

```yaml
# config/dq.yaml
schema_contracts:
  enabled: true
  validation_mode: strict  # or loose
  contracts_path: s3://bucket/schema_definitions/
  required_tables:
    - silver.orders
    - silver.behavior
```

---

## 🔐 Security & Compliance

### Data Classification

**PII Fields**:
- Email addresses
- Phone numbers
- Billing addresses
- Personal names

**Sensitive Fields**:
- Annual revenue
- Transaction amounts
- Probability scores

**Masking Rules**: Configured in `config/prod.yaml`

### Access Control

**Lake Formation**: Fine-grained permissions on Glue tables  
**IAM Policies**: Role-based access to S3 buckets  
**Encryption**: AES-256 at rest, TLS in transit

---

## 📈 Performance Optimization

### Partitioning Strategy

**Bronze**: By source, then date  
**Silver**: By date (order_date, event_ts)  
**Gold**: By geography or industry

### ZORDER Optimization

```python
# Optimize Delta tables for common query patterns
delta.optimize(table_path)
delta.optimize(table_path).zOrder(["customer_id", "order_date"])
```

### Broadcast Joins

Small dimension tables (< 10MB) broadcasted to all executors for faster joins.

---

## 🧪 Testing Strategy

### Unit Tests
```bash
pytest tests/unit/test_extractors.py
pytest tests/unit/test_transformers.py
```

### Integration Tests
```bash
pytest tests/integration/test_bronze_to_silver.py
pytest tests/integration/test_silver_to_gold.py
```

### End-to-End Tests
```bash
python scripts/local/run_etl_end_to_end.py --config config/local.yaml
```

---

## 🎉 Summary

This architecture processes **6 data sources** through **3 layers** (Bronze/Silver/Gold) using:

- **Extractors**: Schema-first with metadata
- **Transformers**: Data quality gates and lineage
- **Loaders**: Multi-format support (Delta/Iceberg/Parquet)
- **Monitoring**: CloudWatch, OpenLineage, DQ tracking
- **Security**: IAM, Lake Formation, encryption
- **Operations**: Scheduled pipelines, backfills, maintenance

**All sources documented, extractors implemented, and ready for production deployment!** 🚀

