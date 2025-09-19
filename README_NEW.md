# PySpark Data Engineering Platform

A production-ready data engineering platform built with PySpark, Delta Lake, and AWS services. This project demonstrates enterprise-grade ETL pipelines with Bronze/Silver/Gold architecture, data quality enforcement, schema evolution, and cloud deployment capabilities.

## 🏗️ Architecture

```mermaid
graph TB
    subgraph "Data Sources"
        A[CSV Files]
        B[JSON Files]
        C[Kafka Streams]
    end
    
    subgraph "Bronze Layer"
        D[Raw Data Ingestion]
        E[Schema Validation]
    end
    
    subgraph "Silver Layer"
        F[Data Cleaning]
        G[Deduplication]
        H[Type Casting]
    end
    
    subgraph "Gold Layer"
        I[Dimension Tables]
        J[Fact Tables]
        K[SCD2 Implementation]
    end
    
    subgraph "Data Quality"
        L[DQ Rules Engine]
        M[Schema Contracts]
        N[Monitoring]
    end
    
    A --> D
    B --> D
    C --> D
    D --> F
    E --> F
    F --> I
    G --> I
    H --> I
    I --> J
    J --> K
    L --> F
    L --> I
    M --> I
    M --> J
```

## 🚀 Quick Start

### Local Development (3 Commands)

```bash
# 1. Setup environment
make venv

# 2. Run ETL pipeline
make run-local

# 3. View results
ls -la data/lake/
```

### Optional Docker Development

```bash
# Start MinIO + Spark services
make up

# View services
make logs

# Stop services
make down
```

### AWS Deployment

```bash
# Set environment variables
export AWS_REGION=us-east-1
export AWS_S3_BUCKET=your-etl-bucket
export AWS_GLUE_DATABASE=etl_database
export AWS_EMR_APPLICATION_ID=your-emr-app-id

# Run on AWS EMR Serverless
make aws
```
