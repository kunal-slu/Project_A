# PySpark Data Engineering Project

## 🎯 Project Overview

This project provides a comprehensive PySpark data engineering pipeline with 5 essential data sources for learning and practice.

## 📊 Data Sources

### 1️⃣ HubSpot CRM
- **Contacts**: Customer contact information (25K records)
- **Deals**: Sales opportunities and pipeline (30K records)

### 2️⃣ Snowflake Warehouse
- **Customers**: Customer master data (50K records)
- **Orders**: Order transactions (100K records)
- **Products**: Product catalog (10K records)

### 3️⃣ Redshift Analytics
- **Customer Behavior**: User behavior analytics (50K records)

### 4️⃣ Stream Data
- **Kafka Events**: Real-time event streaming (100K records)

### 5️⃣ FX Rates
- **Historical Rates**: Exchange rates (20K records)

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- PySpark 3.5+
- Delta Lake

### Installation
```bash
pip install -r requirements.txt
```

### Running the Pipeline
```bash
python src/pyspark_interview_project/pipeline.py config/config-dev.yaml
```

## 📁 Project Structure

```
├── aws/data_fixed/           # Data sources
│   ├── 01_hubspot_crm/       # HubSpot CRM data
│   ├── 02_snowflake_warehouse/ # Snowflake warehouse data
│   ├── 03_redshift_analytics/ # Redshift analytics data
│   ├── 04_stream_data/       # Streaming data
│   └── 05_fx_rates/          # FX rates data
├── config/                   # Configuration files
├── src/pyspark_interview_project/ # Main pipeline code
├── airflow/dags/             # Airflow DAGs
└── docs/                     # Documentation
```

## 🎯 Learning Objectives

- **Data Engineering**: ETL pipelines, data quality, transformations
- **Analytics**: Aggregations, window functions, statistical analysis
- **Performance**: Optimization, partitioning, caching strategies
- **Integration**: Multi-source data integration
- **Real-time Processing**: Streaming data and event processing

## 📚 Documentation

- [Simplified Data Sources](docs/SIMPLIFIED_DATA_SOURCES.md)
- [Data Quality Report](docs/DATA_QUALITY_REPORT.md)

## 🔧 Configuration

All configurations are managed in the `config/` directory:
- `default.yaml` - Base configuration
- `aws.yaml` - AWS-specific settings
- `azure.yaml` - Azure-specific settings
- `local.yaml` - Local development settings

## 🚀 Ready for PySpark Practice!

This project provides realistic, high-quality data for comprehensive PySpark learning and practice.
