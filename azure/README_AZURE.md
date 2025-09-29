# Azure Data Platform - PySpark Data Engineer Project

This directory contains Azure-specific implementations for the PySpark data engineering project, including Databricks, ADLS Gen2, Key Vault, and Data Factory.

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Ingestion     │    │   Azure Data Lake│
│                 │    │                 │    │                 │
│ • REST APIs     │───▶│ • Data Factory  │───▶│ • ADLS Bronze   │
│ • RDS (Postgres)│    │ • Event Hubs    │    │ • ADLS Silver   │
│ • Salesforce    │    │ • Key Vault     │    │ • ADLS Gold     │
│ • Snowflake     │    │ • Managed ID    │    │                 │
│ • Kafka         │    │ • Private End   │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
                       ┌─────────────────┐            │
                       │   Processing    │◀───────────┘
                       │                 │
                       │ • Databricks    │
                       │ • PySpark       │
                       │ • Delta Lake    │
                       └─────────────────┘
                                │
                       ┌─────────────────┐
                       │   Orchestration │
                       │                 │
                       │ • Databricks Jobs│
                       │ • Data Factory  │
                       └─────────────────┘
                                │
                       ┌─────────────────┐
                       │   Data Quality  │
                       │                 │
                       │ • Great Expect. │
                       │ • Data Docs     │
                       └─────────────────┘
```

## 🚀 Golden Path (Copy-Paste Runnable)

### Prerequisites

```bash
# Set environment variables
export AZURE_LOCATION="East US"
export PROJECT="pyspark-de-project"
export ENVIRONMENT="dev"
export AZURE_SUBSCRIPTION_ID=$(az account show --query id --output tsv)

# Verify Azure credentials
az account show
```

### Step 1: Deploy Infrastructure

```bash
# Navigate to Azure infrastructure
cd azure/infra/terraform

# Initialize Terraform
terraform init

# Review the plan
terraform plan -var="project=$PROJECT" -var="environment=$ENVIRONMENT"

# Deploy infrastructure
terraform apply -var="project=$PROJECT" -var="environment=$ENVIRONMENT"

# Save outputs
export STORAGE_ACCOUNT=$(terraform output -raw storage_account_name)
export DATABRICKS_WORKSPACE_URL=$(terraform output -raw databricks_workspace_url)
export KEY_VAULT_NAME=$(terraform output -raw key_vault_name)
export LAKE_ROOT=$(terraform output -raw lake_root_url)
```

### Step 2: Create Secrets in Key Vault

```bash
# Create Salesforce credentials
az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "salesforce-username" \
  --value "your-salesforce-username"

az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "salesforce-password" \
  --value "your-salesforce-password"

az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "salesforce-security-token" \
  --value "your-salesforce-token"

az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "salesforce-domain" \
  --value "login"

# Create Snowflake credentials
az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "snowflake-url" \
  --value "your-snowflake-account.snowflakecomputing.com"

az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "snowflake-username" \
  --value "your-snowflake-username"

az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "snowflake-password" \
  --value "your-snowflake-password"

az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "snowflake-database" \
  --value "SNOWFLAKE_SAMPLE_DATA"

az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "snowflake-schema" \
  --value "TPCH_SF1"

az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "snowflake-warehouse" \
  --value "COMPUTE_WH"
```

### Step 3: Import Notebooks to Databricks

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure Databricks CLI
databricks configure --token

# Import notebooks
databricks workspace import_dir \
  azure/databricks/notebooks/ \
  /Workspace/Repos/your-repo/notebooks/ \
  --language PYTHON
```

### Step 4: Configure Databricks Cluster

```bash
# Create cluster configuration
cat > cluster_config.json << EOF
{
  "cluster_name": "pyspark-de-project-cluster",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "num_workers": 2,
  "spark_conf": {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.AzureBlobLogStore"
  },
  "spark_env_vars": {
    "PYTHONPATH": "/Workspace/Repos/your-repo/src"
  },
  "libraries": [
    {
      "maven": {
        "coordinates": "io.delta:delta-core_2.12:2.4.0"
      }
    },
    {
      "maven": {
        "coordinates": "net.snowflake:snowflake-jdbc:3.13.30"
      }
    },
    {
      "maven": {
        "coordinates": "com.springml:spark-salesforce_2.12:1.1.1"
      }
    }
  ]
}
EOF

# Create cluster
databricks clusters create --json-file cluster_config.json
```

### Step 5: Run Data Pipeline

```bash
# Create job configuration
cat > job_config.json << EOF
{
  "name": "pyspark-de-project-pipeline",
  "existing_cluster_id": "your-cluster-id",
  "notebook_task": {
    "notebook_path": "/Workspace/Repos/your-repo/notebooks/01_ingest_fx_rest",
    "base_parameters": {
      "LAKE_ROOT": "$LAKE_ROOT",
      "LOG_LEVEL": "INFO"
    }
  }
}
EOF

# Run job
databricks jobs create --json-file job_config.json
```

### Step 6: Verify Data in ADLS Gen2

```bash
# List data lake contents
az storage fs file list \
  --account-name $STORAGE_ACCOUNT \
  --file-system lake \
  --path bronze/fx_rates \
  --recursive

# List silver layer
az storage fs file list \
  --account-name $STORAGE_ACCOUNT \
  --file-system lake \
  --path silver/fx_rates \
  --recursive

# List gold layer
az storage fs file list \
  --account-name $STORAGE_ACCOUNT \
  --file-system lake \
  --path gold \
  --recursive
```

### Step 7: Run Data Quality Checks

```bash
# Create DQ job
cat > dq_job.json << EOF
{
  "name": "data-quality-checks",
  "existing_cluster_id": "your-cluster-id",
  "notebook_task": {
    "notebook_path": "/Workspace/Repos/your-repo/notebooks/07_data_quality_checks",
    "base_parameters": {
      "LAKE_ROOT": "$LAKE_ROOT",
      "LOG_LEVEL": "INFO"
    }
  }
}
EOF

# Run DQ job
databricks jobs create --json-file dq_job.json
```

### Step 8: Unity Catalog Registration (Optional)

```bash
# Create catalog
databricks sql query \
  --query "CREATE CATALOG IF NOT EXISTS pyspark_de_project"

# Create schemas
databricks sql query \
  --query "CREATE SCHEMA IF NOT EXISTS pyspark_de_project.silver"

databricks sql query \
  --query "CREATE SCHEMA IF NOT EXISTS pyspark_de_project.gold"

# Register tables
databricks sql query \
  --query "CREATE TABLE IF NOT EXISTS pyspark_de_project.silver.fx_rates USING DELTA LOCATION '$LAKE_ROOT/silver/fx_rates'"
```

## 📁 Directory Structure

```
azure/
├── databricks/notebooks/      # Databricks notebooks
│   ├── 01_ingest_fx_rest.py
│   ├── 02_bronze_to_silver_fx.py
│   ├── 03_salesforce_to_bronze.py
│   ├── 04_kafka_orders_stream.py
│   ├── 05_snowflake_to_bronze.py
│   ├── 06_silver_to_gold.py
│   └── 07_data_quality_checks.py
├── job-json/                  # Databricks job configurations
│   └── databricks_jobs.json
├── adf/pipelines/             # Data Factory pipelines
│   └── main_pipeline.json
├── infra/terraform/           # Infrastructure as Code
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── quality/great_expectations/ # Data quality configurations
│   └── great_expectations.yml
├── monitoring/                # Monitoring configurations
│   └── application_insights.json
├── .github/workflows/         # CI/CD pipelines
│   └── ci.yml
└── README_AZURE.md           # This file
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `LAKE_ROOT` | ADLS Gen2 root path | `abfss://lake@yourstorageaccount.dfs.core.windows.net` |
| `STORAGE_ACCOUNT` | Storage account name | `pysparkdeprojectdevdatalake` |
| `DATABRICKS_WORKSPACE_URL` | Databricks workspace URL | `https://adb-1234567890123456.7.azuredatabricks.net` |
| `KEY_VAULT_NAME` | Key Vault name | `pyspark-de-project-dev-kv` |
| `AZURE_LOCATION` | Azure region | `East US` |

### Databricks Cluster Configuration

```json
{
  "cluster_name": "pyspark-de-project-cluster",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "num_workers": 2,
  "spark_conf": {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.AzureBlobLogStore"
  }
}
```

## 🚀 Usage Examples

### Running Individual Notebooks

```python
# In Databricks notebook
%run /Workspace/Repos/your-repo/notebooks/01_ingest_fx_rest

# With parameters
%run /Workspace/Repos/your-repo/notebooks/02_bronze_to_silver_fx {"LAKE_ROOT": "abfss://lake@yourstorageaccount.dfs.core.windows.net"}
```

### Creating Databricks Jobs

```bash
# Create job from JSON
databricks jobs create --json-file azure/job-json/databricks_jobs.json

# Run job
databricks jobs run-now --job-id <job-id>

# Get job status
databricks jobs get --job-id <job-id>
```

### Accessing Key Vault Secrets

```python
# In Databricks notebook
username = dbutils.secrets.get("salesforce", "username")
password = dbutils.secrets.get("salesforce", "password")
```

### Working with ADLS Gen2

```python
# Read from ADLS Gen2
df = spark.read.format("delta").load("abfss://lake@yourstorageaccount.dfs.core.windows.net/bronze/fx_rates")

# Write to ADLS Gen2
df.write.format("delta").mode("overwrite").save("abfss://lake@yourstorageaccount.dfs.core.windows.net/silver/fx_rates")
```

## 🔍 Monitoring

### Application Insights

- Custom metrics and logs
- Performance monitoring
- Error tracking

### Log Analytics

- Databricks cluster logs
- Application logs
- Custom queries

### Data Quality Reports

- Great Expectations Data Docs
- ADLS Gen2 hosted reports
- Automated quality alerts

## 🛠️ Development

### Local Testing

```bash
# Test Databricks CLI
databricks clusters list

# Test Key Vault access
az keyvault secret show --vault-name $KEY_VAULT_NAME --name salesforce-username

# Test storage access
az storage fs file list --account-name $STORAGE_ACCOUNT --file-system lake
```

### CI/CD Pipeline

The GitHub Actions workflow automatically:
1. Lints and tests code
2. Builds Python wheel
3. Deploys to Databricks
4. Runs data quality checks

## 📚 Additional Resources

- [Azure Databricks Documentation](https://docs.microsoft.com/en-us/azure/databricks/)
- [Delta Lake on Azure](https://docs.delta.io/latest/azure.html)
- [ADLS Gen2 Documentation](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)
- [Azure Key Vault Documentation](https://docs.microsoft.com/en-us/azure/key-vault/)
