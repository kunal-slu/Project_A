# 🚀 Azure Deployment Guide - Complete Step-by-Step Instructions

## 📋 **Prerequisites**

Before starting, ensure you have:
- ✅ Azure subscription (free tier works for testing)
- ✅ Azure CLI installed: `az --version`
- ✅ Python 3.11+ installed
- ✅ Git installed
- ✅ Basic knowledge of Azure services

## 🎯 **Step 1: Azure Account Setup**

### 1.1 Create Azure Account
```bash
# If you don't have an Azure account, create one at:
# https://azure.microsoft.com/free/
```

### 1.2 Install Azure CLI
```bash
# macOS
brew install azure-cli

# Windows
# Download from: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli-windows

# Linux
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
```

### 1.3 Login to Azure
```bash
az login
# This will open a browser window for authentication
```

### 1.4 Set Subscription (if you have multiple)
```bash
# List subscriptions
az account list --output table

# Set active subscription
az account set --subscription "Your-Subscription-Name"
```

## 🏗️ **Step 2: Create Azure Resources**

### 2.1 Create Resource Group
```bash
# Create resource group
az group create \
  --name "pyspark-etl-rg" \
  --location "East US" \
  --tags "project=pyspark-etl" "environment=dev"

# Verify creation
az group show --name "pyspark-etl-rg"
```

### 2.2 Create Storage Account (Data Lake Storage Gen2)
```bash
# Create storage account
az storage account create \
  --name "pysparketlstorage" \
  --resource-group "pyspark-etl-rg" \
  --location "East US" \
  --sku "Standard_LRS" \
  --kind "StorageV2" \
  --hierarchical-namespace true \
  --min-tls-version "TLS1_2"

# Get storage account key
STORAGE_KEY=$(az storage account keys list \
  --account-name "pysparketlstorage" \
  --resource-group "pyspark-etl-rg" \
  --query "[0].value" \
  --output tsv)

echo "Storage Key: $STORAGE_KEY"
```

### 2.3 Create Data Lake File System
```bash
# Create container for lakehouse
az storage container create \
  --name "lakehouse" \
  --account-name "pysparketlstorage" \
  --account-key "$STORAGE_KEY"

# Create additional containers
az storage container create \
  --name "input-data" \
  --account-name "pysparketlstorage" \
  --account-key "$STORAGE_KEY"

az storage container create \
  --name "output-data" \
  --account-name "pysparketlstorage" \
  --account-key "$STORAGE_KEY"
```

### 2.4 Create Azure Databricks Workspace
```bash
# Create Databricks workspace
az databricks workspace create \
  --resource-group "pyspark-etl-rg" \
  --workspace-name "pyspark-etl-workspace" \
  --location "East US" \
  --sku "standard"

# Get workspace URL
WORKSPACE_URL=$(az databricks workspace show \
  --resource-group "pyspark-etl-rg" \
  --workspace-name "pyspark-etl-workspace" \
  --query "workspaceUrl" \
  --output tsv)

echo "Databricks Workspace URL: https://$WORKSPACE_URL"
```

### 2.5 Create Key Vault (for secrets)
```bash
# Create Key Vault
az keyvault create \
  --name "pyspark-etl-kv" \
  --resource-group "pyspark-etl-rg" \
  --location "East US" \
  --sku "standard"

# Store storage account key in Key Vault
az keyvault secret set \
  --vault-name "pyspark-etl-kv" \
  --name "storage-account-key" \
  --value "$STORAGE_KEY"
```

## 📊 **Step 3: Import Data to Azure**

### 3.1 Upload Sample Data to Azure Storage

#### Option A: Using Azure CLI
```bash
# Upload customers data
az storage blob upload \
  --account-name "pysparketlstorage" \
  --account-key "$STORAGE_KEY" \
  --container-name "input-data" \
  --name "customers.csv" \
  --file "data/input_data/customers.csv" \
  --type block

# Upload products data
az storage blob upload \
  --account-name "pysparketlstorage" \
  --account-key "$STORAGE_KEY" \
  --container-name "input-data" \
  --name "products.csv" \
  --file "data/input_data/products.csv" \
  --type block

# Upload orders data
az storage blob upload \
  --account-name "pysparketlstorage" \
  --account-key "$STORAGE_KEY" \
  --container-name "input-data" \
  --name "orders.json" \
  --file "data/input_data/orders.json" \
  --type block

# Upload returns data
az storage blob upload \
  --account-name "pysparketlstorage" \
  --account-key "$STORAGE_KEY" \
  --container-name "input-data" \
  --name "returns.json" \
  --file "data/input_data/returns.json" \
  --type block

# Upload exchange rates
az storage blob upload \
  --account-name "pysparketlstorage" \
  --account-key "$STORAGE_KEY" \
  --container-name "input-data" \
  --name "exchange_rates.csv" \
  --file "data/input_data/exchange_rates.csv" \
  --type block

# Upload inventory data
az storage blob upload \
  --account-name "pysparketlstorage" \
  --account-key "$STORAGE_KEY" \
  --container-name "input-data" \
  --name "inventory_snapshots.csv" \
  --file "data/input_data/inventory_snapshots.csv" \
  --type block

# Upload customer changes
az storage blob upload \
  --account-name "pysparketlstorage" \
  --account-key "$STORAGE_KEY" \
  --container-name "input-data" \
  --name "customers_changes.csv" \
  --file "data/input_data/customers_changes.csv" \
  --type block
```

#### Option B: Using Azure Storage Explorer
1. Download [Azure Storage Explorer](https://azure.microsoft.com/en-us/features/storage-explorer/)
2. Connect to your storage account using the connection string
3. Navigate to `input-data` container
4. Upload all files from `data/input_data/` folder

### 3.2 Verify Data Upload
```bash
# List uploaded files
az storage blob list \
  --account-name "pysparketlstorage" \
  --account-key "$STORAGE_KEY" \
  --container-name "input-data" \
  --output table
```

## ⚙️ **Step 4: Configure Azure Settings**

### 4.1 Update Configuration Files

#### Update `config/config-azure-dev.yaml`:
```yaml
# Azure-specific configuration
azure:
  storage:
    account_name: "pysparketlstorage"
    account_key: "${SECRET:azure:storage-account-key}"
    container_name: "lakehouse"
  databricks:
    workspace_url: "https://${WORKSPACE_URL}"
    token: "${SECRET:databricks:token}"

# Input data paths (Azure Storage)
input:
  customer_path: "abfss://input-data@pysparketlstorage.dfs.core.windows.net/customers.csv"
  product_path: "abfss://input-data@pysparketlstorage.dfs.core.windows.net/products.csv"
  orders_path: "abfss://input-data@pysparketlstorage.dfs.core.windows.net/orders.json"
  returns_path: "abfss://input-data@pysparketlstorage.dfs.core.windows.net/returns.json"
  exchange_rates_path: "abfss://input-data@pysparketlstorage.dfs.core.windows.net/exchange_rates.csv"
  inventory_path: "abfss://input-data@pysparketlstorage.dfs.core.windows.net/inventory_snapshots.csv"
  customers_changes_path: "abfss://input-data@pysparketlstorage.dfs.core.windows.net/customers_changes.csv"

# Output data paths (Azure Storage)
output:
  bronze_path: "abfss://lakehouse@pysparketlstorage.dfs.core.windows.net/bronze"
  silver_path: "abfss://lakehouse@pysparketlstorage.dfs.core.windows.net/silver"
  gold_path: "abfss://lakehouse@pysparketlstorage.dfs.core.windows.net/gold"
  parquet_path: "abfss://output-data@pysparketlstorage.dfs.core.windows.net/final.parquet"
  delta_path: "abfss://output-data@pysparketlstorage.dfs.core.windows.net/final_delta"

# Spark configuration for Azure
spark:
  app_name: "PySpark ETL Azure"
  config:
    spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension"
    spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    spark.hadoop.fs.azure.account.key.pysparketlstorage.dfs.core.windows.net: "${SECRET:azure:storage-account-key}"
    spark.hadoop.fs.azure.account.auth.type.pysparketlstorage.dfs.core.windows.net: "SharedKey"
```

### 4.2 Set Environment Variables
```bash
# Set Azure environment variables
export AZURE_STORAGE_ACCOUNT="pysparketlstorage"
export AZURE_STORAGE_KEY="$STORAGE_KEY"
export DATABRICKS_WORKSPACE_URL="$WORKSPACE_URL"
export DATA_ROOT="abfss://lakehouse@pysparketlstorage.dfs.core.windows.net"
```

## 🎯 **Step 5: Deploy to Azure Databricks**

### 5.1 Access Databricks Workspace
1. Go to: `https://$WORKSPACE_URL`
2. Sign in with your Azure credentials
3. Create a new cluster or use existing one

### 5.2 Create Databricks Cluster
```python
# In Databricks notebook, create cluster with these settings:
{
  "cluster_name": "pyspark-etl-cluster",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "num_workers": 2,
  "autotermination_minutes": 30,
  "spark_conf": {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  },
  "libraries": [
    {
      "pypi": {
        "package": "delta-spark==3.0.0"
      }
    }
  ]
}
```

### 5.3 Upload Project to Databricks
```bash
# Install Databricks CLI
pip install databricks-cli

# Configure Databricks CLI
databricks configure --token

# Create workspace folder
databricks fs mkdirs dbfs:/pyspark-etl

# Upload project files
databricks fs cp -r src/ dbfs:/pyspark-etl/
databricks fs cp -r config/ dbfs:/pyspark-etl/
databricks fs cp requirements.txt dbfs:/pyspark-etl/
```

## 🚀 **Step 6: Run ETL Pipeline on Azure**

### 6.1 Option A: Run via Databricks Notebook

Create a new notebook in Databricks:

```python
# Databricks notebook: Run ETL Pipeline
# Set up environment
dbutils.library.installPyPI("delta-spark", "3.0.0")
dbutils.library.restartPython()

# Import project modules
import sys
sys.path.append("/dbfs/pyspark-etl/src")

from pyspark_interview_project import (
    get_spark_session,
    load_config_resolved,
    create_monitor,
    run_pipeline
)

# Load Azure configuration
config = load_config_resolved("/dbfs/pyspark-etl/config/config-azure-dev.yaml")

# Initialize Spark session
spark = get_spark_session(config)

# Create monitor
monitor = create_monitor(spark, config)

# Run the complete pipeline
with monitor.monitor_pipeline("azure_etl"):
    run_pipeline(spark, config)

print("✅ ETL Pipeline completed successfully on Azure!")
```

### 6.2 Option B: Run via Azure Container Instances

#### Create Dockerfile for Azure:
```dockerfile
# Dockerfile.azure
FROM openjdk:11-jre-slim

# Install Python and dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application
COPY src/ ./src/
COPY config/ ./config/

# Set environment
ENV PYTHONPATH=/app/src
ENV SPARK_HOME=/opt/spark

# Run ETL
CMD ["python3", "-m", "pyspark_interview_project", "config/config-azure-dev.yaml"]
```

#### Deploy to Azure Container Instances:
```bash
# Build and push Docker image
docker build -f Dockerfile.azure -t pyspark-etl:azure .
docker tag pyspark-etl:azure pysparketlstorage.azurecr.io/pyspark-etl:latest

# Create Azure Container Registry
az acr create \
  --resource-group "pyspark-etl-rg" \
  --name "pysparketlstorage" \
  --sku "Basic"

# Push image
az acr login --name "pysparketlstorage"
docker push pysparketlstorage.azurecr.io/pyspark-etl:latest

# Run container
az container create \
  --resource-group "pyspark-etl-rg" \
  --name "pyspark-etl-job" \
  --image "pysparketlstorage.azurecr.io/pyspark-etl:latest" \
  --environment-variables \
    AZURE_STORAGE_ACCOUNT="pysparketlstorage" \
    AZURE_STORAGE_KEY="$STORAGE_KEY" \
  --restart-policy "Never"
```

### 6.3 Option C: Run via Azure Functions

#### Create Azure Function:
```python
# function_app.py
import azure.functions as func
import logging
from pyspark_interview_project import get_spark_session, load_config_resolved, run_pipeline

app = func.FunctionApp()

@app.function_name(name="RunETLPipeline")
@app.schedule(schedule="0 0 2 * * *", arg_name="myTimer", run_on_startup=True)
def run_etl_pipeline(myTimer: func.TimerRequest) -> None:
    """Run ETL pipeline on schedule."""
    try:
        # Load configuration
        config = load_config_resolved("config/config-azure-dev.yaml")
        
        # Initialize Spark session
        spark = get_spark_session(config)
        
        # Run pipeline
        run_pipeline(spark, config)
        
        logging.info("✅ ETL Pipeline completed successfully!")
        
    except Exception as e:
        logging.error(f"❌ ETL Pipeline failed: {str(e)}")
        raise
```

## 📊 **Step 7: Monitor and Validate**

### 7.1 Check Data in Azure Storage
```bash
# List bronze layer data
az storage blob list \
  --account-name "pysparketlstorage" \
  --account-key "$STORAGE_KEY" \
  --container-name "lakehouse" \
  --prefix "bronze/" \
  --output table

# List silver layer data
az storage blob list \
  --account-name "pysparketlstorage" \
  --account-key "$STORAGE_KEY" \
  --container-name "lakehouse" \
  --prefix "silver/" \
  --output table

# List gold layer data
az storage blob list \
  --account-name "pysparketlstorage" \
  --account-key "$STORAGE_KEY" \
  --container-name "lakehouse" \
  --prefix "gold/" \
  --output table
```

### 7.2 Monitor in Azure Portal
1. Go to [Azure Portal](https://portal.azure.com)
2. Navigate to your resource group: `pyspark-etl-rg`
3. Check:
   - **Storage Account**: Monitor data transfer and usage
   - **Databricks Workspace**: View cluster metrics and job history
   - **Key Vault**: Monitor secret access

### 7.3 Validate SCD2 Implementation
```python
# In Databricks notebook, validate SCD2
from tests.test_scd2_validation import validate_scd2_table

# Validate customer SCD2
results = validate_scd2_table(
    spark, 
    "abfss://lakehouse@pysparketlstorage.dfs.core.windows.net/silver/dim_customers_scd2",
    "customer_id"
)

print("SCD2 Validation Results:")
print(f"Validation Passed: {results['validation_passed']}")
print(f"Total Records: {results['total_records']}")
print(f"Current Records: {results['current_records']}")
print(f"Historical Records: {results['historical_records']}")
if results['errors']:
    print(f"Errors: {results['errors']}")
```

## 🔧 **Step 8: Troubleshooting**

### 8.1 Common Issues and Solutions

#### Issue: Authentication Failed
```bash
# Solution: Regenerate storage account key
az storage account keys renew \
  --account-name "pysparketlstorage" \
  --resource-group "pyspark-etl-rg" \
  --key "key1"

# Update Key Vault
az keyvault secret set \
  --vault-name "pyspark-etl-kv" \
  --name "storage-account-key" \
  --value "$NEW_STORAGE_KEY"
```

#### Issue: Delta Lake Not Available
```python
# Solution: Install Delta Lake in Databricks
dbutils.library.installPyPI("delta-spark", "3.0.0")
dbutils.library.restartPython()
```

#### Issue: Memory Issues
```python
# Solution: Increase cluster size or optimize
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

### 8.2 Cost Optimization
```bash
# Stop Databricks cluster when not in use
az databricks cluster delete \
  --workspace-name "pyspark-etl-workspace" \
  --resource-group "pyspark-etl-rg" \
  --cluster-name "pyspark-etl-cluster"

# Use autotermination for clusters
# Set autotermination_minutes: 30 in cluster config
```

## 📈 **Step 9: Production Deployment**

### 9.1 Set Up CI/CD Pipeline
```yaml
# .github/workflows/azure-deploy.yml
name: Deploy to Azure
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    
    - name: Deploy to Azure Databricks
      run: |
        # Deploy code to Databricks
        databricks fs cp -r src/ dbfs:/pyspark-etl/
        databricks fs cp -r config/ dbfs:/pyspark-etl/
        
    - name: Run ETL Pipeline
      run: |
        # Trigger ETL job
        databricks jobs submit --job-id ${{ secrets.DATABRICKS_JOB_ID }}
```

### 9.2 Set Up Monitoring
```python
# Add Azure Monitor integration
from azure.monitor import MonitorClient
from azure.identity import DefaultAzureCredential

# Send metrics to Azure Monitor
monitor_client = MonitorClient(
    credential=DefaultAzureCredential(),
    subscription_id="your-subscription-id"
)
```

## 🎉 **Success! Your ETL Pipeline is Running on Azure**

### **What You've Accomplished:**
1. ✅ **Azure infrastructure** created and configured
2. ✅ **Data imported** to Azure Storage
3. ✅ **ETL pipeline** deployed and running
4. ✅ **SCD2 implementation** validated
5. ✅ **Monitoring** and validation in place

### **Next Steps:**
1. **Scale up**: Increase cluster size for larger datasets
2. **Schedule**: Set up automated ETL runs
3. **Monitor**: Set up alerts and dashboards
4. **Optimize**: Fine-tune performance and costs

**Your PySpark ETL project is now running on Azure!** 🚀
