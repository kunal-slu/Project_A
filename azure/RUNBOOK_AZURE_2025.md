# Azure Data Platform Runbook 2025
**Updated: 2025-01-27**

## 🎯 Overview

This runbook provides step-by-step instructions for deploying and operating the PySpark data engineering platform on Azure. The platform leverages Databricks, ADLS Gen2, Key Vault, and Data Factory for a complete data lakehouse solution.

## 📋 Prerequisites

### Required Tools & Versions
- **Azure CLI**: v2.50.0+
- **Terraform**: v1.6.0+
- **Python**: 3.10+
- **Databricks CLI**: v0.18.0+
- **Docker**: 20.10+ (for local testing)
- **jq**: 1.6+ (for JSON processing)

### Azure Account Setup
- Azure subscription with appropriate permissions
- Service Principal or User with Contributor access
- Azure region: `East US` (recommended)

### Environment Variables
```bash
export AZURE_LOCATION="East US"
export PROJECT="pyspark-de-project"
export ENVIRONMENT="dev"
export AZURE_SUBSCRIPTION_ID=$(az account show --query id --output tsv)
export AZURE_TENANT_ID=$(az account show --query tenantId --output tsv)
```

## 🏗️ Infrastructure Deployment

### Step 1: Deploy Core Infrastructure

```bash
# Navigate to Azure infrastructure
cd azure/infra/terraform

# Initialize Terraform
terraform init

# Review the plan
terraform plan -var="project=$PROJECT" -var="environment=$ENVIRONMENT"

# Deploy infrastructure
terraform apply -var="project=$PROJECT" -var="environment=$ENVIRONMENT"

# Save critical outputs
export RESOURCE_GROUP=$(terraform output -raw resource_group_name)
export STORAGE_ACCOUNT=$(terraform output -raw storage_account_name)
export DATABRICKS_WORKSPACE_URL=$(terraform output -raw databricks_workspace_url)
export KEY_VAULT_NAME=$(terraform output -raw key_vault_name)
export KEY_VAULT_URI=$(terraform output -raw key_vault_uri)
export DATA_FACTORY_NAME=$(terraform output -raw data_factory_name)
export LAKE_ROOT=$(terraform output -raw lake_root_url)
```

### Step 2: Verify Infrastructure

```bash
# Verify resource group
az group show --name $RESOURCE_GROUP

# Verify storage account
az storage account show --name $STORAGE_ACCOUNT --resource-group $RESOURCE_GROUP

# Verify Databricks workspace
az databricks workspace show --resource-group $RESOURCE_GROUP --name "$PROJECT-$ENVIRONMENT-dbw"

# Verify Key Vault
az keyvault show --name $KEY_VAULT_NAME --resource-group $RESOURCE_GROUP
```

## 🔐 Secrets Management

### Step 3: Create Secrets in Key Vault

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

# Create Kafka credentials (if using Confluent Cloud)
az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "kafka-bootstrap-servers" \
  --value "your-confluent-bootstrap-servers"

az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "kafka-api-key" \
  --value "your-confluent-api-key"

az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name "kafka-api-secret" \
  --value "your-confluent-api-secret"
```

## 📊 Databricks Setup

### Step 4: Configure Databricks CLI

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure Databricks CLI
databricks configure --token

# Get Databricks workspace URL
echo "Databricks Workspace URL: $DATABRICKS_WORKSPACE_URL"
```

### Step 5: Import Notebooks to Databricks

```bash
# Create workspace directory
databricks workspace mkdirs /Workspace/Repos/your-repo/notebooks

# Import notebooks
databricks workspace import_dir \
  azure/databricks/notebooks/ \
  /Workspace/Repos/your-repo/notebooks/ \
  --language PYTHON

# Verify import
databricks workspace ls /Workspace/Repos/your-repo/notebooks/
```

### Step 6: Configure Databricks Cluster

```bash
# Create cluster configuration
cat > cluster_config.json << EOF
{
  "cluster_name": "$PROJECT-$ENVIRONMENT-cluster",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "num_workers": 2,
  "spark_conf": {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.AzureBlobLogStore"
  },
  "spark_env_vars": {
    "PYTHONPATH": "/Workspace/Repos/your-repo/src",
    "LAKE_ROOT": "$LAKE_ROOT",
    "LOG_LEVEL": "INFO"
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

# Get cluster ID
export CLUSTER_ID=$(databricks clusters list --output json | jq -r '.clusters[] | select(.cluster_name=="'$PROJECT-$ENVIRONMENT-cluster'") | .cluster_id')
echo "Cluster ID: $CLUSTER_ID"
```

## 🔄 Data Pipeline Execution

### Step 7: FX Data Pipeline (01 → 02)

```bash
# Run FX ingestion notebook
databricks jobs create --json-file - << EOF
{
  "name": "fx-ingestion",
  "existing_cluster_id": "$CLUSTER_ID",
  "notebook_task": {
    "notebook_path": "/Workspace/Repos/your-repo/notebooks/01_ingest_fx_rest",
    "base_parameters": {
      "LAKE_ROOT": "$LAKE_ROOT",
      "LOG_LEVEL": "INFO"
    }
  }
}
EOF

# Run FX bronze to silver processing
databricks jobs create --json-file - << EOF
{
  "name": "fx-bronze-to-silver",
  "existing_cluster_id": "$CLUSTER_ID",
  "notebook_task": {
    "notebook_path": "/Workspace/Repos/your-repo/notebooks/02_bronze_to_silver_fx",
    "base_parameters": {
      "LAKE_ROOT": "$LAKE_ROOT",
      "LOG_LEVEL": "INFO"
    }
  }
}
EOF

# Run both jobs
databricks jobs run-now --job-id <fx-ingestion-job-id>
databricks jobs run-now --job-id <fx-bronze-to-silver-job-id>
```

### Step 8: Salesforce Incremental Pipeline (03)

```bash
# Run Salesforce ingestion notebook
databricks jobs create --json-file - << EOF
{
  "name": "salesforce-ingestion",
  "existing_cluster_id": "$CLUSTER_ID",
  "notebook_task": {
    "notebook_path": "/Workspace/Repos/your-repo/notebooks/03_salesforce_to_bronze",
    "base_parameters": {
      "LAKE_ROOT": "$LAKE_ROOT",
      "LOG_LEVEL": "INFO"
    }
  }
}
EOF

# Run Salesforce job
databricks jobs run-now --job-id <salesforce-ingestion-job-id>
```

### Step 9: Kafka Streaming Pipeline (04)

```bash
# Run Kafka streaming notebook
databricks jobs create --json-file - << EOF
{
  "name": "kafka-streaming",
  "existing_cluster_id": "$CLUSTER_ID",
  "notebook_task": {
    "notebook_path": "/Workspace/Repos/your-repo/notebooks/04_kafka_orders_stream",
    "base_parameters": {
      "LAKE_ROOT": "$LAKE_ROOT",
      "LOG_LEVEL": "INFO"
    }
  }
}
EOF

# Run Kafka streaming job
databricks jobs run-now --job-id <kafka-streaming-job-id>

# Produce sample events (in separate terminal)
python scripts/kafka_producer.py \
  --bootstrap-servers $(az keyvault secret show --vault-name $KEY_VAULT_NAME --name kafka-bootstrap-servers --query value -o tsv) \
  --api-key $(az keyvault secret show --vault-name $KEY_VAULT_NAME --name kafka-api-key --query value -o tsv) \
  --api-secret $(az keyvault secret show --vault-name $KEY_VAULT_NAME --name kafka-api-secret --query value -o tsv) \
  --topic orders_events \
  --num-orders 50
```

### Step 10: Snowflake Backfill Pipeline (05)

```bash
# Run Snowflake ingestion notebook
databricks jobs create --json-file - << EOF
{
  "name": "snowflake-ingestion",
  "existing_cluster_id": "$CLUSTER_ID",
  "notebook_task": {
    "notebook_path": "/Workspace/Repos/your-repo/notebooks/05_snowflake_to_bronze",
    "base_parameters": {
      "LAKE_ROOT": "$LAKE_ROOT",
      "LOG_LEVEL": "INFO"
    }
  }
}
EOF

# Run Snowflake job
databricks jobs run-now --job-id <snowflake-ingestion-job-id>
```

### Step 11: Silver to Gold Processing (06)

```bash
# Run silver to gold notebook
databricks jobs create --json-file - << EOF
{
  "name": "silver-to-gold",
  "existing_cluster_id": "$CLUSTER_ID",
  "notebook_task": {
    "notebook_path": "/Workspace/Repos/your-repo/notebooks/06_silver_to_gold",
    "base_parameters": {
      "LAKE_ROOT": "$LAKE_ROOT",
      "LOG_LEVEL": "INFO"
    }
  }
}
EOF

# Run silver to gold job
databricks jobs run-now --job-id <silver-to-gold-job-id>
```

## 📊 Data Catalog & Querying

### Step 12: Unity Catalog Registration (Optional)

```bash
# Create catalog (if Unity Catalog is enabled)
databricks sql query \
  --query "CREATE CATALOG IF NOT EXISTS $PROJECT"

# Create schemas
databricks sql query \
  --query "CREATE SCHEMA IF NOT EXISTS $PROJECT.silver"

databricks sql query \
  --query "CREATE SCHEMA IF NOT EXISTS $PROJECT.gold"

# Register tables
databricks sql query \
  --query "CREATE TABLE IF NOT EXISTS $PROJECT.silver.fx_rates USING DELTA LOCATION '$LAKE_ROOT/silver/fx_rates'"

databricks sql query \
  --query "CREATE TABLE IF NOT EXISTS $PROJECT.silver.orders USING DELTA LOCATION '$LAKE_ROOT/silver/orders'"

databricks sql query \
  --query "CREATE TABLE IF NOT EXISTS $PROJECT.gold.dim_fx USING DELTA LOCATION '$LAKE_ROOT/gold/dim_fx'"

databricks sql query \
  --query "CREATE TABLE IF NOT EXISTS $PROJECT.gold.fact_orders USING DELTA LOCATION '$LAKE_ROOT/gold/fact_orders'"
```

### Step 13: Query Data in Databricks

```sql
-- Sample queries in Databricks SQL
-- FX rates analysis
SELECT 
    ccy,
    rate_to_base,
    as_of_date,
    rate_category
FROM $PROJECT.silver.fx_rates
WHERE as_of_date >= current_date() - interval 7 days
ORDER BY as_of_date DESC, ccy;

-- Orders analysis
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(total_amount) as total_spent,
    AVG(total_amount) as avg_order_value
FROM $PROJECT.silver.orders
WHERE order_date >= current_date() - interval 30 days
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 10;

-- Cross-layer analysis
SELECT 
    o.customer_id,
    o.order_date,
    o.total_amount,
    f.rate_to_base as usd_rate
FROM $PROJECT.silver.orders o
LEFT JOIN $PROJECT.silver.fx_rates f
  ON o.currency = f.ccy
  AND o.order_date = f.as_of_date
WHERE o.order_date >= current_date() - interval 7 days;
```

## 🔍 Data Quality Gates

### Step 14: Run Data Quality Checks

```bash
# Create data quality notebook
cat > azure/databricks/notebooks/07_data_quality_checks.py << 'EOF'
# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Data Quality Checks
# MAGIC 
# MAGIC This notebook runs Great Expectations data quality checks on silver and gold tables.

# COMMAND ----------

import os
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, min as spark_min, max as spark_max

# Get configuration
lake_root = os.getenv("LAKE_ROOT", "abfss://lake@yourstorageaccount.dfs.core.windows.net")
log_level = os.getenv("LOG_LEVEL", "INFO")

# Set log level
spark.sparkContext.setLogLevel(log_level)

print(f"Lake root: {lake_root}")
print(f"Log level: {log_level}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## FX Rates Data Quality Checks

# COMMAND ----------

def check_fx_rates_quality():
    """Check data quality for FX rates table."""
    print("Checking FX rates data quality...")
    
    # Read silver FX rates
    fx_df = spark.read.format("delta").load(f"{lake_root}/silver/fx_rates")
    
    # Check 1: No null currency codes
    null_ccy_count = fx_df.filter(col("ccy").isNull()).count()
    if null_ccy_count > 0:
        raise Exception(f"FAIL: Found {null_ccy_count} records with null currency codes")
    
    # Check 2: Unique combination of (as_of_date, ccy)
    total_count = fx_df.count()
    unique_count = fx_df.select("as_of_date", "ccy").distinct().count()
    if total_count != unique_count:
        raise Exception(f"FAIL: Found duplicate (as_of_date, ccy) combinations. Total: {total_count}, Unique: {unique_count}")
    
    # Check 3: Positive rates
    negative_rates = fx_df.filter(col("rate_to_base") <= 0).count()
    if negative_rates > 0:
        raise Exception(f"FAIL: Found {negative_rates} records with non-positive rates")
    
    print("✅ FX rates data quality checks passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders Data Quality Checks

# COMMAND ----------

def check_orders_quality():
    """Check data quality for orders table."""
    print("Checking orders data quality...")
    
    # Read silver orders
    orders_df = spark.read.format("delta").load(f"{lake_root}/silver/orders")
    
    # Check 1: No null order IDs
    null_order_id_count = orders_df.filter(col("order_id").isNull()).count()
    if null_order_id_count > 0:
        raise Exception(f"FAIL: Found {null_order_id_count} records with null order IDs")
    
    # Check 2: Non-negative amounts
    negative_amounts = orders_df.filter(col("amount") < 0).count()
    if negative_amounts > 0:
        raise Exception(f"FAIL: Found {negative_amounts} records with negative amounts")
    
    # Check 3: Unique order IDs
    total_count = orders_df.count()
    unique_count = orders_df.select("order_id").distinct().count()
    if total_count != unique_count:
        raise Exception(f"FAIL: Found duplicate order IDs. Total: {total_count}, Unique: {unique_count}")
    
    print("✅ Orders data quality checks passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution

# COMMAND ----------

try:
    # Run quality checks
    check_fx_rates_quality()
    check_orders_quality()
    
    print("🎉 All data quality checks passed successfully!")
    
except Exception as e:
    print(f"❌ Data quality checks failed: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("✅ Data quality checks completed!")
print(f"📊 Checked tables: fx_rates, orders")
print(f"📅 Check date: {datetime.now().isoformat()}")
print(f"💾 Storage location: {lake_root}")
EOF

# Import DQ notebook
databricks workspace import \
  azure/databricks/notebooks/07_data_quality_checks.py \
  /Workspace/Repos/your-repo/notebooks/07_data_quality_checks \
  --language PYTHON

# Run DQ checks
databricks jobs create --json-file - << EOF
{
  "name": "data-quality-checks",
  "existing_cluster_id": "$CLUSTER_ID",
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
databricks jobs run-now --job-id <data-quality-checks-job-id>
```

### Step 15: Demonstrate DQ Failure Behavior

```bash
# Create test data with quality issues
echo '{"ccy": null, "rate_to_base": -1.0, "as_of_date": "2025-01-27"}' | az storage blob upload \
  --account-name $STORAGE_ACCOUNT \
  --container-name lake \
  --name silver/fx_rates/test_bad_data.json \
  --file -

# Run DQ checks (should fail)
databricks jobs run-now --job-id <data-quality-checks-job-id>

# Clean up test data
az storage blob delete \
  --account-name $STORAGE_ACCOUNT \
  --container-name lake \
  --name silver/fx_rates/test_bad_data.json
```

## 🔗 Data Lineage

### Step 16: Configure Data Lineage

```bash
# Create lineage documentation
cat > azure/docs/lineage_setup.md << 'EOF'
# Data Lineage Setup

## OpenLineage Integration

1. Install OpenLineage agent on Databricks cluster
2. Configure environment variables:
   - OPENLINEAGE_URL: http://your-openlineage-server:8080
   - OPENLINEAGE_NAMESPACE: pyspark-de-project-azure

## Lineage Tracking

- Source systems: REST API, Salesforce, Kafka, Snowflake
- Bronze layer: Raw data ingestion
- Silver layer: Cleaned and validated data
- Gold layer: Business-ready data marts

## Lineage Visualization

Use OpenLineage UI to visualize data flow and dependencies.
EOF
```

## 🛡️ Data Governance

### Step 17: Unity Catalog Governance (Optional)

```bash
# Create governance documentation
cat > azure/docs/governance_setup.md << 'EOF'
# Data Governance Setup

## Unity Catalog Configuration

1. Enable Unity Catalog on Databricks workspace
2. Create catalogs and schemas
3. Set up access controls

## Sample Grants

```sql
-- Grant access to silver schema
GRANT USAGE ON SCHEMA pyspark_de_project.silver TO `data_analysts@yourcompany.com`;

-- Grant access to specific table
GRANT SELECT ON TABLE pyspark_de_project.silver.fx_rates TO `data_analysts@yourcompany.com`;

-- Grant access to gold schema
GRANT USAGE ON SCHEMA pyspark_de_project.gold TO `business_users@yourcompany.com`;
GRANT SELECT ON SCHEMA pyspark_de_project.gold TO `business_users@yourcompany.com`;
```

## Data Classification

- PII: Customer email addresses
- Internal: Order amounts, product details
- Public: FX rates, general product information
EOF
```

## 💰 Cost & File Health

### Step 18: Delta Optimization

```bash
# Create Delta optimization notebook
cat > azure/databricks/notebooks/08_delta_optimize_vacuum.py << 'EOF'
# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - Delta Table Optimization and Vacuum
# MAGIC 
# MAGIC This notebook optimizes Delta tables by compacting small files and running vacuum.

# COMMAND ----------

import os
from delta.tables import DeltaTable

# Get configuration
lake_root = os.getenv("LAKE_ROOT", "abfss://lake@yourstorageaccount.dfs.core.windows.net")
log_level = os.getenv("LOG_LEVEL", "INFO")

# Set log level
spark.sparkContext.setLogLevel(log_level)

print(f"Lake root: {lake_root}")
print(f"Log level: {log_level}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Silver Tables

# COMMAND ----------

def optimize_table(table_path: str, table_name: str):
    """Optimize a Delta table."""
    print(f"Optimizing {table_name} at {table_path}")
    
    try:
        # Read Delta table
        delta_table = DeltaTable.forPath(spark, table_path)
        
        # Get current file statistics
        file_stats = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
        num_files = file_stats['numFiles']
        
        print(f"Table {table_name}: {num_files} files")
        
        # Optimize if there are many small files
        if num_files > 100:
            print(f"Optimizing {table_name} - compacting {num_files} files")
            delta_table.optimize().executeCompaction()
            print(f"Optimization completed for {table_name}")
        else:
            print(f"Skipping optimization for {table_name} - only {num_files} files")
            
    except Exception as e:
        print(f"Failed to optimize table {table_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vacuum Tables

# COMMAND ----------

def vacuum_table(table_path: str, table_name: str, retention_hours: int = 168):
    """Vacuum a Delta table."""
    print(f"Vacuuming {table_name} at {table_path}")
    
    try:
        # Read Delta table
        delta_table = DeltaTable.forPath(spark, table_path)
        
        # Run vacuum
        delta_table.vacuum(retentionHours=retention_hours)
        
        print(f"Vacuum completed for {table_name}")
        
    except Exception as e:
        print(f"Failed to vacuum table {table_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution

# COMMAND ----------

# Define tables to optimize
tables = [
    (f"{lake_root}/silver/fx_rates", "fx_rates"),
    (f"{lake_root}/silver/orders", "orders"),
    (f"{lake_root}/gold/dim_fx", "dim_fx"),
    (f"{lake_root}/gold/fact_orders", "fact_orders")
]

# Optimize and vacuum each table
for table_path, table_name in tables:
    try:
        optimize_table(table_path, table_name)
        vacuum_table(table_path, table_name)
    except Exception as e:
        print(f"Failed to process table {table_name}: {e}")

print("✅ Delta optimization and vacuum completed!")
EOF

# Import optimization notebook
databricks workspace import \
  azure/databricks/notebooks/08_delta_optimize_vacuum.py \
  /Workspace/Repos/your-repo/notebooks/08_delta_optimize_vacuum \
  --language PYTHON

# Create optimization job
databricks jobs create --json-file - << EOF
{
  "name": "delta-optimization",
  "existing_cluster_id": "$CLUSTER_ID",
  "notebook_task": {
    "notebook_path": "/Workspace/Repos/your-repo/notebooks/08_delta_optimize_vacuum",
    "base_parameters": {
      "LAKE_ROOT": "$LAKE_ROOT",
      "LOG_LEVEL": "INFO"
    }
  },
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "UTC"
  }
}
EOF
```

## 📊 Monitoring & Alerting

### Step 19: Setup Application Insights

```bash
# Get Application Insights key
export APP_INSIGHTS_KEY=$(az monitor app-insights component show \
  --app "$PROJECT-$ENVIRONMENT-ai" \
  --resource-group $RESOURCE_GROUP \
  --query instrumentationKey \
  --output tsv)

# Create monitoring dashboard
cat > azure/monitoring/dashboard.json << EOF
{
  "version": "Notebook/1.0",
  "items": [
    {
      "type": "heading",
      "level": 1,
      "text": "Data Platform Monitoring Dashboard"
    },
    {
      "type": "heading",
      "level": 2,
      "text": "Key Metrics"
    },
    {
      "type": "text",
      "text": "• Databricks job success rate\n• Data quality check results\n• Storage usage\n• Cost trends"
    }
  ]
}
EOF
```

## 🔄 Disaster Recovery

### Step 20: DR & Rollback Procedures

```bash
# Enable ADLS Gen2 geo-redundant storage
az storage account update \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --replication-type GRS

# Create DR documentation
cat > azure/docs/disaster_recovery.md << 'EOF'
# Disaster Recovery Procedures

## Backup Strategy

1. **ADLS Gen2**: Geo-redundant storage enabled
2. **Databricks**: Notebooks stored in Git
3. **Key Vault**: Soft delete enabled
4. **Terraform**: State stored in Azure Storage

## Recovery Procedures

### Data Recovery
1. Restore from geo-redundant storage
2. Reprocess specific partitions
3. Re-run data quality checks

### Infrastructure Recovery
1. Deploy infrastructure via Terraform
2. Restore secrets from Key Vault
3. Import notebooks to Databricks
4. Recreate jobs and schedules

## Rollback Procedures

### Reprocess by Partition
```python
# Reprocess specific date partition
df = spark.read.format("delta").load(f"{lake_root}/bronze/fx_rates")
df.filter(col("_proc_date") == "2025-01-27").write.mode("overwrite").save(f"{lake_root}/bronze/fx_rates")
```

### Full Pipeline Re-run
```bash
# Re-run entire pipeline for specific date
databricks jobs run-now --job-id <pipeline-job-id> --python-params '["--partition-date", "2025-01-27"]'
```
EOF
```

## 🚨 Troubleshooting

### Common Issues & Solutions

#### Databricks Cluster Issues
```bash
# Check cluster status
databricks clusters get --cluster-id $CLUSTER_ID

# Restart cluster
databricks clusters restart --cluster-id $CLUSTER_ID

# Check cluster logs
databricks clusters events --cluster-id $CLUSTER_ID
```

#### ADLS Gen2 Access Issues
```bash
# Check storage account access
az storage account show --name $STORAGE_ACCOUNT --resource-group $RESOURCE_GROUP

# Check container access
az storage container show --name lake --account-name $STORAGE_ACCOUNT

# Test access from Databricks
# Use dbutils.fs.ls("abfss://lake@yourstorageaccount.dfs.core.windows.net/")
```

#### Key Vault Access Issues
```bash
# Check Key Vault access
az keyvault secret list --vault-name $KEY_VAULT_NAME

# Test secret retrieval
az keyvault secret show --vault-name $KEY_VAULT_NAME --name salesforce-username

# Check Databricks secret scope
# Use dbutils.secrets.list("salesforce")
```

#### Delta Lake Issues
```bash
# Check Delta table history
az storage blob list --account-name $STORAGE_ACCOUNT --container-name lake --prefix silver/fx_rates/_delta_log/

# Repair Delta table (in Databricks)
# spark.sql("REPAIR TABLE delta.`abfss://lake@yourstorageaccount.dfs.core.windows.net/silver/fx_rates`")
```

## 📈 Performance Optimization

### Step 21: Performance Tuning

```bash
# Enable ADLS Gen2 hierarchical namespace optimization
az storage account update \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --enable-hierarchical-namespace true

# Configure Databricks cluster for performance
databricks clusters edit --cluster-id $CLUSTER_ID --json-file - << EOF
{
  "spark_conf": {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true"
  }
}
EOF
```

## 🔐 Security Best Practices

### Step 22: Security Hardening

```bash
# Enable Key Vault soft delete
az keyvault update \
  --name $KEY_VAULT_NAME \
  --resource-group $RESOURCE_GROUP \
  --enable-soft-delete true \
  --soft-delete-retention-days 90

# Enable ADLS Gen2 encryption
az storage account update \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --encryption-services blob

# Configure Databricks security
databricks workspace-conf set --json '{"enableIpAccessLists": true}'
```

## 📋 Maintenance Checklist

### Daily
- [ ] Check Databricks job status
- [ ] Review data quality reports
- [ ] Monitor ADLS Gen2 storage usage
- [ ] Check Application Insights logs

### Weekly
- [ ] Run Delta table optimization
- [ ] Review and clean up old logs
- [ ] Check cost reports
- [ ] Update documentation if needed

### Monthly
- [ ] Review and update access policies
- [ ] Check for security vulnerabilities
- [ ] Review and optimize costs
- [ ] Update dependencies

## 📞 Support Contacts

- **Primary On-Call**: data-engineering@yourcompany.com
- **Secondary On-Call**: platform-engineering@yourcompany.com
- **Escalation**: engineering-manager@yourcompany.com
- **Azure Support**: [Support Case URL]

---

**Last Updated**: 2025-01-27  
**Version**: 1.0  
**Maintainer**: Data Engineering Team
