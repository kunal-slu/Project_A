# 🚀 Azure Quick Start Guide

## ⚡ **5-Minute Azure Setup**

### 1. **Prerequisites Check**
```bash
# Check Azure CLI
az --version

# Login to Azure
az login
```

### 2. **One-Click Deployment**
```bash
# Run automated deployment script
./scripts/azure_deploy.sh
```

### 3. **Access Your Resources**
- **Databricks**: https://[WORKSPACE_URL] (from script output)
- **Storage**: Azure Portal → Storage Accounts → pysparketlstorage
- **Key Vault**: Azure Portal → Key Vaults → pyspark-etl-kv

## 📋 **What Gets Created**

| Resource | Name | Purpose |
|----------|------|---------|
| **Resource Group** | `pyspark-etl-rg` | Groups all resources |
| **Storage Account** | `pysparketlstorage` | Data Lake Storage Gen2 |
| **Databricks** | `pyspark-etl-workspace` | Spark compute |
| **Key Vault** | `pyspark-etl-kv` | Secrets management |

### **Storage Containers**
- `lakehouse/` - Bronze/Silver/Gold data
- `input-data/` - Source files
- `output-data/` - Final results
- `backups/` - Disaster recovery

## 🎯 **Quick Commands**

### **Upload Data**
```bash
# Upload all data files
az storage blob upload-batch \
  --account-name pysparketlstorage \
  --source data/input_data \
  --destination input-data
```

### **Run ETL Pipeline**
```python
# In Databricks notebook
from pyspark_interview_project import run_pipeline, get_spark_session, load_config_resolved

config = load_config_resolved("/dbfs/pyspark-etl/config/config-azure-dev.yaml")
spark = get_spark_session(config)
run_pipeline(spark, config)
```

### **Check Results**
```bash
# List processed data
az storage blob list \
  --account-name pysparketlstorage \
  --container-name lakehouse \
  --prefix gold/ \
  --output table
```

## 🔧 **Troubleshooting**

### **Common Issues**

| Issue | Solution |
|-------|----------|
| **Authentication Failed** | `az login` |
| **Storage Not Found** | Check resource group exists |
| **Databricks Not Loading** | Wait 5-10 minutes after creation |
| **Data Upload Failed** | Check file paths and permissions |

### **Cost Optimization**
```bash
# Stop Databricks cluster when not in use
az databricks cluster delete \
  --workspace-name pyspark-etl-workspace \
  --cluster-name your-cluster-name
```

## 📊 **Monitor Your Pipeline**

### **Azure Portal**
1. Go to [Azure Portal](https://portal.azure.com)
2. Navigate to `pyspark-etl-rg`
3. Check:
   - **Storage Account** → Monitoring
   - **Databricks** → Clusters & Jobs
   - **Key Vault** → Access policies

### **Databricks UI**
- **Clusters**: Monitor compute usage
- **Jobs**: View pipeline runs
- **Data**: Browse lakehouse structure
- **SQL**: Query results directly

## 🎉 **Success Checklist**

- ✅ Azure resources created
- ✅ Data uploaded to storage
- ✅ Databricks workspace accessible
- ✅ ETL pipeline running
- ✅ Results in gold layer
- ✅ SCD2 validation passing

## 📞 **Need Help?**

1. **Check logs**: Azure Portal → Monitor → Logs
2. **Databricks support**: Workspace → Help → Support
3. **Documentation**: See `AZURE_DEPLOYMENT_GUIDE.md`
4. **Community**: Azure Data Engineering forums

---

**Your PySpark ETL is now running on Azure!** 🚀
