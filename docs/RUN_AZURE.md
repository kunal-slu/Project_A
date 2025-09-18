# Running on Azure

## Prerequisites

- Azure CLI configured
- Databricks workspace
- ADLS Gen2 storage account
- Service Principal or Managed Identity

## Setup

1. Install Azure dependencies:
```bash
pip install -e .[azure]
```

2. Configure environment:
```bash
export ENV=azure
export AZURE_TENANT_ID=<tenant-id>
export AZURE_CLIENT_ID=<client-id>
export AZURE_CLIENT_SECRET=<client-secret>
```

## Deployment

### Databricks

1. Create Databricks job:
```bash
databricks jobs create --json @azure/jobs/returns_batch.json
```

2. Run job:
```bash
databricks jobs run-now --job-id <job-id> --notebook-params '{"proc_date": "2024-01-01", "env": "azure"}'
```

### Airflow

1. Deploy DAGs to `azure/dags/`
2. Set Airflow variables:
   - `DATABRICKS_JOB_ID`: Job ID

## ADLS Layout

```
abfss://lake@acct.dfs.core.windows.net/
├── bronze/
├── silver/
├── gold/
├── _checkpoints/
└── _artifacts/
```






