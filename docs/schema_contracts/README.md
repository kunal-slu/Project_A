# Data Contracts

This directory contains schema definitions (data contracts) for all data sources and tables in the data pipeline.

## Schema Files

### Bronze Layer Contracts
- `crm_accounts.schema.json` - Salesforce Accounts object
- `crm_contacts.schema.json` - Salesforce Contacts object
- `crm_opportunities.schema.json` - Salesforce Opportunities object
- `snowflake_orders.schema.json` - Snowflake orders data
- `redshift_behavior.schema.json` - Redshift customer behavior events
- `fx_rates.schema.json` - FX exchange rates
- `kafka_events.schema.json` - Kafka streaming events

### Usage

These schemas are used by `schema_validator.py` to enforce data contracts at bronze ingestion:

```python
from pyspark_interview_project.utils.schema_validator import validate_bronze_ingestion

df_validated, results = validate_bronze_ingestion(
    df, spark, "crm_accounts", 
    schemas_dir="src/pyspark_interview_project/contracts"
)
```

---

**Note**: These schemas define the expected structure and validation rules for each data source, ensuring data quality and schema evolution handling.

