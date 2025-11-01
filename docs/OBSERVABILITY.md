# Observability Guide

## Overview

This document describes the observability stack for the data platform: metrics, logging, lineage, and alerting.

## Components

### 1. Metrics Collection

**Location**: `src/pyspark_interview_project/monitoring/metrics_collector.py`

**Metrics Emitted**:
- `rows_in`: Input record count
- `rows_out`: Output record count
- `duration_ms`: Job duration
- `dq_status`: Data quality status (pass/fail/quarantine)
- `cost_estimate_usd`: Estimated job cost

**Sinks**:
- **Production**: CloudWatch (`put_metric_data`)
- **Development**: JSON logs (`data/metrics/pipeline_metrics.log`)

**Usage**:
```python
from pyspark_interview_project.monitoring.metrics_collector import emit_metrics

emit_metrics(
    job_name="bronze_to_silver_behavior",
    rows_in=50000,
    rows_out=49875,
    duration_seconds=123.45,
    dq_status="pass",
    config=config
)
```

### 2. Run Metrics (Job-Level)

**Location**: `s3://my-etl-lake-demo/meta/runs/dt={date}/{job_name}.json`

**Format**:
```json
{
  "job_name": "bronze_to_silver_behavior",
  "run_id": "2025-10-31T19:20:00Z",
  "rows_in": 50000,
  "rows_out": 49875,
  "dq_failed": 125,
  "duration_ms": 123450,
  "compute_platform": "emr-serverless",
  "cost_estimate_usd": 0.23,
  "status": "success",
  "dq_status": "pass"
}
```

**Collection**:
- Emitted at end of each Spark job
- Partitioned by date for queryability
- Used for trend analysis and debugging

### 3. Logging

**Format**: Structured JSON logs

**Fields**:
- `timestamp`: ISO8601
- `level`: INFO, WARNING, ERROR
- `job_name`: Job identifier
- `run_id`: Unique run ID
- `message`: Log message
- `metadata`: Additional context (row counts, paths, etc.)

**Example**:
```json
{
  "timestamp": "2025-10-31T19:20:00Z",
  "level": "INFO",
  "job_name": "bronze_to_silver_behavior",
  "run_id": "run-20251031-192000",
  "message": "Transformed 49875 records to Silver",
  "metadata": {
    "rows_in": 50000,
    "rows_out": 49875,
    "duration_seconds": 123.45
  }
}
```

**Destinations**:
- **EMR Serverless**: CloudWatch Logs (auto-configured)
- **Local**: `logs/application.log`

### 4. Data Lineage

**Location**: `src/pyspark_interview_project/lineage/openlineage_emitter.py`

**Dataset-Level**:
- Emitted for all jobs (bronze → silver → gold)
- Format: OpenLineage standard
- Endpoint: `OPENLINEAGE_URL` environment variable

**Column-Level** (Showcase):
- Only for `customer_360` job
- Location: `s3://my-etl-lake-demo/meta/lineage/{date}-gold-customer-360.json`

**Example**:
```json
{
  "output_dataset": "gold.customer_360",
  "columns": {
    "customer_id": ["silver.crm_contacts.customer_id"],
    "last_event_ts": ["silver.behavior.event_ts"],
    "lifetime_value_usd": [
      "silver.snowflake_orders.order_amount",
      "silver.fx_rates.rate"
    ]
  }
}
```

**Registry**: `config/lineage.yaml` maps logical datasets to physical paths.

### 5. Data Quality Monitoring

**DQ Watchdog DAG**: Runs hourly, checks:
- **Freshness**: Latest partition age vs SLA
- **Volume**: Row count thresholds
- **GE Checks**: Great Expectations suites

**Alerts**:
- Freshness breach → SNS → Slack
- Volume breach → Email
- GE failure → Slack + Email

**Metrics**:
- `dq_pass_rate`: % of checks passing
- `dq_failure_count`: Count of failed checks
- `freshness_hours`: Age of latest partition

### 6. Cost Monitoring

**Metrics**:
- `job_cost_usd`: Estimated cost per job
- `monthly_cost_usd`: Aggregated monthly spend
- `cost_per_row`: Cost efficiency metric

**Dashboard**: CloudWatch custom dashboard

**Alerts**:
- Job cost > $10: Alert
- Monthly spend > budget: Alert

## CloudWatch Dashboards

### 1. Pipeline Health Dashboard

**Widgets**:
- Success rate (last 24h)
- Average job duration
- DQ pass rate
- Rows processed

### 2. Cost Dashboard

**Widgets**:
- EMR Serverless DPU hours
- S3 storage by class
- Redshift RPU hours
- Monthly spend trend

### 3. Data Freshness Dashboard

**Widgets**:
- Latest partition date per layer
- Age vs SLA (bronze/silver/gold)
- Freshness violations

## Alerting

### SNS Topics

1. **Data Quality Alerts**: `dq-violations`
2. **SLA Breaches**: `sla-breaches`
3. **Job Failures**: `job-failures`
4. **Cost Alerts**: `cost-overruns`

### Slack Integration

```python
# In alerting module
import requests

def send_slack_alert(webhook_url, message):
    payload = {
        "text": f"🚨 Data Platform Alert\n{message}",
        "username": "Data Platform Bot"
    }
    requests.post(webhook_url, json=payload)
```

### Email Alerts

Configured in Airflow DAGs:
```python
default_args = {
    'email_on_failure': True,
    'email': ['data-team@company.com']
}
```

## Querying Metrics

### CloudWatch Insights

```sql
fields @timestamp, job_name, rows_out, duration_ms
| filter status = "success"
| stats avg(duration_ms) by job_name
| sort duration_ms desc
```

### Athena Queries (Run Metrics)

```sql
SELECT 
  job_name,
  date_trunc('hour', from_iso8601_timestamp(run_id)) as hour,
  avg(rows_out) as avg_rows,
  avg(cost_estimate_usd) as avg_cost
FROM "meta"."runs"
WHERE dt >= current_date - interval '7' day
GROUP BY 1, 2
ORDER BY 2 DESC
```

## Best Practices

1. **Always Emit Metrics**: Every job should emit at least row counts and duration
2. **Use Structured Logging**: JSON format for easy parsing
3. **Set Up Alerts Early**: Don't wait for production issues
4. **Monitor Cost Trends**: Review weekly, optimize monthly
5. **Document Runbooks**: Link from alerts to troubleshooting docs

## Runbook Integration

Metrics link to operational runbooks:
- Job failure → `docs/runbooks/TROUBLESHOOTING.md`
- DQ violation → `docs/runbooks/DQ_INVESTIGATION.md`
- SLA breach → `docs/runbooks/DATA_FRESHNESS.md`

---

**Next Steps**: Set up CloudWatch dashboards and SNS topics in AWS Console.

