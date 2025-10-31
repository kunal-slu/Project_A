# Monitoring Setup Guide

This guide explains how to set up monitoring dashboards for the ETL pipeline.

## Overview

The platform emits metrics to:
- **CloudWatch**: AWS-native metrics
- **Prometheus** (optional): Self-hosted monitoring
- **OpenLineage**: Data lineage events

## CloudWatch Dashboard

### Setup

1. **Import Dashboard**:
   ```bash
   aws cloudwatch put-dashboard \
     --dashboard-name "ETL-Pipeline-Metrics" \
     --dashboard-body file://monitoring/cloudwatch/dashboards/pipeline_metrics.json
   ```

2. **Access Dashboard**:
   - Go to CloudWatch Console → Dashboards
   - Select "ETL-Pipeline-Metrics"

### Key Metrics

- **Job Duration**: Average and P95 duration
- **Records Processed**: Throughput metrics
- **DQ Failures**: Data quality check failures
- **Error Rates**: Error counts by type

## Grafana Dashboard

### Prerequisites

- Grafana instance (self-hosted or managed)
- CloudWatch datasource configured
- Prometheus datasource (if using Prometheus)

### Setup

1. **Import Dashboard**:
   - Copy `monitoring/grafana/dashboards/pipeline_overview.json`
   - Grafana → Dashboards → Import
   - Paste JSON content

2. **Configure Data Sources**:
   - CloudWatch: AWS credentials
   - Prometheus: Prometheus server URL

### Dashboard Panels

1. **Pipeline Execution Status**: Success rate percentage
2. **Job Duration Trends**: P95 duration over time
3. **Records Processed**: Throughput by job
4. **Data Quality Trends**: DQ pass rate
5. **Error Rates**: Error counts by type

## Alerts

### CloudWatch Alarms

```yaml
alarms:
  high_error_rate:
    metric: job.errors.total
    threshold: 10
    period: 300
    evaluation_periods: 2
  
  dq_failure:
    metric: dq.checks.failed
    threshold: 5
    period: 300
    evaluation_periods: 1
```

### Slack Notifications

Configure in `config/lineage.yaml`:
```yaml
monitoring:
  notifications:
    slack:
      enabled: true
      webhook_url: "${SLACK_WEBHOOK_URL}"
```

---

**Last Updated**: 2024-01-15

