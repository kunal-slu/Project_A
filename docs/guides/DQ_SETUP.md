# Data Quality Setup Guide

## Overview

Data Quality (DQ) is enforced at multiple layers:
- **Ingestion**: Schema validation on Bronze
- **Transformation**: Expectation suites on Silver
- **Business Logic**: Assertions on Gold

## Configuration

### Global Thresholds

`config/dq.yaml`:
```yaml
global_thresholds:
  null_rate_max: 0.05
  duplicate_rate_max: 0.0
  freshness_hours_max: 24
```

### Table-Level Expectations

`src/pyspark_interview_project/dq/suites/silver_orders.yml`:
```yaml
expectations:
  - expect_column_values_to_not_be_null: order_id
  - expect_column_values_to_be_unique: order_id
  - expect_column_max_to_be_between: total_amount, min_value=0, max_value=100000
```

## Running DQ Checks

### Via DAG
The `dags/dq_validation_dag.py` runs DQ checks after each Silver/Gold load.

### Via Script
```bash
python aws/scripts/run_ge_checks.py --suite silver_orders.yml
```

### Via Code
```python
from pyspark_interview_project.dq import DQRunner
runner = DQRunner(spark, config)
runner.validate_table("silver.orders")
```

## Alerting

When DQ fails:
- CloudWatch alarms trigger
- Slack/PagerDuty notifications sent
- Jobs pause until fixed
- Details logged to `logs/dq_failures.log`

## Monitoring

- DQ Pass Rate: CloudWatch metric
- Failed Tables: CloudWatch log insights query
- Historical Trend: Looker/Business Intelligence

## Best Practices

1. **Start Strict**: Begin with looser thresholds, tighten over time
2. **Monitor Trends**: Watch for gradual degradation
3. **Fail Fast**: Stop downstream jobs when DQ fails
4. **Document**: Record why each expectation exists

