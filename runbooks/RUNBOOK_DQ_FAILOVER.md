# Runbook: DQ Failover Procedure

## Purpose

This runbook describes how to bypass DQ gates for non-PII tables when critical business needs require it, with proper approval process.

---

## When to Use

**ONLY** when:
- ✅ DQ failure is blocking critical business operation
- ✅ Issue is confirmed non-PII data
- ✅ Approval received from data engineering lead
- ✅ Incident ticket created
- ✅ Alternative remediation plan documented

**NEVER** bypass for:
- ❌ PII data (customer emails, SSNs, etc.)
- ❌ Financial transactions
- ❌ Compliance-critical data
- ❌ Without approval

---

## Pre-Approval Checklist

- [ ] Incident ticket created (JIRA/Servicenow)
- [ ] Data engineering lead approval obtained
- [ ] Impact assessment documented
- [ ] Rollback plan documented
- [ ] Timeline for fix agreed upon

---

## Procedure

### Step 1: Document Issue

```bash
# Create incident ticket
# Include:
# - Table name
# - DQ failure details
# - Business impact
# - Proposed bypass duration
```

### Step 2: Get Approval

Contact data engineering lead with:
- Incident ticket link
- Table name and DQ failure details
- Business justification
- Proposed fix timeline

**Expected response time:** 1 hour for critical issues

### Step 3: Bypass DQ Gate (Non-PII Only)

**Option A: Airflow DAG Parameter**

In Airflow DAG, add bypass parameter:

```python
# In dq_gate_silver task
dq_check_silver = EmrServerlessJob(
    task_id="dq_gate_silver",
    args={
        "--config": "config/prod.yaml",
        "--suite": "silver.orders",
        "--bypass": "{{ params.bypass_dq }}",  # Set to "true" if approved
    }
)
```

**Option B: Config Override**

```yaml
# config/dq.yaml
dq:
  enabled: true
  strict_mode: false  # Set to false for bypass (after approval)
  
  bypass_tables:  # Only if approved
    - silver.orders  # Example: approved for 24 hours
```

**Option C: Direct SQL**

```sql
-- In Athena/Redshift
-- Manually promote data (last resort)
-- Document in incident ticket
```

### Step 4: Document Bypass

```bash
# Update incident ticket with:
# - Bypass method used
# - Duration
# - Monitoring plan
# - Fix timeline
```

### Step 5: Monitor

```bash
# Watch for:
# - Downstream failures
# - Data quality degradation
# - Business impact

# Set up CloudWatch alarms for downstream jobs
```

### Step 6: Restore DQ Gate

After fix is deployed:

```bash
# 1. Re-enable DQ
# config/dq.yaml: strict_mode: true

# 2. Re-run failed job
# aws emr-serverless start-job-run ...

# 3. Verify DQ passes
# 4. Close incident ticket
```

---

## Rollback Procedure

If bypass causes issues:

```bash
# 1. Immediately disable bypass
# config/dq.yaml: strict_mode: true

# 2. Revert to last known good state
# aws s3 sync s3://bucket/silver/orders/latest/ s3://bucket/silver/orders/backup/

# 3. Notify stakeholders
# 4. Update incident ticket
```

---

## Approval Matrix

| Table Type | Approval Required | Max Bypass Duration |
|------------|-------------------|---------------------|
| Non-PII Silver | Data Eng Lead | 24 hours |
| Non-PII Gold | Data Eng Lead + Manager | 12 hours |
| PII Any Layer | **NEVER ALLOWED** | N/A |
| Financial | **NEVER ALLOWED** | N/A |

---

## Contact

- **Data Engineering Lead:** data-eng-lead@company.com
- **On-Call Engineer:** Check PagerDuty
- **Incident Response:** incident@company.com

---

## Example Incident

**Incident:** DQ failure on silver.orders (non-PII)
**Business Impact:** Orders dashboard blocked
**Approval:** ✅ Received from data-eng-lead
**Bypass Duration:** 6 hours
**Fix Timeline:** Next release (24 hours)

**Action Taken:**
1. Set `bypass_dq: true` in Airflow DAG config
2. Re-run silver_to_gold job
3. Monitor downstream jobs
4. Deploy fix in next release

---

**Last Updated:** 2025-01-15  
**Owner:** Data Engineering Team

