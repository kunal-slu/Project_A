# Runbook: PII (Personally Identifiable Information) Handling

This document outlines how PII is identified, masked, and handled throughout the data pipeline.

## PII Classification

### Defined PII Fields

Based on `config/dq.yaml` and schema definitions:

**CRM (Salesforce)**:
- `Contacts.Email` - Email addresses
- `Contacts.Phone` - Phone numbers
- `Contacts.MailingStreet`, `MailingCity` - Address information
- `Accounts.Phone` - Account phone numbers

**Other Sources**:
- Customer names (in some contexts)
- Social Security Numbers (if present)
- Credit card numbers (if present)

## Masking Strategy

### Silver Layer Masking

PII is masked during the Bronze → Silver transformation using `aws/jobs/maintenance/apply_data_masking.py`.

### Masking Methods

1. **Hashing** (for identifiers that need to be joinable):
   ```python
   # SHA256 hash (one-way, deterministic)
   email_hash = sha256(email.lower().strip()).hexdigest()
   ```

2. **Redaction** (for display):
   ```python
   # Phone: (555) ***-****
   # Email: ***@example.com
   ```

3. **Nullification** (for non-essential PII):
   ```python
   # Replace with NULL in certain environments
   phone_masked = None
   ```

### Implementation

See `src/pyspark_interview_project/transform/mask_pii.py` (if implemented) or `aws/jobs/maintenance/apply_data_masking.py`.

Example:
```python
from pyspark.sql.functions import sha2, regexp_replace

# Hash email
df = df.withColumn("email_masked", sha2(col("email").cast("string"), 256))

# Mask phone
df = df.withColumn(
    "phone_masked",
    regexp_replace(col("phone"), r"(\d{3})\d{3}-(\d{4})", r"\1***-\2")
)
```

## Access Control by Layer

### Bronze Layer
- **Access**: Data Engineers only
- **PII**: Unmasked (raw data)
- **Purpose**: Full data available for transformation development

### Silver Layer
- **Access**: Data Engineers, Data Scientists (with masking)
- **PII**: Masked/hashed
- **Purpose**: Cleaned data with PII protection

### Gold Layer
- **Access**: Data Analysts, Business Users
- **PII**: Minimal or aggregated only
- **Purpose**: Business-ready metrics without sensitive data

## Lake Formation Integration

Lake Formation tags are applied to PII columns:

```yaml
# In aws/terraform/lake_formation.tf
lf_tag: contains_pii: true
lf_tag: classification: confidential
```

Access rules:
- **Data Engineers**: Full access (unmasked)
- **Data Analysts**: Access with masking transformation
- **Business Users**: No access to PII columns

## Configuration

### PII Field Definitions

In `config/dq.yaml`:
```yaml
pii_columns:
  crm_contacts:
    - email
    - phone
    - mailing_street
    - mailing_city
  crm_accounts:
    - phone
```

### Masking Configuration

In `config/prod.yaml` or `config/local.yaml`:
```yaml
data_masking:
  enabled: true
  method: hash  # hash, redact, or null
  salt: "${MASKING_SALT_SECRET}"  # From Secrets Manager
```

## Compliance

### GDPR/CCPA Requirements

1. **Right to be Forgotten**: Support data deletion
2. **Data Portability**: Export masked data
3. **Access Requests**: Provide access to masked data
4. **Consent Management**: Track consent for data processing

### Data Retention

PII data retention policies:
- **Bronze**: 90 days (raw data)
- **Silver**: 365 days (masked data)
- **Gold**: 7 years (aggregated only)

See `config/retention-config.yaml` for detailed retention rules.

## Monitoring

### PII Access Logging

All access to PII columns is logged:
- **CloudTrail**: IAM access logs
- **Lake Formation**: Column-level access logs
- **Custom Metrics**: PII column access counts

### Alerting

Alerts triggered for:
- Unauthorized PII access attempts
- Unmasked PII in Gold layer
- PII data breaches (DLQ entries with PII)

## Remediation

### If Unmasked PII Detected

1. **Immediate**: Quarantine affected data
2. **Investigate**: Identify root cause
3. **Fix**: Re-run masking job
4. **Verify**: Confirm PII is masked
5. **Notify**: Alert security team if breach suspected

### Data Deletion Requests

For GDPR/CCPA deletion requests:
1. Identify all PII for the individual
2. Delete from Bronze (raw data)
3. Update Silver (set PII fields to NULL)
4. Aggregate Gold (remove from aggregates if identifiable)
5. Document deletion in audit log

---

**Last Updated**: 2024-01-15  
**Policy Version**: 1.0.0  
**Maintained By**: Data Engineering & Security Teams

