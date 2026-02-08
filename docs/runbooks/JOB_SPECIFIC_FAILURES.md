# Job-Specific Failure Playbooks (Project A)

This runbook provides **job-specific failure symptoms**, **triage checks**, and **exact run scripts** to recover.
Use this when a single job fails and you need a fast path to isolate and rerun it.

---

## 1) Ingestion Jobs (Bronze)

**Jobs**
- `fx_json_to_bronze`
- `snowflake_to_bronze`
- `crm_to_bronze`
- `redshift_to_bronze`
- `kafka_events_to_bronze`

**Common failures**
- Input file missing or wrong path
- Schema mismatch or parsing errors
- Permission errors on output

**Checks**
- Verify `local/config/local.yaml` paths under `sources.*.base_path`
- Validate input files exist under `data/bronze/...`
- Inspect raw file format (CSV vs JSON)

**Rerun command**
```bash
scripts/run_job.sh fx_json_to_bronze local/config/local.yaml local
```

---

## 2) Bronze → Silver

**Job**
- `bronze_to_silver`

**Common failures**
- Contract violation (missing columns, null PKs)
- Schema evolution gate failure
- Type casting errors

**Checks**
- Confirm contracts in `config/contracts/silver_contracts.yaml`
- Review schema baselines in `artifacts/schema_baselines/silver/`
- Check recent changes to Bronze ingestion

**Rerun command**
```bash
scripts/run_job.sh bronze_to_silver local/config/local.yaml local
```

---

## 3) Silver → Gold

**Job**
- `silver_to_gold`

**Common failures**
- Missing upstream Silver tables
- Schema evolution gate failure on Gold tables
- Join explosions or missing FK data

**Checks**
- Inspect Silver output in `data/silver/`
- Review schema baselines in `artifacts/schema_baselines/gold/`
- Check log for join warnings (orphaned keys)

**Rerun command**
```bash
scripts/run_job.sh silver_to_gold local/config/local.yaml local
```

---

## 4) Data Quality Gates

**Jobs**
- `dq_silver_gate`
- `dq_gold_gate`
- `run_comprehensive_dq.py`

**Common failures**
- Contract violations
- Null/uniqueness failures
- Referential integrity failures

**Checks**
- Inspect DQ report in `artifacts/dq/reports/`
- Review `config/contracts/*.yaml` and `config/contracts/*.json`

**Rerun command**
```bash
scripts/run_comprehensive_dq.sh all local/config/local.yaml local
```

---

## 5) Publish Jobs

**Jobs**
- `publish_gold_to_snowflake`
- `publish_gold_to_redshift`

**Common failures**
- Warehouse connectivity issues
- Missing target tables
- Invalid credentials

**Checks**
- Validate credentials in secrets manager (or `.env` in local)
- Confirm output data exists in `data/gold/`

**Rerun command**
```bash
scripts/run_job.sh publish_gold_to_snowflake local/config/local.yaml local
```

---

## 6) Schema Evolution Gates (Breaking Change)

**Symptoms**
- Job fails with “Schema evolution breaking change detected”

**Steps**
1. Compare current output schema with baseline in `artifacts/schema_baselines/<layer>/`
2. If change is intended, update contracts and baseline carefully
3. If change is unintentional, revert the transform logic

---

## 7) Where to Look First (Quick Debug Order)

1. Job logs (`logs/` or terminal output)
2. DQ report (`artifacts/dq/reports/`)
3. Schema baselines (`artifacts/schema_baselines/`)
4. Input data presence (`data/bronze/`)
5. Contracts (`config/contracts/`)

---

## 8) “One-Liner” Run Commands Reference

```bash
scripts/run_job.sh fx_json_to_bronze
scripts/run_job.sh snowflake_to_bronze
scripts/run_job.sh crm_to_bronze
scripts/run_job.sh redshift_to_bronze
scripts/run_job.sh bronze_to_silver
scripts/run_job.sh silver_to_gold
scripts/run_comprehensive_dq.sh all
```
