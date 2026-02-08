# RUNBOOK

Operational ownership for Project_A pipelines.

## Ownership

- Primary owner: Data Engineering
- Secondary owner: Analytics Engineering
- Escalation: Platform Team
- On-call channel: `#data-alerts`

## SLAs and Freshness Targets

- Bronze ingestion: complete by 02:15 UTC
- Silver transform: complete by 02:30 UTC
- Gold transform: complete by 03:00 UTC
- Streaming lag: < 5 minutes

Reference: `docs/runbooks/DATA_SLA.md`

## Failure Policy (Fail Fast)

Pipeline run must fail on:
- null primary keys
- duplicate primary keys
- referential integrity violations
- contract-required column nulls
- out-of-range contract violations

Contracts: `config/contracts/silver_contracts.yaml`  
Runtime enforcement: `src/project_a/contracts/runtime_contracts.py`

## Incident Response

1. Identify failed job from logs and `artifacts/metrics/pipeline_runs.jsonl`.
2. Triage failure class:
   - Contract/DQ failure
   - Infra/runtime failure
   - Upstream source delay
3. If contract/DQ:
   - stop downstream execution
   - inspect violating dataset
   - validate remediation against contract
4. Re-run only affected stage.
5. Post update in `#data-alerts` with ETA and impact.

## Recovery

- Backfill strategy: rolling reprocess window for late orders (default 3 days)
- Idempotency: dedupe by `order_id`, latest `updated_at` wins
- Restore point: rerun from Bronze immutable source

## Observability

Per-job run metrics emitted to:
- `artifacts/metrics/pipeline_runs.jsonl`

Key signals:
- `duration_seconds`
- `status`
- output metrics from job result payload

Duration threshold breaches are logged from config:
- `monitoring.thresholds.<job_name>_duration_seconds`
