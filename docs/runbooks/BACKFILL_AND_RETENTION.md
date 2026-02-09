# Backfill + Retention Runbook

## Backfill (Historical Reprocessing)

**When to use**
- Late‑arriving data discovered
- Upstream reprocessing required
- Bugs fixed in transformations

**Command**
```bash
python3 scripts/backfill_range.py \
  --start-date 2026-01-01 \
  --end-date 2026-01-07 \
  --jobs snowflake_to_bronze,bronze_to_silver,silver_to_gold \
  --config local/config/local.yaml
```

**Idempotency**
- The script writes a sentinel file per day/job in `artifacts/backfill/`
- Re‑runs skip completed days unless `--force` is used

---

## Retention Cleanup

**When to use**
- Local disk growing too fast
- Old partitions no longer needed for reprocessing

**Command**
```bash
python3 scripts/retention_cleanup.py --config local/config/local.yaml --days 30
```

---

## Audit + Alert Trails

All backfill runs write audit + alert events:
- `artifacts/audit/pipeline_run_audit.jsonl`
- `artifacts/alerts/alerts.jsonl`
