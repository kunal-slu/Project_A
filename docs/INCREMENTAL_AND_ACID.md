# Incremental and ACID Strategy

## Incremental Strategy

Implemented for `orders_silver` in `jobs/transform/bronze_to_silver.py`:

- Rolling window reprocessing (`lookback_days`, default `3`)
- Merge candidates from:
  - existing Silver rows within lookback window
  - incoming Bronze rows within lookback window
- Idempotent dedupe:
  - key: `order_id`
  - latest record: highest `updated_at`
- Historical rows outside lookback are preserved unchanged

Config:
- `incremental.orders_lookback_days`
- `contracts.path` with `orders_silver.late_data.lookback_days`

## Late-Arriving Data

Late updates are absorbed by reprocessing recent partitions and deduplicating by primary key.  
This avoids full refresh while capturing corrections.

## ACID Strategy

- Bronze: Parquet (immutable/raw)
- Curated layers: contract-validated writes
- Iceberg support remains available for curated tables:
  - utility: `src/project_a/iceberg_utils.py`
  - job: `jobs/iceberg/orders_silver_to_iceberg.py`

Recommendation:
- Use Iceberg/Delta for tables requiring transactional merge semantics and frequent corrections.
