# Design Tradeoffs and Non-Goals

This project is intentionally scoped for reliability and clarity over maximal tool count.

## Key Tradeoffs

1. Bronze as Parquet, not ACID:
   - Keeps ingestion simple and cheap.
   - ACID guarantees are applied at curated layers.

2. Strict contracts at Silver:
   - Fail-fast prevents bad data from propagating.
   - Higher short-term failure rate, lower long-term trust risk.

3. Rolling-window incremental strategy:
   - Reprocesses recent data to capture late updates.
   - Slight extra compute for deterministic correctness.

4. Runtime metrics via artifacts:
   - Lightweight and local-friendly.
   - Can be forwarded to CloudWatch/Prometheus later.

## Non-Goals

- Not adding Kafka-first streaming orchestration as core path.
- Not adding Kubernetes for local/dev execution.
- Not adding additional clouds or polyglot runtimes.
- Not adding dashboard-heavy observability before core data guarantees.

Reason: These would increase complexity without improving core correctness guarantees.
