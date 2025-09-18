### What this project is
A hands-on PySpark data engineering pipeline that ingests raw files, cleans/enriches them, enforces quality, and publishes analytics-ready tables using a lakehouse layout (Bronze → Silver → Gold) on Delta Lake. It also includes streaming, Airflow orchestration, tests, and CI.

### How to run (quick start)
- Create venv and install:
  - `python3 -m venv .venv && source .venv/bin/activate`
  - `pip install -r requirements.txt && pip install -e .`
- Run tests: `env -u SPARK_HOME pytest -q | cat`
- Run pipeline: `env -u SPARK_HOME python -m pyspark_interview_project config/config-dev.yaml | cat`
- Optional Airflow: see `README.md`

### End-to-end pipeline flow (step-by-step)
1) Read config
   - File: `config/config-dev.yaml`
   - Points to input files, output directories, Spark settings, DQ rules, streaming checkpoints.

2) Start application
   - Entry: `src/pyspark_interview_project/__main__.py`
   - Loads YAML, configures logging, creates a Spark session (with Delta), calls `run_pipeline(...)`.

3) Spark session with Delta
   - File: `src/pyspark_interview_project/utils.py`
   - Builds a Spark session tuned for Delta Lake (AQE, skew join mitigation, broadcast threshold, Delta extensions).

4) Extract (typed schemas)
   - File: `src/pyspark_interview_project/extract.py`
   - Reads CSV/JSON with explicit schemas:
     - `extract_customers(...)`, `extract_products(...)`
     - `extract_orders_json(...)` (includes nested `payment`)
     - `extract_returns(...)`, `extract_exchange_rates(...)`, `extract_inventory_snapshots(...)`

5) Bronze (raw, conformed)
   - File: `src/pyspark_interview_project/pipeline.py`
   - Writes each extracted DataFrame to `data/lakehouse/bronze/*` via Delta (see `load.py`).

6) Data quality checks (pre-transform)
   - File: `src/pyspark_interview_project/dq_checks.py`
   - Checks executed:
     - Non-null and unique on `customer_id`
     - Referential integrity (orders → customers/products)
     - Age range bounds
     - Email regex via Spark `rlike` (no Python UDF)

7) Transform and enrich
   - File: `src/pyspark_interview_project/transform.py`
   - Highlights:
     - `join_examples(...)` joins customers/products/orders
     - `enrich_customers(...)` email domain, region, age buckets
     - `enrich_products(...)` `price_band`, `tags_array`
     - `clean_orders(...)` parse timestamps, deduplicate newest
     - `normalize_currency(...)` join FX to compute USD amounts
     - `join_returns(...)`, `join_inventory(...)`
     - `window_functions_demo(...)`, `udf_examples(...)` for advanced patterns
     - `build_fact_orders(...)` creates `revenue` and `order_ym`
     - Optional SCD2 for customers via `build_customers_scd2(...)`

8) Silver (cleansed/enriched dims)
   - Writes `dim_customers`, `dim_products`, `orders_cleansed` into `data/lakehouse/silver/*` (Delta).

9) Gold (facts/marts) with MERGE upsert
   - Builds `fact_orders` from joined/normalized data.
   - Writes via idempotent Delta MERGE by `order_id`, partitioned by `order_ym`.
   - Helper: `src/pyspark_interview_project/delta_utils.py` → `merge_upsert(...)`.

10) Validate outputs and record metrics
   - File: `src/pyspark_interview_project/validate.py`
   - Ensures Parquet output exists and contains rows.
   - Writes run metrics to `data/metrics/pipeline_metrics.json`.

11) Optional streaming
   - File: `src/pyspark_interview_project/streaming.py`
   - `stream_orders_to_bronze(...)`: ingest micro-batches (filesystem or Kafka) to bronze.
   - `stream_to_fact_orders(...)`: watermark on `order_ts`, dedup by `order_id`, normalize currency, write streaming fact partitioned by `order_ym`.

12) Optional Delta maintenance
   - Guarded maintenance block in `pipeline.py` (set via config) and separate Airflow DAG `airflow/dags/delta_maintenance_dag.py` to run OPTIMIZE (Z-ORDER) + VACUUM.

### Key files by purpose
- `src/pyspark_interview_project/__main__.py`: CLI entry point.
- `src/pyspark_interview_project/utils.py`: Spark session (Delta-enabled).
- `src/pyspark_interview_project/extract.py`: schema-enforced readers.
- `src/pyspark_interview_project/transform.py`: business logic and demos.
- `src/pyspark_interview_project/load.py`: Parquet/Delta/Avro/JSON IO helpers.
- `src/pyspark_interview_project/dq_checks.py`: data quality utilities.
- `src/pyspark_interview_project/validate.py`: post-write validations.
- `src/pyspark_interview_project/delta_utils.py`: Delta MERGE upsert helper.
- `src/pyspark_interview_project/streaming.py`: structured streaming flows.
- `src/pyspark_interview_project/pipeline.py`: orchestrates the ETL.
- `airflow/dags/pyspark_etl_dag.py`: pipeline DAG.
- `airflow/dags/delta_maintenance_dag.py`: maintenance DAG.
- `tests/*`: pytest suite.

### Lakehouse layout (where data lands)
- Bronze (raw): `data/lakehouse/bronze/*`
- Silver (cleansed/enriched): `data/lakehouse/silver/*`
- Gold (facts/marts): `data/lakehouse/gold/fact_orders`

### Extend as you learn
- New inputs: add schemas/readers in `extract.py`.
- New transforms: add functions in `transform.py`, call them from `pipeline.py`.
- New DQ: implement checks in `dq_checks.py` and invoke in `pipeline.py`.
- New outputs: use `merge_upsert(...)` for idempotent facts.

