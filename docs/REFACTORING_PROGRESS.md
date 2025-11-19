# Project A Refactoring Progress

## ✅ Completed

### 1. Core Architecture
- ✅ `src/project_a/core/config.py` - ProjectConfig class
- ✅ `src/project_a/core/context.py` - JobContext class (Spark + Delta builder)
- ✅ `src/project_a/core/base_job.py` - BaseJob abstract class
- ✅ `src/project_a/core/__init__.py` - Exports

### 2. Unified I/O
- ✅ `src/project_a/io/reader.py` - Unified readers (CSV, JSON, Delta, Parquet)
- ✅ `src/project_a/io/writer.py` - Unified writers (Delta/Parquet with validation)
- ✅ `src/project_a/io/__init__.py` - Exports

### 3. Schema Fixes
- ✅ Updated `src/project_a/schemas/bronze_schemas.py` to match ACTUAL CSV headers:
  - CRM: Id, Name, AccountId, etc. (not normalized yet)
  - Snowflake: customer_id, first_name, last_name, total_amount, etc.
  - Redshift: behavior_id, customer_id, event_timestamp, conversion_value, etc.
  - Kafka: event_id, topic, partition, value (JSON string), etc.

### 4. Single Entry Point
- ✅ `jobs/run_pipeline.py` - Unified entry point routing to all jobs

## 🚧 In Progress

### 5. Transform Modules
- ⏳ `src/project_a/transform/bronze_to_silver.py` - BronzeToSilverJob (needs refactoring)
- ⏳ `src/project_a/transform/silver_to_gold.py` - SilverToGoldJob (needs refactoring)

### 6. Ingestion Jobs
- ⏳ `src/project_a/ingest/snowflake_to_bronze.py`
- ⏳ `src/project_a/ingest/redshift_to_bronze.py`
- ⏳ `src/project_a/ingest/crm_to_bronze.py`
- ⏳ `src/project_a/ingest/fx_to_bronze.py`
- ⏳ `src/project_a/ingest/kafka_events_to_bronze.py`

### 7. Streaming
- ⏳ `src/project_a/streaming/kafka_producer.py` - Unified producer (local + MSK)
- ⏳ `src/project_a/streaming/kafka_consumer.py` - Unified streaming consumer

## 📋 TODO

### 8. Airflow Cleanup
- [ ] Keep only 2 DAGs:
  - `project_a_daily_pipeline.py` (Bronze→Silver→Gold)
  - `project_a_streaming_pipeline.py` (optional)
- [ ] Use Airflow Variables for all AWS IDs
- [ ] Remove hardcoded paths
- [ ] Fix import errors

### 9. Terraform Cleanup
- [ ] Remove unused modules
- [ ] Fix duplicate resources
- [ ] Ensure `terraform fmt` and `terraform validate` pass
- [ ] Match bucket names with config

### 10. File Cleanup
- [ ] Delete unused Python files
- [ ] Delete duplicate jobs
- [ ] Remove experimental utilities
- [ ] Archive old transform scripts

### 11. Tests
- [ ] Schema validation tests
- [ ] End-to-end Bronze→Silver→Gold test
- [ ] Kafka producer/consumer connectivity test

## 🎯 Next Steps

1. **Complete Transform Modules**: Refactor `bronze_to_silver.py` and `silver_to_gold.py` to use BaseJob
2. **Create Ingestion Jobs**: Implement all Bronze ingestion jobs using BaseJob
3. **Create Streaming Jobs**: Unified Kafka producer and consumer
4. **Clean Up Airflow**: Consolidate to 2 DAGs
5. **Clean Up Terraform**: Remove unused modules
6. **Delete Unused Files**: Remove all duplicate/unused code
7. **Add Tests**: Minimal test suite

## 📝 Notes

- All schemas now match actual CSV headers (not normalized)
- Transformation logic will normalize Bronze → Silver schemas
- Path resolution is handled by `JobContext.resolve_path()`
- Configuration is centralized in `ProjectConfig`
- All jobs inherit from `BaseJob` for consistency

