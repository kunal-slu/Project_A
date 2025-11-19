# Codebase Analysis Report

Generated: analyze_codebase.py

## Duplicate Modules

### bronze_schemas
- `src/project_a/pyspark_interview_project/schemas/bronze_schemas.py`
- `src/project_a/schemas/bronze_schemas.py`

### cli
- `src/project_a/cli.py`
- `src/project_a/pyspark_interview_project/cli.py`

### cloudwatch_metrics
- `src/project_a/pyspark_interview_project/utils/cloudwatch_metrics.py`
- `src/project_a/utils/cloudwatch_metrics.py`

### config
- `src/project_a/core/config.py`
- `src/project_a/pyspark_interview_project/utils/config.py`
- `src/project_a/utils/config.py`

### config_loader
- `src/project_a/config_loader.py`
- `src/project_a/pyspark_interview_project/config_loader.py`

### contracts
- `src/project_a/pyspark_interview_project/utils/contracts.py`
- `src/project_a/utils/contracts.py`

### delta_utils
- `src/project_a/delta_utils.py`
- `src/project_a/pyspark_interview_project/delta_utils.py`

### error_lanes
- `src/project_a/pyspark_interview_project/utils/error_lanes.py`
- `src/project_a/utils/error_lanes.py`

### fx_json_reader
- `src/project_a/pyspark_interview_project/extract/fx_json_reader.py`
- `src/project_a/extract/fx_json_reader.py`

### gate
- `src/project_a/pyspark_interview_project/dq/gate.py`
- `src/project_a/dq/gate.py`

### gold_writer
- `src/project_a/pyspark_interview_project/gold_writer.py`
- `src/project_a/pyspark_interview_project/jobs/gold_writer.py`

### kafka_orders_stream
- `src/project_a/pyspark_interview_project/extract/kafka_orders_stream.py`
- `src/project_a/pyspark_interview_project/jobs/kafka_orders_stream.py`

### lineage_emitter
- `src/project_a/pyspark_interview_project/monitoring/lineage_emitter.py`
- `src/project_a/monitoring/lineage_emitter.py`

### logging
- `src/project_a/pyspark_interview_project/utils/logging.py`
- `src/project_a/utils/logging.py`

### metrics
- `src/project_a/pyspark_interview_project/utils/metrics.py`
- `src/project_a/pyspark_interview_project/monitoring/metrics.py`

### metrics_collector
- `src/project_a/pyspark_interview_project/metrics_collector.py`
- `src/project_a/pyspark_interview_project/monitoring/metrics_collector.py`

### path_resolver
- `src/project_a/pyspark_interview_project/io/path_resolver.py`
- `src/project_a/pyspark_interview_project/utils/path_resolver.py`
- `src/project_a/utils/path_resolver.py`

### performance_optimizer
- `src/project_a/pyspark_interview_project/performance_optimizer.py`
- `src/project_a/dq/performance_optimizer.py`

### run_audit
- `src/project_a/pyspark_interview_project/utils/run_audit.py`
- `src/project_a/utils/run_audit.py`

### run_pipeline
- `src/project_a/pipeline/run_pipeline.py`
- `src/project_a/pyspark_interview_project/pipeline/run_pipeline.py`

### schema_validator
- `src/project_a/pyspark_interview_project/schema_validator.py`
- `src/project_a/pyspark_interview_project/utils/schema_validator.py`

### spark_session
- `src/project_a/pyspark_interview_project/utils/spark_session.py`
- `src/project_a/utils/spark_session.py`


## Files with Old Imports

- `scripts/cleanup_and_align_aws.py`

## Potentially Unused Files

- `aws/dags/maintenance_dag.py`
- `aws/jobs/analytics/build_sales_fact_table.py`
- `aws/jobs/maintenance/apply_data_masking.py`
- `aws/jobs/maintenance/delta_optimize_vacuum.py`
- `aws/scripts/create_cloudwatch_alarms.py`
- `aws/scripts/maintenance/dr_snapshot_export.py`
- `aws/scripts/register_glue_table.py`
- `aws/scripts/run_ge_checks.py`
- `aws/scripts/utilities/lf_tags_seed.py`
- `aws/scripts/utilities/notify_on_sla_breach.py`
- `aws/scripts/utilities/register_glue_tables.py`
- `aws/scripts/utilities/run_ge_checks.py`
- `aws/tests/conftest.py`
- `jobs/publish/gold_to_snowflake.py`
- `jobs/publish/publish_gold_to_redshift.py`
- `jobs/publish/publish_gold_to_snowflake.py`
- `local/scripts/run_local_etl_safe.py`
- `local/scripts/show_s3_output_data.py`
- `local/tests/local/run_etl_end_to_end.py`
- `local/tests/local/verify_enterprise_setup.py`
- `local/tests/smoke_validate.py`
- `scripts/analyze_codebase.py`
- `scripts/cleanup_and_align_aws.py`
- `scripts/convert_fx_csv_to_json.py`
- `scripts/deep_review_fixes.py`
- `scripts/maintenance/backfill_range.py`
- `scripts/maintenance/optimize_tables.py`
- `scripts/organize_data_directory.py`
- `scripts/run_aws_emr_serverless.py`
- `scripts/upload_jobs_to_s3.py`
- `scripts/validate_source_data.py`
- `tests/conftest.py`
- `tests/dev_secret_probe.py`
