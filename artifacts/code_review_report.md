# Project_A Deep Code Review Report

## File Health Report
| File | Type | Status | Evidence | Risks |
|---|---|---|---|---|
| `.github/.github/workflows/ci.yml` | `.yml` | ❌ Unused / broken | - | s3://[^\s"\']+, (?i)secret |
| `.github/workflows/ci-cd-complete.yml` | `.yml` | ❌ Unused / broken | - | localhost, (?i)secret |
| `.github/workflows/ci-cd-pipeline.yml` | `.yml` | ❌ Unused / broken | - | s3://[^\s"\']+, (?i)secret |
| `.github/workflows/ci.yml` | `.yml` | ❌ Unused / broken | - | - |
| `.github/workflows/data-ci.yml` | `.yml` | ❌ Unused / broken | - | - |
| `.github/workflows/deploy.yml` | `.yml` | ❌ Unused / broken | - | (?i)secret |
| `.github/workflows/infra-plan.yml` | `.yml` | ❌ Unused / broken | - | (?i)secret |
| `.github/workflows/release.yml` | `.yml` | ❌ Unused / broken | - | s3://[^\s"\']+, (?i)secret |
| `.pre-commit-config.yaml` | `.yaml` | ❌ Unused / broken | - | (?i)secret |
| `aws/athena_queries/sample_queries.sql` | `.sql` | ❌ Unused / broken | - | - |
| `aws/config/dev.yaml` | `.yaml` | ⚠️ Used but risky | referenced | s3://[^\s"\']+, localhost, (?i)secret |
| `aws/config/environments/prod.yaml` | `.yaml` | ⚠️ Used but risky | referenced | s3://[^\s"\']+, (?i)secret |
| `aws/config/schemas/fx_rates_bronze.json` | `.json` | ❌ Unused / broken | - | - |
| `aws/config/schemas/snowflake_orders_bronze.json` | `.json` | ✅ Used & valid | referenced | - |
| `aws/config/shared/dq.yaml` | `.yaml` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `aws/config/shared/dq_thresholds.yaml` | `.yaml` | ❌ Unused / broken | - | - |
| `aws/config/shared/lineage.yaml` | `.yaml` | ✅ Used & valid | referenced | - |
| `aws/dags/daily_batch_pipeline_dag.py` | `.py` | ⚠️ Used but risky | referenced, airflow_dag | s3://[^\s"\']+ |
| `aws/dags_legacy/daily_pipeline_dag_complete.py` | `.py` | ⚠️ Used but risky | airflow_dag | s3://[^\s"\']+ |
| `aws/dags_legacy/dq_watchdog_dag.py` | `.py` | ✅ Used & valid | airflow_dag | - |
| `aws/dags_legacy/maintenance_dag.py` | `.py` | ⚠️ Used but risky | airflow_dag | s3://[^\s"\']+ |
| `aws/dags_legacy/project_a_daily_pipeline.py` | `.py` | ⚠️ Used but risky | airflow_dag | s3://[^\s"\']+ |
| `aws/dags_legacy/salesforce_ingestion_dag.py` | `.py` | ⚠️ Used but risky | airflow_dag | s3://[^\s"\']+ |
| `aws/data/samples/fx/fx_rates_historical.json` | `.json` | ✅ Used & valid | referenced | - |
| `aws/ddl/create_tables.sql` | `.sql` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `aws/emr_configs/logging.yaml` | `.yaml` | ✅ Used & valid | referenced | - |
| `aws/jobs/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/jobs/ingest/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/jobs/ingest/crm_accounts_ingest.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/jobs/ingest/crm_contacts_ingest.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/jobs/ingest/crm_opportunities_ingest.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/jobs/ingest/fx_rates_ingest.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/jobs/ingest/kafka_orders_stream.py` | `.py` | ❌ Unused / broken | - | (?i)password\s*=, (?i)secret |
| `aws/jobs/ingest/redshift_behavior_ingest.py` | `.py` | ⚠️ Used but risky | referenced | (?i)password\s*= |
| `aws/jobs/ingest/salesforce_to_bronze.py` | `.py` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `aws/jobs/ingest/snowflake_to_bronze.py` | `.py` | ⚠️ Used but risky | referenced | (?i)password\s*= |
| `aws/jobs/maintenance/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/jobs/maintenance/apply_data_masking.py` | `.py` | ❌ Unused / broken | - | - |
| `aws/jobs/maintenance/delta_optimize_vacuum.py` | `.py` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `aws/jobs/transform/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/jobs/transform/bronze_to_silver.py` | `.py` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `aws/jobs/transform/dq_check_bronze.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/jobs/transform/dq_check_silver.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/jobs/transform/silver_to_gold.py` | `.py` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `aws/jobs/transform/snowflake_bronze_to_silver_merge.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/jobs_legacy/analytics/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/jobs_legacy/analytics/build_customer_dimension.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/jobs_legacy/analytics/build_marketing_attribution.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/jobs_legacy/analytics/build_sales_fact_table.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/jobs_legacy/analytics/update_customer_dimension_scd2.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/scripts/backfill_bronze_for_date.sh` | `.sh` | ❌ Unused / broken | - | - |
| `aws/scripts/backfill_gold_for_date.sh` | `.sh` | ❌ Unused / broken | - | - |
| `aws/scripts/backfill_silver_for_date.sh` | `.sh` | ❌ Unused / broken | - | - |
| `aws/scripts/build_and_upload_package.sh` | `.sh` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `aws/scripts/build_dependencies_zip.sh` | `.sh` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `aws/scripts/check_job_logs.sh` | `.sh` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `aws/scripts/create_cloudwatch_alarms.py` | `.py` | ❌ Unused / broken | - | - |
| `aws/scripts/create_secrets.sh` | `.sh` | ❌ Unused / broken | - | (?i)secret |
| `aws/scripts/deploy_mwaa_dags.sh` | `.sh` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `aws/scripts/deploy_phase1.sh` | `.sh` | ❌ Unused / broken | - | (?i)secret |
| `aws/scripts/deploy_to_aws.sh` | `.sh` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `aws/scripts/deployment/aws_production_deploy.sh` | `.sh` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `aws/scripts/deployment/teardown.sh` | `.sh` | ✅ Used & valid | referenced | - |
| `aws/scripts/dq/check_data_quality.sh` | `.sh` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `aws/scripts/emr_job_template.sh` | `.sh` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `aws/scripts/maintenance/backfill_bronze_for_date.sh` | `.sh` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `aws/scripts/maintenance/dr_snapshot_export.py` | `.py` | ❌ Unused / broken | - | - |
| `aws/scripts/monitor_job.sh` | `.sh` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `aws/scripts/register_glue_table.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `aws/scripts/run_and_verify_bronze.sh` | `.sh` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `aws/scripts/run_aws_emr_serverless.py` | `.py` | ⚠️ Used but risky | entrypoint | s3://[^\s"\']+ |
| `aws/scripts/run_ge_checks.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/scripts/submit_bronze_job.sh` | `.sh` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `aws/scripts/submit_silver_job.sh` | `.sh` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `aws/scripts/sync_dags_to_airflow.sh` | `.sh` | ❌ Unused / broken | - | - |
| `aws/scripts/sync_data_to_s3.sh` | `.sh` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `aws/scripts/test_emr_job.sh` | `.sh` | ⚠️ Used but risky | referenced | s3://[^\s"\']+, (?i)secret |
| `aws/scripts/upload_configs.sh` | `.sh` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `aws/scripts/upload_missing_aws_data.sh` | `.sh` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `aws/scripts/upload_missing_bronze_files.sh` | `.sh` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `aws/scripts/upload_sample_data_to_s3.sh` | `.sh` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `aws/scripts/utilities/emit_lineage_and_metrics.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/scripts/utilities/emr_submit.sh` | `.sh` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `aws/scripts/utilities/lf_tags_seed.py` | `.py` | ❌ Unused / broken | - | - |
| `aws/scripts/utilities/notify_on_sla_breach.py` | `.py` | ❌ Unused / broken | - | - |
| `aws/scripts/utilities/register_glue_tables.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/scripts/utilities/run_ge_checks.py` | `.py` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `aws/scripts/utilities/source_terraform_outputs.sh` | `.sh` | ❌ Unused / broken | - | - |
| `aws/scripts/verify_bronze_data.sh` | `.sh` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `aws/scripts/verify_phase1.sh` | `.sh` | ✅ Used & valid | referenced | - |
| `aws/scripts/wait_for_job_completion.sh` | `.sh` | ✅ Used & valid | referenced | - |
| `aws/terraform/terraform-outputs.dev.json` | `.json` | ✅ Used & valid | referenced | - |
| `aws/tests/conftest.py` | `.py` | ❌ Unused / broken | - | - |
| `aws/tests/test_dag_imports.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/tests/test_prod_config_contract.py` | `.py` | ✅ Used & valid | referenced | - |
| `aws/tests/test_schema_contracts.py` | `.py` | ✅ Used & valid | referenced | - |
| `config/config.schema.json` | `.json` | ⚠️ Used but risky | referenced | (?i)secret |
| `config/contracts/fact_orders.json` | `.json` | ❌ Unused / broken | - | - |
| `config/dev.yaml` | `.yaml` | ⚠️ Used but risky | referenced | s3://[^\s"\']+, (?i)secret |
| `config/dq/dq_rules.yaml` | `.yaml` | ❌ Unused / broken | - | - |
| `config/dq_thresholds.yaml` | `.yaml` | ❌ Unused / broken | - | - |
| `config/lineage.yaml` | `.yaml` | ⚠️ Used but risky | referenced | s3://[^\s"\']+, localhost |
| `config/prod.yaml` | `.yaml` | ⚠️ Used but risky | referenced | s3://[^\s"\']+, (?i)secret |
| `config/schema_definitions/bronze/crm_accounts.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/bronze/crm_accounts.schema.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/bronze/crm_contacts.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/bronze/crm_contacts.schema.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/bronze/crm_opportunities.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/bronze/crm_opportunities.schema.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/bronze/fx_rates.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/bronze/fx_rates.schema.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/bronze/kafka_events.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/bronze/kafka_events.schema.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/bronze/redshift_behavior.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/bronze/redshift_behavior.schema.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/bronze/snowflake_customers.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/bronze/snowflake_customers.schema.json` | `.json` | ✅ Used & valid | referenced | - |
| `config/schema_definitions/bronze/snowflake_orders.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/bronze/snowflake_orders.schema.json` | `.json` | ✅ Used & valid | referenced | - |
| `config/schema_definitions/bronze/snowflake_products.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/bronze/snowflake_products.schema.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/customers_bronze.json` | `.json` | ✅ Used & valid | referenced | - |
| `config/schema_definitions/fx_rates_bronze.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/orders_bronze.json` | `.json` | ✅ Used & valid | referenced | - |
| `config/schema_definitions/products_bronze.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/redshift_behavior_bronze.json` | `.json` | ✅ Used & valid | referenced | - |
| `config/schema_definitions/returns_bronze.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/salesforce_accounts_bronze.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/salesforce_cases_bronze.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/salesforce_contacts_bronze.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/salesforce_leads_bronze.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/salesforce_opportunities_bronze.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/salesforce_products_bronze.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/salesforce_solutions_bronze.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/salesforce_tasks_bronze.json` | `.json` | ❌ Unused / broken | - | - |
| `config/schema_definitions/snowflake_orders_bronze.json` | `.json` | ✅ Used & valid | referenced | - |
| `data/bronze/fx/json/fx_rates_historical.json` | `.json` | ✅ Used & valid | referenced | - |
| `data/profile_data.py` | `.py` | ❌ Unused / broken | - | - |
| `data/samples/fx/fx_rates_historical.json` | `.json` | ✅ Used & valid | referenced | - |
| `dbt/dbt_project.yml` | `.yml` | ✅ Used & valid | dbt_project | - |
| `dbt/models/marts/dimensions/dim_customer.sql` | `.sql` | ✅ Used & valid | referenced, dbt_project | - |
| `dbt/models/marts/dimensions/dim_product.sql` | `.sql` | ✅ Used & valid | referenced, dbt_project | - |
| `dbt/models/marts/dimensions/schema.yml` | `.yml` | ✅ Used & valid | dbt_project | - |
| `dbt/models/marts/facts/fct_orders.sql` | `.sql` | ✅ Used & valid | dbt_project | - |
| `dbt/models/marts/facts/schema.yml` | `.yml` | ✅ Used & valid | dbt_project | - |
| `dbt/models/marts/fct_customer_engagement.sql` | `.sql` | ✅ Used & valid | dbt_project | - |
| `dbt/models/sources.yml` | `.yml` | ✅ Used & valid | referenced, dbt_project | - |
| `dbt/models/staging/stg_customer_behavior.sql` | `.sql` | ✅ Used & valid | dbt_project | - |
| `docker-compose-airflow.yml` | `.yml` | ⚠️ Used but risky | referenced | localhost |
| `docker-compose-monitoring.yml` | `.yml` | ❌ Unused / broken | - | localhost, (?i)password\s*= |
| `docker-compose-production.yml` | `.yml` | ❌ Unused / broken | - | localhost |
| `docker-compose.yml` | `.yml` | ❌ Unused / broken | - | 127\.0\.0\.1, (?i)password\s*= |
| `docker/docker-compose.yml` | `.yml` | ❌ Unused / broken | - | localhost |
| `jobs/dq/dq_gate.py` | `.py` | ✅ Used & valid | referenced | - |
| `jobs/dq/run_comprehensive_dq.py` | `.py` | ❌ Unused / broken | - | - |
| `jobs/iceberg/orders_silver_to_iceberg.py` | `.py` | ❌ Unused / broken | - | - |
| `jobs/ingest/_lib/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `jobs/ingest/_lib/watermark.py` | `.py` | ❌ Unused / broken | - | - |
| `jobs/ingest/crm_to_bronze.py` | `.py` | ✅ Used & valid | imported, referenced | - |
| `jobs/ingest/fx_json_to_bronze.py` | `.py` | ⚠️ Used but risky | imported, referenced | s3://[^\s"\']+ |
| `jobs/ingest/fx_to_bronze.py` | `.py` | ✅ Used & valid | imported, referenced | - |
| `jobs/ingest/ingest_crm_to_s3.py` | `.py` | ⚠️ Used but risky | imported | s3://[^\s"\']+ |
| `jobs/ingest/ingest_snowflake_to_s3.py` | `.py` | ⚠️ Used but risky | imported | s3://[^\s"\']+, (?i)secret |
| `jobs/ingest/kafka_events_to_bronze.py` | `.py` | ✅ Used & valid | imported, referenced | - |
| `jobs/ingest/redshift_to_bronze.py` | `.py` | ✅ Used & valid | imported, referenced | - |
| `jobs/ingest/snowflake_to_bronze.py` | `.py` | ✅ Used & valid | imported, referenced | - |
| `jobs/publish/gold_to_snowflake.py` | `.py` | ❌ Unused / broken | - | (?i)password\s*=, (?i)secret |
| `jobs/publish/publish_gold_to_redshift.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+, (?i)password\s*= |
| `jobs/publish/publish_gold_to_snowflake.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `jobs/run_pipeline.py` | `.py` | ⚠️ Used but risky | entrypoint, referenced | s3://[^\s"\']+ |
| `jobs/streaming/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `jobs/streaming/kafka_producer.py` | `.py` | ✅ Used & valid | imported, referenced | - |
| `jobs/transform/bronze_to_silver.py` | `.py` | ✅ Used & valid | imported, referenced | - |
| `jobs/transform/raw_to_bronze.py` | `.py` | ⚠️ Used but risky | imported | s3://[^\s"\']+ |
| `jobs/transform/silver_to_gold.py` | `.py` | ✅ Used & valid | imported, referenced | - |
| `jobs_legacy/transform/bronze_to_silver_old.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `jobs_legacy/transform/silver_to_gold_old.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `local/config/local.yaml` | `.yaml` | ⚠️ Used but risky | referenced | file:///[^\s"\']+, localhost |
| `local/scripts/dq/check_data_quality.py` | `.py` | ✅ Used & valid | referenced | - |
| `local/scripts/dq/check_s3_data_quality.py` | `.py` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `local/scripts/dq/compare_local_aws_dq.py` | `.py` | ❌ Unused / broken | - | - |
| `local/scripts/kafka_producer.py` | `.py` | ⚠️ Used but risky | referenced | localhost |
| `local/scripts/quick_check.py` | `.py` | ❌ Unused / broken | - | - |
| `local/scripts/regenerate_source_data.py` | `.py` | ✅ Used & valid | referenced | - |
| `local/scripts/run_full_pipeline.sh` | `.sh` | ❌ Unused / broken | - | - |
| `local/scripts/run_local_etl.sh` | `.sh` | ❌ Unused / broken | - | - |
| `local/scripts/run_local_etl_fixed.sh` | `.sh` | ❌ Unused / broken | - | - |
| `local/scripts/run_local_etl_safe.py` | `.py` | ❌ Unused / broken | - | - |
| `local/scripts/setup_airflow_local.sh` | `.sh` | ❌ Unused / broken | - | s3://[^\s"\']+, localhost |
| `local/scripts/show_s3_output_data.py` | `.py` | ❌ Unused / broken | - | - |
| `local/scripts/validate_source_data.py` | `.py` | ❌ Unused / broken | - | - |
| `local/tests/load_test_pipeline.py` | `.py` | ❌ Unused / broken | - | - |
| `local/tests/local/crm_etl_pipeline.py` | `.py` | ❌ Unused / broken | - | - |
| `local/tests/local/run_etl_end_to_end.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `local/tests/local/run_pipeline.py` | `.py` | ✅ Used & valid | referenced | - |
| `local/tests/local/test_dq_lineage_integration.py` | `.py` | ❌ Unused / broken | - | - |
| `local/tests/local/test_etl_pipeline.py` | `.py` | ❌ Unused / broken | - | - |
| `local/tests/local/test_etl_simple.py` | `.py` | ❌ Unused / broken | - | - |
| `local/tests/local/verify_enterprise_setup.py` | `.py` | ❌ Unused / broken | - | (?i)secret |
| `local/tests/smoke_validate.py` | `.py` | ❌ Unused / broken | - | - |
| `scripts/convert_fx_csv_to_json.py` | `.py` | ✅ Used & valid | referenced | - |
| `scripts/emr_submit.sh` | `.sh` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `scripts/fetch_silver_to_gold_logs.sh` | `.sh` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `scripts/final_cleanup.py` | `.py` | ❌ Unused / broken | - | - |
| `scripts/final_validation.py` | `.py` | ❌ Unused / broken | - | - |
| `scripts/kafka_consumer.py` | `.py` | ❌ Unused / broken | - | localhost |
| `scripts/kafka_producer.py` | `.py` | ⚠️ Used but risky | referenced | (?i)secret |
| `scripts/maintenance/backfill_range.py` | `.py` | ✅ Used & valid | referenced | - |
| `scripts/maintenance/optimize_tables.py` | `.py` | ❌ Unused / broken | - | - |
| `scripts/organize_data_directory.py` | `.py` | ❌ Unused / broken | - | - |
| `scripts/run_phase4_jobs.sh` | `.sh` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `scripts/show_pipeline_results.py` | `.py` | ❌ Unused / broken | - | - |
| `scripts/sync_data_sources.sh` | `.sh` | ❌ Unused / broken | - | - |
| `scripts/upload_jobs_to_s3.py` | `.py` | ⚠️ Used but risky | referenced | s3://[^\s"\']+, (?i)secret |
| `scripts/validate_all_components.py` | `.py` | ❌ Unused / broken | - | - |
| `src/dq/ge_checkpoint.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/archival/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/archival/policy.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/cdc/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/cdc/change_capture.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/cicd/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/cicd/pipeline.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/cli.py` | `.py` | ✅ Used & valid | entrypoint | - |
| `src/project_a/config_loader.py` | `.py` | ⚠️ Used but risky | referenced | (?i)secret |
| `src/project_a/contracts/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/contracts/management.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/core/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/core/base_job.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/core/config.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+, (?i)secret |
| `src/project_a/core/context.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+, localhost |
| `src/project_a/cost/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/cost/optimization.py` | `.py` | ⚠️ Used but risky | referenced | (?i)secret |
| `src/project_a/delta_utils.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/disaster_recovery/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/disaster_recovery/plans.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/dq/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/dq/automation.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/dq/comprehensive_validator.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/dq/file_integrity_checker.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/dq/gate.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/dq/kafka_streaming_validator.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/dq/performance_optimizer.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/dq/referential_integrity.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/dq/run_ge.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/dq/schema_drift_checker.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/extract/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/extract/fx_json_reader.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/iceberg_utils.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/io/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/io/reader.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/io/writer.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/jobs/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/jobs/_compat.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/jobs/bronze_to_silver.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/jobs/contacts_silver.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/jobs/crm_to_bronze.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/jobs/dq_gold_gate.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/jobs/dq_silver_gate.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/jobs/fx_json_to_bronze.py` | `.py` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `src/project_a/jobs/kafka_csv_to_bronze.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/jobs/kafka_orders_stream.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/jobs/publish_gold_to_redshift.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/jobs/publish_gold_to_snowflake.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/jobs/redshift_to_bronze.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/jobs/salesforce_to_bronze.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/jobs/silver_to_gold.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/jobs/snowflake_to_bronze.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/legacy/__main__.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/advanced_dq_monitoring.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/api/customer_api.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/cicd_manager.py` | `.py` | ❌ Unused / broken | - | (?i)secret |
| `src/project_a/legacy/cli.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/common/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/legacy/common/dq.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/common/scd2.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/legacy/config/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/legacy/config/paths.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/config_loader.py` | `.py` | ⚠️ Used but risky | referenced | (?i)secret |
| `src/project_a/legacy/config_model.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/contracts/crm_accounts.schema.json` | `.json` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/contracts/crm_contacts.schema.json` | `.json` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/contracts/crm_opportunities.schema.json` | `.json` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/contracts/fx_rates.schema.json` | `.json` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/contracts/kafka_events.schema.json` | `.json` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/contracts/redshift_behavior.schema.json` | `.json` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/contracts/snowflake_orders.schema.json` | `.json` | ✅ Used & valid | referenced | - |
| `src/project_a/legacy/data_contracts.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/data_quality_suite.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/delta_lake_standard.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/delta_utils.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/disaster_recovery.py` | `.py` | ❌ Unused / broken | - | (?i)secret |
| `src/project_a/legacy/dq/run_ge_customer_behavior.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/dq/smoke.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/dq_checks.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/dr/dr/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/legacy/dr/dr_runner.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/enterprise_data_platform.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/gold_writer.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/incremental_loading.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/ingestion_pipeline.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/io_utils.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/jobs/dim_customer_scd2.py` | `.py` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `src/project_a/legacy/jobs/fx_bronze_to_silver.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/jobs/fx_to_bronze.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/legacy/jobs/gold_star_schema.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/legacy/jobs/gold_writer.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/jobs/kafka_orders_stream.py` | `.py` | ❌ Unused / broken | - | (?i)password\s*=, (?i)secret |
| `src/project_a/legacy/jobs/load_to_snowflake.py` | `.py` | ❌ Unused / broken | - | (?i)secret |
| `src/project_a/legacy/jobs/reconciliation_job.py` | `.py` | ❌ Unused / broken | - | (?i)secret |
| `src/project_a/legacy/jobs/salesforce_bronze_to_silver.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/jobs/salesforce_to_bronze.py` | `.py` | ⚠️ Used but risky | referenced | (?i)password\s*=, (?i)secret |
| `src/project_a/legacy/jobs/snowflake_bronze_to_silver_merge.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/legacy/jobs/snowflake_to_bronze.py` | `.py` | ⚠️ Used but risky | referenced | (?i)password\s*= |
| `src/project_a/legacy/jobs/update_customer_dimension_scd2.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/legacy/lineage_tracker.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/load/write_idempotent.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/legacy/logging_config.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/logging_setup.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/metrics/ingest_pipeline_metrics.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/metrics/metrics/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/legacy/metrics/metrics/metrics_exporter.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/metrics/metrics/sink.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/metrics_collector.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/modeling.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/legacy/pipeline/pipeline/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/legacy/pipeline/pipeline/run_pipeline.py` | `.py` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `src/project_a/legacy/pipeline/scd2_customers.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/pipeline_core.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/schema/validator.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/schemas/returns_raw.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/streaming/kafka_customer_events.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+, localhost |
| `src/project_a/legacy/streaming_core.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/transform/bronze_to_silver_multi_source.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/legacy/transform/enrich_with_fx.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/transform/incremental_customer_dim_upsert.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/legacy/transform/pii_masking.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/utils/dlq_handler.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/legacy/utils/freshness_guards.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/legacy/validation/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/lineage/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/lineage/tracking.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/metadata/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/metadata/catalog.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/monitoring/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/monitoring/lineage_emitter.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/monitoring/metrics.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/performance/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/performance/optimization.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/pipeline/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/pipeline/run_pipeline.py` | `.py` | ⚠️ Used but risky | entrypoint, referenced | s3://[^\s"\']+ |
| `src/project_a/privacy/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/privacy/compliance.py` | `.py` | ❌ Unused / broken | - | (?i)secret |
| `src/project_a/pyspark_interview_project/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/pyspark_interview_project/dq/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/pyspark_interview_project/dq/gate.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/pyspark_interview_project/dq/ge_runner.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/pyspark_interview_project/dq/great_expectations_runner.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/dq/rules.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/dq/runner.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/pyspark_interview_project/dq/suites/gold_revenue.yml` | `.yml` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/dq/suites/silver_fx_rates.yml` | `.yml` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/dq/suites/silver_orders.yml` | `.yml` | ✅ Used & valid | referenced | - |
| `src/project_a/pyspark_interview_project/extract/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/pyspark_interview_project/extract/base_extractor.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/extract/fx_json_reader.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/pyspark_interview_project/extract/kafka_orders_stream.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+, localhost |
| `src/project_a/pyspark_interview_project/extract/redshift_behavior.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/pyspark_interview_project/extract/snowflake_orders.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+, (?i)secret |
| `src/project_a/pyspark_interview_project/io/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/pyspark_interview_project/io/delta_writer.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/io/path_resolver.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/io/publish.py` | `.py` | ❌ Unused / broken | - | (?i)secret |
| `src/project_a/pyspark_interview_project/io/snowflake_writer.py` | `.py` | ❌ Unused / broken | - | (?i)secret |
| `src/project_a/pyspark_interview_project/io/write_table.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/pyspark_interview_project/lineage/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/pyspark_interview_project/lineage/openlineage_emitter.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/monitoring.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/monitoring/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/pyspark_interview_project/monitoring/alerts.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/monitoring/lineage_decorator.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/pyspark_interview_project/monitoring/lineage_emitter.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/pyspark_interview_project/monitoring/metrics.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/pyspark_interview_project/monitoring/metrics_collector.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/performance_optimizer.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/production_pipeline.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/schema_validator.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/schemas/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/pyspark_interview_project/schemas/bronze_schemas.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/schemas/production_schemas.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/standard_etl_pipeline.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/transform/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/pyspark_interview_project/transform/base_transformer.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/transform/bronze_loaders.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/transform/gold_builders.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/transform/silver_builders.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/unity_catalog.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/utils/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/pyspark_interview_project/utils/checkpoint.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/pyspark_interview_project/utils/cloudwatch_metrics.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/utils/config.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/utils/config_loader.py` | `.py` | ⚠️ Used but risky | referenced | s3://[^\s"\']+, (?i)secret |
| `src/project_a/pyspark_interview_project/utils/contracts.py` | `.py` | ⚠️ Used but risky | referenced | s3://[^\s"\']+ |
| `src/project_a/pyspark_interview_project/utils/dq_utils.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/utils/error_lanes.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/pyspark_interview_project/utils/io.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/utils/logging.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/utils/metrics.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/pyspark_interview_project/utils/path_resolver.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/pyspark_interview_project/utils/pii_utils.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/utils/run_audit.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/pyspark_interview_project/utils/safe_writer.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/utils/schema_validator.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/pyspark_interview_project/utils/secrets.py` | `.py` | ❌ Unused / broken | - | (?i)secret |
| `src/project_a/pyspark_interview_project/utils/spark_session.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/utils/state_store.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/utils/watermark.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/pyspark_interview_project/utils/watermark_utils.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/pyspark_interview_project/validate.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/schema/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/schema/validator.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/schemas/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/schemas/bronze_schemas.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/security/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/security/access_control.py` | `.py` | ❌ Unused / broken | - | (?i)secret |
| `src/project_a/testing/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/testing/framework.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/transform/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/utils/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/utils/cloudwatch_metrics.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/utils/config.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+, (?i)secret |
| `src/project_a/utils/contracts.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/utils/error_lanes.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/utils/logging.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/utils/path_resolver.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+, file:///[^\s"\']+, localhost |
| `src/project_a/utils/run_audit.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `src/project_a/utils/schema_validator.py` | `.py` | ❌ Unused / broken | - | - |
| `src/project_a/utils/spark.py` | `.py` | ✅ Used & valid | referenced | - |
| `src/project_a/utils/spark_session.py` | `.py` | ❌ Unused / broken | - | localhost, 127\.0\.0\.1 |
| `src/pyspark_interview_project/__init__.py` | `.py` | ✅ Used & valid | referenced | - |
| `steps_bronze_to_silver.json` | `.json` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `steps_silver_to_gold.json` | `.json` | ❌ Unused / broken | - | s3://[^\s"\']+ |
| `test_pipeline.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/conftest.py` | `.py` | ❌ Unused / broken | - | localhost, 127\.0\.0\.1 |
| `tests/data/orders.json` | `.json` | ✅ Used & valid | referenced | - |
| `tests/delta_test.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/dev_secret_probe.py` | `.py` | ⚠️ Used but risky | referenced | (?i)secret |
| `tests/test_bronze_to_silver.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_bronze_to_silver_behavior.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_bronze_to_silver_orders.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_collect_run_summary.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_config.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_config_validation.py` | `.py` | ❌ Unused / broken | - | (?i)password\s*=, (?i)secret |
| `tests/test_contracts.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_contracts_customers.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_dag_imports.py` | `.py` | ✅ Used & valid | referenced | - |
| `tests/test_dq_checks.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_dq_policies.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_extract.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_fx_transform.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_glue_catalog_contract.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_gold_contract.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_great_expectations_runner.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_integration.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_integration_comprehensive.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_lineage_emitter.py` | `.py` | ❌ Unused / broken | - | s3://[^\s"\']+, localhost |
| `tests/test_load.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_metrics_collector.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_module_imports.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_monitoring.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_orders_transform.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_performance_optimization.py` | `.py` | ✅ Used & valid | referenced | - |
| `tests/test_pii_utils.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_pipeline_components.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_pipeline_integration.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_publish_gold_to_snowflake.py` | `.py` | ❌ Unused / broken | - | (?i)secret |
| `tests/test_quality_gate.py` | `.py` | ✅ Used & valid | referenced | - |
| `tests/test_safe_writer.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_scd2.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_scd2_validation.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_schema_alignment.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_schema_validator.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_schemas.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_secrets.py` | `.py` | ❌ Unused / broken | - | (?i)secret |
| `tests/test_silver_behavior_contract.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_silver_build_customer_360.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_spark_session.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_transform.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_validate.py` | `.py` | ❌ Unused / broken | - | - |
| `tests/test_watermark_utils.py` | `.py` | ❌ Unused / broken | - | - |
| `tools/validate_aws_etl.py` | `.py` | ⚠️ Used but risky | entrypoint, referenced | s3://[^\s"\']+ |
| `tools/validate_local_etl.py` | `.py` | ⚠️ Used but risky | entrypoint, referenced | s3://[^\s"\']+ |

## Function & Class Health Report (Heuristic)
| Symbol | Kind | File:Line | Status | Evidence |
|---|---|---|---|---|
| `notify_failure` | `FunctionDef` | `aws/dags_legacy/daily_pipeline_dag_complete.py:15` | ✅ Works (referenced) | refs=4 |
| `register_all_glue_tables` | `FunctionDef` | `aws/dags_legacy/daily_pipeline_dag_complete.py:57` | ✅ Works (referenced) | refs=2 |
| `check_data_quality` | `FunctionDef` | `aws/dags_legacy/dq_watchdog_dag.py:34` | ✅ Works (referenced) | refs=8 |
| `send_alert_if_needed` | `FunctionDef` | `aws/dags_legacy/dq_watchdog_dag.py:105` | ✅ Works (referenced) | refs=2 |
| `notify_failure` | `FunctionDef` | `aws/dags_legacy/project_a_daily_pipeline.py:39` | ✅ Works (referenced) | refs=4 |
| `get_job_driver` | `FunctionDef` | `aws/dags_legacy/project_a_daily_pipeline.py:121` | ✅ Works (referenced) | refs=12 |
| `check_salesforce_data_quality` | `FunctionDef` | `aws/dags_legacy/salesforce_ingestion_dag.py:48` | ✅ Works (referenced) | refs=2 |
| `run_crm_accounts_ingest` | `FunctionDef` | `aws/jobs/ingest/crm_accounts_ingest.py:27` | ✅ Works (referenced) | refs=2 |
| `run_crm_contacts_ingest` | `FunctionDef` | `aws/jobs/ingest/crm_contacts_ingest.py:28` | ✅ Works (referenced) | refs=2 |
| `run_crm_opportunities_ingest` | `FunctionDef` | `aws/jobs/ingest/crm_opportunities_ingest.py:25` | ✅ Works (referenced) | refs=2 |
| `get_fx_api_config` | `FunctionDef` | `aws/jobs/ingest/fx_rates_ingest.py:33` | ✅ Works (referenced) | refs=2 |
| `fetch_fx_rates` | `FunctionDef` | `aws/jobs/ingest/fx_rates_ingest.py:47` | ✅ Works (referenced) | refs=6 |
| `fetch_financial_metrics` | `FunctionDef` | `aws/jobs/ingest/fx_rates_ingest.py:69` | ✅ Works (referenced) | refs=2 |
| `create_fx_rates_dataframe` | `FunctionDef` | `aws/jobs/ingest/fx_rates_ingest.py:115` | ✅ Works (referenced) | refs=2 |
| `create_financial_metrics_dataframe` | `FunctionDef` | `aws/jobs/ingest/fx_rates_ingest.py:137` | ✅ Works (referenced) | refs=2 |
| `add_metadata_columns` | `FunctionDef` | `aws/jobs/ingest/fx_rates_ingest.py:159` | ✅ Works (referenced) | refs=13 |
| `write_to_bronze` | `FunctionDef` | `aws/jobs/ingest/fx_rates_ingest.py:181` | ✅ Works (referenced) | refs=12 |
| `process_fx_ingestion` | `FunctionDef` | `aws/jobs/ingest/fx_rates_ingest.py:201` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `aws/jobs/ingest/fx_rates_ingest.py:232` | ✅ Works (referenced) | refs=480 |
| `get_kafka_config` | `FunctionDef` | `aws/jobs/ingest/kafka_orders_stream.py:32` | ✅ Works (referenced) | refs=4 |
| `get_orders_schema` | `FunctionDef` | `aws/jobs/ingest/kafka_orders_stream.py:54` | ✅ Works (referenced) | refs=4 |
| `process_orders_stream` | `FunctionDef` | `aws/jobs/ingest/kafka_orders_stream.py:73` | ✅ Works (referenced) | refs=4 |
| `main` | `FunctionDef` | `aws/jobs/ingest/kafka_orders_stream.py:213` | ✅ Works (referenced) | refs=480 |
| `get_redshift_connection_string` | `FunctionDef` | `aws/jobs/ingest/redshift_behavior_ingest.py:28` | ✅ Works (referenced) | refs=2 |
| `get_behavior_query` | `FunctionDef` | `aws/jobs/ingest/redshift_behavior_ingest.py:48` | ✅ Works (referenced) | refs=2 |
| `extract_redshift_behavior_data` | `FunctionDef` | `aws/jobs/ingest/redshift_behavior_ingest.py:79` | ✅ Works (referenced) | refs=2 |
| `add_metadata_columns` | `FunctionDef` | `aws/jobs/ingest/redshift_behavior_ingest.py:110` | ✅ Works (referenced) | refs=13 |
| `write_to_bronze` | `FunctionDef` | `aws/jobs/ingest/redshift_behavior_ingest.py:132` | ✅ Works (referenced) | refs=12 |
| `process_redshift_behavior_ingestion` | `FunctionDef` | `aws/jobs/ingest/redshift_behavior_ingest.py:152` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `aws/jobs/ingest/redshift_behavior_ingest.py:181` | ✅ Works (referenced) | refs=480 |
| `upload_to_s3` | `FunctionDef` | `aws/jobs/ingest/salesforce_to_bronze.py:30` | ✅ Works (referenced) | refs=6 |
| `validate_data_quality` | `FunctionDef` | `aws/jobs/ingest/salesforce_to_bronze.py:62` | ✅ Works (referenced) | refs=8 |
| `main` | `FunctionDef` | `aws/jobs/ingest/salesforce_to_bronze.py:123` | ✅ Works (referenced) | refs=480 |
| `get_snowflake_connection_string` | `FunctionDef` | `aws/jobs/ingest/snowflake_to_bronze.py:26` | ✅ Works (referenced) | refs=4 |
| `get_sample_queries` | `FunctionDef` | `aws/jobs/ingest/snowflake_to_bronze.py:46` | ✅ Works (referenced) | refs=4 |
| `extract_snowflake_data` | `FunctionDef` | `aws/jobs/ingest/snowflake_to_bronze.py:105` | ✅ Works (referenced) | refs=4 |
| `add_metadata_columns` | `FunctionDef` | `aws/jobs/ingest/snowflake_to_bronze.py:139` | ✅ Works (referenced) | refs=13 |
| `write_to_bronze` | `FunctionDef` | `aws/jobs/ingest/snowflake_to_bronze.py:159` | ✅ Works (referenced) | refs=12 |
| `process_snowflake_backfill` | `FunctionDef` | `aws/jobs/ingest/snowflake_to_bronze.py:179` | ✅ Works (referenced) | refs=4 |
| `main` | `FunctionDef` | `aws/jobs/ingest/snowflake_to_bronze.py:211` | ✅ Works (referenced) | refs=480 |
| `DataMaskingEngine` | `ClassDef` | `aws/jobs/maintenance/apply_data_masking.py:40` | ✅ Works (referenced) | refs=2 |
| `__init__` | `FunctionDef` | `aws/jobs/maintenance/apply_data_masking.py:43` | ✅ Works (referenced) | refs=162 |
| `apply_masking_strategies` | `FunctionDef` | `aws/jobs/maintenance/apply_data_masking.py:54` | ✅ Works (referenced) | refs=2 |
| `_get_masking_strategy` | `FunctionDef` | `aws/jobs/maintenance/apply_data_masking.py:97` | ✅ Works (referenced) | refs=3 |
| `_apply_column_masking` | `FunctionDef` | `aws/jobs/maintenance/apply_data_masking.py:136` | ✅ Works (referenced) | refs=3 |
| `create_masking_audit_table` | `FunctionDef` | `aws/jobs/maintenance/apply_data_masking.py:217` | ✅ Works (referenced) | refs=2 |
| `mask_silver_to_gold_table` | `FunctionDef` | `aws/jobs/maintenance/apply_data_masking.py:238` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `aws/jobs/maintenance/apply_data_masking.py:275` | ✅ Works (referenced) | refs=480 |
| `optimize_table` | `FunctionDef` | `aws/jobs/maintenance/delta_optimize_vacuum.py:27` | ✅ Works (referenced) | refs=18 |
| `vacuum_table` | `FunctionDef` | `aws/jobs/maintenance/delta_optimize_vacuum.py:60` | ✅ Works (referenced) | refs=6 |
| `optimize_all_tables` | `FunctionDef` | `aws/jobs/maintenance/delta_optimize_vacuum.py:80` | ✅ Works (referenced) | refs=2 |
| `vacuum_all_tables` | `FunctionDef` | `aws/jobs/maintenance/delta_optimize_vacuum.py:126` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `aws/jobs/maintenance/delta_optimize_vacuum.py:155` | ✅ Works (referenced) | refs=480 |
| `main` | `FunctionDef` | `aws/jobs/transform/bronze_to_silver.py:25` | ✅ Works (referenced) | refs=480 |
| `main` | `FunctionDef` | `aws/jobs/transform/dq_check_bronze.py:24` | ✅ Works (referenced) | refs=480 |
| `main` | `FunctionDef` | `aws/jobs/transform/dq_check_silver.py:23` | ✅ Works (referenced) | refs=480 |
| `main` | `FunctionDef` | `aws/jobs/transform/silver_to_gold.py:25` | ✅ Works (referenced) | refs=480 |
| `merge_orders_data` | `FunctionDef` | `aws/jobs/transform/snowflake_bronze_to_silver_merge.py:27` | ✅ Works (referenced) | refs=4 |
| `merge_customers_data` | `FunctionDef` | `aws/jobs/transform/snowflake_bronze_to_silver_merge.py:117` | ✅ Works (referenced) | refs=4 |
| `merge_lineitems_data` | `FunctionDef` | `aws/jobs/transform/snowflake_bronze_to_silver_merge.py:204` | ✅ Works (referenced) | refs=4 |
| `process_snowflake_merge` | `FunctionDef` | `aws/jobs/transform/snowflake_bronze_to_silver_merge.py:313` | ✅ Works (referenced) | refs=4 |
| `main` | `FunctionDef` | `aws/jobs/transform/snowflake_bronze_to_silver_merge.py:334` | ✅ Works (referenced) | refs=480 |
| `build_customer_dimension` | `FunctionDef` | `aws/jobs_legacy/analytics/build_customer_dimension.py:35` | ✅ Works (referenced) | refs=7 |
| `main` | `FunctionDef` | `aws/jobs_legacy/analytics/build_customer_dimension.py:117` | ✅ Works (referenced) | refs=480 |
| `build_marketing_attribution` | `FunctionDef` | `aws/jobs_legacy/analytics/build_marketing_attribution.py:29` | ✅ Works (referenced) | refs=3 |
| `main` | `FunctionDef` | `aws/jobs_legacy/analytics/build_marketing_attribution.py:120` | ✅ Works (referenced) | refs=480 |
| `build_sales_fact_table` | `FunctionDef` | `aws/jobs_legacy/analytics/build_sales_fact_table.py:37` | ✅ Works (referenced) | refs=5 |
| `main` | `FunctionDef` | `aws/jobs_legacy/analytics/build_sales_fact_table.py:104` | ✅ Works (referenced) | refs=480 |
| `update_customer_dimension_scd2` | `FunctionDef` | `aws/jobs_legacy/analytics/update_customer_dimension_scd2.py:32` | ✅ Works (referenced) | refs=5 |
| `analyze_scd2_changes` | `FunctionDef` | `aws/jobs_legacy/analytics/update_customer_dimension_scd2.py:212` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `aws/jobs_legacy/analytics/update_customer_dimension_scd2.py:246` | ✅ Works (referenced) | refs=480 |
| `create_dq_alarm` | `FunctionDef` | `aws/scripts/create_cloudwatch_alarms.py:22` | ✅ Works (referenced) | refs=2 |
| `create_latency_alarm` | `FunctionDef` | `aws/scripts/create_cloudwatch_alarms.py:41` | ✅ Works (referenced) | refs=4 |
| `create_cost_alarm` | `FunctionDef` | `aws/scripts/create_cloudwatch_alarms.py:59` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `aws/scripts/create_cloudwatch_alarms.py:77` | ✅ Works (referenced) | refs=480 |
| `DRSnapshotExporter` | `ClassDef` | `aws/scripts/maintenance/dr_snapshot_export.py:24` | ✅ Works (referenced) | refs=2 |
| `__init__` | `FunctionDef` | `aws/scripts/maintenance/dr_snapshot_export.py:27` | ✅ Works (referenced) | refs=162 |
| `get_latest_partitions` | `FunctionDef` | `aws/scripts/maintenance/dr_snapshot_export.py:44` | ✅ Works (referenced) | refs=2 |
| `copy_partition` | `FunctionDef` | `aws/scripts/maintenance/dr_snapshot_export.py:80` | ✅ Works (referenced) | refs=2 |
| `export_table` | `FunctionDef` | `aws/scripts/maintenance/dr_snapshot_export.py:103` | ✅ Works (referenced) | refs=2 |
| `export_all_tables` | `FunctionDef` | `aws/scripts/maintenance/dr_snapshot_export.py:142` | ✅ Works (referenced) | refs=2 |
| `create_dr_metadata` | `FunctionDef` | `aws/scripts/maintenance/dr_snapshot_export.py:194` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `aws/scripts/maintenance/dr_snapshot_export.py:229` | ✅ Works (referenced) | refs=480 |
| `create_glue_table` | `FunctionDef` | `aws/scripts/register_glue_table.py:15` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `aws/scripts/register_glue_table.py:77` | ✅ Works (referenced) | refs=480 |
| `create_artifact_package` | `FunctionDef` | `aws/scripts/run_aws_emr_serverless.py:23` | ✅ Works (referenced) | refs=2 |
| `upload_to_s3` | `FunctionDef` | `aws/scripts/run_aws_emr_serverless.py:52` | ✅ Works (referenced) | refs=6 |
| `submit_emr_serverless_job` | `FunctionDef` | `aws/scripts/run_aws_emr_serverless.py:74` | ✅ Works (referenced) | refs=2 |
| `wait_for_job_completion` | `FunctionDef` | `aws/scripts/run_aws_emr_serverless.py:124` | ✅ Works (referenced) | refs=4 |
| `main` | `FunctionDef` | `aws/scripts/run_aws_emr_serverless.py:155` | ✅ Works (referenced) | refs=480 |
| `run_ge_suite` | `FunctionDef` | `aws/scripts/run_ge_checks.py:31` | ✅ Works (referenced) | refs=2 |
| `_filter_critical_expectations` | `FunctionDef` | `aws/scripts/run_ge_checks.py:96` | ✅ Works (referenced) | refs=2 |
| `save_dq_results` | `FunctionDef` | `aws/scripts/run_ge_checks.py:108` | ✅ Works (referenced) | refs=2 |
| `send_dq_alert` | `FunctionDef` | `aws/scripts/run_ge_checks.py:127` | ✅ Works (referenced) | refs=2 |
| `RunMetadata` | `ClassDef` | `aws/scripts/utilities/emit_lineage_and_metrics.py:32` | ✅ Works (referenced) | refs=7 |
| `LineageEvent` | `ClassDef` | `aws/scripts/utilities/emit_lineage_and_metrics.py:49` | ✅ Works (referenced) | refs=14 |
| `LineageEmitter` | `ClassDef` | `aws/scripts/utilities/emit_lineage_and_metrics.py:63` | ✅ Works (referenced) | refs=8 |
| `__init__` | `FunctionDef` | `aws/scripts/utilities/emit_lineage_and_metrics.py:66` | ✅ Works (referenced) | refs=162 |
| `create_run_metadata` | `FunctionDef` | `aws/scripts/utilities/emit_lineage_and_metrics.py:73` | ✅ Works (referenced) | refs=2 |
| `emit_lineage_event` | `FunctionDef` | `aws/scripts/utilities/emit_lineage_and_metrics.py:94` | ✅ Works (referenced) | refs=12 |
| `_emit_to_cloudwatch` | `FunctionDef` | `aws/scripts/utilities/emit_lineage_and_metrics.py:120` | ✅ Works (referenced) | refs=2 |
| `_emit_to_openlineage` | `FunctionDef` | `aws/scripts/utilities/emit_lineage_and_metrics.py:160` | ✅ Works (referenced) | refs=2 |
| `log_table_metrics` | `FunctionDef` | `aws/scripts/utilities/emit_lineage_and_metrics.py:171` | ✅ Works (referenced) | refs=2 |
| `finalize_run_metadata` | `FunctionDef` | `aws/scripts/utilities/emit_lineage_and_metrics.py:205` | ⚠️ Risky (only definition found) | refs=1 |
| `emit_pipeline_lineage` | `FunctionDef` | `aws/scripts/utilities/emit_lineage_and_metrics.py:258` | ✅ Works (referenced) | refs=4 |
| `log_table_operation` | `FunctionDef` | `aws/scripts/utilities/emit_lineage_and_metrics.py:294` | ✅ Works (referenced) | refs=3 |
| `main` | `FunctionDef` | `aws/scripts/utilities/emit_lineage_and_metrics.py:328` | ✅ Works (referenced) | refs=480 |
| `create_lf_tags` | `FunctionDef` | `aws/scripts/utilities/lf_tags_seed.py:17` | ✅ Works (referenced) | refs=2 |
| `attach_tags_to_table` | `FunctionDef` | `aws/scripts/utilities/lf_tags_seed.py:38` | ✅ Works (referenced) | refs=4 |
| `attach_tags_to_column` | `FunctionDef` | `aws/scripts/utilities/lf_tags_seed.py:64` | ✅ Works (referenced) | refs=2 |
| `create_grant_policy` | `FunctionDef` | `aws/scripts/utilities/lf_tags_seed.py:97` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `aws/scripts/utilities/lf_tags_seed.py:134` | ✅ Works (referenced) | refs=480 |
| `SLAChecker` | `ClassDef` | `aws/scripts/utilities/notify_on_sla_breach.py:27` | ✅ Works (referenced) | refs=2 |
| `__init__` | `FunctionDef` | `aws/scripts/utilities/notify_on_sla_breach.py:30` | ✅ Works (referenced) | refs=162 |
| `get_job_metrics` | `FunctionDef` | `aws/scripts/utilities/notify_on_sla_breach.py:42` | ✅ Works (referenced) | refs=2 |
| `check_sla_breach` | `FunctionDef` | `aws/scripts/utilities/notify_on_sla_breach.py:98` | ✅ Works (referenced) | refs=2 |
| `send_slack_alert` | `FunctionDef` | `aws/scripts/utilities/notify_on_sla_breach.py:138` | ✅ Works (referenced) | refs=10 |
| `send_sns_alert` | `FunctionDef` | `aws/scripts/utilities/notify_on_sla_breach.py:193` | ✅ Works (referenced) | refs=2 |
| `check_and_alert` | `FunctionDef` | `aws/scripts/utilities/notify_on_sla_breach.py:212` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `aws/scripts/utilities/notify_on_sla_breach.py:241` | ✅ Works (referenced) | refs=480 |
| `register_delta_table` | `FunctionDef` | `aws/scripts/utilities/register_glue_tables.py:29` | ✅ Works (referenced) | refs=3 |
| `scan_and_register_tables` | `FunctionDef` | `aws/scripts/utilities/register_glue_tables.py:60` | ✅ Works (referenced) | refs=2 |
| `create_database_if_not_exists` | `FunctionDef` | `aws/scripts/utilities/register_glue_tables.py:111` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `aws/scripts/utilities/register_glue_tables.py:134` | ✅ Works (referenced) | refs=480 |
| `load_expectation_suite` | `FunctionDef` | `aws/scripts/utilities/run_ge_checks.py:26` | ✅ Works (referenced) | refs=2 |
| `run_yaml_policy` | `FunctionDef` | `aws/scripts/utilities/run_ge_checks.py:40` | ✅ Works (referenced) | refs=6 |
| `run_expectation_checks` | `FunctionDef` | `aws/scripts/utilities/run_ge_checks.py:140` | ✅ Works (referenced) | refs=2 |
| `run_single_expectation` | `FunctionDef` | `aws/scripts/utilities/run_ge_checks.py:184` | ✅ Works (referenced) | refs=2 |
| `write_results_to_s3` | `FunctionDef` | `aws/scripts/utilities/run_ge_checks.py:299` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `aws/scripts/utilities/run_ge_checks.py:337` | ✅ Works (referenced) | refs=480 |
| `aws_root` | `FunctionDef` | `aws/tests/conftest.py:11` | ✅ Works (referenced) | refs=7 |
| `terraform_dir` | `FunctionDef` | `aws/tests/conftest.py:17` | ⚠️ Risky (only definition found) | refs=1 |
| `jobs_dir` | `FunctionDef` | `aws/tests/conftest.py:23` | ⚠️ Risky (only definition found) | refs=1 |
| `config_dir` | `FunctionDef` | `aws/tests/conftest.py:29` | ✅ Works (referenced) | refs=16 |
| `test_import_all_dags` | `FunctionDef` | `aws/tests/test_dag_imports.py:17` | ⚠️ Risky (only definition found) | refs=1 |
| `test_dag_files_exist` | `FunctionDef` | `aws/tests/test_dag_imports.py:50` | ⚠️ Risky (only definition found) | refs=1 |
| `test_prod_config_exists` | `FunctionDef` | `aws/tests/test_prod_config_contract.py:11` | ⚠️ Risky (only definition found) | refs=1 |
| `test_prod_config_has_required_fields` | `FunctionDef` | `aws/tests/test_prod_config_contract.py:17` | ⚠️ Risky (only definition found) | refs=1 |
| `test_schema_contracts_exist` | `FunctionDef` | `aws/tests/test_schema_contracts.py:11` | ⚠️ Risky (only definition found) | refs=1 |
| `test_all_schemas_valid` | `FunctionDef` | `aws/tests/test_schema_contracts.py:17` | ⚠️ Risky (only definition found) | refs=1 |
| `infer_type` | `FunctionDef` | `data/profile_data.py:21` | ⚠️ Risky (only definition found) | refs=1 |
| `profile_csv` | `FunctionDef` | `data/profile_data.py:55` | ✅ Works (referenced) | refs=2 |
| `profile_json` | `FunctionDef` | `data/profile_data.py:107` | ✅ Works (referenced) | refs=2 |
| `find_all_data_files` | `FunctionDef` | `data/profile_data.py:165` | ✅ Works (referenced) | refs=2 |
| `generate_profile_report` | `FunctionDef` | `data/profile_data.py:181` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `data/profile_data.py:272` | ✅ Works (referenced) | refs=480 |
| `main` | `FunctionDef` | `jobs/dq/dq_gate.py:18` | ✅ Works (referenced) | refs=480 |
| `load_bronze_tables` | `FunctionDef` | `jobs/dq/run_comprehensive_dq.py:53` | ✅ Works (referenced) | refs=3 |
| `load_silver_tables` | `FunctionDef` | `jobs/dq/run_comprehensive_dq.py:130` | ✅ Works (referenced) | refs=3 |
| `load_gold_tables` | `FunctionDef` | `jobs/dq/run_comprehensive_dq.py:158` | ✅ Works (referenced) | refs=4 |
| `main` | `FunctionDef` | `jobs/dq/run_comprehensive_dq.py:184` | ✅ Works (referenced) | refs=480 |
| `OrdersSilverToIcebergJob` | `ClassDef` | `jobs/iceberg/orders_silver_to_iceberg.py:30` | ✅ Works (referenced) | refs=2 |
| `__init__` | `FunctionDef` | `jobs/iceberg/orders_silver_to_iceberg.py:41` | ✅ Works (referenced) | refs=162 |
| `run` | `FunctionDef` | `jobs/iceberg/orders_silver_to_iceberg.py:47` | ✅ Works (referenced) | refs=1411 |
| `_table_exists` | `FunctionDef` | `jobs/iceberg/orders_silver_to_iceberg.py:96` | ✅ Works (referenced) | refs=20 |
| `_initial_migration` | `FunctionDef` | `jobs/iceberg/orders_silver_to_iceberg.py:104` | ✅ Works (referenced) | refs=2 |
| `_incremental_update` | `FunctionDef` | `jobs/iceberg/orders_silver_to_iceberg.py:135` | ✅ Works (referenced) | refs=3 |
| `main` | `FunctionDef` | `jobs/iceberg/orders_silver_to_iceberg.py:182` | ✅ Works (referenced) | refs=480 |
| `read_watermark` | `FunctionDef` | `jobs/ingest/_lib/watermark.py:22` | ✅ Works (referenced) | refs=3 |
| `write_watermark` | `FunctionDef` | `jobs/ingest/_lib/watermark.py:64` | ✅ Works (referenced) | refs=3 |
| `CrmToBronzeJob` | `ClassDef` | `jobs/ingest/crm_to_bronze.py:16` | ✅ Works (referenced) | refs=6 |
| `__init__` | `FunctionDef` | `jobs/ingest/crm_to_bronze.py:19` | ✅ Works (referenced) | refs=162 |
| `run` | `FunctionDef` | `jobs/ingest/crm_to_bronze.py:23` | ✅ Works (referenced) | refs=1411 |
| `extract_fx_json_to_bronze` | `FunctionDef` | `jobs/ingest/fx_json_to_bronze.py:51` | ✅ Works (referenced) | refs=3 |
| `main` | `FunctionDef` | `jobs/ingest/fx_json_to_bronze.py:155` | ✅ Works (referenced) | refs=480 |
| `FxToBronzeJob` | `ClassDef` | `jobs/ingest/fx_to_bronze.py:16` | ✅ Works (referenced) | refs=5 |
| `__init__` | `FunctionDef` | `jobs/ingest/fx_to_bronze.py:19` | ✅ Works (referenced) | refs=162 |
| `run` | `FunctionDef` | `jobs/ingest/fx_to_bronze.py:23` | ✅ Works (referenced) | refs=1411 |
| `ingest_crm_to_s3_raw` | `FunctionDef` | `jobs/ingest/ingest_crm_to_s3.py:30` | ✅ Works (referenced) | refs=7 |
| `main` | `FunctionDef` | `jobs/ingest/ingest_crm_to_s3.py:120` | ✅ Works (referenced) | refs=480 |
| `ingest_snowflake_to_s3_raw` | `FunctionDef` | `jobs/ingest/ingest_snowflake_to_s3.py:30` | ✅ Works (referenced) | refs=7 |
| `main` | `FunctionDef` | `jobs/ingest/ingest_snowflake_to_s3.py:109` | ✅ Works (referenced) | refs=480 |
| `KafkaEventsToBronzeJob` | `ClassDef` | `jobs/ingest/kafka_events_to_bronze.py:16` | ✅ Works (referenced) | refs=6 |
| `__init__` | `FunctionDef` | `jobs/ingest/kafka_events_to_bronze.py:19` | ✅ Works (referenced) | refs=162 |
| `run` | `FunctionDef` | `jobs/ingest/kafka_events_to_bronze.py:23` | ✅ Works (referenced) | refs=1411 |
| `RedshiftToBronzeJob` | `ClassDef` | `jobs/ingest/redshift_to_bronze.py:16` | ✅ Works (referenced) | refs=6 |
| `__init__` | `FunctionDef` | `jobs/ingest/redshift_to_bronze.py:19` | ✅ Works (referenced) | refs=162 |
| `run` | `FunctionDef` | `jobs/ingest/redshift_to_bronze.py:23` | ✅ Works (referenced) | refs=1411 |
| `SnowflakeToBronzeJob` | `ClassDef` | `jobs/ingest/snowflake_to_bronze.py:16` | ✅ Works (referenced) | refs=6 |
| `__init__` | `FunctionDef` | `jobs/ingest/snowflake_to_bronze.py:19` | ✅ Works (referenced) | refs=162 |
| `run` | `FunctionDef` | `jobs/ingest/snowflake_to_bronze.py:23` | ✅ Works (referenced) | refs=1411 |
| `get_snowflake_options` | `FunctionDef` | `jobs/publish/gold_to_snowflake.py:30` | ✅ Works (referenced) | refs=2 |
| `load_gold_table` | `FunctionDef` | `jobs/publish/gold_to_snowflake.py:99` | ✅ Works (referenced) | refs=6 |
| `prepare_dataframe_for_snowflake` | `FunctionDef` | `jobs/publish/gold_to_snowflake.py:141` | ✅ Works (referenced) | refs=2 |
| `merge_into_snowflake` | `FunctionDef` | `jobs/publish/gold_to_snowflake.py:172` | ✅ Works (referenced) | refs=2 |
| `publish_to_snowflake` | `FunctionDef` | `jobs/publish/gold_to_snowflake.py:263` | ✅ Works (referenced) | refs=3 |
| `main` | `FunctionDef` | `jobs/publish/gold_to_snowflake.py:317` | ✅ Works (referenced) | refs=480 |
| `parse_args` | `FunctionDef` | `jobs/publish/publish_gold_to_redshift.py:17` | ✅ Works (referenced) | refs=74 |
| `main` | `FunctionDef` | `jobs/publish/publish_gold_to_redshift.py:25` | ✅ Works (referenced) | refs=480 |
| `parse_args` | `FunctionDef` | `jobs/publish/publish_gold_to_snowflake.py:17` | ✅ Works (referenced) | refs=74 |
| `main` | `FunctionDef` | `jobs/publish/publish_gold_to_snowflake.py:25` | ✅ Works (referenced) | refs=480 |
| `main` | `FunctionDef` | `jobs/run_pipeline.py:43` | ✅ Works (referenced) | refs=480 |
| `KafkaProducerJob` | `ClassDef` | `jobs/streaming/kafka_producer.py:16` | ✅ Works (referenced) | refs=3 |
| `__init__` | `FunctionDef` | `jobs/streaming/kafka_producer.py:19` | ✅ Works (referenced) | refs=162 |
| `run` | `FunctionDef` | `jobs/streaming/kafka_producer.py:23` | ✅ Works (referenced) | refs=1411 |
| `BronzeToSilverJob` | `ClassDef` | `jobs/transform/bronze_to_silver.py:16` | ✅ Works (referenced) | refs=6 |
| `__init__` | `FunctionDef` | `jobs/transform/bronze_to_silver.py:19` | ✅ Works (referenced) | refs=162 |
| `run` | `FunctionDef` | `jobs/transform/bronze_to_silver.py:23` | ✅ Works (referenced) | refs=1411 |
| `transform_crm_data` | `FunctionDef` | `jobs/transform/bronze_to_silver.py:81` | ✅ Works (referenced) | refs=2 |
| `transform_snowflake_data` | `FunctionDef` | `jobs/transform/bronze_to_silver.py:159` | ✅ Works (referenced) | refs=2 |
| `transform_redshift_data` | `FunctionDef` | `jobs/transform/bronze_to_silver.py:245` | ✅ Works (referenced) | refs=2 |
| `transform_fx_data` | `FunctionDef` | `jobs/transform/bronze_to_silver.py:266` | ✅ Works (referenced) | refs=2 |
| `transform_kafka_events` | `FunctionDef` | `jobs/transform/bronze_to_silver.py:277` | ✅ Works (referenced) | refs=2 |
| `raw_to_bronze` | `FunctionDef` | `jobs/transform/raw_to_bronze.py:22` | ✅ Works (referenced) | refs=8 |
| `main` | `FunctionDef` | `jobs/transform/raw_to_bronze.py:92` | ✅ Works (referenced) | refs=480 |
| `SilverToGoldJob` | `ClassDef` | `jobs/transform/silver_to_gold.py:16` | ✅ Works (referenced) | refs=6 |
| `__init__` | `FunctionDef` | `jobs/transform/silver_to_gold.py:19` | ✅ Works (referenced) | refs=162 |
| `run` | `FunctionDef` | `jobs/transform/silver_to_gold.py:23` | ✅ Works (referenced) | refs=1411 |
| `build_customer_dimension` | `FunctionDef` | `jobs/transform/silver_to_gold.py:81` | ✅ Works (referenced) | refs=7 |
| `build_product_dimension` | `FunctionDef` | `jobs/transform/silver_to_gold.py:94` | ✅ Works (referenced) | refs=4 |
| `build_fact_orders` | `FunctionDef` | `jobs/transform/silver_to_gold.py:107` | ✅ Works (referenced) | refs=10 |
| `build_customer_360` | `FunctionDef` | `jobs/transform/silver_to_gold.py:130` | ✅ Works (referenced) | refs=27 |
| `build_behavior_analytics` | `FunctionDef` | `jobs/transform/silver_to_gold.py:163` | ✅ Works (referenced) | refs=2 |
| `load_crm_bronze` | `FunctionDef` | `jobs_legacy/transform/bronze_to_silver_old.py:76` | ✅ Works (referenced) | refs=5 |
| `load_redshift_behavior_bronze` | `FunctionDef` | `jobs_legacy/transform/bronze_to_silver_old.py:191` | ✅ Works (referenced) | refs=5 |
| `load_snowflake_bronze` | `FunctionDef` | `jobs_legacy/transform/bronze_to_silver_old.py:237` | ✅ Works (referenced) | refs=5 |
| `load_fx_bronze` | `FunctionDef` | `jobs_legacy/transform/bronze_to_silver_old.py:291` | ✅ Works (referenced) | refs=3 |
| `load_kafka_bronze` | `FunctionDef` | `jobs_legacy/transform/bronze_to_silver_old.py:322` | ✅ Works (referenced) | refs=5 |
| `build_customers_silver` | `FunctionDef` | `jobs_legacy/transform/bronze_to_silver_old.py:368` | ✅ Works (referenced) | refs=6 |
| `build_orders_silver` | `FunctionDef` | `jobs_legacy/transform/bronze_to_silver_old.py:438` | ✅ Works (referenced) | refs=5 |
| `build_products_silver` | `FunctionDef` | `jobs_legacy/transform/bronze_to_silver_old.py:552` | ✅ Works (referenced) | refs=5 |
| `build_behavior_silver` | `FunctionDef` | `jobs_legacy/transform/bronze_to_silver_old.py:570` | ✅ Works (referenced) | refs=5 |
| `load_fx_bronze` | `FunctionDef` | `jobs_legacy/transform/bronze_to_silver_old.py:599` | ✅ Works (referenced) | refs=3 |
| `build_fx_silver` | `FunctionDef` | `jobs_legacy/transform/bronze_to_silver_old.py:614` | ✅ Works (referenced) | refs=2 |
| `build_order_events_silver` | `FunctionDef` | `jobs_legacy/transform/bronze_to_silver_old.py:645` | ✅ Works (referenced) | refs=2 |
| `write_silver` | `FunctionDef` | `jobs_legacy/transform/bronze_to_silver_old.py:732` | ✅ Works (referenced) | refs=14 |
| `bronze_to_silver_complete` | `FunctionDef` | `jobs_legacy/transform/bronze_to_silver_old.py:818` | ✅ Works (referenced) | refs=5 |
| `main` | `FunctionDef` | `jobs_legacy/transform/bronze_to_silver_old.py:902` | ✅ Works (referenced) | refs=480 |
| `build_dim_date` | `FunctionDef` | `jobs_legacy/transform/silver_to_gold_old.py:47` | ✅ Works (referenced) | refs=8 |
| `build_dim_customer` | `FunctionDef` | `jobs_legacy/transform/silver_to_gold_old.py:79` | ✅ Works (referenced) | refs=7 |
| `build_dim_product` | `FunctionDef` | `jobs_legacy/transform/silver_to_gold_old.py:190` | ✅ Works (referenced) | refs=8 |
| `add_revenue_usd` | `FunctionDef` | `jobs_legacy/transform/silver_to_gold_old.py:262` | ⚠️ Risky (only definition found) | refs=1 |
| `build_fact_orders` | `FunctionDef` | `jobs_legacy/transform/silver_to_gold_old.py:313` | ✅ Works (referenced) | refs=10 |
| `build_customer_360` | `FunctionDef` | `jobs_legacy/transform/silver_to_gold_old.py:481` | ✅ Works (referenced) | refs=27 |
| `build_product_performance` | `FunctionDef` | `jobs_legacy/transform/silver_to_gold_old.py:621` | ✅ Works (referenced) | refs=5 |
| `silver_to_gold_complete` | `FunctionDef` | `jobs_legacy/transform/silver_to_gold_old.py:726` | ✅ Works (referenced) | refs=5 |
| `read_silver_table_or_empty` | `FunctionDef` | `jobs_legacy/transform/silver_to_gold_old.py:777` | ✅ Works (referenced) | refs=5 |
| `delete_if_exists` | `FunctionDef` | `jobs_legacy/transform/silver_to_gold_old.py:872` | ✅ Works (referenced) | refs=10 |
| `main` | `FunctionDef` | `jobs_legacy/transform/silver_to_gold_old.py:986` | ✅ Works (referenced) | refs=480 |
| `check_table_quality` | `FunctionDef` | `local/scripts/dq/check_data_quality.py:29` | ✅ Works (referenced) | refs=8 |
| `check_bronze_quality` | `FunctionDef` | `local/scripts/dq/check_data_quality.py:96` | ✅ Works (referenced) | refs=2 |
| `check_silver_quality` | `FunctionDef` | `local/scripts/dq/check_data_quality.py:166` | ✅ Works (referenced) | refs=2 |
| `check_gold_quality` | `FunctionDef` | `local/scripts/dq/check_data_quality.py:188` | ✅ Works (referenced) | refs=2 |
| `print_quality_report` | `FunctionDef` | `local/scripts/dq/check_data_quality.py:210` | ✅ Works (referenced) | refs=8 |
| `compare_local_vs_aws` | `FunctionDef` | `local/scripts/dq/check_data_quality.py:238` | ⚠️ Risky (only definition found) | refs=1 |
| `main` | `FunctionDef` | `local/scripts/dq/check_data_quality.py:283` | ✅ Works (referenced) | refs=480 |
| `check_s3_path_exists` | `FunctionDef` | `local/scripts/dq/check_s3_data_quality.py:27` | ✅ Works (referenced) | refs=8 |
| `count_s3_objects` | `FunctionDef` | `local/scripts/dq/check_s3_data_quality.py:37` | ✅ Works (referenced) | refs=3 |
| `get_s3_path_size` | `FunctionDef` | `local/scripts/dq/check_s3_data_quality.py:50` | ✅ Works (referenced) | refs=8 |
| `check_bronze_s3_quality` | `FunctionDef` | `local/scripts/dq/check_s3_data_quality.py:64` | ✅ Works (referenced) | refs=2 |
| `check_silver_s3_quality` | `FunctionDef` | `local/scripts/dq/check_s3_data_quality.py:212` | ✅ Works (referenced) | refs=2 |
| `check_gold_s3_quality` | `FunctionDef` | `local/scripts/dq/check_s3_data_quality.py:251` | ✅ Works (referenced) | refs=2 |
| `print_quality_report` | `FunctionDef` | `local/scripts/dq/check_s3_data_quality.py:290` | ✅ Works (referenced) | refs=8 |
| `main` | `FunctionDef` | `local/scripts/dq/check_s3_data_quality.py:314` | ✅ Works (referenced) | refs=480 |
| `run_dq_check` | `FunctionDef` | `local/scripts/dq/compare_local_aws_dq.py:19` | ✅ Works (referenced) | refs=10 |
| `compare_results` | `FunctionDef` | `local/scripts/dq/compare_local_aws_dq.py:54` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `local/scripts/dq/compare_local_aws_dq.py:74` | ✅ Works (referenced) | refs=480 |
| `parse_kafka_csv_row` | `FunctionDef` | `local/scripts/kafka_producer.py:38` | ✅ Works (referenced) | refs=2 |
| `produce_events` | `FunctionDef` | `local/scripts/kafka_producer.py:54` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `local/scripts/kafka_producer.py:152` | ✅ Works (referenced) | refs=480 |
| `main` | `FunctionDef` | `local/scripts/quick_check.py:28` | ✅ Works (referenced) | refs=480 |
| `generate_customer_id` | `FunctionDef` | `local/scripts/regenerate_source_data.py:48` | ✅ Works (referenced) | refs=9 |
| `generate_product_id` | `FunctionDef` | `local/scripts/regenerate_source_data.py:53` | ✅ Works (referenced) | refs=3 |
| `generate_order_id` | `FunctionDef` | `local/scripts/regenerate_source_data.py:58` | ✅ Works (referenced) | refs=4 |
| `generate_account_id` | `FunctionDef` | `local/scripts/regenerate_source_data.py:63` | ⚠️ Risky (only definition found) | refs=1 |
| `generate_contact_id` | `FunctionDef` | `local/scripts/regenerate_source_data.py:68` | ✅ Works (referenced) | refs=2 |
| `generate_opportunity_id` | `FunctionDef` | `local/scripts/regenerate_source_data.py:73` | ✅ Works (referenced) | refs=2 |
| `generate_event_id` | `FunctionDef` | `local/scripts/regenerate_source_data.py:78` | ✅ Works (referenced) | refs=2 |
| `random_date` | `FunctionDef` | `local/scripts/regenerate_source_data.py:83` | ✅ Works (referenced) | refs=19 |
| `random_datetime` | `FunctionDef` | `local/scripts/regenerate_source_data.py:91` | ✅ Works (referenced) | refs=6 |
| `generate_crm_accounts` | `FunctionDef` | `local/scripts/regenerate_source_data.py:99` | ✅ Works (referenced) | refs=2 |
| `generate_crm_contacts` | `FunctionDef` | `local/scripts/regenerate_source_data.py:144` | ✅ Works (referenced) | refs=2 |
| `generate_crm_opportunities` | `FunctionDef` | `local/scripts/regenerate_source_data.py:200` | ✅ Works (referenced) | refs=2 |
| `generate_snowflake_customers` | `FunctionDef` | `local/scripts/regenerate_source_data.py:243` | ✅ Works (referenced) | refs=2 |
| `generate_snowflake_products` | `FunctionDef` | `local/scripts/regenerate_source_data.py:282` | ✅ Works (referenced) | refs=2 |
| `generate_snowflake_orders` | `FunctionDef` | `local/scripts/regenerate_source_data.py:307` | ✅ Works (referenced) | refs=2 |
| `generate_redshift_behavior` | `FunctionDef` | `local/scripts/regenerate_source_data.py:345` | ✅ Works (referenced) | refs=2 |
| `generate_kafka_events` | `FunctionDef` | `local/scripts/regenerate_source_data.py:389` | ✅ Works (referenced) | refs=2 |
| `generate_fx_rates_csv` | `FunctionDef` | `local/scripts/regenerate_source_data.py:457` | ✅ Works (referenced) | refs=2 |
| `generate_fx_rates_json` | `FunctionDef` | `local/scripts/regenerate_source_data.py:489` | ✅ Works (referenced) | refs=2 |
| `write_csv` | `FunctionDef` | `local/scripts/regenerate_source_data.py:523` | ✅ Works (referenced) | refs=10 |
| `write_json_lines` | `FunctionDef` | `local/scripts/regenerate_source_data.py:535` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `local/scripts/regenerate_source_data.py:546` | ✅ Works (referenced) | refs=480 |
| `check_spark_environment` | `FunctionDef` | `local/scripts/run_local_etl_safe.py:20` | ✅ Works (referenced) | refs=2 |
| `run_bronze_to_silver` | `FunctionDef` | `local/scripts/run_local_etl_safe.py:57` | ✅ Works (referenced) | refs=5 |
| `run_silver_to_gold` | `FunctionDef` | `local/scripts/run_local_etl_safe.py:106` | ✅ Works (referenced) | refs=5 |
| `verify_outputs` | `FunctionDef` | `local/scripts/run_local_etl_safe.py:138` | ✅ Works (referenced) | refs=5 |
| `main` | `FunctionDef` | `local/scripts/run_local_etl_safe.py:212` | ✅ Works (referenced) | refs=480 |
| `load_config_resolved` | `FunctionDef` | `local/scripts/show_s3_output_data.py:24` | ✅ Works (referenced) | refs=107 |
| `read_parquet_from_s3` | `FunctionDef` | `local/scripts/show_s3_output_data.py:28` | ✅ Works (referenced) | refs=3 |
| `list_s3_parquet_files` | `FunctionDef` | `local/scripts/show_s3_output_data.py:38` | ✅ Works (referenced) | refs=3 |
| `show_silver_data` | `FunctionDef` | `local/scripts/show_s3_output_data.py:49` | ✅ Works (referenced) | refs=2 |
| `show_gold_data` | `FunctionDef` | `local/scripts/show_s3_output_data.py:83` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `local/scripts/show_s3_output_data.py:117` | ✅ Works (referenced) | refs=480 |
| `create_spark` | `FunctionDef` | `local/scripts/validate_source_data.py:23` | ✅ Works (referenced) | refs=2 |
| `validate_joins` | `FunctionDef` | `local/scripts/validate_source_data.py:35` | ✅ Works (referenced) | refs=2 |
| `validate_data_quality` | `FunctionDef` | `local/scripts/validate_source_data.py:184` | ✅ Works (referenced) | refs=8 |
| `main` | `FunctionDef` | `local/scripts/validate_source_data.py:223` | ✅ Works (referenced) | refs=480 |
| `generate_test_data` | `FunctionDef` | `local/tests/load_test_pipeline.py:21` | ✅ Works (referenced) | refs=2 |
| `benchmark_ingestion` | `FunctionDef` | `local/tests/load_test_pipeline.py:42` | ✅ Works (referenced) | refs=2 |
| `run_load_test` | `FunctionDef` | `local/tests/load_test_pipeline.py:61` | ✅ Works (referenced) | refs=2 |
| `extract_crm_data` | `FunctionDef` | `local/tests/local/crm_etl_pipeline.py:40` | ✅ Works (referenced) | refs=2 |
| `transform_to_bronze` | `FunctionDef` | `local/tests/local/crm_etl_pipeline.py:73` | ✅ Works (referenced) | refs=2 |
| `transform_to_silver` | `FunctionDef` | `local/tests/local/crm_etl_pipeline.py:99` | ✅ Works (referenced) | refs=2 |
| `transform_to_gold` | `FunctionDef` | `local/tests/local/crm_etl_pipeline.py:167` | ✅ Works (referenced) | refs=2 |
| `generate_analytics_report` | `FunctionDef` | `local/tests/local/crm_etl_pipeline.py:244` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `local/tests/local/crm_etl_pipeline.py:288` | ✅ Works (referenced) | refs=480 |
| `check_data_exists` | `FunctionDef` | `local/tests/local/run_etl_end_to_end.py:53` | ✅ Works (referenced) | refs=8 |
| `run_etl_pipeline` | `FunctionDef` | `local/tests/local/run_etl_end_to_end.py:77` | ✅ Works (referenced) | refs=3 |
| `PipelineOrchestrator` | `ClassDef` | `local/tests/local/run_pipeline.py:39` | ✅ Works (referenced) | refs=7 |
| `__init__` | `FunctionDef` | `local/tests/local/run_pipeline.py:42` | ✅ Works (referenced) | refs=162 |
| `run_ingest_job` | `FunctionDef` | `local/tests/local/run_pipeline.py:48` | ✅ Works (referenced) | refs=7 |
| `run_bronze_ingestion` | `FunctionDef` | `local/tests/local/run_pipeline.py:89` | ✅ Works (referenced) | refs=4 |
| `run_data_quality_checks` | `FunctionDef` | `local/tests/local/run_pipeline.py:117` | ✅ Works (referenced) | refs=4 |
| `run_silver_transformations` | `FunctionDef` | `local/tests/local/run_pipeline.py:134` | ✅ Works (referenced) | refs=4 |
| `run_gold_analytics` | `FunctionDef` | `local/tests/local/run_pipeline.py:162` | ✅ Works (referenced) | refs=4 |
| `run_final_quality_checks` | `FunctionDef` | `local/tests/local/run_pipeline.py:193` | ✅ Works (referenced) | refs=2 |
| `emit_lineage_and_metrics` | `FunctionDef` | `local/tests/local/run_pipeline.py:210` | ✅ Works (referenced) | refs=10 |
| `run_complete_pipeline` | `FunctionDef` | `local/tests/local/run_pipeline.py:227` | ✅ Works (referenced) | refs=3 |
| `print_pipeline_summary` | `FunctionDef` | `local/tests/local/run_pipeline.py:274` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `local/tests/local/run_pipeline.py:298` | ✅ Works (referenced) | refs=480 |
| `test_dq_integration` | `FunctionDef` | `local/tests/local/test_dq_lineage_integration.py:22` | ✅ Works (referenced) | refs=2 |
| `ensure_bronze_data` | `FunctionDef` | `local/tests/local/test_etl_pipeline.py:33` | ✅ Works (referenced) | refs=2 |
| `run_bronze_to_silver` | `FunctionDef` | `local/tests/local/test_etl_pipeline.py:60` | ✅ Works (referenced) | refs=5 |
| `run_silver_to_gold` | `FunctionDef` | `local/tests/local/test_etl_pipeline.py:78` | ✅ Works (referenced) | refs=5 |
| `verify_results` | `FunctionDef` | `local/tests/local/test_etl_pipeline.py:111` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `local/tests/local/test_etl_pipeline.py:155` | ✅ Works (referenced) | refs=480 |
| `test_bronze_to_silver` | `FunctionDef` | `local/tests/local/test_etl_simple.py:22` | ✅ Works (referenced) | refs=4 |
| `test_silver_to_gold` | `FunctionDef` | `local/tests/local/test_etl_simple.py:35` | ✅ Works (referenced) | refs=3 |
| `verify_outputs` | `FunctionDef` | `local/tests/local/test_etl_simple.py:64` | ✅ Works (referenced) | refs=5 |
| `main` | `FunctionDef` | `local/tests/local/test_etl_simple.py:106` | ✅ Works (referenced) | refs=480 |
| `verify_config_files` | `FunctionDef` | `local/tests/local/verify_enterprise_setup.py:32` | ✅ Works (referenced) | refs=2 |
| `verify_bronze_scripts` | `FunctionDef` | `local/tests/local/verify_enterprise_setup.py:59` | ✅ Works (referenced) | refs=2 |
| `verify_pipeline_driver` | `FunctionDef` | `local/tests/local/verify_enterprise_setup.py:87` | ✅ Works (referenced) | refs=2 |
| `verify_dq_enforcement` | `FunctionDef` | `local/tests/local/verify_enterprise_setup.py:117` | ✅ Works (referenced) | refs=2 |
| `verify_aws_infrastructure` | `FunctionDef` | `local/tests/local/verify_enterprise_setup.py:144` | ✅ Works (referenced) | refs=2 |
| `verify_schema_documentation` | `FunctionDef` | `local/tests/local/verify_enterprise_setup.py:197` | ✅ Works (referenced) | refs=2 |
| `verify_delta_lake_outputs` | `FunctionDef` | `local/tests/local/verify_enterprise_setup.py:232` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `local/tests/local/verify_enterprise_setup.py:270` | ✅ Works (referenced) | refs=480 |
| `main` | `FunctionDef` | `local/tests/smoke_validate.py:13` | ✅ Works (referenced) | refs=480 |
| `convert_csv_to_json_lines` | `FunctionDef` | `scripts/convert_fx_csv_to_json.py:13` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `scripts/convert_fx_csv_to_json.py:68` | ✅ Works (referenced) | refs=480 |
| `delete_pycache_dirs` | `FunctionDef` | `scripts/final_cleanup.py:22` | ✅ Works (referenced) | refs=2 |
| `delete_pyc_files` | `FunctionDef` | `scripts/final_cleanup.py:34` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `scripts/final_cleanup.py:46` | ✅ Works (referenced) | refs=480 |
| `OrderEventConsumer` | `ClassDef` | `scripts/kafka_consumer.py:19` | ✅ Works (referenced) | refs=2 |
| `__init__` | `FunctionDef` | `scripts/kafka_consumer.py:22` | ✅ Works (referenced) | refs=162 |
| `consume_messages` | `FunctionDef` | `scripts/kafka_consumer.py:39` | ✅ Works (referenced) | refs=2 |
| `consume_continuous` | `FunctionDef` | `scripts/kafka_consumer.py:89` | ✅ Works (referenced) | refs=2 |
| `get_topic_info` | `FunctionDef` | `scripts/kafka_consumer.py:130` | ✅ Works (referenced) | refs=2 |
| `close` | `FunctionDef` | `scripts/kafka_consumer.py:156` | ✅ Works (referenced) | refs=33 |
| `main` | `FunctionDef` | `scripts/kafka_consumer.py:161` | ✅ Works (referenced) | refs=480 |
| `create_sample_order` | `FunctionDef` | `scripts/kafka_producer.py:22` | ✅ Works (referenced) | refs=2 |
| `produce_orders` | `FunctionDef` | `scripts/kafka_producer.py:45` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `scripts/kafka_producer.py:110` | ✅ Works (referenced) | refs=480 |
| `load_config` | `FunctionDef` | `scripts/maintenance/backfill_range.py:28` | ✅ Works (referenced) | refs=155 |
| `parse_date_range` | `FunctionDef` | `scripts/maintenance/backfill_range.py:34` | ✅ Works (referenced) | refs=2 |
| `backfill_table` | `FunctionDef` | `scripts/maintenance/backfill_range.py:57` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `scripts/maintenance/backfill_range.py:123` | ✅ Works (referenced) | refs=480 |
| `optimize_table` | `FunctionDef` | `scripts/maintenance/optimize_tables.py:27` | ✅ Works (referenced) | refs=18 |
| `main` | `FunctionDef` | `scripts/maintenance/optimize_tables.py:74` | ✅ Works (referenced) | refs=480 |
| `organize_data_directory` | `FunctionDef` | `scripts/organize_data_directory.py:36` | ✅ Works (referenced) | refs=2 |
| `count_files` | `FunctionDef` | `scripts/show_pipeline_results.py:17` | ✅ Works (referenced) | refs=8 |
| `format_size` | `FunctionDef` | `scripts/show_pipeline_results.py:24` | ✅ Works (referenced) | refs=7 |
| `get_dir_size` | `FunctionDef` | `scripts/show_pipeline_results.py:33` | ✅ Works (referenced) | refs=7 |
| `main` | `FunctionDef` | `scripts/show_pipeline_results.py:47` | ✅ Works (referenced) | refs=480 |
| `upload_jobs_to_s3` | `FunctionDef` | `scripts/upload_jobs_to_s3.py:10` | ✅ Works (referenced) | refs=4 |
| `ComponentValidator` | `ClassDef` | `scripts/validate_all_components.py:20` | ✅ Works (referenced) | refs=2 |
| `__init__` | `FunctionDef` | `scripts/validate_all_components.py:23` | ✅ Works (referenced) | refs=162 |
| `validate_all` | `FunctionDef` | `scripts/validate_all_components.py:28` | ✅ Works (referenced) | refs=2 |
| `validate_module_structure` | `FunctionDef` | `scripts/validate_all_components.py:48` | ✅ Works (referenced) | refs=2 |
| `validate_imports` | `FunctionDef` | `scripts/validate_all_components.py:86` | ✅ Works (referenced) | refs=2 |
| `validate_code_quality` | `FunctionDef` | `scripts/validate_all_components.py:125` | ✅ Works (referenced) | refs=2 |
| `print_summary` | `FunctionDef` | `scripts/validate_all_components.py:153` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `scripts/validate_all_components.py:181` | ✅ Works (referenced) | refs=480 |
| `GECheckpointRunner` | `ClassDef` | `src/dq/ge_checkpoint.py:20` | ✅ Works (referenced) | refs=2 |
| `__init__` | `FunctionDef` | `src/dq/ge_checkpoint.py:23` | ✅ Works (referenced) | refs=162 |
| `_init_ge_context` | `FunctionDef` | `src/dq/ge_checkpoint.py:37` | ✅ Works (referenced) | refs=2 |
| `run_checkpoint` | `FunctionDef` | `src/dq/ge_checkpoint.py:45` | ✅ Works (referenced) | refs=18 |
| `_analyze_checkpoint_result` | `FunctionDef` | `src/dq/ge_checkpoint.py:79` | ✅ Works (referenced) | refs=2 |
| `_get_expectation_severity` | `FunctionDef` | `src/dq/ge_checkpoint.py:136` | ✅ Works (referenced) | refs=2 |
| `run_silver_quality_checks` | `FunctionDef` | `src/dq/ge_checkpoint.py:150` | ✅ Works (referenced) | refs=2 |
| `run_gold_quality_checks` | `FunctionDef` | `src/dq/ge_checkpoint.py:166` | ✅ Works (referenced) | refs=2 |
| `publish_results` | `FunctionDef` | `src/dq/ge_checkpoint.py:182` | ✅ Works (referenced) | refs=2 |
| `_upload_data_docs_to_s3` | `FunctionDef` | `src/dq/ge_checkpoint.py:198` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `src/dq/ge_checkpoint.py:208` | ✅ Works (referenced) | refs=480 |
| `RetentionPolicy` | `ClassDef` | `src/project_a/archival/policy.py:18` | ✅ Works (referenced) | refs=17 |
| `ArchiveManager` | `ClassDef` | `src/project_a/archival/policy.py:31` | ✅ Works (referenced) | refs=7 |
| `__init__` | `FunctionDef` | `src/project_a/archival/policy.py:34` | ✅ Works (referenced) | refs=162 |
| `archive_dataset` | `FunctionDef` | `src/project_a/archival/policy.py:43` | ✅ Works (referenced) | refs=4 |
| `_get_directory_size` | `FunctionDef` | `src/project_a/archival/policy.py:82` | ✅ Works (referenced) | refs=3 |
| `get_archive_info` | `FunctionDef` | `src/project_a/archival/policy.py:91` | ⚠️ Risky (only definition found) | refs=1 |
| `restore_from_archive` | `FunctionDef` | `src/project_a/archival/policy.py:111` | ⚠️ Risky (only definition found) | refs=1 |
| `RetentionPolicyManager` | `ClassDef` | `src/project_a/archival/policy.py:135` | ✅ Works (referenced) | refs=6 |
| `__init__` | `FunctionDef` | `src/project_a/archival/policy.py:138` | ✅ Works (referenced) | refs=162 |
| `create_policy` | `FunctionDef` | `src/project_a/archival/policy.py:144` | ✅ Works (referenced) | refs=2 |
| `save_policy` | `FunctionDef` | `src/project_a/archival/policy.py:176` | ✅ Works (referenced) | refs=2 |
| `load_policy` | `FunctionDef` | `src/project_a/archival/policy.py:193` | ✅ Works (referenced) | refs=2 |
| `get_policy` | `FunctionDef` | `src/project_a/archival/policy.py:217` | ✅ Works (referenced) | refs=6 |
| `list_policies` | `FunctionDef` | `src/project_a/archival/policy.py:223` | ✅ Works (referenced) | refs=2 |
| `LifecycleManager` | `ClassDef` | `src/project_a/archival/policy.py:228` | ✅ Works (referenced) | refs=3 |
| `__init__` | `FunctionDef` | `src/project_a/archival/policy.py:231` | ✅ Works (referenced) | refs=162 |
| `evaluate_lifecycle` | `FunctionDef` | `src/project_a/archival/policy.py:236` | ✅ Works (referenced) | refs=4 |
| `execute_lifecycle_action` | `FunctionDef` | `src/project_a/archival/policy.py:286` | ✅ Works (referenced) | refs=5 |
| `_find_applicable_policy` | `FunctionDef` | `src/project_a/archival/policy.py:346` | ✅ Works (referenced) | refs=2 |
| `run_lifecycle_management` | `FunctionDef` | `src/project_a/archival/policy.py:357` | ✅ Works (referenced) | refs=3 |
| `get_archive_manager` | `FunctionDef` | `src/project_a/archival/policy.py:383` | ✅ Works (referenced) | refs=6 |
| `get_policy_manager` | `FunctionDef` | `src/project_a/archival/policy.py:396` | ✅ Works (referenced) | refs=3 |
| `get_lifecycle_manager` | `FunctionDef` | `src/project_a/archival/policy.py:408` | ✅ Works (referenced) | refs=4 |
| `create_retention_policy` | `FunctionDef` | `src/project_a/archival/policy.py:418` | ⚠️ Risky (only definition found) | refs=1 |
| `archive_dataset` | `FunctionDef` | `src/project_a/archival/policy.py:442` | ✅ Works (referenced) | refs=4 |
| `evaluate_lifecycle` | `FunctionDef` | `src/project_a/archival/policy.py:450` | ✅ Works (referenced) | refs=4 |
| `execute_lifecycle_action` | `FunctionDef` | `src/project_a/archival/policy.py:456` | ✅ Works (referenced) | refs=5 |
| `run_lifecycle_management` | `FunctionDef` | `src/project_a/archival/policy.py:462` | ✅ Works (referenced) | refs=3 |
| `ChangeType` | `ClassDef` | `src/project_a/cdc/change_capture.py:19` | ✅ Works (referenced) | refs=18 |
| `ChangeRecord` | `ClassDef` | `src/project_a/cdc/change_capture.py:27` | ✅ Works (referenced) | refs=6 |
| `WatermarkManager` | `ClassDef` | `src/project_a/cdc/change_capture.py:41` | ✅ Works (referenced) | refs=7 |
| `__init__` | `FunctionDef` | `src/project_a/cdc/change_capture.py:44` | ✅ Works (referenced) | refs=162 |
| `get_watermark` | `FunctionDef` | `src/project_a/cdc/change_capture.py:49` | ✅ Works (referenced) | refs=33 |
| `set_watermark` | `FunctionDef` | `src/project_a/cdc/change_capture.py:59` | ✅ Works (referenced) | refs=14 |
| `update_watermark` | `FunctionDef` | `src/project_a/cdc/change_capture.py:75` | ✅ Works (referenced) | refs=2 |
| `ChangeCaptureBuffer` | `ClassDef` | `src/project_a/cdc/change_capture.py:86` | ✅ Works (referenced) | refs=6 |
| `__init__` | `FunctionDef` | `src/project_a/cdc/change_capture.py:89` | ✅ Works (referenced) | refs=162 |
| `append_change` | `FunctionDef` | `src/project_a/cdc/change_capture.py:94` | ✅ Works (referenced) | refs=2 |
| `get_changes_since` | `FunctionDef` | `src/project_a/cdc/change_capture.py:125` | ✅ Works (referenced) | refs=2 |
| `compact_changes` | `FunctionDef` | `src/project_a/cdc/change_capture.py:175` | ⚠️ Risky (only definition found) | refs=1 |
| `IncrementalProcessor` | `ClassDef` | `src/project_a/cdc/change_capture.py:200` | ✅ Works (referenced) | refs=3 |
| `__init__` | `FunctionDef` | `src/project_a/cdc/change_capture.py:203` | ✅ Works (referenced) | refs=162 |
| `detect_changes` | `FunctionDef` | `src/project_a/cdc/change_capture.py:208` | ✅ Works (referenced) | refs=4 |
| `process_incremental` | `FunctionDef` | `src/project_a/cdc/change_capture.py:247` | ✅ Works (referenced) | refs=4 |
| `get_incremental_data` | `FunctionDef` | `src/project_a/cdc/change_capture.py:289` | ✅ Works (referenced) | refs=3 |
| `get_watermark_manager` | `FunctionDef` | `src/project_a/cdc/change_capture.py:326` | ✅ Works (referenced) | refs=7 |
| `get_change_buffer` | `FunctionDef` | `src/project_a/cdc/change_capture.py:338` | ✅ Works (referenced) | refs=2 |
| `get_incremental_processor` | `FunctionDef` | `src/project_a/cdc/change_capture.py:350` | ✅ Works (referenced) | refs=3 |
| `process_incremental_data` | `FunctionDef` | `src/project_a/cdc/change_capture.py:360` | ⚠️ Risky (only definition found) | refs=1 |
| `get_incremental_data` | `FunctionDef` | `src/project_a/cdc/change_capture.py:368` | ✅ Works (referenced) | refs=3 |
| `get_watermark` | `FunctionDef` | `src/project_a/cdc/change_capture.py:376` | ✅ Works (referenced) | refs=33 |
| `set_watermark` | `FunctionDef` | `src/project_a/cdc/change_capture.py:382` | ✅ Works (referenced) | refs=14 |
| `PipelineStage` | `ClassDef` | `src/project_a/cicd/pipeline.py:20` | ✅ Works (referenced) | refs=15 |
| `PipelineStatus` | `ClassDef` | `src/project_a/cicd/pipeline.py:28` | ✅ Works (referenced) | refs=16 |
| `PipelineJob` | `ClassDef` | `src/project_a/cicd/pipeline.py:37` | ✅ Works (referenced) | refs=7 |
| `PipelineRun` | `ClassDef` | `src/project_a/cicd/pipeline.py:55` | ✅ Works (referenced) | refs=11 |
| `BuildManager` | `ClassDef` | `src/project_a/cicd/pipeline.py:72` | ✅ Works (referenced) | refs=2 |
| `__init__` | `FunctionDef` | `src/project_a/cicd/pipeline.py:75` | ✅ Works (referenced) | refs=162 |
| `build_artifact` | `FunctionDef` | `src/project_a/cicd/pipeline.py:80` | ✅ Works (referenced) | refs=2 |
| `run_unit_tests` | `FunctionDef` | `src/project_a/cicd/pipeline.py:134` | ✅ Works (referenced) | refs=6 |
| `DeploymentManager` | `ClassDef` | `src/project_a/cicd/pipeline.py:160` | ✅ Works (referenced) | refs=2 |
| `__init__` | `FunctionDef` | `src/project_a/cicd/pipeline.py:163` | ✅ Works (referenced) | refs=162 |
| `deploy_to_environment` | `FunctionDef` | `src/project_a/cicd/pipeline.py:168` | ✅ Works (referenced) | refs=3 |
| `rollback_deployment` | `FunctionDef` | `src/project_a/cicd/pipeline.py:213` | ⚠️ Risky (only definition found) | refs=1 |
| `PipelineManager` | `ClassDef` | `src/project_a/cicd/pipeline.py:225` | ✅ Works (referenced) | refs=7 |
| `__init__` | `FunctionDef` | `src/project_a/cicd/pipeline.py:228` | ✅ Works (referenced) | refs=162 |
| `_load_pipeline_config` | `FunctionDef` | `src/project_a/cicd/pipeline.py:243` | ✅ Works (referenced) | refs=2 |
| `create_pipeline_run` | `FunctionDef` | `src/project_a/cicd/pipeline.py:267` | ✅ Works (referenced) | refs=2 |
| `execute_pipeline` | `FunctionDef` | `src/project_a/cicd/pipeline.py:304` | ✅ Works (referenced) | refs=2 |
| `_execute_stage` | `FunctionDef` | `src/project_a/cicd/pipeline.py:344` | ✅ Works (referenced) | refs=2 |
| `_execute_build_stage` | `FunctionDef` | `src/project_a/cicd/pipeline.py:404` | ✅ Works (referenced) | refs=2 |
| `_execute_test_stage` | `FunctionDef` | `src/project_a/cicd/pipeline.py:427` | ✅ Works (referenced) | refs=2 |
| `_execute_integration_test_stage` | `FunctionDef` | `src/project_a/cicd/pipeline.py:433` | ✅ Works (referenced) | refs=2 |
| `_execute_deploy_stage` | `FunctionDef` | `src/project_a/cicd/pipeline.py:443` | ✅ Works (referenced) | refs=3 |
| `get_pipeline_status` | `FunctionDef` | `src/project_a/cicd/pipeline.py:465` | ✅ Works (referenced) | refs=3 |
| `cancel_pipeline` | `FunctionDef` | `src/project_a/cicd/pipeline.py:471` | ✅ Works (referenced) | refs=3 |
| `PipelineOrchestrator` | `ClassDef` | `src/project_a/cicd/pipeline.py:478` | ✅ Works (referenced) | refs=7 |
| `__init__` | `FunctionDef` | `src/project_a/cicd/pipeline.py:481` | ✅ Works (referenced) | refs=162 |
| `trigger_pipeline` | `FunctionDef` | `src/project_a/cicd/pipeline.py:486` | ✅ Works (referenced) | refs=3 |
| `schedule_pipeline` | `FunctionDef` | `src/project_a/cicd/pipeline.py:503` | ✅ Works (referenced) | refs=3 |
| `get_active_runs` | `FunctionDef` | `src/project_a/cicd/pipeline.py:516` | ⚠️ Risky (only definition found) | refs=1 |
| `get_pipeline_manager` | `FunctionDef` | `src/project_a/cicd/pipeline.py:527` | ✅ Works (referenced) | refs=7 |
| `get_pipeline_orchestrator` | `FunctionDef` | `src/project_a/cicd/pipeline.py:542` | ✅ Works (referenced) | refs=3 |
| `trigger_pipeline` | `FunctionDef` | `src/project_a/cicd/pipeline.py:551` | ✅ Works (referenced) | refs=3 |
| `schedule_pipeline` | `FunctionDef` | `src/project_a/cicd/pipeline.py:562` | ✅ Works (referenced) | refs=3 |
| `get_pipeline_status` | `FunctionDef` | `src/project_a/cicd/pipeline.py:570` | ✅ Works (referenced) | refs=3 |
| `cancel_pipeline` | `FunctionDef` | `src/project_a/cicd/pipeline.py:576` | ✅ Works (referenced) | refs=3 |
| `main` | `FunctionDef` | `src/project_a/cli.py:17` | ✅ Works (referenced) | refs=480 |
| `run_ingest` | `FunctionDef` | `src/project_a/cli.py:74` | ✅ Works (referenced) | refs=13 |
| `run_transform` | `FunctionDef` | `src/project_a/cli.py:82` | ✅ Works (referenced) | refs=9 |
| `run_validate` | `FunctionDef` | `src/project_a/cli.py:90` | ✅ Works (referenced) | refs=6 |
| `run_pipeline` | `FunctionDef` | `src/project_a/cli.py:98` | ✅ Works (referenced) | refs=25 |
| `etl_main` | `FunctionDef` | `src/project_a/cli.py:118` | ⚠️ Risky (only definition found) | refs=1 |
| `dq_main` | `FunctionDef` | `src/project_a/cli.py:123` | ⚠️ Risky (only definition found) | refs=1 |
| `ConfigLoader` | `ClassDef` | `src/project_a/config_loader.py:25` | ✅ Works (referenced) | refs=2 |
| `__init__` | `FunctionDef` | `src/project_a/config_loader.py:28` | ✅ Works (referenced) | refs=162 |
| `load_config` | `FunctionDef` | `src/project_a/config_loader.py:33` | ✅ Works (referenced) | refs=155 |
| `validate_config` | `FunctionDef` | `src/project_a/config_loader.py:38` | ✅ Works (referenced) | refs=4 |
| `get_config` | `FunctionDef` | `src/project_a/config_loader.py:69` | ✅ Works (referenced) | refs=7 |
| `_get_dbutils` | `FunctionDef` | `src/project_a/config_loader.py:76` | ✅ Works (referenced) | refs=6 |
| `_resolve_value` | `FunctionDef` | `src/project_a/config_loader.py:93` | ✅ Works (referenced) | refs=11 |
| `replace_secret` | `FunctionDef` | `src/project_a/config_loader.py:98` | ✅ Works (referenced) | refs=6 |
| `replace_var` | `FunctionDef` | `src/project_a/config_loader.py:116` | ✅ Works (referenced) | refs=12 |
| `_resolve_secrets` | `FunctionDef` | `src/project_a/config_loader.py:137` | ✅ Works (referenced) | refs=16 |
| `load_config_resolved` | `FunctionDef` | `src/project_a/config_loader.py:150` | ✅ Works (referenced) | refs=107 |
| `ContractStatus` | `ClassDef` | `src/project_a/contracts/management.py:19` | ✅ Works (referenced) | refs=7 |
| `SchemaField` | `ClassDef` | `src/project_a/contracts/management.py:28` | ✅ Works (referenced) | refs=8 |
| `DataContract` | `ClassDef` | `src/project_a/contracts/management.py:39` | ✅ Works (referenced) | refs=23 |
| `SchemaRegistry` | `ClassDef` | `src/project_a/contracts/management.py:56` | ✅ Works (referenced) | refs=11 |
| `__init__` | `FunctionDef` | `src/project_a/contracts/management.py:59` | ✅ Works (referenced) | refs=162 |
| `register_contract` | `FunctionDef` | `src/project_a/contracts/management.py:66` | ✅ Works (referenced) | refs=4 |
| `get_contract` | `FunctionDef` | `src/project_a/contracts/management.py:105` | ✅ Works (referenced) | refs=18 |
| `get_all_versions` | `FunctionDef` | `src/project_a/contracts/management.py:152` | ⚠️ Risky (only definition found) | refs=1 |
| `_validate_contract` | `FunctionDef` | `src/project_a/contracts/management.py:161` | ✅ Works (referenced) | refs=2 |
| `check_compatibility` | `FunctionDef` | `src/project_a/contracts/management.py:184` | ✅ Works (referenced) | refs=4 |
| `ContractValidator` | `ClassDef` | `src/project_a/contracts/management.py:246` | ✅ Works (referenced) | refs=6 |
| `__init__` | `FunctionDef` | `src/project_a/contracts/management.py:249` | ✅ Works (referenced) | refs=162 |
| `validate_data` | `FunctionDef` | `src/project_a/contracts/management.py:253` | ✅ Works (referenced) | refs=18 |
| `_validate_field` | `FunctionDef` | `src/project_a/contracts/management.py:293` | ✅ Works (referenced) | refs=2 |
| `_check_type_compatibility` | `FunctionDef` | `src/project_a/contracts/management.py:309` | ✅ Works (referenced) | refs=2 |
| `_check_constraints` | `FunctionDef` | `src/project_a/contracts/management.py:340` | ✅ Works (referenced) | refs=2 |
| `ContractManager` | `ClassDef` | `src/project_a/contracts/management.py:376` | ✅ Works (referenced) | refs=7 |
| `__init__` | `FunctionDef` | `src/project_a/contracts/management.py:379` | ✅ Works (referenced) | refs=162 |
| `create_contract` | `FunctionDef` | `src/project_a/contracts/management.py:384` | ✅ Works (referenced) | refs=3 |
| `update_contract` | `FunctionDef` | `src/project_a/contracts/management.py:406` | ✅ Works (referenced) | refs=3 |
| `deprecate_contract` | `FunctionDef` | `src/project_a/contracts/management.py:461` | ✅ Works (referenced) | refs=3 |
| `get_schema_registry` | `FunctionDef` | `src/project_a/contracts/management.py:499` | ✅ Works (referenced) | refs=8 |
| `get_contract_validator` | `FunctionDef` | `src/project_a/contracts/management.py:510` | ✅ Works (referenced) | refs=5 |
| `get_contract_manager` | `FunctionDef` | `src/project_a/contracts/management.py:519` | ✅ Works (referenced) | refs=4 |
| `create_contract` | `FunctionDef` | `src/project_a/contracts/management.py:529` | ✅ Works (referenced) | refs=3 |
| `get_contract` | `FunctionDef` | `src/project_a/contracts/management.py:537` | ✅ Works (referenced) | refs=18 |
| `validate_data` | `FunctionDef` | `src/project_a/contracts/management.py:543` | ✅ Works (referenced) | refs=18 |
| `update_contract` | `FunctionDef` | `src/project_a/contracts/management.py:550` | ✅ Works (referenced) | refs=3 |
| `deprecate_contract` | `FunctionDef` | `src/project_a/contracts/management.py:558` | ✅ Works (referenced) | refs=3 |
| `check_compatibility` | `FunctionDef` | `src/project_a/contracts/management.py:564` | ✅ Works (referenced) | refs=4 |
| `BaseJob` | `ClassDef` | `src/project_a/core/base_job.py:24` | ✅ Works (referenced) | refs=25 |
| `__init__` | `FunctionDef` | `src/project_a/core/base_job.py:35` | ✅ Works (referenced) | refs=162 |
| `run` | `FunctionDef` | `src/project_a/core/base_job.py:46` | ✅ Works (referenced) | refs=1411 |
| `execute` | `FunctionDef` | `src/project_a/core/base_job.py:58` | ✅ Works (referenced) | refs=89 |
| `spark` | `FunctionDef` | `src/project_a/core/base_job.py:78` | ✅ Works (referenced) | refs=3266 |
| `apply_dq_rules` | `FunctionDef` | `src/project_a/core/base_job.py:84` | ✅ Works (referenced) | refs=12 |
| `log_lineage` | `FunctionDef` | `src/project_a/core/base_job.py:90` | ✅ Works (referenced) | refs=8 |
| `ProjectConfig` | `ClassDef` | `src/project_a/core/config.py:20` | ✅ Works (referenced) | refs=37 |
| `__init__` | `FunctionDef` | `src/project_a/core/config.py:31` | ✅ Works (referenced) | refs=162 |
| `_load` | `FunctionDef` | `src/project_a/core/config.py:47` | ✅ Works (referenced) | refs=148 |
| `_resolve_value` | `FunctionDef` | `src/project_a/core/config.py:69` | ✅ Works (referenced) | refs=11 |
| `replace_env` | `FunctionDef` | `src/project_a/core/config.py:75` | ✅ Works (referenced) | refs=4 |
| `replace_var` | `FunctionDef` | `src/project_a/core/config.py:82` | ✅ Works (referenced) | refs=12 |
| `_resolve` | `FunctionDef` | `src/project_a/core/config.py:100` | ✅ Works (referenced) | refs=173 |
| `resolve_recursive` | `FunctionDef` | `src/project_a/core/config.py:103` | ✅ Works (referenced) | refs=4 |
| `environment` | `FunctionDef` | `src/project_a/core/config.py:116` | ✅ Works (referenced) | refs=375 |
| `project_name` | `FunctionDef` | `src/project_a/core/config.py:121` | ✅ Works (referenced) | refs=6 |
| `paths` | `FunctionDef` | `src/project_a/core/config.py:126` | ✅ Works (referenced) | refs=276 |
| `sources` | `FunctionDef` | `src/project_a/core/config.py:131` | ✅ Works (referenced) | refs=199 |
| `tables` | `FunctionDef` | `src/project_a/core/config.py:136` | ✅ Works (referenced) | refs=625 |
| `aws` | `FunctionDef` | `src/project_a/core/config.py:141` | ✅ Works (referenced) | refs=668 |
| `buckets` | `FunctionDef` | `src/project_a/core/config.py:146` | ✅ Works (referenced) | refs=29 |
| `glue` | `FunctionDef` | `src/project_a/core/config.py:151` | ✅ Works (referenced) | refs=50 |
| `emr` | `FunctionDef` | `src/project_a/core/config.py:156` | ✅ Works (referenced) | refs=203 |
| `kafka` | `FunctionDef` | `src/project_a/core/config.py:161` | ✅ Works (referenced) | refs=334 |
| `get` | `FunctionDef` | `src/project_a/core/config.py:165` | ✅ Works (referenced) | refs=7013 |
| `is_local` | `FunctionDef` | `src/project_a/core/config.py:178` | ✅ Works (referenced) | refs=21 |
| `is_aws` | `FunctionDef` | `src/project_a/core/config.py:183` | ⚠️ Risky (only definition found) | refs=1 |
| `_append_extension` | `FunctionDef` | `src/project_a/core/context.py:22` | ✅ Works (referenced) | refs=4 |
| `_merge_packages` | `FunctionDef` | `src/project_a/core/context.py:31` | ✅ Works (referenced) | refs=4 |
| `JobContext` | `ClassDef` | `src/project_a/core/context.py:46` | ✅ Works (referenced) | refs=14 |
| `__init__` | `FunctionDef` | `src/project_a/core/context.py:57` | ✅ Works (referenced) | refs=162 |
| `__enter__` | `FunctionDef` | `src/project_a/core/context.py:70` | ✅ Works (referenced) | refs=2 |
| `__exit__` | `FunctionDef` | `src/project_a/core/context.py:75` | ✅ Works (referenced) | refs=2 |
| `_build_spark` | `FunctionDef` | `src/project_a/core/context.py:83` | ✅ Works (referenced) | refs=8 |
| `_configure_s3` | `FunctionDef` | `src/project_a/core/context.py:166` | ✅ Works (referenced) | refs=2 |
| `resolve_path` | `FunctionDef` | `src/project_a/core/context.py:200` | ✅ Works (referenced) | refs=20 |
| `get_kafka_bootstrap_servers` | `FunctionDef` | `src/project_a/core/context.py:216` | ✅ Works (referenced) | refs=2 |
| `ResourceUsage` | `ClassDef` | `src/project_a/cost/optimization.py:20` | ✅ Works (referenced) | refs=5 |
| `CostOptimizationRecommendation` | `ClassDef` | `src/project_a/cost/optimization.py:32` | ⚠️ Risky (only definition found) | refs=1 |
| `ResourceMonitor` | `ClassDef` | `src/project_a/cost/optimization.py:44` | ✅ Works (referenced) | refs=7 |
| `__init__` | `FunctionDef` | `src/project_a/cost/optimization.py:47` | ✅ Works (referenced) | refs=162 |
| `collect_current_usage` | `FunctionDef` | `src/project_a/cost/optimization.py:53` | ✅ Works (referenced) | refs=2 |
| `_log_resource_usage` | `FunctionDef` | `src/project_a/cost/optimization.py:91` | ✅ Works (referenced) | refs=2 |
| `analyze_usage_patterns` | `FunctionDef` | `src/project_a/cost/optimization.py:108` | ✅ Works (referenced) | refs=4 |
| `AWSCostAnalyzer` | `ClassDef` | `src/project_a/cost/optimization.py:180` | ✅ Works (referenced) | refs=6 |
| `__init__` | `FunctionDef` | `src/project_a/cost/optimization.py:183` | ✅ Works (referenced) | refs=162 |
| `get_cost_analysis` | `FunctionDef` | `src/project_a/cost/optimization.py:200` | ✅ Works (referenced) | refs=3 |
| `_generate_cost_recommendations` | `FunctionDef` | `src/project_a/cost/optimization.py:255` | ✅ Works (referenced) | refs=2 |
| `CostOptimizer` | `ClassDef` | `src/project_a/cost/optimization.py:306` | ✅ Works (referenced) | refs=3 |
| `__init__` | `FunctionDef` | `src/project_a/cost/optimization.py:309` | ✅ Works (referenced) | refs=162 |
| `run_cost_optimization_analysis` | `FunctionDef` | `src/project_a/cost/optimization.py:314` | ✅ Works (referenced) | refs=3 |
| `_save_analysis_results` | `FunctionDef` | `src/project_a/cost/optimization.py:365` | ✅ Works (referenced) | refs=2 |
| `get_optimization_dashboard_data` | `FunctionDef` | `src/project_a/cost/optimization.py:374` | ✅ Works (referenced) | refs=3 |
| `get_resource_monitor` | `FunctionDef` | `src/project_a/cost/optimization.py:419` | ✅ Works (referenced) | refs=7 |
| `get_aws_analyzer` | `FunctionDef` | `src/project_a/cost/optimization.py:431` | ✅ Works (referenced) | refs=2 |
| `get_cost_optimizer` | `FunctionDef` | `src/project_a/cost/optimization.py:441` | ✅ Works (referenced) | refs=3 |
| `collect_resource_usage` | `FunctionDef` | `src/project_a/cost/optimization.py:453` | ⚠️ Risky (only definition found) | refs=1 |
| `analyze_resource_usage` | `FunctionDef` | `src/project_a/cost/optimization.py:459` | ⚠️ Risky (only definition found) | refs=1 |
| `run_cost_optimization_analysis` | `FunctionDef` | `src/project_a/cost/optimization.py:465` | ✅ Works (referenced) | refs=3 |
| `get_optimization_dashboard_data` | `FunctionDef` | `src/project_a/cost/optimization.py:473` | ✅ Works (referenced) | refs=3 |
| `spark_session` | `FunctionDef` | `src/project_a/delta_utils.py:16` | ✅ Works (referenced) | refs=177 |
| `write_staging` | `FunctionDef` | `src/project_a/delta_utils.py:44` | ✅ Works (referenced) | refs=3 |
| `merge_publish` | `FunctionDef` | `src/project_a/delta_utils.py:59` | ✅ Works (referenced) | refs=3 |
| `optimize_table` | `FunctionDef` | `src/project_a/delta_utils.py:99` | ✅ Works (referenced) | refs=18 |
| `vacuum_table` | `FunctionDef` | `src/project_a/delta_utils.py:114` | ✅ Works (referenced) | refs=6 |
| `get_table_history` | `FunctionDef` | `src/project_a/delta_utils.py:124` | ⚠️ Risky (only definition found) | refs=1 |
| `get_table_details` | `FunctionDef` | `src/project_a/delta_utils.py:129` | ⚠️ Risky (only definition found) | refs=1 |
| `BackupPlan` | `ClassDef` | `src/project_a/disaster_recovery/plans.py:18` | ✅ Works (referenced) | refs=8 |
| `RecoveryPlan` | `ClassDef` | `src/project_a/disaster_recovery/plans.py:33` | ✅ Works (referenced) | refs=9 |
| `BackupManager` | `ClassDef` | `src/project_a/disaster_recovery/plans.py:45` | ✅ Works (referenced) | refs=7 |
| `__init__` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:48` | ✅ Works (referenced) | refs=162 |
| `create_backup` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:55` | ✅ Works (referenced) | refs=7 |
| `_get_path_size` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:122` | ✅ Works (referenced) | refs=2 |
| `_encrypt_backup` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:134` | ✅ Works (referenced) | refs=2 |
| `restore_backup` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:142` | ✅ Works (referenced) | refs=4 |
| `cleanup_old_backups` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:191` | ✅ Works (referenced) | refs=3 |
| `RecoveryManager` | `ClassDef` | `src/project_a/disaster_recovery/plans.py:219` | ✅ Works (referenced) | refs=5 |
| `__init__` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:222` | ✅ Works (referenced) | refs=162 |
| `execute_recovery` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:226` | ✅ Works (referenced) | refs=3 |
| `_find_latest_backup` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:274` | ✅ Works (referenced) | refs=3 |
| `validate_recovery_plan` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:293` | ✅ Works (referenced) | refs=3 |
| `DRPlanManager` | `ClassDef` | `src/project_a/disaster_recovery/plans.py:318` | ✅ Works (referenced) | refs=3 |
| `__init__` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:321` | ✅ Works (referenced) | refs=162 |
| `create_backup_plan` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:327` | ✅ Works (referenced) | refs=3 |
| `create_recovery_plan` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:361` | ✅ Works (referenced) | refs=3 |
| `save_backup_plan` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:391` | ✅ Works (referenced) | refs=2 |
| `save_recovery_plan` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:409` | ✅ Works (referenced) | refs=2 |
| `get_backup_plan` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:425` | ✅ Works (referenced) | refs=2 |
| `get_recovery_plan` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:449` | ✅ Works (referenced) | refs=3 |
| `get_backup_manager` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:478` | ✅ Works (referenced) | refs=7 |
| `get_dr_plan_manager` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:491` | ✅ Works (referenced) | refs=6 |
| `get_recovery_manager` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:503` | ✅ Works (referenced) | refs=5 |
| `create_backup_plan` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:512` | ✅ Works (referenced) | refs=3 |
| `create_recovery_plan` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:538` | ✅ Works (referenced) | refs=3 |
| `execute_recovery` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:560` | ✅ Works (referenced) | refs=3 |
| `validate_recovery_plan` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:571` | ✅ Works (referenced) | refs=3 |
| `create_backup` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:582` | ✅ Works (referenced) | refs=7 |
| `restore_backup` | `FunctionDef` | `src/project_a/disaster_recovery/plans.py:593` | ✅ Works (referenced) | refs=4 |
| `QualityCheckType` | `ClassDef` | `src/project_a/dq/automation.py:21` | ✅ Works (referenced) | refs=6 |
| `QualityResult` | `ClassDef` | `src/project_a/dq/automation.py:31` | ✅ Works (referenced) | refs=19 |
| `DataQualityProfiler` | `ClassDef` | `src/project_a/dq/automation.py:41` | ✅ Works (referenced) | refs=6 |
| `__init__` | `FunctionDef` | `src/project_a/dq/automation.py:44` | ✅ Works (referenced) | refs=162 |
| `profile_dataset` | `FunctionDef` | `src/project_a/dq/automation.py:49` | ✅ Works (referenced) | refs=3 |
| `_extract_schema` | `FunctionDef` | `src/project_a/dq/automation.py:72` | ✅ Works (referenced) | refs=2 |
| `_calculate_basic_stats` | `FunctionDef` | `src/project_a/dq/automation.py:79` | ✅ Works (referenced) | refs=2 |
| `_calculate_completeness` | `FunctionDef` | `src/project_a/dq/automation.py:99` | ✅ Works (referenced) | refs=2 |
| `_calculate_uniqueness` | `FunctionDef` | `src/project_a/dq/automation.py:110` | ✅ Works (referenced) | refs=2 |
| `_analyze_data_types` | `FunctionDef` | `src/project_a/dq/automation.py:123` | ✅ Works (referenced) | refs=2 |
| `_detect_outliers` | `FunctionDef` | `src/project_a/dq/automation.py:150` | ✅ Works (referenced) | refs=2 |
| `_calculate_correlations` | `FunctionDef` | `src/project_a/dq/automation.py:185` | ✅ Works (referenced) | refs=2 |
| `_calculate_quality_score` | `FunctionDef` | `src/project_a/dq/automation.py:205` | ✅ Works (referenced) | refs=2 |
| `_save_profile` | `FunctionDef` | `src/project_a/dq/automation.py:228` | ✅ Works (referenced) | refs=2 |
| `DataQualityChecker` | `ClassDef` | `src/project_a/dq/automation.py:237` | ✅ Works (referenced) | refs=5 |
| `__init__` | `FunctionDef` | `src/project_a/dq/automation.py:240` | ✅ Works (referenced) | refs=162 |
| `run_completeness_check` | `FunctionDef` | `src/project_a/dq/automation.py:244` | ✅ Works (referenced) | refs=4 |
| `run_uniqueness_check` | `FunctionDef` | `src/project_a/dq/automation.py:276` | ✅ Works (referenced) | refs=8 |
| `run_range_check` | `FunctionDef` | `src/project_a/dq/automation.py:305` | ✅ Works (referenced) | refs=8 |
| `run_pattern_check` | `FunctionDef` | `src/project_a/dq/automation.py:347` | ✅ Works (referenced) | refs=4 |
| `generate_quality_report` | `FunctionDef` | `src/project_a/dq/automation.py:378` | ✅ Works (referenced) | refs=7 |
| `get_profiler` | `FunctionDef` | `src/project_a/dq/automation.py:407` | ✅ Works (referenced) | refs=2 |
| `get_checker` | `FunctionDef` | `src/project_a/dq/automation.py:418` | ✅ Works (referenced) | refs=3 |
| `profile_dataset` | `FunctionDef` | `src/project_a/dq/automation.py:426` | ✅ Works (referenced) | refs=3 |
| `run_quality_check` | `FunctionDef` | `src/project_a/dq/automation.py:432` | ✅ Works (referenced) | refs=3 |
| `generate_quality_report` | `FunctionDef` | `src/project_a/dq/automation.py:457` | ✅ Works (referenced) | refs=7 |
| `ComprehensiveValidator` | `ClassDef` | `src/project_a/dq/comprehensive_validator.py:31` | ✅ Works (referenced) | refs=3 |
| `__init__` | `FunctionDef` | `src/project_a/dq/comprehensive_validator.py:34` | ✅ Works (referenced) | refs=162 |
| `validate_bronze_layer` | `FunctionDef` | `src/project_a/dq/comprehensive_validator.py:42` | ✅ Works (referenced) | refs=4 |
| `validate_silver_layer` | `FunctionDef` | `src/project_a/dq/comprehensive_validator.py:93` | ✅ Works (referenced) | refs=6 |
| `validate_gold_layer` | `FunctionDef` | `src/project_a/dq/comprehensive_validator.py:146` | ✅ Works (referenced) | refs=6 |
| `_analyze_nulls` | `FunctionDef` | `src/project_a/dq/comprehensive_validator.py:179` | ✅ Works (referenced) | refs=4 |
| `_validate_timestamps` | `FunctionDef` | `src/project_a/dq/comprehensive_validator.py:195` | ✅ Works (referenced) | refs=2 |
| `_validate_fact_orders` | `FunctionDef` | `src/project_a/dq/comprehensive_validator.py:224` | ✅ Works (referenced) | refs=2 |
| `_validate_dim_customer` | `FunctionDef` | `src/project_a/dq/comprehensive_validator.py:247` | ✅ Works (referenced) | refs=2 |
| `generate_comprehensive_report` | `FunctionDef` | `src/project_a/dq/comprehensive_validator.py:264` | ✅ Works (referenced) | refs=2 |
| `get_summary` | `FunctionDef` | `src/project_a/dq/comprehensive_validator.py:310` | ✅ Works (referenced) | refs=4 |
| `FileIntegrityChecker` | `ClassDef` | `src/project_a/dq/file_integrity_checker.py:23` | ⚠️ Risky (only definition found) | refs=1 |
| `__init__` | `FunctionDef` | `src/project_a/dq/file_integrity_checker.py:26` | ✅ Works (referenced) | refs=162 |
| `compare_local_s3` | `FunctionDef` | `src/project_a/dq/file_integrity_checker.py:34` | ⚠️ Risky (only definition found) | refs=1 |
| `check_s3_object_integrity` | `FunctionDef` | `src/project_a/dq/file_integrity_checker.py:108` | ⚠️ Risky (only definition found) | refs=1 |
| `check_partition_count` | `FunctionDef` | `src/project_a/dq/file_integrity_checker.py:124` | ⚠️ Risky (only definition found) | refs=1 |
| `get_all_issues` | `FunctionDef` | `src/project_a/dq/file_integrity_checker.py:160` | ✅ Works (referenced) | refs=4 |
| `generate_report` | `FunctionDef` | `src/project_a/dq/file_integrity_checker.py:164` | ✅ Works (referenced) | refs=4 |
| `DQCheckResult` | `ClassDef` | `src/project_a/dq/gate.py:17` | ✅ Works (referenced) | refs=10 |
| `run_not_null_checks` | `FunctionDef` | `src/project_a/dq/gate.py:26` | ✅ Works (referenced) | refs=4 |
| `run_uniqueness_check` | `FunctionDef` | `src/project_a/dq/gate.py:56` | ✅ Works (referenced) | refs=8 |
| `run_range_check` | `FunctionDef` | `src/project_a/dq/gate.py:82` | ✅ Works (referenced) | refs=8 |
| `write_dq_result` | `FunctionDef` | `src/project_a/dq/gate.py:114` | ✅ Works (referenced) | refs=4 |
| `run_dq_gate` | `FunctionDef` | `src/project_a/dq/gate.py:136` | ✅ Works (referenced) | refs=6 |
| `KafkaStreamingValidator` | `ClassDef` | `src/project_a/dq/kafka_streaming_validator.py:24` | ✅ Works (referenced) | refs=3 |
| `__init__` | `FunctionDef` | `src/project_a/dq/kafka_streaming_validator.py:27` | ✅ Works (referenced) | refs=162 |
| `validate_streaming_fitness` | `FunctionDef` | `src/project_a/dq/kafka_streaming_validator.py:31` | ⚠️ Risky (only definition found) | refs=1 |
| `_check_timestamp_monotonicity` | `FunctionDef` | `src/project_a/dq/kafka_streaming_validator.py:89` | ✅ Works (referenced) | refs=2 |
| `_check_event_type_diversity` | `FunctionDef` | `src/project_a/dq/kafka_streaming_validator.py:110` | ✅ Works (referenced) | refs=2 |
| `_check_session_consistency` | `FunctionDef` | `src/project_a/dq/kafka_streaming_validator.py:120` | ✅ Works (referenced) | refs=2 |
| `_check_cardinality` | `FunctionDef` | `src/project_a/dq/kafka_streaming_validator.py:147` | ✅ Works (referenced) | refs=2 |
| `_check_late_events` | `FunctionDef` | `src/project_a/dq/kafka_streaming_validator.py:173` | ✅ Works (referenced) | refs=2 |
| `_check_out_of_order` | `FunctionDef` | `src/project_a/dq/kafka_streaming_validator.py:195` | ✅ Works (referenced) | refs=2 |
| `get_all_issues` | `FunctionDef` | `src/project_a/dq/kafka_streaming_validator.py:219` | ✅ Works (referenced) | refs=4 |
| `PerformanceOptimizer` | `ClassDef` | `src/project_a/dq/performance_optimizer.py:20` | ✅ Works (referenced) | refs=22 |
| `__init__` | `FunctionDef` | `src/project_a/dq/performance_optimizer.py:23` | ✅ Works (referenced) | refs=162 |
| `analyze_table_performance` | `FunctionDef` | `src/project_a/dq/performance_optimizer.py:28` | ⚠️ Risky (only definition found) | refs=1 |
| `_check_data_skew` | `FunctionDef` | `src/project_a/dq/performance_optimizer.py:82` | ✅ Works (referenced) | refs=2 |
| `_check_column_types` | `FunctionDef` | `src/project_a/dq/performance_optimizer.py:111` | ✅ Works (referenced) | refs=2 |
| `_check_partitioning` | `FunctionDef` | `src/project_a/dq/performance_optimizer.py:142` | ✅ Works (referenced) | refs=2 |
| `get_recommendations` | `FunctionDef` | `src/project_a/dq/performance_optimizer.py:166` | ⚠️ Risky (only definition found) | refs=1 |
| `generate_report` | `FunctionDef` | `src/project_a/dq/performance_optimizer.py:170` | ✅ Works (referenced) | refs=4 |
| `ReferentialIntegrityChecker` | `ClassDef` | `src/project_a/dq/referential_integrity.py:21` | ✅ Works (referenced) | refs=3 |
| `__init__` | `FunctionDef` | `src/project_a/dq/referential_integrity.py:24` | ✅ Works (referenced) | refs=162 |
| `check_foreign_key` | `FunctionDef` | `src/project_a/dq/referential_integrity.py:28` | ✅ Works (referenced) | refs=6 |
| `check_orders_customers` | `FunctionDef` | `src/project_a/dq/referential_integrity.py:91` | ✅ Works (referenced) | refs=2 |
| `check_orders_products` | `FunctionDef` | `src/project_a/dq/referential_integrity.py:99` | ✅ Works (referenced) | refs=2 |
| `check_opportunities_accounts` | `FunctionDef` | `src/project_a/dq/referential_integrity.py:105` | ⚠️ Risky (only definition found) | refs=1 |
| `check_behavior_customers` | `FunctionDef` | `src/project_a/dq/referential_integrity.py:113` | ⚠️ Risky (only definition found) | refs=1 |
| `check_kafka_customers` | `FunctionDef` | `src/project_a/dq/referential_integrity.py:121` | ⚠️ Risky (only definition found) | refs=1 |
| `check_duplicate_ids` | `FunctionDef` | `src/project_a/dq/referential_integrity.py:132` | ✅ Works (referenced) | refs=2 |
| `get_all_issues` | `FunctionDef` | `src/project_a/dq/referential_integrity.py:162` | ✅ Works (referenced) | refs=4 |
| `generate_report` | `FunctionDef` | `src/project_a/dq/referential_integrity.py:166` | ✅ Works (referenced) | refs=4 |
| `run_checkpoint` | `FunctionDef` | `src/project_a/dq/run_ge.py:14` | ✅ Works (referenced) | refs=18 |
| `run_contract_validation` | `FunctionDef` | `src/project_a/dq/run_ge.py:47` | ⚠️ Risky (only definition found) | refs=1 |
| `SchemaDriftChecker` | `ClassDef` | `src/project_a/dq/schema_drift_checker.py:22` | ✅ Works (referenced) | refs=3 |
| `__init__` | `FunctionDef` | `src/project_a/dq/schema_drift_checker.py:25` | ✅ Works (referenced) | refs=162 |
| `compare_schemas` | `FunctionDef` | `src/project_a/dq/schema_drift_checker.py:29` | ✅ Works (referenced) | refs=4 |
| `_check_id_pattern` | `FunctionDef` | `src/project_a/dq/schema_drift_checker.py:124` | ✅ Works (referenced) | refs=2 |
| `validate_dataframe` | `FunctionDef` | `src/project_a/dq/schema_drift_checker.py:130` | ✅ Works (referenced) | refs=7 |
| `get_all_issues` | `FunctionDef` | `src/project_a/dq/schema_drift_checker.py:137` | ✅ Works (referenced) | refs=4 |
| `generate_report` | `FunctionDef` | `src/project_a/dq/schema_drift_checker.py:141` | ✅ Works (referenced) | refs=4 |
| `read_fx_json` | `FunctionDef` | `src/project_a/extract/fx_json_reader.py:18` | ✅ Works (referenced) | refs=8 |
| `read_fx_rates_from_bronze` | `FunctionDef` | `src/project_a/extract/fx_json_reader.py:115` | ✅ Works (referenced) | refs=9 |
| `IcebergConfig` | `ClassDef` | `src/project_a/iceberg_utils.py:29` | ✅ Works (referenced) | refs=11 |
| `get_spark_config` | `FunctionDef` | `src/project_a/iceberg_utils.py:38` | ✅ Works (referenced) | refs=5 |
| `IcebergWriter` | `ClassDef` | `src/project_a/iceberg_utils.py:81` | ✅ Works (referenced) | refs=5 |
| `__init__` | `FunctionDef` | `src/project_a/iceberg_utils.py:88` | ✅ Works (referenced) | refs=162 |
| `write_overwrite` | `FunctionDef` | `src/project_a/iceberg_utils.py:92` | ✅ Works (referenced) | refs=2 |
| `write_append` | `FunctionDef` | `src/project_a/iceberg_utils.py:107` | ✅ Works (referenced) | refs=3 |
| `write_merge` | `FunctionDef` | `src/project_a/iceberg_utils.py:121` | ✅ Works (referenced) | refs=3 |
| `create_table` | `FunctionDef` | `src/project_a/iceberg_utils.py:157` | ✅ Works (referenced) | refs=13 |
| `IcebergReader` | `ClassDef` | `src/project_a/iceberg_utils.py:188` | ✅ Works (referenced) | refs=4 |
| `__init__` | `FunctionDef` | `src/project_a/iceberg_utils.py:191` | ✅ Works (referenced) | refs=162 |
| `read_current` | `FunctionDef` | `src/project_a/iceberg_utils.py:195` | ✅ Works (referenced) | refs=2 |
| `read_snapshot` | `FunctionDef` | `src/project_a/iceberg_utils.py:208` | ⚠️ Risky (only definition found) | refs=1 |
| `read_as_of_timestamp` | `FunctionDef` | `src/project_a/iceberg_utils.py:227` | ⚠️ Risky (only definition found) | refs=1 |
| `get_snapshots` | `FunctionDef` | `src/project_a/iceberg_utils.py:246` | ✅ Works (referenced) | refs=2 |
| `initialize_iceberg_spark` | `FunctionDef` | `src/project_a/iceberg_utils.py:259` | ⚠️ Risky (only definition found) | refs=1 |
| `read_bronze_table` | `FunctionDef` | `src/project_a/io/reader.py:20` | ✅ Works (referenced) | refs=3 |
| `read_csv_with_schema` | `FunctionDef` | `src/project_a/io/reader.py:99` | ✅ Works (referenced) | refs=5 |
| `read_json_with_schema` | `FunctionDef` | `src/project_a/io/reader.py:116` | ✅ Works (referenced) | refs=5 |
| `read_delta_table` | `FunctionDef` | `src/project_a/io/reader.py:127` | ✅ Works (referenced) | refs=4 |
| `validate_schema_before_write` | `FunctionDef` | `src/project_a/io/writer.py:19` | ✅ Works (referenced) | refs=4 |
| `write_silver_table` | `FunctionDef` | `src/project_a/io/writer.py:37` | ✅ Works (referenced) | refs=5 |
| `write_gold_table` | `FunctionDef` | `src/project_a/io/writer.py:89` | ✅ Works (referenced) | refs=3 |
| `__getattr__` | `FunctionDef` | `src/project_a/jobs/__init__.py:33` | ✅ Works (referenced) | refs=2 |
| `_arg_value` | `FunctionDef` | `src/project_a/jobs/_compat.py:26` | ✅ Works (referenced) | refs=4 |
| `namespace_to_argv` | `FunctionDef` | `src/project_a/jobs/_compat.py:32` | ✅ Works (referenced) | refs=2 |
| `patched_argv` | `FunctionDef` | `src/project_a/jobs/_compat.py:67` | ✅ Works (referenced) | refs=2 |
| `call_module_main` | `FunctionDef` | `src/project_a/jobs/_compat.py:77` | ✅ Works (referenced) | refs=19 |
| `run_job_class` | `FunctionDef` | `src/project_a/jobs/_compat.py:101` | ✅ Works (referenced) | refs=13 |
| `main` | `FunctionDef` | `src/project_a/jobs/bronze_to_silver.py:10` | ✅ Works (referenced) | refs=480 |
| `transform_contacts` | `FunctionDef` | `src/project_a/jobs/contacts_silver.py:30` | ✅ Works (referenced) | refs=2 |
| `run` | `FunctionDef` | `src/project_a/jobs/contacts_silver.py:41` | ✅ Works (referenced) | refs=1411 |
| `main` | `FunctionDef` | `src/project_a/jobs/crm_to_bronze.py:10` | ✅ Works (referenced) | refs=480 |
| `main` | `FunctionDef` | `src/project_a/jobs/dq_gold_gate.py:10` | ✅ Works (referenced) | refs=480 |
| `main` | `FunctionDef` | `src/project_a/jobs/dq_silver_gate.py:10` | ✅ Works (referenced) | refs=480 |
| `main` | `FunctionDef` | `src/project_a/jobs/fx_json_to_bronze.py:57` | ✅ Works (referenced) | refs=480 |
| `main` | `FunctionDef` | `src/project_a/jobs/kafka_csv_to_bronze.py:10` | ✅ Works (referenced) | refs=480 |
| `main` | `FunctionDef` | `src/project_a/jobs/publish_gold_to_redshift.py:10` | ✅ Works (referenced) | refs=480 |
| `main` | `FunctionDef` | `src/project_a/jobs/publish_gold_to_snowflake.py:10` | ✅ Works (referenced) | refs=480 |
| `main` | `FunctionDef` | `src/project_a/jobs/redshift_to_bronze.py:10` | ✅ Works (referenced) | refs=480 |
| `main` | `FunctionDef` | `src/project_a/jobs/silver_to_gold.py:10` | ✅ Works (referenced) | refs=480 |
| `main` | `FunctionDef` | `src/project_a/jobs/snowflake_to_bronze.py:10` | ✅ Works (referenced) | refs=480 |
| `load_config` | `FunctionDef` | `src/project_a/legacy/__main__.py:21` | ✅ Works (referenced) | refs=155 |
| `main` | `FunctionDef` | `src/project_a/legacy/__main__.py:49` | ✅ Works (referenced) | refs=480 |
| `QualitySeverity` | `ClassDef` | `src/project_a/legacy/advanced_dq_monitoring.py:27` | ✅ Works (referenced) | refs=20 |
| `QualityStatus` | `ClassDef` | `src/project_a/legacy/advanced_dq_monitoring.py:36` | ✅ Works (referenced) | refs=37 |
| `QualityCheck` | `ClassDef` | `src/project_a/legacy/advanced_dq_monitoring.py:46` | ✅ Works (referenced) | refs=32 |
| `QualityResult` | `ClassDef` | `src/project_a/legacy/advanced_dq_monitoring.py:64` | ✅ Works (referenced) | refs=19 |
| `AdvancedDataQualityManager` | `ClassDef` | `src/project_a/legacy/advanced_dq_monitoring.py:78` | ✅ Works (referenced) | refs=7 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/advanced_dq_monitoring.py:83` | ✅ Works (referenced) | refs=162 |
| `_load_quality_configs` | `FunctionDef` | `src/project_a/legacy/advanced_dq_monitoring.py:92` | ✅ Works (referenced) | refs=2 |
| `add_quality_check` | `FunctionDef` | `src/project_a/legacy/advanced_dq_monitoring.py:98` | ✅ Works (referenced) | refs=5 |
| `create_completeness_check` | `FunctionDef` | `src/project_a/legacy/advanced_dq_monitoring.py:116` | ✅ Works (referenced) | refs=3 |
| `create_uniqueness_check` | `FunctionDef` | `src/project_a/legacy/advanced_dq_monitoring.py:152` | ✅ Works (referenced) | refs=3 |
| `create_range_check` | `FunctionDef` | `src/project_a/legacy/advanced_dq_monitoring.py:188` | ✅ Works (referenced) | refs=3 |
| `create_pattern_check` | `FunctionDef` | `src/project_a/legacy/advanced_dq_monitoring.py:227` | ✅ Works (referenced) | refs=2 |
| `run_quality_check` | `FunctionDef` | `src/project_a/legacy/advanced_dq_monitoring.py:266` | ✅ Works (referenced) | refs=3 |
| `_run_completeness_check` | `FunctionDef` | `src/project_a/legacy/advanced_dq_monitoring.py:338` | ✅ Works (referenced) | refs=2 |
| `_run_uniqueness_check` | `FunctionDef` | `src/project_a/legacy/advanced_dq_monitoring.py:375` | ✅ Works (referenced) | refs=2 |
| `_run_range_check` | `FunctionDef` | `src/project_a/legacy/advanced_dq_monitoring.py:412` | ✅ Works (referenced) | refs=2 |
| `_run_pattern_check` | `FunctionDef` | `src/project_a/legacy/advanced_dq_monitoring.py:455` | ✅ Works (referenced) | refs=2 |
| `run_all_quality_checks` | `FunctionDef` | `src/project_a/legacy/advanced_dq_monitoring.py:496` | ✅ Works (referenced) | refs=3 |
| `check_sla_compliance` | `FunctionDef` | `src/project_a/legacy/advanced_dq_monitoring.py:516` | ✅ Works (referenced) | refs=4 |
| `generate_quality_report` | `FunctionDef` | `src/project_a/legacy/advanced_dq_monitoring.py:593` | ✅ Works (referenced) | refs=7 |
| `_generate_recommendations` | `FunctionDef` | `src/project_a/legacy/advanced_dq_monitoring.py:686` | ✅ Works (referenced) | refs=4 |
| `export_quality_metrics` | `FunctionDef` | `src/project_a/legacy/advanced_dq_monitoring.py:719` | ⚠️ Risky (only definition found) | refs=1 |
| `setup_advanced_data_quality` | `FunctionDef` | `src/project_a/legacy/advanced_dq_monitoring.py:802` | ⚠️ Risky (only definition found) | refs=1 |
| `run_quality_pipeline` | `FunctionDef` | `src/project_a/legacy/advanced_dq_monitoring.py:868` | ⚠️ Risky (only definition found) | refs=1 |
| `CustomerResponse` | `ClassDef` | `src/project_a/legacy/api/customer_api.py:42` | ✅ Works (referenced) | refs=5 |
| `health_check` | `FunctionDef` | `src/project_a/legacy/api/customer_api.py:56` | ✅ Works (referenced) | refs=37 |
| `get_customer` | `FunctionDef` | `src/project_a/legacy/api/customer_api.py:61` | ⚠️ Risky (only definition found) | refs=1 |
| `list_customers` | `FunctionDef` | `src/project_a/legacy/api/customer_api.py:100` | ⚠️ Risky (only definition found) | refs=1 |
| `get_stats` | `FunctionDef` | `src/project_a/legacy/api/customer_api.py:146` | ⚠️ Risky (only definition found) | refs=1 |
| `_get_spark_session` | `FunctionDef` | `src/project_a/legacy/api/customer_api.py:174` | ✅ Works (referenced) | refs=4 |
| `_load_config` | `FunctionDef` | `src/project_a/legacy/api/customer_api.py:188` | ✅ Works (referenced) | refs=4 |
| `Environment` | `ClassDef` | `src/project_a/legacy/cicd_manager.py:28` | ✅ Works (referenced) | refs=84 |
| `DeploymentStatus` | `ClassDef` | `src/project_a/legacy/cicd_manager.py:37` | ✅ Works (referenced) | refs=14 |
| `DeploymentType` | `ClassDef` | `src/project_a/legacy/cicd_manager.py:47` | ✅ Works (referenced) | refs=16 |
| `DeploymentConfig` | `ClassDef` | `src/project_a/legacy/cicd_manager.py:57` | ✅ Works (referenced) | refs=11 |
| `DeploymentResult` | `ClassDef` | `src/project_a/legacy/cicd_manager.py:74` | ✅ Works (referenced) | refs=31 |
| `CICDManager` | `ClassDef` | `src/project_a/legacy/cicd_manager.py:91` | ✅ Works (referenced) | refs=5 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:96` | ✅ Works (referenced) | refs=162 |
| `_load_environment_configs` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:103` | ✅ Works (referenced) | refs=2 |
| `create_deployment` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:122` | ✅ Works (referenced) | refs=2 |
| `deploy_to_environment` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:188` | ✅ Works (referenced) | refs=3 |
| `_validate_deployment` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:306` | ✅ Works (referenced) | refs=2 |
| `_run_automated_tests` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:354` | ✅ Works (referenced) | refs=2 |
| `_run_unit_tests` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:392` | ✅ Works (referenced) | refs=2 |
| `_run_integration_tests` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:420` | ✅ Works (referenced) | refs=2 |
| `_run_security_tests` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:448` | ✅ Works (referenced) | refs=2 |
| `_execute_deployment` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:478` | ✅ Works (referenced) | refs=2 |
| `_execute_blue_green_deployment` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:528` | ✅ Works (referenced) | refs=2 |
| `_execute_rolling_deployment` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:565` | ✅ Works (referenced) | refs=2 |
| `_execute_canary_deployment` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:598` | ✅ Works (referenced) | refs=2 |
| `_execute_immediate_deployment` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:634` | ✅ Works (referenced) | refs=2 |
| `_run_health_checks` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:658` | ✅ Works (referenced) | refs=12 |
| `_run_health_checks_green` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:689` | ✅ Works (referenced) | refs=3 |
| `_run_health_checks_rolling` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:714` | ✅ Works (referenced) | refs=2 |
| `_run_health_checks_canary` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:733` | ✅ Works (referenced) | refs=2 |
| `_run_health_checks_immediate` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:754` | ✅ Works (referenced) | refs=3 |
| `_run_target_health_check` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:775` | ✅ Works (referenced) | refs=3 |
| `_run_canary_tests` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:786` | ✅ Works (referenced) | refs=2 |
| `_run_canary_health_check` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:799` | ✅ Works (referenced) | refs=2 |
| `_is_environment_available` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:812` | ✅ Works (referenced) | refs=2 |
| `_apply_environment_variables` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:838` | ✅ Works (referenced) | refs=2 |
| `_apply_secrets` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:858` | ✅ Works (referenced) | refs=2 |
| `_trigger_rollback` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:878` | ✅ Works (referenced) | refs=4 |
| `_execute_rollback` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:899` | ✅ Works (referenced) | refs=2 |
| `get_deployment_status` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:936` | ⚠️ Risky (only definition found) | refs=1 |
| `get_deployment_history` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:940` | ⚠️ Risky (only definition found) | refs=1 |
| `export_deployment_report` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:954` | ⚠️ Risky (only definition found) | refs=1 |
| `setup_cicd_manager` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:1034` | ⚠️ Risky (only definition found) | refs=1 |
| `run_deployment_pipeline` | `FunctionDef` | `src/project_a/legacy/cicd_manager.py:1057` | ⚠️ Risky (only definition found) | refs=1 |
| `load_config` | `FunctionDef` | `src/project_a/legacy/cli.py:24` | ✅ Works (referenced) | refs=155 |
| `run_ingest` | `FunctionDef` | `src/project_a/legacy/cli.py:36` | ✅ Works (referenced) | refs=13 |
| `run_transform` | `FunctionDef` | `src/project_a/legacy/cli.py:77` | ✅ Works (referenced) | refs=9 |
| `run_validate` | `FunctionDef` | `src/project_a/legacy/cli.py:97` | ✅ Works (referenced) | refs=6 |
| `run_load` | `FunctionDef` | `src/project_a/legacy/cli.py:154` | ✅ Works (referenced) | refs=5 |
| `main` | `FunctionDef` | `src/project_a/legacy/cli.py:202` | ✅ Works (referenced) | refs=480 |
| `require_not_null` | `FunctionDef` | `src/project_a/legacy/common/dq.py:20` | ✅ Works (referenced) | refs=4 |
| `require_unique_keys` | `FunctionDef` | `src/project_a/legacy/common/dq.py:63` | ✅ Works (referenced) | refs=4 |
| `control_total` | `FunctionDef` | `src/project_a/legacy/common/dq.py:115` | ✅ Works (referenced) | refs=7 |
| `validate_schema` | `FunctionDef` | `src/project_a/legacy/common/dq.py:174` | ✅ Works (referenced) | refs=34 |
| `run_dq_checks` | `FunctionDef` | `src/project_a/legacy/common/dq.py:214` | ✅ Works (referenced) | refs=3 |
| `generate_dq_report` | `FunctionDef` | `src/project_a/legacy/common/dq.py:286` | ✅ Works (referenced) | refs=4 |
| `SCD2Config` | `ClassDef` | `src/project_a/legacy/common/scd2.py:22` | ✅ Works (referenced) | refs=10 |
| `apply_scd2` | `FunctionDef` | `src/project_a/legacy/common/scd2.py:39` | ✅ Works (referenced) | refs=19 |
| `validate_scd2_table` | `FunctionDef` | `src/project_a/legacy/common/scd2.py:165` | ✅ Works (referenced) | refs=5 |
| `get_scd2_history` | `FunctionDef` | `src/project_a/legacy/common/scd2.py:258` | ⚠️ Risky (only definition found) | refs=1 |
| `get_current_records` | `FunctionDef` | `src/project_a/legacy/common/scd2.py:280` | ⚠️ Risky (only definition found) | refs=1 |
| `ConfigLoader` | `ClassDef` | `src/project_a/legacy/config_loader.py:25` | ✅ Works (referenced) | refs=2 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/config_loader.py:28` | ✅ Works (referenced) | refs=162 |
| `load_config` | `FunctionDef` | `src/project_a/legacy/config_loader.py:33` | ✅ Works (referenced) | refs=155 |
| `validate_config` | `FunctionDef` | `src/project_a/legacy/config_loader.py:38` | ✅ Works (referenced) | refs=4 |
| `get_config` | `FunctionDef` | `src/project_a/legacy/config_loader.py:69` | ✅ Works (referenced) | refs=7 |
| `_get_dbutils` | `FunctionDef` | `src/project_a/legacy/config_loader.py:76` | ✅ Works (referenced) | refs=6 |
| `_resolve_value` | `FunctionDef` | `src/project_a/legacy/config_loader.py:93` | ✅ Works (referenced) | refs=11 |
| `replace_secret` | `FunctionDef` | `src/project_a/legacy/config_loader.py:98` | ✅ Works (referenced) | refs=6 |
| `replace_var` | `FunctionDef` | `src/project_a/legacy/config_loader.py:116` | ✅ Works (referenced) | refs=12 |
| `_resolve_secrets` | `FunctionDef` | `src/project_a/legacy/config_loader.py:137` | ✅ Works (referenced) | refs=16 |
| `load_config_resolved` | `FunctionDef` | `src/project_a/legacy/config_loader.py:150` | ✅ Works (referenced) | refs=107 |
| `IO` | `ClassDef` | `src/project_a/legacy/config_model.py:11` | ✅ Works (referenced) | refs=378 |
| `PathSet` | `ClassDef` | `src/project_a/legacy/config_model.py:18` | ✅ Works (referenced) | refs=4 |
| `Paths` | `ClassDef` | `src/project_a/legacy/config_model.py:26` | ✅ Works (referenced) | refs=5 |
| `SparkConfig` | `ClassDef` | `src/project_a/legacy/config_model.py:34` | ✅ Works (referenced) | refs=3 |
| `DataQuality` | `ClassDef` | `src/project_a/legacy/config_model.py:43` | ✅ Works (referenced) | refs=36 |
| `AppConfig` | `ClassDef` | `src/project_a/legacy/config_model.py:53` | ⚠️ Risky (only definition found) | refs=1 |
| `Config` | `ClassDef` | `src/project_a/legacy/config_model.py:64` | ✅ Works (referenced) | refs=499 |
| `validate_paths` | `FunctionDef` | `src/project_a/legacy/config_model.py:67` | ⚠️ Risky (only definition found) | refs=1 |
| `get_spark_config` | `FunctionDef` | `src/project_a/legacy/config_model.py:78` | ✅ Works (referenced) | refs=5 |
| `ContractSeverity` | `ClassDef` | `src/project_a/legacy/data_contracts.py:33` | ✅ Works (referenced) | refs=22 |
| `SchemaChangeType` | `ClassDef` | `src/project_a/legacy/data_contracts.py:41` | ✅ Works (referenced) | refs=5 |
| `DataConstraint` | `ClassDef` | `src/project_a/legacy/data_contracts.py:53` | ✅ Works (referenced) | refs=20 |
| `TableContract` | `ClassDef` | `src/project_a/legacy/data_contracts.py:66` | ✅ Works (referenced) | refs=30 |
| `DataContractManager` | `ClassDef` | `src/project_a/legacy/data_contracts.py:84` | ✅ Works (referenced) | refs=4 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:89` | ✅ Works (referenced) | refs=162 |
| `_load_existing_contracts` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:99` | ✅ Works (referenced) | refs=2 |
| `_schema_from_json` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:146` | ✅ Works (referenced) | refs=2 |
| `_type_from_json` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:162` | ✅ Works (referenced) | refs=5 |
| `_create_default_contracts` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:190` | ✅ Works (referenced) | refs=2 |
| `_create_returns_raw_contract` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:207` | ✅ Works (referenced) | refs=2 |
| `_create_customers_raw_contract` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:306` | ✅ Works (referenced) | refs=2 |
| `_create_products_raw_contract` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:356` | ✅ Works (referenced) | refs=2 |
| `_create_orders_raw_contract` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:398` | ✅ Works (referenced) | refs=2 |
| `_create_inventory_contract` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:441` | ✅ Works (referenced) | refs=2 |
| `_create_fx_rates_contract` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:474` | ✅ Works (referenced) | refs=2 |
| `validate_contract` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:506` | ✅ Works (referenced) | refs=5 |
| `_validate_schema` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:561` | ✅ Works (referenced) | refs=4 |
| `_validate_constraints` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:619` | ✅ Works (referenced) | refs=2 |
| `_validate_single_constraint` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:648` | ✅ Works (referenced) | refs=2 |
| `_generate_recommendations` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:714` | ✅ Works (referenced) | refs=4 |
| `detect_schema_drift` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:752` | ✅ Works (referenced) | refs=3 |
| `_is_backward_incompatible_change` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:828` | ✅ Works (referenced) | refs=2 |
| `_generate_evolution_recommendations` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:849` | ✅ Works (referenced) | refs=2 |
| `evolve_contract` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:882` | ⚠️ Risky (only definition found) | refs=1 |
| `_type_from_string` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:952` | ✅ Works (referenced) | refs=3 |
| `_increment_version` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:964` | ✅ Works (referenced) | refs=2 |
| `_save_contract` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:975` | ✅ Works (referenced) | refs=3 |
| `get_contract_summary` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:1025` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `src/project_a/legacy/data_contracts.py:1043` | ✅ Works (referenced) | refs=480 |
| `DataQualitySuite` | `ClassDef` | `src/project_a/legacy/data_quality_suite.py:26` | ✅ Works (referenced) | refs=3 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/data_quality_suite.py:31` | ✅ Works (referenced) | refs=162 |
| `validate_schema` | `FunctionDef` | `src/project_a/legacy/data_quality_suite.py:36` | ✅ Works (referenced) | refs=34 |
| `check_completeness` | `FunctionDef` | `src/project_a/legacy/data_quality_suite.py:70` | ✅ Works (referenced) | refs=2 |
| `check_uniqueness` | `FunctionDef` | `src/project_a/legacy/data_quality_suite.py:116` | ✅ Works (referenced) | refs=7 |
| `check_consistency` | `FunctionDef` | `src/project_a/legacy/data_quality_suite.py:146` | ✅ Works (referenced) | refs=2 |
| `check_accuracy` | `FunctionDef` | `src/project_a/legacy/data_quality_suite.py:190` | ✅ Works (referenced) | refs=2 |
| `check_timeliness` | `FunctionDef` | `src/project_a/legacy/data_quality_suite.py:272` | ✅ Works (referenced) | refs=2 |
| `calculate_statistics` | `FunctionDef` | `src/project_a/legacy/data_quality_suite.py:317` | ✅ Works (referenced) | refs=2 |
| `detect_anomalies` | `FunctionDef` | `src/project_a/legacy/data_quality_suite.py:372` | ⚠️ Risky (only definition found) | refs=1 |
| `generate_quality_report` | `FunctionDef` | `src/project_a/legacy/data_quality_suite.py:465` | ✅ Works (referenced) | refs=7 |
| `run_comprehensive_validation` | `FunctionDef` | `src/project_a/legacy/data_quality_suite.py:508` | ⚠️ Risky (only definition found) | refs=1 |
| `StandardDeltaLake` | `ClassDef` | `src/project_a/legacy/delta_lake_standard.py:17` | ✅ Works (referenced) | refs=8 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/delta_lake_standard.py:20` | ✅ Works (referenced) | refs=162 |
| `create_initial_table` | `FunctionDef` | `src/project_a/legacy/delta_lake_standard.py:25` | ✅ Works (referenced) | refs=2 |
| `append_data` | `FunctionDef` | `src/project_a/legacy/delta_lake_standard.py:58` | ✅ Works (referenced) | refs=3 |
| `update_data` | `FunctionDef` | `src/project_a/legacy/delta_lake_standard.py:84` | ✅ Works (referenced) | refs=6 |
| `_create_schema_string` | `FunctionDef` | `src/project_a/legacy/delta_lake_standard.py:110` | ✅ Works (referenced) | refs=2 |
| `_create_stats_json` | `FunctionDef` | `src/project_a/legacy/delta_lake_standard.py:134` | ✅ Works (referenced) | refs=4 |
| `_write_transaction_log` | `FunctionDef` | `src/project_a/legacy/delta_lake_standard.py:147` | ✅ Works (referenced) | refs=4 |
| `get_table_info` | `FunctionDef` | `src/project_a/legacy/delta_lake_standard.py:158` | ✅ Works (referenced) | refs=5 |
| `show_version_history` | `FunctionDef` | `src/project_a/legacy/delta_lake_standard.py:170` | ✅ Works (referenced) | refs=7 |
| `create_standard_delta_tables` | `FunctionDef` | `src/project_a/legacy/delta_lake_standard.py:190` | ✅ Works (referenced) | refs=4 |
| `generate_customers_data` | `FunctionDef` | `src/project_a/legacy/delta_lake_standard.py:275` | ✅ Works (referenced) | refs=2 |
| `generate_orders_data` | `FunctionDef` | `src/project_a/legacy/delta_lake_standard.py:295` | ✅ Works (referenced) | refs=2 |
| `generate_analytics_data` | `FunctionDef` | `src/project_a/legacy/delta_lake_standard.py:319` | ✅ Works (referenced) | refs=2 |
| `generate_revenue_data` | `FunctionDef` | `src/project_a/legacy/delta_lake_standard.py:330` | ✅ Works (referenced) | refs=2 |
| `generate_updated_customers_data` | `FunctionDef` | `src/project_a/legacy/delta_lake_standard.py:337` | ✅ Works (referenced) | refs=2 |
| `generate_updated_orders_data` | `FunctionDef` | `src/project_a/legacy/delta_lake_standard.py:362` | ✅ Works (referenced) | refs=2 |
| `generate_updated_analytics_data` | `FunctionDef` | `src/project_a/legacy/delta_lake_standard.py:373` | ✅ Works (referenced) | refs=2 |
| `generate_updated_revenue_data` | `FunctionDef` | `src/project_a/legacy/delta_lake_standard.py:384` | ✅ Works (referenced) | refs=2 |
| `read_delta` | `FunctionDef` | `src/project_a/legacy/delta_utils.py:13` | ✅ Works (referenced) | refs=55 |
| `write_delta` | `FunctionDef` | `src/project_a/legacy/delta_utils.py:33` | ✅ Works (referenced) | refs=43 |
| `DeltaUtils` | `ClassDef` | `src/project_a/legacy/delta_utils.py:50` | ✅ Works (referenced) | refs=3 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/delta_utils.py:55` | ✅ Works (referenced) | refs=162 |
| `optimize_table` | `FunctionDef` | `src/project_a/legacy/delta_utils.py:58` | ✅ Works (referenced) | refs=18 |
| `set_retention_policy` | `FunctionDef` | `src/project_a/legacy/delta_utils.py:78` | ✅ Works (referenced) | refs=3 |
| `cleanup_old_partitions` | `FunctionDef` | `src/project_a/legacy/delta_utils.py:99` | ✅ Works (referenced) | refs=2 |
| `get_table_stats` | `FunctionDef` | `src/project_a/legacy/delta_utils.py:131` | ✅ Works (referenced) | refs=3 |
| `run_maintenance_routine` | `FunctionDef` | `src/project_a/legacy/delta_utils.py:156` | ✅ Works (referenced) | refs=2 |
| `BackupStrategy` | `ClassDef` | `src/project_a/legacy/disaster_recovery.py:32` | ✅ Works (referenced) | refs=11 |
| `ReplicationConfig` | `ClassDef` | `src/project_a/legacy/disaster_recovery.py:48` | ✅ Works (referenced) | refs=8 |
| `DisasterRecoveryExecutor` | `ClassDef` | `src/project_a/legacy/disaster_recovery.py:61` | ✅ Works (referenced) | refs=6 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/disaster_recovery.py:66` | ✅ Works (referenced) | refs=162 |
| `_init_azure_clients` | `FunctionDef` | `src/project_a/legacy/disaster_recovery.py:85` | ✅ Works (referenced) | refs=2 |
| `_init_mock_clients` | `FunctionDef` | `src/project_a/legacy/disaster_recovery.py:114` | ✅ Works (referenced) | refs=3 |
| `load_backup_strategy` | `FunctionDef` | `src/project_a/legacy/disaster_recovery.py:121` | ✅ Works (referenced) | refs=2 |
| `save_backup_strategy` | `FunctionDef` | `src/project_a/legacy/disaster_recovery.py:157` | ✅ Works (referenced) | refs=2 |
| `load_replication_configs` | `FunctionDef` | `src/project_a/legacy/disaster_recovery.py:172` | ✅ Works (referenced) | refs=2 |
| `_create_default_replication_configs` | `FunctionDef` | `src/project_a/legacy/disaster_recovery.py:204` | ✅ Works (referenced) | refs=2 |
| `execute_backup` | `FunctionDef` | `src/project_a/legacy/disaster_recovery.py:247` | ✅ Works (referenced) | refs=3 |
| `_backup_table` | `FunctionDef` | `src/project_a/legacy/disaster_recovery.py:309` | ✅ Works (referenced) | refs=2 |
| `_get_table_path` | `FunctionDef` | `src/project_a/legacy/disaster_recovery.py:368` | ✅ Works (referenced) | refs=3 |
| `_compress_backup` | `FunctionDef` | `src/project_a/legacy/disaster_recovery.py:381` | ✅ Works (referenced) | refs=2 |
| `_upload_backup_to_storage` | `FunctionDef` | `src/project_a/legacy/disaster_recovery.py:402` | ✅ Works (referenced) | refs=2 |
| `_cleanup_old_backups` | `FunctionDef` | `src/project_a/legacy/disaster_recovery.py:432` | ✅ Works (referenced) | refs=2 |
| `execute_replication` | `FunctionDef` | `src/project_a/legacy/disaster_recovery.py:461` | ✅ Works (referenced) | refs=3 |
| `_replicate_table` | `FunctionDef` | `src/project_a/legacy/disaster_recovery.py:507` | ✅ Works (referenced) | refs=2 |
| `_save_backup_metrics` | `FunctionDef` | `src/project_a/legacy/disaster_recovery.py:554` | ✅ Works (referenced) | refs=2 |
| `_save_replication_metrics` | `FunctionDef` | `src/project_a/legacy/disaster_recovery.py:564` | ✅ Works (referenced) | refs=2 |
| `get_dr_status` | `FunctionDef` | `src/project_a/legacy/disaster_recovery.py:574` | ✅ Works (referenced) | refs=2 |
| `run_dr_workflow` | `FunctionDef` | `src/project_a/legacy/disaster_recovery.py:621` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `src/project_a/legacy/disaster_recovery.py:685` | ✅ Works (referenced) | refs=480 |
| `get_spark` | `FunctionDef` | `src/project_a/legacy/dq/run_ge_customer_behavior.py:23` | ✅ Works (referenced) | refs=61 |
| `main` | `FunctionDef` | `src/project_a/legacy/dq/run_ge_customer_behavior.py:28` | ✅ Works (referenced) | refs=480 |
| `assert_non_null` | `FunctionDef` | `src/project_a/legacy/dq/smoke.py:13` | ✅ Works (referenced) | refs=13 |
| `assert_no_duplicates` | `FunctionDef` | `src/project_a/legacy/dq/smoke.py:27` | ✅ Works (referenced) | refs=4 |
| `assert_positive_values` | `FunctionDef` | `src/project_a/legacy/dq/smoke.py:41` | ✅ Works (referenced) | refs=4 |
| `assert_valid_dates` | `FunctionDef` | `src/project_a/legacy/dq/smoke.py:53` | ✅ Works (referenced) | refs=2 |
| `dq_customers` | `FunctionDef` | `src/project_a/legacy/dq/smoke.py:65` | ✅ Works (referenced) | refs=2 |
| `dq_orders` | `FunctionDef` | `src/project_a/legacy/dq/smoke.py:85` | ✅ Works (referenced) | refs=2 |
| `dq_products` | `FunctionDef` | `src/project_a/legacy/dq/smoke.py:109` | ✅ Works (referenced) | refs=2 |
| `run_dq_checks` | `FunctionDef` | `src/project_a/legacy/dq/smoke.py:127` | ✅ Works (referenced) | refs=3 |
| `DQChecks` | `ClassDef` | `src/project_a/legacy/dq_checks.py:6` | ✅ Works (referenced) | refs=7 |
| `assert_non_null` | `FunctionDef` | `src/project_a/legacy/dq_checks.py:7` | ✅ Works (referenced) | refs=13 |
| `assert_unique` | `FunctionDef` | `src/project_a/legacy/dq_checks.py:13` | ✅ Works (referenced) | refs=7 |
| `assert_age_range` | `FunctionDef` | `src/project_a/legacy/dq_checks.py:20` | ⚠️ Risky (only definition found) | refs=1 |
| `assert_email_valid` | `FunctionDef` | `src/project_a/legacy/dq_checks.py:26` | ⚠️ Risky (only definition found) | refs=1 |
| `assert_referential_integrity` | `FunctionDef` | `src/project_a/legacy/dq_checks.py:34` | ⚠️ Risky (only definition found) | refs=1 |
| `assert_values_in_set` | `FunctionDef` | `src/project_a/legacy/dq_checks.py:53` | ⚠️ Risky (only definition found) | refs=1 |
| `_read_json` | `FunctionDef` | `src/project_a/legacy/dr/dr_runner.py:10` | ✅ Works (referenced) | refs=4 |
| `run_backup_and_replication` | `FunctionDef` | `src/project_a/legacy/dr/dr_runner.py:14` | ⚠️ Risky (only definition found) | refs=1 |
| `PlatformHealth` | `ClassDef` | `src/project_a/legacy/enterprise_data_platform.py:36` | ✅ Works (referenced) | refs=2 |
| `PlatformMetrics` | `ClassDef` | `src/project_a/legacy/enterprise_data_platform.py:51` | ✅ Works (referenced) | refs=2 |
| `EnterpriseDataPlatform` | `ClassDef` | `src/project_a/legacy/enterprise_data_platform.py:64` | ✅ Works (referenced) | refs=9 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/enterprise_data_platform.py:69` | ✅ Works (referenced) | refs=162 |
| `_initialize_platform` | `FunctionDef` | `src/project_a/legacy/enterprise_data_platform.py:103` | ✅ Works (referenced) | refs=2 |
| `_setup_platform_governance` | `FunctionDef` | `src/project_a/legacy/enterprise_data_platform.py:148` | ✅ Works (referenced) | refs=2 |
| `_check_platform_health` | `FunctionDef` | `src/project_a/legacy/enterprise_data_platform.py:179` | ✅ Works (referenced) | refs=3 |
| `create_data_pipeline` | `FunctionDef` | `src/project_a/legacy/enterprise_data_platform.py:286` | ⚠️ Risky (only definition found) | refs=1 |
| `run_data_quality_checks` | `FunctionDef` | `src/project_a/legacy/enterprise_data_platform.py:398` | ✅ Works (referenced) | refs=4 |
| `optimize_table_performance` | `FunctionDef` | `src/project_a/legacy/enterprise_data_platform.py:450` | ⚠️ Risky (only definition found) | refs=1 |
| `get_platform_summary` | `FunctionDef` | `src/project_a/legacy/enterprise_data_platform.py:506` | ✅ Works (referenced) | refs=3 |
| `_update_platform_metrics` | `FunctionDef` | `src/project_a/legacy/enterprise_data_platform.py:590` | ✅ Works (referenced) | refs=3 |
| `export_platform_config` | `FunctionDef` | `src/project_a/legacy/enterprise_data_platform.py:652` | ⚠️ Risky (only definition found) | refs=1 |
| `setup_enterprise_data_platform` | `FunctionDef` | `src/project_a/legacy/enterprise_data_platform.py:683` | ⚠️ Risky (only definition found) | refs=1 |
| `run_platform_health_check` | `FunctionDef` | `src/project_a/legacy/enterprise_data_platform.py:709` | ⚠️ Risky (only definition found) | refs=1 |
| `GoldWriter` | `ClassDef` | `src/project_a/legacy/gold_writer.py:12` | ⚠️ Risky (only definition found) | refs=1 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/gold_writer.py:15` | ✅ Works (referenced) | refs=162 |
| `publish_gold` | `FunctionDef` | `src/project_a/legacy/gold_writer.py:18` | ✅ Works (referenced) | refs=47 |
| `publish_customer_analytics` | `FunctionDef` | `src/project_a/legacy/gold_writer.py:84` | ✅ Works (referenced) | refs=2 |
| `publish_order_analytics` | `FunctionDef` | `src/project_a/legacy/gold_writer.py:93` | ✅ Works (referenced) | refs=2 |
| `publish_monthly_revenue` | `FunctionDef` | `src/project_a/legacy/gold_writer.py:102` | ✅ Works (referenced) | refs=2 |
| `_generate_customer_analytics` | `FunctionDef` | `src/project_a/legacy/gold_writer.py:111` | ✅ Works (referenced) | refs=4 |
| `_generate_order_analytics` | `FunctionDef` | `src/project_a/legacy/gold_writer.py:121` | ✅ Works (referenced) | refs=2 |
| `_generate_monthly_revenue` | `FunctionDef` | `src/project_a/legacy/gold_writer.py:140` | ✅ Works (referenced) | refs=4 |
| `IncrementalLoader` | `ClassDef` | `src/project_a/legacy/incremental_loading.py:24` | ✅ Works (referenced) | refs=3 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/incremental_loading.py:30` | ✅ Works (referenced) | refs=162 |
| `get_latest_watermark` | `FunctionDef` | `src/project_a/legacy/incremental_loading.py:34` | ⚠️ Risky (only definition found) | refs=1 |
| `filter_incremental_data` | `FunctionDef` | `src/project_a/legacy/incremental_loading.py:57` | ✅ Works (referenced) | refs=3 |
| `apply_scd_type2` | `FunctionDef` | `src/project_a/legacy/incremental_loading.py:77` | ⚠️ Risky (only definition found) | refs=1 |
| `detect_changes` | `FunctionDef` | `src/project_a/legacy/incremental_loading.py:209` | ✅ Works (referenced) | refs=4 |
| `apply_cdc_changes` | `FunctionDef` | `src/project_a/legacy/incremental_loading.py:265` | ⚠️ Risky (only definition found) | refs=1 |
| `create_watermark_table` | `FunctionDef` | `src/project_a/legacy/incremental_loading.py:354` | ⚠️ Risky (only definition found) | refs=1 |
| `optimize_for_incremental_processing` | `FunctionDef` | `src/project_a/legacy/incremental_loading.py:385` | ⚠️ Risky (only definition found) | refs=1 |
| `IngestionPipeline` | `ClassDef` | `src/project_a/legacy/ingestion_pipeline.py:38` | ✅ Works (referenced) | refs=5 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:48` | ✅ Works (referenced) | refs=162 |
| `run_full_pipeline` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:65` | ✅ Works (referenced) | refs=2 |
| `_validate_bronze_layer` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:112` | ✅ Works (referenced) | refs=2 |
| `_validate_table` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:140` | ✅ Works (referenced) | refs=2 |
| `_validate_customers` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:164` | ✅ Works (referenced) | refs=2 |
| `_validate_orders` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:193` | ✅ Works (referenced) | refs=2 |
| `_validate_returns` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:210` | ✅ Works (referenced) | refs=2 |
| `_validate_products` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:229` | ✅ Works (referenced) | refs=2 |
| `_process_silver_layer` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:246` | ✅ Works (referenced) | refs=2 |
| `_transform_customers_silver` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:296` | ✅ Works (referenced) | refs=2 |
| `_transform_products_silver` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:332` | ✅ Works (referenced) | refs=2 |
| `_transform_orders_silver` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:355` | ✅ Works (referenced) | refs=2 |
| `_transform_returns_silver` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:384` | ✅ Works (referenced) | refs=2 |
| `_transform_inventory_silver` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:413` | ✅ Works (referenced) | refs=2 |
| `_process_gold_layer` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:439` | ✅ Works (referenced) | refs=2 |
| `_create_customer_metrics` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:488` | ✅ Works (referenced) | refs=2 |
| `_create_product_metrics` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:513` | ✅ Works (referenced) | refs=2 |
| `_create_order_metrics` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:537` | ✅ Works (referenced) | refs=2 |
| `_create_return_metrics` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:564` | ✅ Works (referenced) | refs=2 |
| `_create_inventory_metrics` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:586` | ✅ Works (referenced) | refs=2 |
| `_optimize_performance` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:608` | ✅ Works (referenced) | refs=2 |
| `_optimize_table` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:646` | ✅ Works (referenced) | refs=3 |
| `_record_lineage` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:671` | ✅ Works (referenced) | refs=2 |
| `_save_pipeline_metrics` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:685` | ✅ Works (referenced) | refs=3 |
| `main` | `FunctionDef` | `src/project_a/legacy/ingestion_pipeline.py:702` | ✅ Works (referenced) | refs=480 |
| `_delta_supported` | `FunctionDef` | `src/project_a/legacy/io_utils.py:12` | ✅ Works (referenced) | refs=2 |
| `_is_delta_table` | `FunctionDef` | `src/project_a/legacy/io_utils.py:22` | ✅ Works (referenced) | refs=2 |
| `write_parquet` | `FunctionDef` | `src/project_a/legacy/io_utils.py:38` | ✅ Works (referenced) | refs=5 |
| `write_delta` | `FunctionDef` | `src/project_a/legacy/io_utils.py:53` | ✅ Works (referenced) | refs=43 |
| `read_delta_or_parquet` | `FunctionDef` | `src/project_a/legacy/io_utils.py:74` | ⚠️ Risky (only definition found) | refs=1 |
| `build_dim_customer_scd2` | `FunctionDef` | `src/project_a/legacy/jobs/dim_customer_scd2.py:40` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `src/project_a/legacy/jobs/dim_customer_scd2.py:153` | ✅ Works (referenced) | refs=480 |
| `deduplicate_fx_rates` | `FunctionDef` | `src/project_a/legacy/jobs/fx_bronze_to_silver.py:25` | ✅ Works (referenced) | refs=5 |
| `add_rate_categories` | `FunctionDef` | `src/project_a/legacy/jobs/fx_bronze_to_silver.py:57` | ✅ Works (referenced) | refs=6 |
| `validate_fx_rates` | `FunctionDef` | `src/project_a/legacy/jobs/fx_bronze_to_silver.py:85` | ✅ Works (referenced) | refs=11 |
| `main` | `FunctionDef` | `src/project_a/legacy/jobs/fx_bronze_to_silver.py:127` | ✅ Works (referenced) | refs=480 |
| `fetch_fx_rates` | `FunctionDef` | `src/project_a/legacy/jobs/fx_to_bronze.py:24` | ✅ Works (referenced) | refs=6 |
| `create_fx_schema` | `FunctionDef` | `src/project_a/legacy/jobs/fx_to_bronze.py:40` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `src/project_a/legacy/jobs/fx_to_bronze.py:52` | ✅ Works (referenced) | refs=480 |
| `build_dim_date` | `FunctionDef` | `src/project_a/legacy/jobs/gold_star_schema.py:36` | ✅ Works (referenced) | refs=8 |
| `build_dim_product` | `FunctionDef` | `src/project_a/legacy/jobs/gold_star_schema.py:86` | ✅ Works (referenced) | refs=8 |
| `build_gold_star_schema` | `FunctionDef` | `src/project_a/legacy/jobs/gold_star_schema.py:165` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `src/project_a/legacy/jobs/gold_star_schema.py:291` | ✅ Works (referenced) | refs=480 |
| `publish_gold_idempotent` | `FunctionDef` | `src/project_a/legacy/jobs/gold_writer.py:12` | ✅ Works (referenced) | refs=4 |
| `publish_customer_analytics` | `FunctionDef` | `src/project_a/legacy/jobs/gold_writer.py:76` | ✅ Works (referenced) | refs=2 |
| `publish_order_analytics` | `FunctionDef` | `src/project_a/legacy/jobs/gold_writer.py:85` | ✅ Works (referenced) | refs=2 |
| `publish_monthly_revenue` | `FunctionDef` | `src/project_a/legacy/jobs/gold_writer.py:94` | ✅ Works (referenced) | refs=2 |
| `get_kafka_config` | `FunctionDef` | `src/project_a/legacy/jobs/kafka_orders_stream.py:34` | ✅ Works (referenced) | refs=4 |
| `get_orders_schema` | `FunctionDef` | `src/project_a/legacy/jobs/kafka_orders_stream.py:56` | ✅ Works (referenced) | refs=4 |
| `process_orders_stream` | `FunctionDef` | `src/project_a/legacy/jobs/kafka_orders_stream.py:73` | ✅ Works (referenced) | refs=4 |
| `main` | `FunctionDef` | `src/project_a/legacy/jobs/kafka_orders_stream.py:212` | ✅ Works (referenced) | refs=480 |
| `load_delta_to_snowflake` | `FunctionDef` | `src/project_a/legacy/jobs/load_to_snowflake.py:26` | ✅ Works (referenced) | refs=2 |
| `_build_merge_sql` | `FunctionDef` | `src/project_a/legacy/jobs/load_to_snowflake.py:122` | ✅ Works (referenced) | refs=2 |
| `load_gold_tables_to_snowflake` | `FunctionDef` | `src/project_a/legacy/jobs/load_to_snowflake.py:154` | ✅ Works (referenced) | refs=2 |
| `compute_table_hash` | `FunctionDef` | `src/project_a/legacy/jobs/reconciliation_job.py:17` | ✅ Works (referenced) | refs=3 |
| `reconcile_tables` | `FunctionDef` | `src/project_a/legacy/jobs/reconciliation_job.py:40` | ✅ Works (referenced) | refs=3 |
| `reconcile_snowflake_to_s3` | `FunctionDef` | `src/project_a/legacy/jobs/reconciliation_job.py:104` | ✅ Works (referenced) | refs=3 |
| `reconcile_redshift_to_s3` | `FunctionDef` | `src/project_a/legacy/jobs/reconciliation_job.py:155` | ✅ Works (referenced) | refs=3 |
| `transform_accounts_to_silver` | `FunctionDef` | `src/project_a/legacy/jobs/salesforce_bronze_to_silver.py:24` | ✅ Works (referenced) | refs=2 |
| `transform_leads_to_silver` | `FunctionDef` | `src/project_a/legacy/jobs/salesforce_bronze_to_silver.py:55` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `src/project_a/legacy/jobs/salesforce_bronze_to_silver.py:86` | ✅ Works (referenced) | refs=480 |
| `get_salesforce_credentials` | `FunctionDef` | `src/project_a/legacy/jobs/salesforce_to_bronze.py:26` | ✅ Works (referenced) | refs=2 |
| `get_salesforce_connection_string` | `FunctionDef` | `src/project_a/legacy/jobs/salesforce_to_bronze.py:60` | ✅ Works (referenced) | refs=2 |
| `get_last_checkpoint` | `FunctionDef` | `src/project_a/legacy/jobs/salesforce_to_bronze.py:78` | ✅ Works (referenced) | refs=3 |
| `save_checkpoint` | `FunctionDef` | `src/project_a/legacy/jobs/salesforce_to_bronze.py:103` | ✅ Works (referenced) | refs=3 |
| `extract_salesforce_data` | `FunctionDef` | `src/project_a/legacy/jobs/salesforce_to_bronze.py:126` | ✅ Works (referenced) | refs=3 |
| `get_lead_soql` | `FunctionDef` | `src/project_a/legacy/jobs/salesforce_to_bronze.py:160` | ✅ Works (referenced) | refs=2 |
| `get_account_soql` | `FunctionDef` | `src/project_a/legacy/jobs/salesforce_to_bronze.py:194` | ✅ Works (referenced) | refs=2 |
| `add_metadata_columns` | `FunctionDef` | `src/project_a/legacy/jobs/salesforce_to_bronze.py:231` | ✅ Works (referenced) | refs=13 |
| `write_to_bronze` | `FunctionDef` | `src/project_a/legacy/jobs/salesforce_to_bronze.py:251` | ✅ Works (referenced) | refs=12 |
| `process_salesforce_incremental` | `FunctionDef` | `src/project_a/legacy/jobs/salesforce_to_bronze.py:271` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `src/project_a/legacy/jobs/salesforce_to_bronze.py:325` | ✅ Works (referenced) | refs=480 |
| `merge_orders_data` | `FunctionDef` | `src/project_a/legacy/jobs/snowflake_bronze_to_silver_merge.py:27` | ✅ Works (referenced) | refs=4 |
| `merge_customers_data` | `FunctionDef` | `src/project_a/legacy/jobs/snowflake_bronze_to_silver_merge.py:117` | ✅ Works (referenced) | refs=4 |
| `merge_lineitems_data` | `FunctionDef` | `src/project_a/legacy/jobs/snowflake_bronze_to_silver_merge.py:204` | ✅ Works (referenced) | refs=4 |
| `process_snowflake_merge` | `FunctionDef` | `src/project_a/legacy/jobs/snowflake_bronze_to_silver_merge.py:313` | ✅ Works (referenced) | refs=4 |
| `main` | `FunctionDef` | `src/project_a/legacy/jobs/snowflake_bronze_to_silver_merge.py:334` | ✅ Works (referenced) | refs=480 |
| `get_snowflake_connection_string` | `FunctionDef` | `src/project_a/legacy/jobs/snowflake_to_bronze.py:25` | ✅ Works (referenced) | refs=4 |
| `get_sample_queries` | `FunctionDef` | `src/project_a/legacy/jobs/snowflake_to_bronze.py:45` | ✅ Works (referenced) | refs=4 |
| `extract_snowflake_data` | `FunctionDef` | `src/project_a/legacy/jobs/snowflake_to_bronze.py:104` | ✅ Works (referenced) | refs=4 |
| `add_metadata_columns` | `FunctionDef` | `src/project_a/legacy/jobs/snowflake_to_bronze.py:138` | ✅ Works (referenced) | refs=13 |
| `write_to_bronze` | `FunctionDef` | `src/project_a/legacy/jobs/snowflake_to_bronze.py:158` | ✅ Works (referenced) | refs=12 |
| `process_snowflake_backfill` | `FunctionDef` | `src/project_a/legacy/jobs/snowflake_to_bronze.py:178` | ✅ Works (referenced) | refs=4 |
| `main` | `FunctionDef` | `src/project_a/legacy/jobs/snowflake_to_bronze.py:210` | ✅ Works (referenced) | refs=480 |
| `hash_record` | `FunctionDef` | `src/project_a/legacy/jobs/update_customer_dimension_scd2.py:24` | ✅ Works (referenced) | refs=3 |
| `update_customer_dimension_scd2` | `FunctionDef` | `src/project_a/legacy/jobs/update_customer_dimension_scd2.py:44` | ✅ Works (referenced) | refs=5 |
| `LineageEvent` | `ClassDef` | `src/project_a/legacy/lineage_tracker.py:14` | ✅ Works (referenced) | refs=14 |
| `LineageTracker` | `ClassDef` | `src/project_a/legacy/lineage_tracker.py:27` | ✅ Works (referenced) | refs=10 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/lineage_tracker.py:32` | ✅ Works (referenced) | refs=162 |
| `start_event` | `FunctionDef` | `src/project_a/legacy/lineage_tracker.py:36` | ⚠️ Risky (only definition found) | refs=1 |
| `complete_event` | `FunctionDef` | `src/project_a/legacy/lineage_tracker.py:55` | ⚠️ Risky (only definition found) | refs=1 |
| `get_lineage_summary` | `FunctionDef` | `src/project_a/legacy/lineage_tracker.py:64` | ✅ Works (referenced) | refs=2 |
| `_get_summary_by_type` | `FunctionDef` | `src/project_a/legacy/lineage_tracker.py:72` | ✅ Works (referenced) | refs=2 |
| `export_lineage` | `FunctionDef` | `src/project_a/legacy/lineage_tracker.py:79` | ⚠️ Risky (only definition found) | refs=1 |
| `get_run_id` | `FunctionDef` | `src/project_a/legacy/load/write_idempotent.py:18` | ✅ Works (referenced) | refs=3 |
| `write_bronze_idempotent` | `FunctionDef` | `src/project_a/legacy/load/write_idempotent.py:25` | ⚠️ Risky (only definition found) | refs=1 |
| `write_silver_idempotent` | `FunctionDef` | `src/project_a/legacy/load/write_idempotent.py:104` | ✅ Works (referenced) | refs=2 |
| `write_gold_idempotent` | `FunctionDef` | `src/project_a/legacy/load/write_idempotent.py:171` | ⚠️ Risky (only definition found) | refs=1 |
| `JsonFormatter` | `ClassDef` | `src/project_a/legacy/logging_config.py:8` | ✅ Works (referenced) | refs=4 |
| `format` | `FunctionDef` | `src/project_a/legacy/logging_config.py:9` | ✅ Works (referenced) | refs=982 |
| `configure_logging` | `FunctionDef` | `src/project_a/legacy/logging_config.py:21` | ✅ Works (referenced) | refs=3 |
| `new_run_id` | `FunctionDef` | `src/project_a/legacy/logging_config.py:37` | ✅ Works (referenced) | refs=4 |
| `log_metric` | `FunctionDef` | `src/project_a/legacy/logging_config.py:41` | ✅ Works (referenced) | refs=10 |
| `log_pipeline_event` | `FunctionDef` | `src/project_a/legacy/logging_config.py:52` | ✅ Works (referenced) | refs=10 |
| `JsonFormatter` | `ClassDef` | `src/project_a/legacy/logging_setup.py:10` | ✅ Works (referenced) | refs=4 |
| `format` | `FunctionDef` | `src/project_a/legacy/logging_setup.py:11` | ✅ Works (referenced) | refs=982 |
| `get_logger` | `FunctionDef` | `src/project_a/legacy/logging_setup.py:29` | ✅ Works (referenced) | refs=7 |
| `new_correlation_id` | `FunctionDef` | `src/project_a/legacy/logging_setup.py:39` | ✅ Works (referenced) | refs=3 |
| `ingest_metrics_json` | `FunctionDef` | `src/project_a/legacy/metrics/ingest_pipeline_metrics.py:7` | ⚠️ Risky (only definition found) | refs=1 |
| `start_metrics_server` | `FunctionDef` | `src/project_a/legacy/metrics/metrics/metrics_exporter.py:9` | ⚠️ Risky (only definition found) | refs=1 |
| `MetricsSink` | `ClassDef` | `src/project_a/legacy/metrics/metrics/sink.py:4` | ✅ Works (referenced) | refs=3 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/metrics/metrics/sink.py:5` | ✅ Works (referenced) | refs=162 |
| `incr` | `FunctionDef` | `src/project_a/legacy/metrics/metrics/sink.py:10` | ✅ Works (referenced) | refs=175 |
| `gauge` | `FunctionDef` | `src/project_a/legacy/metrics/metrics/sink.py:13` | ✅ Works (referenced) | refs=18 |
| `timing` | `FunctionDef` | `src/project_a/legacy/metrics/metrics/sink.py:16` | ✅ Works (referenced) | refs=4 |
| `pipeline_metric` | `FunctionDef` | `src/project_a/legacy/metrics/metrics/sink.py:19` | ✅ Works (referenced) | refs=38 |
| `data_quality_metric` | `FunctionDef` | `src/project_a/legacy/metrics/metrics/sink.py:26` | ✅ Works (referenced) | refs=4 |
| `StdoutSink` | `ClassDef` | `src/project_a/legacy/metrics/metrics/sink.py:34` | ✅ Works (referenced) | refs=4 |
| `incr` | `FunctionDef` | `src/project_a/legacy/metrics/metrics/sink.py:35` | ✅ Works (referenced) | refs=175 |
| `gauge` | `FunctionDef` | `src/project_a/legacy/metrics/metrics/sink.py:46` | ✅ Works (referenced) | refs=18 |
| `timing` | `FunctionDef` | `src/project_a/legacy/metrics/metrics/sink.py:57` | ✅ Works (referenced) | refs=4 |
| `pipeline_metric` | `FunctionDef` | `src/project_a/legacy/metrics/metrics/sink.py:68` | ✅ Works (referenced) | refs=38 |
| `data_quality_metric` | `FunctionDef` | `src/project_a/legacy/metrics/metrics/sink.py:80` | ✅ Works (referenced) | refs=4 |
| `create_metrics` | `FunctionDef` | `src/project_a/legacy/metrics/metrics/sink.py:93` | ✅ Works (referenced) | refs=7 |
| `MockMetric` | `ClassDef` | `src/project_a/legacy/metrics_collector.py:31` | ✅ Works (referenced) | refs=12 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/metrics_collector.py:32` | ✅ Works (referenced) | refs=162 |
| `inc` | `FunctionDef` | `src/project_a/legacy/metrics_collector.py:35` | ✅ Works (referenced) | refs=462 |
| `set` | `FunctionDef` | `src/project_a/legacy/metrics_collector.py:38` | ✅ Works (referenced) | refs=1011 |
| `observe` | `FunctionDef` | `src/project_a/legacy/metrics_collector.py:41` | ✅ Works (referenced) | refs=6 |
| `time` | `FunctionDef` | `src/project_a/legacy/metrics_collector.py:44` | ✅ Works (referenced) | refs=2513 |
| `MockContext` | `ClassDef` | `src/project_a/legacy/metrics_collector.py:47` | ✅ Works (referenced) | refs=2 |
| `__enter__` | `FunctionDef` | `src/project_a/legacy/metrics_collector.py:48` | ✅ Works (referenced) | refs=2 |
| `__exit__` | `FunctionDef` | `src/project_a/legacy/metrics_collector.py:51` | ✅ Works (referenced) | refs=2 |
| `MetricsCollector` | `ClassDef` | `src/project_a/legacy/metrics_collector.py:59` | ✅ Works (referenced) | refs=19 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/metrics_collector.py:65` | ✅ Works (referenced) | refs=162 |
| `_init_prometheus_metrics` | `FunctionDef` | `src/project_a/legacy/metrics_collector.py:77` | ✅ Works (referenced) | refs=2 |
| `_init_mock_metrics` | `FunctionDef` | `src/project_a/legacy/metrics_collector.py:144` | ✅ Works (referenced) | refs=2 |
| `collect_pipeline_metrics` | `FunctionDef` | `src/project_a/legacy/metrics_collector.py:157` | ✅ Works (referenced) | refs=5 |
| `_update_prometheus_metrics` | `FunctionDef` | `src/project_a/legacy/metrics_collector.py:234` | ✅ Works (referenced) | refs=2 |
| `get_prometheus_metrics` | `FunctionDef` | `src/project_a/legacy/metrics_collector.py:289` | ✅ Works (referenced) | refs=3 |
| `get_open_telemetry_metrics` | `FunctionDef` | `src/project_a/legacy/metrics_collector.py:304` | ✅ Works (referenced) | refs=3 |
| `get_metrics_summary` | `FunctionDef` | `src/project_a/legacy/metrics_collector.py:375` | ✅ Works (referenced) | refs=4 |
| `export_metrics_to_file` | `FunctionDef` | `src/project_a/legacy/metrics_collector.py:392` | ✅ Works (referenced) | refs=2 |
| `create_metrics_dashboard_data` | `FunctionDef` | `src/project_a/legacy/metrics_collector.py:420` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `src/project_a/legacy/metrics_collector.py:474` | ✅ Works (referenced) | refs=480 |
| `add_surrogate_key` | `FunctionDef` | `src/project_a/legacy/modeling.py:23` | ✅ Works (referenced) | refs=6 |
| `build_dim_date` | `FunctionDef` | `src/project_a/legacy/modeling.py:40` | ✅ Works (referenced) | refs=8 |
| `build_dim_products_scd2_base` | `FunctionDef` | `src/project_a/legacy/modeling.py:70` | ⚠️ Risky (only definition found) | refs=1 |
| `build_dim_category` | `FunctionDef` | `src/project_a/legacy/modeling.py:88` | ⚠️ Risky (only definition found) | refs=1 |
| `build_dim_brand` | `FunctionDef` | `src/project_a/legacy/modeling.py:102` | ⚠️ Risky (only definition found) | refs=1 |
| `normalize_dim_products` | `FunctionDef` | `src/project_a/legacy/modeling.py:116` | ⚠️ Risky (only definition found) | refs=1 |
| `build_dim_geography` | `FunctionDef` | `src/project_a/legacy/modeling.py:131` | ⚠️ Risky (only definition found) | refs=1 |
| `normalize_dim_customers` | `FunctionDef` | `src/project_a/legacy/modeling.py:142` | ⚠️ Risky (only definition found) | refs=1 |
| `fx_json_to_bronze_main` | `FunctionDef` | `src/project_a/legacy/pipeline/pipeline/run_pipeline.py:51` | ✅ Works (referenced) | refs=3 |
| `bronze_to_silver_main` | `FunctionDef` | `src/project_a/legacy/pipeline/pipeline/run_pipeline.py:55` | ✅ Works (referenced) | refs=3 |
| `silver_to_gold_main` | `FunctionDef` | `src/project_a/legacy/pipeline/pipeline/run_pipeline.py:59` | ✅ Works (referenced) | refs=3 |
| `publish_gold_to_snowflake_main` | `FunctionDef` | `src/project_a/legacy/pipeline/pipeline/run_pipeline.py:63` | ✅ Works (referenced) | refs=3 |
| `parse_args` | `FunctionDef` | `src/project_a/legacy/pipeline/pipeline/run_pipeline.py:80` | ✅ Works (referenced) | refs=74 |
| `main` | `FunctionDef` | `src/project_a/legacy/pipeline/pipeline/run_pipeline.py:122` | ✅ Works (referenced) | refs=480 |
| `scd2_merge_customers` | `FunctionDef` | `src/project_a/legacy/pipeline/scd2_customers.py:13` | ⚠️ Risky (only definition found) | refs=1 |
| `write_delta` | `FunctionDef` | `src/project_a/legacy/pipeline_core.py:10` | ✅ Works (referenced) | refs=43 |
| `run_pipeline` | `FunctionDef` | `src/project_a/legacy/pipeline_core.py:14` | ✅ Works (referenced) | refs=25 |
| `main` | `FunctionDef` | `src/project_a/legacy/pipeline_core.py:34` | ✅ Works (referenced) | refs=480 |
| `SchemaValidator` | `ClassDef` | `src/project_a/legacy/schema/validator.py:14` | ✅ Works (referenced) | refs=36 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/schema/validator.py:17` | ✅ Works (referenced) | refs=162 |
| `validate_bronze_schema` | `FunctionDef` | `src/project_a/legacy/schema/validator.py:20` | ✅ Works (referenced) | refs=4 |
| `validate_silver_schema` | `FunctionDef` | `src/project_a/legacy/schema/validator.py:35` | ✅ Works (referenced) | refs=5 |
| `validate_gold_schema` | `FunctionDef` | `src/project_a/legacy/schema/validator.py:77` | ✅ Works (referenced) | refs=6 |
| `_is_schema_compatible` | `FunctionDef` | `src/project_a/legacy/schema/validator.py:114` | ✅ Works (referenced) | refs=2 |
| `_schemas_match_exactly` | `FunctionDef` | `src/project_a/legacy/schema/validator.py:150` | ✅ Works (referenced) | refs=2 |
| `_log_schema_differences` | `FunctionDef` | `src/project_a/legacy/schema/validator.py:154` | ✅ Works (referenced) | refs=2 |
| `get_spark` | `FunctionDef` | `src/project_a/legacy/streaming/kafka_customer_events.py:18` | ✅ Works (referenced) | refs=61 |
| `main` | `FunctionDef` | `src/project_a/legacy/streaming/kafka_customer_events.py:34` | ✅ Works (referenced) | refs=480 |
| `write_stream` | `FunctionDef` | `src/project_a/legacy/streaming_core.py:11` | ⚠️ Risky (only definition found) | refs=1 |
| `create_dlq_handler` | `FunctionDef` | `src/project_a/legacy/streaming_core.py:78` | ⚠️ Risky (only definition found) | refs=1 |
| `handle_dlq` | `FunctionDef` | `src/project_a/legacy/streaming_core.py:93` | ✅ Works (referenced) | refs=2 |
| `validate_streaming_config` | `FunctionDef` | `src/project_a/legacy/streaming_core.py:120` | ⚠️ Risky (only definition found) | refs=1 |
| `bronze_to_silver_multi_source` | `FunctionDef` | `src/project_a/legacy/transform/bronze_to_silver_multi_source.py:38` | ✅ Works (referenced) | refs=2 |
| `join_silver_sources_on_customer_id` | `FunctionDef` | `src/project_a/legacy/transform/bronze_to_silver_multi_source.py:198` | ⚠️ Risky (only definition found) | refs=1 |
| `enrich_with_fx` | `FunctionDef` | `src/project_a/legacy/transform/enrich_with_fx.py:11` | ✅ Works (referenced) | refs=3 |
| `deduplicate_by_latest` | `FunctionDef` | `src/project_a/legacy/transform/incremental_customer_dim_upsert.py:39` | ✅ Works (referenced) | refs=2 |
| `incremental_customer_dim_upsert` | `FunctionDef` | `src/project_a/legacy/transform/incremental_customer_dim_upsert.py:60` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `src/project_a/legacy/transform/incremental_customer_dim_upsert.py:196` | ✅ Works (referenced) | refs=480 |
| `_mask_email` | `FunctionDef` | `src/project_a/legacy/transform/pii_masking.py:15` | ✅ Works (referenced) | refs=5 |
| `_mask_phone` | `FunctionDef` | `src/project_a/legacy/transform/pii_masking.py:26` | ✅ Works (referenced) | refs=5 |
| `_mask_ssn` | `FunctionDef` | `src/project_a/legacy/transform/pii_masking.py:33` | ✅ Works (referenced) | refs=4 |
| `_mask_name` | `FunctionDef` | `src/project_a/legacy/transform/pii_masking.py:40` | ✅ Works (referenced) | refs=3 |
| `_mask_ip` | `FunctionDef` | `src/project_a/legacy/transform/pii_masking.py:49` | ✅ Works (referenced) | refs=2 |
| `apply_pii_masking` | `FunctionDef` | `src/project_a/legacy/transform/pii_masking.py:67` | ✅ Works (referenced) | refs=6 |
| `write_to_dlq` | `FunctionDef` | `src/project_a/legacy/utils/dlq_handler.py:17` | ⚠️ Risky (only definition found) | refs=1 |
| `quarantine_data` | `FunctionDef` | `src/project_a/legacy/utils/dlq_handler.py:89` | ⚠️ Risky (only definition found) | refs=1 |
| `FreshnessGuard` | `ClassDef` | `src/project_a/legacy/utils/freshness_guards.py:15` | ✅ Works (referenced) | refs=9 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/utils/freshness_guards.py:18` | ✅ Works (referenced) | refs=162 |
| `check_freshness` | `FunctionDef` | `src/project_a/legacy/utils/freshness_guards.py:21` | ✅ Works (referenced) | refs=7 |
| `HubSpotFreshnessGuard` | `ClassDef` | `src/project_a/legacy/utils/freshness_guards.py:56` | ⚠️ Risky (only definition found) | refs=1 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/utils/freshness_guards.py:59` | ✅ Works (referenced) | refs=162 |
| `check_freshness` | `FunctionDef` | `src/project_a/legacy/utils/freshness_guards.py:63` | ✅ Works (referenced) | refs=7 |
| `SnowflakeFreshnessGuard` | `ClassDef` | `src/project_a/legacy/utils/freshness_guards.py:89` | ⚠️ Risky (only definition found) | refs=1 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/utils/freshness_guards.py:92` | ✅ Works (referenced) | refs=162 |
| `check_freshness` | `FunctionDef` | `src/project_a/legacy/utils/freshness_guards.py:95` | ✅ Works (referenced) | refs=7 |
| `KafkaFreshnessGuard` | `ClassDef` | `src/project_a/legacy/utils/freshness_guards.py:101` | ⚠️ Risky (only definition found) | refs=1 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/utils/freshness_guards.py:104` | ✅ Works (referenced) | refs=162 |
| `check_freshness` | `FunctionDef` | `src/project_a/legacy/utils/freshness_guards.py:108` | ✅ Works (referenced) | refs=7 |
| `FXFreshnessGuard` | `ClassDef` | `src/project_a/legacy/utils/freshness_guards.py:130` | ⚠️ Risky (only definition found) | refs=1 |
| `__init__` | `FunctionDef` | `src/project_a/legacy/utils/freshness_guards.py:133` | ✅ Works (referenced) | refs=162 |
| `check_freshness` | `FunctionDef` | `src/project_a/legacy/utils/freshness_guards.py:136` | ✅ Works (referenced) | refs=7 |
| `apply_freshness_window` | `FunctionDef` | `src/project_a/legacy/utils/freshness_guards.py:142` | ⚠️ Risky (only definition found) | refs=1 |
| `LineageEvent` | `ClassDef` | `src/project_a/lineage/tracking.py:18` | ✅ Works (referenced) | refs=14 |
| `LineageTracker` | `ClassDef` | `src/project_a/lineage/tracking.py:34` | ✅ Works (referenced) | refs=10 |
| `__init__` | `FunctionDef` | `src/project_a/lineage/tracking.py:37` | ✅ Works (referenced) | refs=162 |
| `track_transformation` | `FunctionDef` | `src/project_a/lineage/tracking.py:42` | ✅ Works (referenced) | refs=3 |
| `_save_lineage_event` | `FunctionDef` | `src/project_a/lineage/tracking.py:79` | ✅ Works (referenced) | refs=2 |
| `_update_dataset_lineage` | `FunctionDef` | `src/project_a/lineage/tracking.py:87` | ✅ Works (referenced) | refs=2 |
| `_load_dataset_lineage` | `FunctionDef` | `src/project_a/lineage/tracking.py:112` | ✅ Works (referenced) | refs=3 |
| `get_lineage_graph` | `FunctionDef` | `src/project_a/lineage/tracking.py:119` | ⚠️ Risky (only definition found) | refs=1 |
| `LineageDecorator` | `ClassDef` | `src/project_a/lineage/tracking.py:144` | ✅ Works (referenced) | refs=2 |
| `__init__` | `FunctionDef` | `src/project_a/lineage/tracking.py:147` | ✅ Works (referenced) | refs=162 |
| `__call__` | `FunctionDef` | `src/project_a/lineage/tracking.py:150` | ⚠️ Risky (only definition found) | refs=1 |
| `decorator` | `FunctionDef` | `src/project_a/lineage/tracking.py:151` | ✅ Works (referenced) | refs=21 |
| `wrapper` | `FunctionDef` | `src/project_a/lineage/tracking.py:152` | ✅ Works (referenced) | refs=23 |
| `_extract_record_count` | `FunctionDef` | `src/project_a/lineage/tracking.py:195` | ✅ Works (referenced) | refs=2 |
| `get_lineage_tracker` | `FunctionDef` | `src/project_a/lineage/tracking.py:214` | ✅ Works (referenced) | refs=5 |
| `track_lineage` | `FunctionDef` | `src/project_a/lineage/tracking.py:226` | ✅ Works (referenced) | refs=3 |
| `DatasetMetadata` | `ClassDef` | `src/project_a/metadata/catalog.py:19` | ✅ Works (referenced) | refs=11 |
| `MetadataCatalog` | `ClassDef` | `src/project_a/metadata/catalog.py:38` | ✅ Works (referenced) | refs=6 |
| `__init__` | `FunctionDef` | `src/project_a/metadata/catalog.py:41` | ✅ Works (referenced) | refs=162 |
| `register_dataset` | `FunctionDef` | `src/project_a/metadata/catalog.py:48` | ✅ Works (referenced) | refs=5 |
| `_save_metadata` | `FunctionDef` | `src/project_a/metadata/catalog.py:90` | ✅ Works (referenced) | refs=3 |
| `get_dataset` | `FunctionDef` | `src/project_a/metadata/catalog.py:98` | ✅ Works (referenced) | refs=23 |
| `update_dataset_stats` | `FunctionDef` | `src/project_a/metadata/catalog.py:112` | ⚠️ Risky (only definition found) | refs=1 |
| `search_datasets` | `FunctionDef` | `src/project_a/metadata/catalog.py:128` | ✅ Works (referenced) | refs=5 |
| `get_schema_compatibility` | `FunctionDef` | `src/project_a/metadata/catalog.py:156` | ⚠️ Risky (only definition found) | refs=1 |
| `MetadataExtractor` | `ClassDef` | `src/project_a/metadata/catalog.py:193` | ✅ Works (referenced) | refs=3 |
| `extract_from_dataframe` | `FunctionDef` | `src/project_a/metadata/catalog.py:197` | ⚠️ Risky (only definition found) | refs=1 |
| `extract_from_csv` | `FunctionDef` | `src/project_a/metadata/catalog.py:215` | ⚠️ Risky (only definition found) | refs=1 |
| `get_catalog` | `FunctionDef` | `src/project_a/metadata/catalog.py:228` | ✅ Works (referenced) | refs=6 |
| `register_dataset` | `FunctionDef` | `src/project_a/metadata/catalog.py:240` | ✅ Works (referenced) | refs=5 |
| `get_dataset_metadata` | `FunctionDef` | `src/project_a/metadata/catalog.py:268` | ✅ Works (referenced) | refs=3 |
| `search_datasets` | `FunctionDef` | `src/project_a/metadata/catalog.py:274` | ✅ Works (referenced) | refs=5 |
| `LineageEmitter` | `ClassDef` | `src/project_a/monitoring/lineage_emitter.py:19` | ✅ Works (referenced) | refs=8 |
| `__init__` | `FunctionDef` | `src/project_a/monitoring/lineage_emitter.py:24` | ✅ Works (referenced) | refs=162 |
| `_headers` | `FunctionDef` | `src/project_a/monitoring/lineage_emitter.py:47` | ✅ Works (referenced) | refs=2 |
| `_get_api_path` | `FunctionDef` | `src/project_a/monitoring/lineage_emitter.py:54` | ✅ Works (referenced) | refs=4 |
| `emit_job` | `FunctionDef` | `src/project_a/monitoring/lineage_emitter.py:61` | ✅ Works (referenced) | refs=19 |
| `load_lineage_config` | `FunctionDef` | `src/project_a/monitoring/lineage_emitter.py:178` | ✅ Works (referenced) | refs=7 |
| `MetricType` | `ClassDef` | `src/project_a/monitoring/metrics.py:22` | ✅ Works (referenced) | refs=7 |
| `Metric` | `ClassDef` | `src/project_a/monitoring/metrics.py:30` | ✅ Works (referenced) | refs=153 |
| `MetricsCollector` | `ClassDef` | `src/project_a/monitoring/metrics.py:40` | ✅ Works (referenced) | refs=19 |
| `__init__` | `FunctionDef` | `src/project_a/monitoring/metrics.py:43` | ✅ Works (referenced) | refs=162 |
| `record_metric` | `FunctionDef` | `src/project_a/monitoring/metrics.py:50` | ✅ Works (referenced) | refs=4 |
| `increment_counter` | `FunctionDef` | `src/project_a/monitoring/metrics.py:81` | ✅ Works (referenced) | refs=3 |
| `set_gauge` | `FunctionDef` | `src/project_a/monitoring/metrics.py:85` | ✅ Works (referenced) | refs=8 |
| `observe_histogram` | `FunctionDef` | `src/project_a/monitoring/metrics.py:89` | ✅ Works (referenced) | refs=3 |
| `collect_system_metrics` | `FunctionDef` | `src/project_a/monitoring/metrics.py:93` | ✅ Works (referenced) | refs=5 |
| `PipelineMonitor` | `ClassDef` | `src/project_a/monitoring/metrics.py:101` | ✅ Works (referenced) | refs=11 |
| `__init__` | `FunctionDef` | `src/project_a/monitoring/metrics.py:104` | ✅ Works (referenced) | refs=162 |
| `monitor_execution` | `FunctionDef` | `src/project_a/monitoring/metrics.py:108` | ✅ Works (referenced) | refs=2 |
| `AlertManager` | `ClassDef` | `src/project_a/monitoring/metrics.py:142` | ✅ Works (referenced) | refs=5 |
| `__init__` | `FunctionDef` | `src/project_a/monitoring/metrics.py:145` | ✅ Works (referenced) | refs=162 |
| `add_alert_rule` | `FunctionDef` | `src/project_a/monitoring/metrics.py:151` | ✅ Works (referenced) | refs=3 |
| `evaluate_alerts` | `FunctionDef` | `src/project_a/monitoring/metrics.py:169` | ✅ Works (referenced) | refs=3 |
| `_read_recent_metrics` | `FunctionDef` | `src/project_a/monitoring/metrics.py:208` | ✅ Works (referenced) | refs=2 |
| `_matches_labels` | `FunctionDef` | `src/project_a/monitoring/metrics.py:225` | ✅ Works (referenced) | refs=2 |
| `_evaluate_condition` | `FunctionDef` | `src/project_a/monitoring/metrics.py:232` | ✅ Works (referenced) | refs=2 |
| `_save_alerts` | `FunctionDef` | `src/project_a/monitoring/metrics.py:248` | ✅ Works (referenced) | refs=2 |
| `get_metrics_collector` | `FunctionDef` | `src/project_a/monitoring/metrics.py:261` | ✅ Works (referenced) | refs=8 |
| `get_pipeline_monitor` | `FunctionDef` | `src/project_a/monitoring/metrics.py:273` | ✅ Works (referenced) | refs=4 |
| `get_alert_manager` | `FunctionDef` | `src/project_a/monitoring/metrics.py:282` | ✅ Works (referenced) | refs=3 |
| `monitor_pipeline` | `FunctionDef` | `src/project_a/monitoring/metrics.py:294` | ✅ Works (referenced) | refs=4 |
| `add_alert_rule` | `FunctionDef` | `src/project_a/monitoring/metrics.py:300` | ✅ Works (referenced) | refs=3 |
| `evaluate_alerts` | `FunctionDef` | `src/project_a/monitoring/metrics.py:308` | ✅ Works (referenced) | refs=3 |
| `PerformanceMetric` | `ClassDef` | `src/project_a/performance/optimization.py:21` | ✅ Works (referenced) | refs=19 |
| `PerformanceResult` | `ClassDef` | `src/project_a/performance/optimization.py:31` | ✅ Works (referenced) | refs=4 |
| `OptimizationRecommendation` | `ClassDef` | `src/project_a/performance/optimization.py:41` | ✅ Works (referenced) | refs=5 |
| `PerformanceMonitor` | `ClassDef` | `src/project_a/performance/optimization.py:52` | ✅ Works (referenced) | refs=7 |
| `__init__` | `FunctionDef` | `src/project_a/performance/optimization.py:55` | ✅ Works (referenced) | refs=162 |
| `start_monitoring` | `FunctionDef` | `src/project_a/performance/optimization.py:65` | ⚠️ Risky (only definition found) | refs=1 |
| `stop_monitoring` | `FunctionDef` | `src/project_a/performance/optimization.py:73` | ⚠️ Risky (only definition found) | refs=1 |
| `_collect_system_metrics` | `FunctionDef` | `src/project_a/performance/optimization.py:80` | ✅ Works (referenced) | refs=2 |
| `measure_execution_time` | `FunctionDef` | `src/project_a/performance/optimization.py:107` | ✅ Works (referenced) | refs=3 |
| `benchmark_data_processing` | `FunctionDef` | `src/project_a/performance/optimization.py:134` | ✅ Works (referenced) | refs=3 |
| `_log_metric` | `FunctionDef` | `src/project_a/performance/optimization.py:176` | ✅ Works (referenced) | refs=6 |
| `get_performance_summary` | `FunctionDef` | `src/project_a/performance/optimization.py:195` | ✅ Works (referenced) | refs=6 |
| `SparkOptimizer` | `ClassDef` | `src/project_a/performance/optimization.py:257` | ✅ Works (referenced) | refs=6 |
| `__init__` | `FunctionDef` | `src/project_a/performance/optimization.py:260` | ✅ Works (referenced) | refs=162 |
| `optimize_dataframe_operations` | `FunctionDef` | `src/project_a/performance/optimization.py:270` | ⚠️ Risky (only definition found) | refs=1 |
| `suggest_partitioning_strategy` | `FunctionDef` | `src/project_a/performance/optimization.py:282` | ✅ Works (referenced) | refs=4 |
| `optimize_join_operations` | `FunctionDef` | `src/project_a/performance/optimization.py:317` | ✅ Works (referenced) | refs=3 |
| `PerformanceAnalyzer` | `ClassDef` | `src/project_a/performance/optimization.py:348` | ✅ Works (referenced) | refs=3 |
| `__init__` | `FunctionDef` | `src/project_a/performance/optimization.py:351` | ✅ Works (referenced) | refs=162 |
| `analyze_dataframe_performance` | `FunctionDef` | `src/project_a/performance/optimization.py:356` | ✅ Works (referenced) | refs=3 |
| `generate_performance_report` | `FunctionDef` | `src/project_a/performance/optimization.py:381` | ✅ Works (referenced) | refs=3 |
| `get_performance_monitor` | `FunctionDef` | `src/project_a/performance/optimization.py:413` | ✅ Works (referenced) | refs=7 |
| `get_spark_optimizer` | `FunctionDef` | `src/project_a/performance/optimization.py:424` | ✅ Works (referenced) | refs=6 |
| `get_performance_analyzer` | `FunctionDef` | `src/project_a/performance/optimization.py:432` | ✅ Works (referenced) | refs=3 |
| `measure_execution_time` | `FunctionDef` | `src/project_a/performance/optimization.py:442` | ✅ Works (referenced) | refs=3 |
| `benchmark_data_processing` | `FunctionDef` | `src/project_a/performance/optimization.py:448` | ✅ Works (referenced) | refs=3 |
| `analyze_dataframe_performance` | `FunctionDef` | `src/project_a/performance/optimization.py:454` | ✅ Works (referenced) | refs=3 |
| `generate_performance_report` | `FunctionDef` | `src/project_a/performance/optimization.py:460` | ✅ Works (referenced) | refs=3 |
| `suggest_partitioning_strategy` | `FunctionDef` | `src/project_a/performance/optimization.py:466` | ✅ Works (referenced) | refs=4 |
| `optimize_join_operations` | `FunctionDef` | `src/project_a/performance/optimization.py:472` | ✅ Works (referenced) | refs=3 |
| `__getattr__` | `FunctionDef` | `src/project_a/pipeline/__init__.py:17` | ✅ Works (referenced) | refs=2 |
| `DummyModule` | `ClassDef` | `src/project_a/pipeline/run_pipeline.py:53` | ✅ Works (referenced) | refs=12 |
| `main` | `FunctionDef` | `src/project_a/pipeline/run_pipeline.py:54` | ✅ Works (referenced) | refs=480 |
| `parse_args` | `FunctionDef` | `src/project_a/pipeline/run_pipeline.py:90` | ✅ Works (referenced) | refs=74 |
| `main` | `FunctionDef` | `src/project_a/pipeline/run_pipeline.py:137` | ✅ Works (referenced) | refs=480 |
| `PrivacyRegulation` | `ClassDef` | `src/project_a/privacy/compliance.py:23` | ✅ Works (referenced) | refs=7 |
| `DataSensitivity` | `ClassDef` | `src/project_a/privacy/compliance.py:30` | ✅ Works (referenced) | refs=27 |
| `PrivacyImpactAssessment` | `ClassDef` | `src/project_a/privacy/compliance.py:40` | ✅ Works (referenced) | refs=4 |
| `DataSubjectRequest` | `ClassDef` | `src/project_a/privacy/compliance.py:56` | ✅ Works (referenced) | refs=6 |
| `PIIDetector` | `ClassDef` | `src/project_a/privacy/compliance.py:68` | ✅ Works (referenced) | refs=5 |
| `__init__` | `FunctionDef` | `src/project_a/privacy/compliance.py:71` | ✅ Works (referenced) | refs=162 |
| `detect_pii_in_dataframe` | `FunctionDef` | `src/project_a/privacy/compliance.py:85` | ✅ Works (referenced) | refs=2 |
| `classify_sensitivity` | `FunctionDef` | `src/project_a/privacy/compliance.py:103` | ✅ Works (referenced) | refs=2 |
| `DataAnonymizer` | `ClassDef` | `src/project_a/privacy/compliance.py:146` | ✅ Works (referenced) | refs=2 |
| `__init__` | `FunctionDef` | `src/project_a/privacy/compliance.py:149` | ✅ Works (referenced) | refs=162 |
| `anonymize_dataframe` | `FunctionDef` | `src/project_a/privacy/compliance.py:152` | ✅ Works (referenced) | refs=2 |
| `_anonymize_column` | `FunctionDef` | `src/project_a/privacy/compliance.py:174` | ✅ Works (referenced) | refs=2 |
| `_hash_value` | `FunctionDef` | `src/project_a/privacy/compliance.py:187` | ✅ Works (referenced) | refs=3 |
| `_mask_value` | `FunctionDef` | `src/project_a/privacy/compliance.py:191` | ✅ Works (referenced) | refs=2 |
| `_truncate_value` | `FunctionDef` | `src/project_a/privacy/compliance.py:197` | ✅ Works (referenced) | refs=2 |
| `PrivacyManager` | `ClassDef` | `src/project_a/privacy/compliance.py:204` | ✅ Works (referenced) | refs=6 |
| `__init__` | `FunctionDef` | `src/project_a/privacy/compliance.py:207` | ✅ Works (referenced) | refs=162 |
| `conduct_privacy_impact_assessment` | `FunctionDef` | `src/project_a/privacy/compliance.py:219` | ✅ Works (referenced) | refs=3 |
| `create_data_subject_request` | `FunctionDef` | `src/project_a/privacy/compliance.py:273` | ✅ Works (referenced) | refs=3 |
| `process_data_subject_request` | `FunctionDef` | `src/project_a/privacy/compliance.py:312` | ✅ Works (referenced) | refs=3 |
| `detect_and_classify_pii` | `FunctionDef` | `src/project_a/privacy/compliance.py:334` | ✅ Works (referenced) | refs=5 |
| `anonymize_data` | `FunctionDef` | `src/project_a/privacy/compliance.py:343` | ✅ Works (referenced) | refs=5 |
| `generate_privacy_report` | `FunctionDef` | `src/project_a/privacy/compliance.py:349` | ✅ Works (referenced) | refs=3 |
| `ComplianceChecker` | `ClassDef` | `src/project_a/privacy/compliance.py:366` | ✅ Works (referenced) | refs=3 |
| `__init__` | `FunctionDef` | `src/project_a/privacy/compliance.py:369` | ✅ Works (referenced) | refs=162 |
| `check_gdpr_compliance` | `FunctionDef` | `src/project_a/privacy/compliance.py:373` | ✅ Works (referenced) | refs=4 |
| `check_ccpa_compliance` | `FunctionDef` | `src/project_a/privacy/compliance.py:398` | ✅ Works (referenced) | refs=4 |
| `run_compliance_audit` | `FunctionDef` | `src/project_a/privacy/compliance.py:427` | ✅ Works (referenced) | refs=3 |
| `get_privacy_manager` | `FunctionDef` | `src/project_a/privacy/compliance.py:463` | ✅ Works (referenced) | refs=10 |
| `get_compliance_checker` | `FunctionDef` | `src/project_a/privacy/compliance.py:475` | ✅ Works (referenced) | refs=4 |
| `detect_and_classify_pii` | `FunctionDef` | `src/project_a/privacy/compliance.py:484` | ✅ Works (referenced) | refs=5 |
| `anonymize_data` | `FunctionDef` | `src/project_a/privacy/compliance.py:490` | ✅ Works (referenced) | refs=5 |
| `conduct_privacy_impact_assessment` | `FunctionDef` | `src/project_a/privacy/compliance.py:498` | ✅ Works (referenced) | refs=3 |
| `create_data_subject_request` | `FunctionDef` | `src/project_a/privacy/compliance.py:512` | ✅ Works (referenced) | refs=3 |
| `process_data_subject_request` | `FunctionDef` | `src/project_a/privacy/compliance.py:520` | ✅ Works (referenced) | refs=3 |
| `check_gdpr_compliance` | `FunctionDef` | `src/project_a/privacy/compliance.py:526` | ✅ Works (referenced) | refs=4 |
| `check_ccpa_compliance` | `FunctionDef` | `src/project_a/privacy/compliance.py:532` | ✅ Works (referenced) | refs=4 |
| `run_compliance_audit` | `FunctionDef` | `src/project_a/privacy/compliance.py:538` | ✅ Works (referenced) | refs=3 |
| `generate_privacy_report` | `FunctionDef` | `src/project_a/privacy/compliance.py:546` | ✅ Works (referenced) | refs=3 |
| `DQGate` | `ClassDef` | `src/project_a/pyspark_interview_project/dq/gate.py:18` | ✅ Works (referenced) | refs=4 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/gate.py:29` | ✅ Works (referenced) | refs=162 |
| `check_and_block` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/gate.py:39` | ✅ Works (referenced) | refs=3 |
| `enforce_dq_gate` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/gate.py:103` | ⚠️ Risky (only definition found) | refs=1 |
| `GERunner` | `ClassDef` | `src/project_a/pyspark_interview_project/dq/ge_runner.py:27` | ✅ Works (referenced) | refs=4 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/ge_runner.py:30` | ✅ Works (referenced) | refs=162 |
| `_load_dq_config` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/ge_runner.py:47` | ✅ Works (referenced) | refs=2 |
| `run_suite` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/ge_runner.py:62` | ✅ Works (referenced) | refs=14 |
| `_load_suite_config` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/ge_runner.py:147` | ✅ Works (referenced) | refs=2 |
| `_run_expectation` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/ge_runner.py:152` | ✅ Works (referenced) | refs=2 |
| `_write_results` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/ge_runner.py:176` | ✅ Works (referenced) | refs=2 |
| `_emit_metrics` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/ge_runner.py:200` | ✅ Works (referenced) | refs=8 |
| `GreatExpectationsRunner` | `ClassDef` | `src/project_a/pyspark_interview_project/dq/great_expectations_runner.py:14` | ✅ Works (referenced) | refs=14 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/great_expectations_runner.py:25` | ✅ Works (referenced) | refs=162 |
| `init_context` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/great_expectations_runner.py:35` | ✅ Works (referenced) | refs=11 |
| `run_checkpoint` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/great_expectations_runner.py:61` | ✅ Works (referenced) | refs=18 |
| `_build_summary` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/great_expectations_runner.py:146` | ✅ Works (referenced) | refs=2 |
| `_get_data_docs_url` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/great_expectations_runner.py:207` | ✅ Works (referenced) | refs=2 |
| `validate_dataframe` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/great_expectations_runner.py:231` | ✅ Works (referenced) | refs=7 |
| `run_dq_checkpoint` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/great_expectations_runner.py:305` | ✅ Works (referenced) | refs=4 |
| `DataQualityRule` | `ClassDef` | `src/project_a/pyspark_interview_project/dq/rules.py:14` | ✅ Works (referenced) | refs=7 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/rules.py:17` | ✅ Works (referenced) | refs=162 |
| `check` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/rules.py:21` | ✅ Works (referenced) | refs=1934 |
| `NotNullRule` | `ClassDef` | `src/project_a/pyspark_interview_project/dq/rules.py:26` | ✅ Works (referenced) | refs=4 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/rules.py:29` | ✅ Works (referenced) | refs=162 |
| `check` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/rules.py:33` | ✅ Works (referenced) | refs=1934 |
| `UniqueRule` | `ClassDef` | `src/project_a/pyspark_interview_project/dq/rules.py:50` | ✅ Works (referenced) | refs=4 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/rules.py:53` | ✅ Works (referenced) | refs=162 |
| `check` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/rules.py:57` | ✅ Works (referenced) | refs=1934 |
| `ExpressionRule` | `ClassDef` | `src/project_a/pyspark_interview_project/dq/rules.py:76` | ✅ Works (referenced) | refs=4 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/rules.py:79` | ✅ Works (referenced) | refs=162 |
| `check` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/rules.py:83` | ✅ Works (referenced) | refs=1934 |
| `create_rule_from_config` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/rules.py:118` | ✅ Works (referenced) | refs=3 |
| `DQResult` | `ClassDef` | `src/project_a/pyspark_interview_project/dq/runner.py:18` | ✅ Works (referenced) | refs=9 |
| `__post_init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/runner.py:25` | ✅ Works (referenced) | refs=3 |
| `run_suite` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/runner.py:32` | ✅ Works (referenced) | refs=14 |
| `run_yaml_policy` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/runner.py:63` | ✅ Works (referenced) | refs=6 |
| `print_dq_summary` | `FunctionDef` | `src/project_a/pyspark_interview_project/dq/runner.py:143` | ✅ Works (referenced) | refs=3 |
| `extract_fx_rates` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/__init__.py:30` | ✅ Works (referenced) | refs=3 |
| `BaseExtractor` | `ClassDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:27` | ✅ Works (referenced) | refs=12 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:30` | ✅ Works (referenced) | refs=162 |
| `_read_local_csv` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:45` | ✅ Works (referenced) | refs=7 |
| `_add_metadata` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:50` | ✅ Works (referenced) | refs=4 |
| `_get_watermark` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:60` | ✅ Works (referenced) | refs=6 |
| `_set_watermark` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:67` | ✅ Works (referenced) | refs=4 |
| `extract` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:72` | ✅ Works (referenced) | refs=266 |
| `extract_with_metrics` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:85` | ✅ Works (referenced) | refs=2 |
| `SalesforceExtractor` | `ClassDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:130` | ✅ Works (referenced) | refs=5 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:133` | ✅ Works (referenced) | refs=162 |
| `extract` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:137` | ✅ Works (referenced) | refs=266 |
| `_create_empty_dataframe` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:146` | ✅ Works (referenced) | refs=4 |
| `CRMExtractor` | `ClassDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:155` | ✅ Works (referenced) | refs=5 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:158` | ✅ Works (referenced) | refs=162 |
| `extract` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:162` | ✅ Works (referenced) | refs=266 |
| `_create_empty_dataframe` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:170` | ✅ Works (referenced) | refs=4 |
| `HubSpotExtractor` | `ClassDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:178` | ✅ Works (referenced) | refs=5 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:181` | ✅ Works (referenced) | refs=162 |
| `extract` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:185` | ✅ Works (referenced) | refs=266 |
| `_extract_from_api` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:212` | ✅ Works (referenced) | refs=2 |
| `SnowflakeExtractor` | `ClassDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:220` | ✅ Works (referenced) | refs=5 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:223` | ✅ Works (referenced) | refs=162 |
| `extract` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:226` | ✅ Works (referenced) | refs=266 |
| `_build_query` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:262` | ✅ Works (referenced) | refs=4 |
| `RedshiftExtractor` | `ClassDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:270` | ✅ Works (referenced) | refs=5 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:273` | ✅ Works (referenced) | refs=162 |
| `extract` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:276` | ✅ Works (referenced) | refs=266 |
| `_build_query` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:306` | ✅ Works (referenced) | refs=4 |
| `FXRatesExtractor` | `ClassDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:313` | ✅ Works (referenced) | refs=6 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:316` | ✅ Works (referenced) | refs=162 |
| `extract` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:321` | ✅ Works (referenced) | refs=266 |
| `_extract_from_rest_api` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:343` | ✅ Works (referenced) | refs=2 |
| `get_extractor` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:383` | ✅ Works (referenced) | refs=4 |
| `extract_incremental` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/base_extractor.py:402` | ✅ Works (referenced) | refs=3 |
| `read_fx_json` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/fx_json_reader.py:18` | ✅ Works (referenced) | refs=8 |
| `read_fx_rates_from_bronze` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/fx_json_reader.py:108` | ✅ Works (referenced) | refs=9 |
| `get_kafka_stream` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/kafka_orders_stream.py:25` | ✅ Works (referenced) | refs=4 |
| `parse_kafka_messages` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/kafka_orders_stream.py:68` | ✅ Works (referenced) | refs=2 |
| `write_kafka_stream_to_bronze` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/kafka_orders_stream.py:91` | ✅ Works (referenced) | refs=2 |
| `stream_orders_from_kafka` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/kafka_orders_stream.py:133` | ✅ Works (referenced) | refs=2 |
| `save_kafka_offsets` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/kafka_orders_stream.py:180` | ⚠️ Risky (only definition found) | refs=1 |
| `extract_redshift_behavior` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/redshift_behavior.py:28` | ✅ Works (referenced) | refs=14 |
| `extract_snowflake_orders` | `FunctionDef` | `src/project_a/pyspark_interview_project/extract/snowflake_orders.py:29` | ✅ Works (referenced) | refs=11 |
| `get_write_format` | `FunctionDef` | `src/project_a/pyspark_interview_project/io/delta_writer.py:17` | ✅ Works (referenced) | refs=4 |
| `delete_if_exists_local` | `FunctionDef` | `src/project_a/pyspark_interview_project/io/delta_writer.py:33` | ✅ Works (referenced) | refs=3 |
| `write_table` | `FunctionDef` | `src/project_a/pyspark_interview_project/io/delta_writer.py:45` | ✅ Works (referenced) | refs=20 |
| `optimize_table` | `FunctionDef` | `src/project_a/pyspark_interview_project/io/delta_writer.py:113` | ✅ Works (referenced) | refs=18 |
| `vacuum_table` | `FunctionDef` | `src/project_a/pyspark_interview_project/io/delta_writer.py:161` | ✅ Works (referenced) | refs=6 |
| `resolve` | `FunctionDef` | `src/project_a/pyspark_interview_project/io/path_resolver.py:4` | ✅ Works (referenced) | refs=340 |
| `publish_to_snowflake` | `FunctionDef` | `src/project_a/pyspark_interview_project/io/publish.py:18` | ✅ Works (referenced) | refs=3 |
| `publish_to_redshift` | `FunctionDef` | `src/project_a/pyspark_interview_project/io/publish.py:113` | ⚠️ Risky (only definition found) | refs=1 |
| `get_snowflake_credentials` | `FunctionDef` | `src/project_a/pyspark_interview_project/io/snowflake_writer.py:17` | ✅ Works (referenced) | refs=21 |
| `write_df_to_snowflake` | `FunctionDef` | `src/project_a/pyspark_interview_project/io/snowflake_writer.py:24` | ✅ Works (referenced) | refs=5 |
| `_write_with_merge` | `FunctionDef` | `src/project_a/pyspark_interview_project/io/snowflake_writer.py:92` | ✅ Works (referenced) | refs=2 |
| `_build_snowflake_merge_sql` | `FunctionDef` | `src/project_a/pyspark_interview_project/io/snowflake_writer.py:132` | ✅ Works (referenced) | refs=2 |
| `write_table` | `FunctionDef` | `src/project_a/pyspark_interview_project/io/write_table.py:16` | ✅ Works (referenced) | refs=20 |
| `read_table` | `FunctionDef` | `src/project_a/pyspark_interview_project/io/write_table.py:169` | ✅ Works (referenced) | refs=9 |
| `_get_endpoint` | `FunctionDef` | `src/project_a/pyspark_interview_project/lineage/openlineage_emitter.py:20` | ✅ Works (referenced) | refs=2 |
| `_get_namespace` | `FunctionDef` | `src/project_a/pyspark_interview_project/lineage/openlineage_emitter.py:25` | ✅ Works (referenced) | refs=3 |
| `emit` | `FunctionDef` | `src/project_a/pyspark_interview_project/lineage/openlineage_emitter.py:29` | ✅ Works (referenced) | refs=283 |
| `build_dataset` | `FunctionDef` | `src/project_a/pyspark_interview_project/lineage/openlineage_emitter.py:51` | ✅ Works (referenced) | refs=3 |
| `emit_job_event` | `FunctionDef` | `src/project_a/pyspark_interview_project/lineage/openlineage_emitter.py:55` | ✅ Works (referenced) | refs=3 |
| `emit_bronze_to_silver` | `FunctionDef` | `src/project_a/pyspark_interview_project/lineage/openlineage_emitter.py:79` | ⚠️ Risky (only definition found) | refs=1 |
| `emit_silver_to_gold` | `FunctionDef` | `src/project_a/pyspark_interview_project/lineage/openlineage_emitter.py:92` | ⚠️ Risky (only definition found) | refs=1 |
| `PipelineMetrics` | `ClassDef` | `src/project_a/pyspark_interview_project/monitoring.py:26` | ✅ Works (referenced) | refs=10 |
| `to_dict` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:41` | ✅ Works (referenced) | refs=6 |
| `Alert` | `ClassDef` | `src/project_a/pyspark_interview_project/monitoring.py:51` | ✅ Works (referenced) | refs=39 |
| `__post_init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:64` | ✅ Works (referenced) | refs=3 |
| `MetricsCollector` | `ClassDef` | `src/project_a/pyspark_interview_project/monitoring.py:69` | ✅ Works (referenced) | refs=19 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:72` | ✅ Works (referenced) | refs=162 |
| `start_pipeline` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:77` | ✅ Works (referenced) | refs=3 |
| `end_pipeline` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:87` | ✅ Works (referenced) | refs=5 |
| `update_metrics` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:105` | ⚠️ Risky (only definition found) | refs=1 |
| `get_pipeline_metrics` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:113` | ⚠️ Risky (only definition found) | refs=1 |
| `get_all_metrics` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:117` | ✅ Works (referenced) | refs=2 |
| `collect_system_metrics` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:121` | ✅ Works (referenced) | refs=5 |
| `HealthChecker` | `ClassDef` | `src/project_a/pyspark_interview_project/monitoring.py:148` | ✅ Works (referenced) | refs=2 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:151` | ✅ Works (referenced) | refs=162 |
| `register_default_checks` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:156` | ✅ Works (referenced) | refs=2 |
| `register_check` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:163` | ✅ Works (referenced) | refs=5 |
| `run_health_checks` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:167` | ✅ Works (referenced) | refs=14 |
| `_check_spark_session` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:187` | ✅ Works (referenced) | refs=2 |
| `_check_data_sources` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:207` | ✅ Works (referenced) | refs=2 |
| `_check_data_quality` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:217` | ✅ Works (referenced) | refs=2 |
| `_check_performance` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:226` | ✅ Works (referenced) | refs=2 |
| `AlertManager` | `ClassDef` | `src/project_a/pyspark_interview_project/monitoring.py:249` | ✅ Works (referenced) | refs=5 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:252` | ✅ Works (referenced) | refs=162 |
| `_setup_handlers` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:258` | ✅ Works (referenced) | refs=2 |
| `create_alert` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:276` | ✅ Works (referenced) | refs=4 |
| `_send_notifications` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:301` | ✅ Works (referenced) | refs=2 |
| `_send_slack_alert` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:309` | ✅ Works (referenced) | refs=2 |
| `_send_email_alert` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:357` | ✅ Works (referenced) | refs=2 |
| `get_alerts` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:362` | ✅ Works (referenced) | refs=2 |
| `acknowledge_alert` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:376` | ⚠️ Risky (only definition found) | refs=1 |
| `PipelineMonitor` | `ClassDef` | `src/project_a/pyspark_interview_project/monitoring.py:387` | ✅ Works (referenced) | refs=11 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:390` | ✅ Works (referenced) | refs=162 |
| `monitor_pipeline` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:402` | ✅ Works (referenced) | refs=4 |
| `run_health_check` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:423` | ✅ Works (referenced) | refs=15 |
| `get_metrics_summary` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:466` | ✅ Works (referenced) | refs=4 |
| `cleanup_old_metrics` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:502` | ⚠️ Risky (only definition found) | refs=1 |
| `create_monitor` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:529` | ⚠️ Risky (only definition found) | refs=1 |
| `monitor_pipeline_execution` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:534` | ⚠️ Risky (only definition found) | refs=1 |
| `decorator` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:537` | ✅ Works (referenced) | refs=21 |
| `wrapper` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring.py:538` | ✅ Works (referenced) | refs=23 |
| `lineage_job` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/__init__.py:23` | ✅ Works (referenced) | refs=26 |
| `emit_lineage_event` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/__init__.py:26` | ✅ Works (referenced) | refs=12 |
| `emit_start` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/__init__.py:29` | ✅ Works (referenced) | refs=17 |
| `emit_complete` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/__init__.py:32` | ✅ Works (referenced) | refs=16 |
| `emit_fail` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/__init__.py:35` | ✅ Works (referenced) | refs=12 |
| `send_slack_alert` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/__init__.py:48` | ✅ Works (referenced) | refs=10 |
| `send_email_alert` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/__init__.py:51` | ✅ Works (referenced) | refs=6 |
| `alert_on_dq_failure` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/__init__.py:54` | ✅ Works (referenced) | refs=4 |
| `alert_on_sla_breach` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/__init__.py:57` | ✅ Works (referenced) | refs=4 |
| `send_slack_alert` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/alerts.py:15` | ✅ Works (referenced) | refs=10 |
| `send_email_alert` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/alerts.py:83` | ✅ Works (referenced) | refs=6 |
| `alert_on_dq_failure` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/alerts.py:104` | ✅ Works (referenced) | refs=4 |
| `alert_on_sla_breach` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/alerts.py:126` | ✅ Works (referenced) | refs=4 |
| `_load_lineage_config` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/lineage_decorator.py:24` | ✅ Works (referenced) | refs=2 |
| `_extract_df_metadata` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/lineage_decorator.py:36` | ✅ Works (referenced) | refs=2 |
| `_post_lineage_event` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/lineage_decorator.py:51` | ✅ Works (referenced) | refs=4 |
| `lineage_job` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/lineage_decorator.py:152` | ✅ Works (referenced) | refs=26 |
| `decorator` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/lineage_decorator.py:176` | ✅ Works (referenced) | refs=21 |
| `wrapper` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/lineage_decorator.py:178` | ✅ Works (referenced) | refs=23 |
| `LineageEmitter` | `ClassDef` | `src/project_a/pyspark_interview_project/monitoring/lineage_emitter.py:19` | ✅ Works (referenced) | refs=8 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/lineage_emitter.py:24` | ✅ Works (referenced) | refs=162 |
| `_get_api_path` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/lineage_emitter.py:46` | ✅ Works (referenced) | refs=4 |
| `emit_job` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/lineage_emitter.py:53` | ✅ Works (referenced) | refs=19 |
| `load_lineage_config` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/lineage_emitter.py:170` | ✅ Works (referenced) | refs=7 |
| `put_metric` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/metrics.py:24` | ✅ Works (referenced) | refs=39 |
| `emit_duration` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/metrics.py:64` | ✅ Works (referenced) | refs=25 |
| `emit_count` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/metrics.py:69` | ✅ Works (referenced) | refs=6 |
| `emit_size` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/metrics.py:74` | ✅ Works (referenced) | refs=3 |
| `emit_dq_metrics` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/metrics.py:79` | ✅ Works (referenced) | refs=3 |
| `MetricsCollector` | `ClassDef` | `src/project_a/pyspark_interview_project/monitoring/metrics_collector.py:25` | ✅ Works (referenced) | refs=19 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/metrics_collector.py:28` | ✅ Works (referenced) | refs=162 |
| `emit_rowcount` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/metrics_collector.py:54` | ✅ Works (referenced) | refs=38 |
| `emit_duration` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/metrics_collector.py:93` | ✅ Works (referenced) | refs=25 |
| `_log_local_metric` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/metrics_collector.py:133` | ✅ Works (referenced) | refs=3 |
| `get_metrics_collector` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/metrics_collector.py:156` | ✅ Works (referenced) | refs=8 |
| `emit_rowcount` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/metrics_collector.py:164` | ✅ Works (referenced) | refs=38 |
| `emit_duration` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/metrics_collector.py:175` | ✅ Works (referenced) | refs=25 |
| `emit_metrics` | `FunctionDef` | `src/project_a/pyspark_interview_project/monitoring/metrics_collector.py:186` | ✅ Works (referenced) | refs=20 |
| `PerformanceMetrics` | `ClassDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:29` | ✅ Works (referenced) | refs=10 |
| `__post_init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:45` | ✅ Works (referenced) | refs=3 |
| `CacheManager` | `ClassDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:50` | ✅ Works (referenced) | refs=10 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:53` | ✅ Works (referenced) | refs=162 |
| `smart_cache` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:68` | ✅ Works (referenced) | refs=12 |
| `_estimate_size_mb` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:113` | ✅ Works (referenced) | refs=6 |
| `_select_optimal_storage_level` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:126` | ✅ Works (referenced) | refs=2 |
| `_get_storage_level_name` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:137` | ✅ Works (referenced) | refs=2 |
| `get_cached_dataset` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:150` | ⚠️ Risky (only definition found) | refs=1 |
| `get_cache_hit_ratio` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:160` | ✅ Works (referenced) | refs=3 |
| `unpersist_dataset` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:170` | ✅ Works (referenced) | refs=4 |
| `clear_all_caches` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:181` | ✅ Works (referenced) | refs=3 |
| `get_cache_summary` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:187` | ✅ Works (referenced) | refs=3 |
| `PerformanceBenchmark` | `ClassDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:199` | ✅ Works (referenced) | refs=9 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:202` | ✅ Works (referenced) | refs=162 |
| `benchmark_dataset` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:206` | ✅ Works (referenced) | refs=14 |
| `_run_benchmark_operation` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:271` | ✅ Works (referenced) | refs=2 |
| `_estimate_size_mb` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:291` | ✅ Works (referenced) | refs=6 |
| `export_benchmark_results` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:301` | ✅ Works (referenced) | refs=4 |
| `PerformanceOptimizer` | `ClassDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:315` | ✅ Works (referenced) | refs=22 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:318` | ✅ Works (referenced) | refs=162 |
| `_configure_spark` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:333` | ✅ Works (referenced) | refs=2 |
| `optimize_returns_raw` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:354` | ✅ Works (referenced) | refs=7 |
| `_optimize_shuffle_partitions` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:387` | ✅ Works (referenced) | refs=5 |
| `_apply_z_ordering` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:415` | ✅ Works (referenced) | refs=2 |
| `_optimize_file_compaction` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:436` | ✅ Works (referenced) | refs=5 |
| `optimize_broadcast_joins` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:459` | ⚠️ Risky (only definition found) | refs=1 |
| `run_full_optimization_pipeline` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:481` | ✅ Works (referenced) | refs=3 |
| `_apply_general_optimizations` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:510` | ✅ Works (referenced) | refs=2 |
| `_run_comprehensive_benchmark` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:525` | ✅ Works (referenced) | refs=2 |
| `_estimate_size_mb` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:535` | ✅ Works (referenced) | refs=6 |
| `get_performance_summary` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:545` | ✅ Works (referenced) | refs=6 |
| `create_performance_optimizer` | `FunctionDef` | `src/project_a/pyspark_interview_project/performance_optimizer.py:570` | ✅ Works (referenced) | refs=5 |
| `ProductionETLPipeline` | `ClassDef` | `src/project_a/pyspark_interview_project/production_pipeline.py:20` | ✅ Works (referenced) | refs=2 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/production_pipeline.py:25` | ✅ Works (referenced) | refs=162 |
| `create_delta_lake_structure` | `FunctionDef` | `src/project_a/pyspark_interview_project/production_pipeline.py:29` | ✅ Works (referenced) | refs=2 |
| `_generate_schema_string` | `FunctionDef` | `src/project_a/pyspark_interview_project/production_pipeline.py:81` | ✅ Works (referenced) | refs=2 |
| `_pandas_to_spark_type` | `FunctionDef` | `src/project_a/pyspark_interview_project/production_pipeline.py:95` | ✅ Works (referenced) | refs=2 |
| `_generate_stats` | `FunctionDef` | `src/project_a/pyspark_interview_project/production_pipeline.py:108` | ✅ Works (referenced) | refs=5 |
| `process_existing_data` | `FunctionDef` | `src/project_a/pyspark_interview_project/production_pipeline.py:120` | ✅ Works (referenced) | refs=2 |
| `_create_initial_layers` | `FunctionDef` | `src/project_a/pyspark_interview_project/production_pipeline.py:143` | ✅ Works (referenced) | refs=2 |
| `_create_sample_bronze_data` | `FunctionDef` | `src/project_a/pyspark_interview_project/production_pipeline.py:169` | ✅ Works (referenced) | refs=2 |
| `_create_sample_silver_data` | `FunctionDef` | `src/project_a/pyspark_interview_project/production_pipeline.py:204` | ✅ Works (referenced) | refs=2 |
| `_create_sample_gold_data` | `FunctionDef` | `src/project_a/pyspark_interview_project/production_pipeline.py:229` | ✅ Works (referenced) | refs=2 |
| `_process_layer` | `FunctionDef` | `src/project_a/pyspark_interview_project/production_pipeline.py:266` | ✅ Works (referenced) | refs=4 |
| `demonstrate_time_travel` | `FunctionDef` | `src/project_a/pyspark_interview_project/production_pipeline.py:295` | ✅ Works (referenced) | refs=4 |
| `_create_table_version` | `FunctionDef` | `src/project_a/pyspark_interview_project/production_pipeline.py:327` | ✅ Works (referenced) | refs=2 |
| `_create_silver_table_version` | `FunctionDef` | `src/project_a/pyspark_interview_project/production_pipeline.py:396` | ✅ Works (referenced) | refs=2 |
| `_create_gold_table_version` | `FunctionDef` | `src/project_a/pyspark_interview_project/production_pipeline.py:498` | ✅ Works (referenced) | refs=2 |
| `_show_version_history` | `FunctionDef` | `src/project_a/pyspark_interview_project/production_pipeline.py:570` | ✅ Works (referenced) | refs=4 |
| `run_pipeline` | `FunctionDef` | `src/project_a/pyspark_interview_project/production_pipeline.py:596` | ✅ Works (referenced) | refs=25 |
| `_log_final_counts` | `FunctionDef` | `src/project_a/pyspark_interview_project/production_pipeline.py:617` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `src/project_a/pyspark_interview_project/production_pipeline.py:643` | ✅ Works (referenced) | refs=480 |
| `SchemaValidator` | `ClassDef` | `src/project_a/pyspark_interview_project/schema_validator.py:21` | ✅ Works (referenced) | refs=36 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/schema_validator.py:24` | ✅ Works (referenced) | refs=162 |
| `validate_bronze_schema` | `FunctionDef` | `src/project_a/pyspark_interview_project/schema_validator.py:28` | ✅ Works (referenced) | refs=4 |
| `validate_silver_schema` | `FunctionDef` | `src/project_a/pyspark_interview_project/schema_validator.py:49` | ✅ Works (referenced) | refs=5 |
| `validate_gold_schema` | `FunctionDef` | `src/project_a/pyspark_interview_project/schema_validator.py:103` | ✅ Works (referenced) | refs=6 |
| `_compare_schemas` | `FunctionDef` | `src/project_a/pyspark_interview_project/schema_validator.py:151` | ✅ Works (referenced) | refs=2 |
| `_validate_against_contract` | `FunctionDef` | `src/project_a/pyspark_interview_project/schema_validator.py:199` | ✅ Works (referenced) | refs=2 |
| `validate_table` | `FunctionDef` | `src/project_a/pyspark_interview_project/schema_validator.py:233` | ✅ Works (referenced) | refs=4 |
| `validate_schema_evolution` | `FunctionDef` | `src/project_a/pyspark_interview_project/schema_validator.py:255` | ⚠️ Risky (only definition found) | refs=1 |
| `SchemaRegistry` | `ClassDef` | `src/project_a/pyspark_interview_project/schemas/production_schemas.py:159` | ✅ Works (referenced) | refs=11 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/schemas/production_schemas.py:170` | ✅ Works (referenced) | refs=162 |
| `_load_all_schemas` | `FunctionDef` | `src/project_a/pyspark_interview_project/schemas/production_schemas.py:180` | ✅ Works (referenced) | refs=2 |
| `get_schema` | `FunctionDef` | `src/project_a/pyspark_interview_project/schemas/production_schemas.py:195` | ✅ Works (referenced) | refs=32 |
| `validate_schema` | `FunctionDef` | `src/project_a/pyspark_interview_project/schemas/production_schemas.py:213` | ✅ Works (referenced) | refs=34 |
| `save_schema_version` | `FunctionDef` | `src/project_a/pyspark_interview_project/schemas/production_schemas.py:306` | ⚠️ Risky (only definition found) | refs=1 |
| `get_schema` | `FunctionDef` | `src/project_a/pyspark_interview_project/schemas/production_schemas.py:338` | ✅ Works (referenced) | refs=32 |
| `validate_dataframe_schema` | `FunctionDef` | `src/project_a/pyspark_interview_project/schemas/production_schemas.py:351` | ✅ Works (referenced) | refs=4 |
| `validate_schema_drift` | `FunctionDef` | `src/project_a/pyspark_interview_project/schemas/production_schemas.py:379` | ✅ Works (referenced) | refs=6 |
| `StandardETLPipeline` | `ClassDef` | `src/project_a/pyspark_interview_project/standard_etl_pipeline.py:21` | ✅ Works (referenced) | refs=4 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/standard_etl_pipeline.py:26` | ✅ Works (referenced) | refs=162 |
| `run_pipeline` | `FunctionDef` | `src/project_a/pyspark_interview_project/standard_etl_pipeline.py:30` | ✅ Works (referenced) | refs=25 |
| `create_standard_tables` | `FunctionDef` | `src/project_a/pyspark_interview_project/standard_etl_pipeline.py:56` | ✅ Works (referenced) | refs=2 |
| `_add_new_versions` | `FunctionDef` | `src/project_a/pyspark_interview_project/standard_etl_pipeline.py:71` | ✅ Works (referenced) | refs=2 |
| `_generate_customer_data` | `FunctionDef` | `src/project_a/pyspark_interview_project/standard_etl_pipeline.py:110` | ✅ Works (referenced) | refs=2 |
| `_generate_order_data` | `FunctionDef` | `src/project_a/pyspark_interview_project/standard_etl_pipeline.py:140` | ✅ Works (referenced) | refs=2 |
| `_generate_customer_analytics` | `FunctionDef` | `src/project_a/pyspark_interview_project/standard_etl_pipeline.py:172` | ✅ Works (referenced) | refs=4 |
| `_generate_monthly_revenue` | `FunctionDef` | `src/project_a/pyspark_interview_project/standard_etl_pipeline.py:188` | ✅ Works (referenced) | refs=4 |
| `process_bronze_layer` | `FunctionDef` | `src/project_a/pyspark_interview_project/standard_etl_pipeline.py:197` | ✅ Works (referenced) | refs=2 |
| `process_silver_layer` | `FunctionDef` | `src/project_a/pyspark_interview_project/standard_etl_pipeline.py:218` | ✅ Works (referenced) | refs=4 |
| `process_gold_layer` | `FunctionDef` | `src/project_a/pyspark_interview_project/standard_etl_pipeline.py:239` | ✅ Works (referenced) | refs=4 |
| `demonstrate_time_travel` | `FunctionDef` | `src/project_a/pyspark_interview_project/standard_etl_pipeline.py:260` | ✅ Works (referenced) | refs=4 |
| `main` | `FunctionDef` | `src/project_a/pyspark_interview_project/standard_etl_pipeline.py:282` | ✅ Works (referenced) | refs=480 |
| `cleanse_customers` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/__init__.py:36` | ✅ Works (referenced) | refs=2 |
| `cleanse_products` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/__init__.py:41` | ✅ Works (referenced) | refs=2 |
| `cleanse_orders` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/__init__.py:46` | ✅ Works (referenced) | refs=2 |
| `cleanse_returns` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/__init__.py:51` | ✅ Works (referenced) | refs=2 |
| `enrich_with_fx_rates` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/__init__.py:56` | ✅ Works (referenced) | refs=2 |
| `enrich_with_customer_data` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/__init__.py:61` | ✅ Works (referenced) | refs=2 |
| `enrich_with_product_data` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/__init__.py:66` | ✅ Works (referenced) | refs=2 |
| `build_customer_dimension` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/__init__.py:71` | ✅ Works (referenced) | refs=7 |
| `build_product_dimension` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/__init__.py:76` | ✅ Works (referenced) | refs=4 |
| `build_sales_fact_table` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/__init__.py:81` | ✅ Works (referenced) | refs=5 |
| `build_revenue_summary` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/__init__.py:86` | ✅ Works (referenced) | refs=2 |
| `broadcast_join_demo` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/__init__.py:91` | ✅ Works (referenced) | refs=5 |
| `window_function_demo` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/__init__.py:96` | ✅ Works (referenced) | refs=2 |
| `validate_not_null` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/base_transformer.py:24` | ✅ Works (referenced) | refs=6 |
| `validate_unique` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/base_transformer.py:27` | ✅ Works (referenced) | refs=6 |
| `BaseTransformer` | `ClassDef` | `src/project_a/pyspark_interview_project/transform/base_transformer.py:34` | ✅ Works (referenced) | refs=6 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/base_transformer.py:37` | ✅ Works (referenced) | refs=162 |
| `_trim_string_columns` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/base_transformer.py:50` | ✅ Works (referenced) | refs=2 |
| `_handle_nulls` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/base_transformer.py:57` | ✅ Works (referenced) | refs=2 |
| `_add_metadata` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/base_transformer.py:62` | ✅ Works (referenced) | refs=4 |
| `_validate_dq` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/base_transformer.py:73` | ⚠️ Risky (only definition found) | refs=1 |
| `transform` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/base_transformer.py:82` | ✅ Works (referenced) | refs=375 |
| `transform_with_cleanup` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/base_transformer.py:96` | ⚠️ Risky (only definition found) | refs=1 |
| `BronzeToSilverTransformer` | `ClassDef` | `src/project_a/pyspark_interview_project/transform/base_transformer.py:122` | ✅ Works (referenced) | refs=4 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/base_transformer.py:125` | ✅ Works (referenced) | refs=162 |
| `transform` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/base_transformer.py:128` | ✅ Works (referenced) | refs=375 |
| `SilverToGoldTransformer` | `ClassDef` | `src/project_a/pyspark_interview_project/transform/base_transformer.py:133` | ✅ Works (referenced) | refs=4 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/base_transformer.py:136` | ✅ Works (referenced) | refs=162 |
| `transform` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/base_transformer.py:139` | ✅ Works (referenced) | refs=375 |
| `load_with_fallback` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/bronze_loaders.py:17` | ✅ Works (referenced) | refs=9 |
| `load_crm_bronze_data` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/bronze_loaders.py:87` | ✅ Works (referenced) | refs=3 |
| `load_snowflake_bronze_data` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/bronze_loaders.py:139` | ✅ Works (referenced) | refs=3 |
| `load_redshift_behavior_bronze_data` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/bronze_loaders.py:190` | ✅ Works (referenced) | refs=3 |
| `load_kafka_bronze_data` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/bronze_loaders.py:229` | ✅ Works (referenced) | refs=3 |
| `build_dim_date` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/gold_builders.py:16` | ✅ Works (referenced) | refs=8 |
| `build_dim_customer` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/gold_builders.py:122` | ✅ Works (referenced) | refs=7 |
| `build_dim_product` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/gold_builders.py:201` | ✅ Works (referenced) | refs=8 |
| `build_fact_orders` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/gold_builders.py:265` | ✅ Works (referenced) | refs=10 |
| `build_customer_360` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/gold_builders.py:369` | ✅ Works (referenced) | refs=27 |
| `build_product_performance` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/gold_builders.py:437` | ✅ Works (referenced) | refs=5 |
| `build_customers_silver` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/silver_builders.py:16` | ✅ Works (referenced) | refs=6 |
| `build_orders_silver` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/silver_builders.py:133` | ✅ Works (referenced) | refs=5 |
| `build_products_silver` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/silver_builders.py:299` | ✅ Works (referenced) | refs=5 |
| `build_behavior_silver` | `FunctionDef` | `src/project_a/pyspark_interview_project/transform/silver_builders.py:327` | ✅ Works (referenced) | refs=5 |
| `UnityCatalogManager` | `ClassDef` | `src/project_a/pyspark_interview_project/unity_catalog.py:22` | ✅ Works (referenced) | refs=7 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/unity_catalog.py:27` | ✅ Works (referenced) | refs=162 |
| `_get_dbutils` | `FunctionDef` | `src/project_a/pyspark_interview_project/unity_catalog.py:32` | ✅ Works (referenced) | refs=6 |
| `create_catalog` | `FunctionDef` | `src/project_a/pyspark_interview_project/unity_catalog.py:46` | ✅ Works (referenced) | refs=5 |
| `create_schema` | `FunctionDef` | `src/project_a/pyspark_interview_project/unity_catalog.py:81` | ✅ Works (referenced) | refs=5 |
| `create_table` | `FunctionDef` | `src/project_a/pyspark_interview_project/unity_catalog.py:119` | ✅ Works (referenced) | refs=13 |
| `grant_permissions` | `FunctionDef` | `src/project_a/pyspark_interview_project/unity_catalog.py:179` | ✅ Works (referenced) | refs=2 |
| `create_external_location` | `FunctionDef` | `src/project_a/pyspark_interview_project/unity_catalog.py:223` | ⚠️ Risky (only definition found) | refs=1 |
| `share_table` | `FunctionDef` | `src/project_a/pyspark_interview_project/unity_catalog.py:264` | ⚠️ Risky (only definition found) | refs=1 |
| `get_table_lineage` | `FunctionDef` | `src/project_a/pyspark_interview_project/unity_catalog.py:314` | ⚠️ Risky (only definition found) | refs=1 |
| `set_data_classification` | `FunctionDef` | `src/project_a/pyspark_interview_project/unity_catalog.py:366` | ⚠️ Risky (only definition found) | refs=1 |
| `create_data_governance_policy` | `FunctionDef` | `src/project_a/pyspark_interview_project/unity_catalog.py:407` | ✅ Works (referenced) | refs=3 |
| `audit_table_access` | `FunctionDef` | `src/project_a/pyspark_interview_project/unity_catalog.py:443` | ⚠️ Risky (only definition found) | refs=1 |
| `get_governance_summary` | `FunctionDef` | `src/project_a/pyspark_interview_project/unity_catalog.py:499` | ⚠️ Risky (only definition found) | refs=1 |
| `setup_unity_catalog_governance` | `FunctionDef` | `src/project_a/pyspark_interview_project/unity_catalog.py:541` | ✅ Works (referenced) | refs=3 |
| `list_catalogs` | `FunctionDef` | `src/project_a/pyspark_interview_project/unity_catalog.py:618` | ✅ Works (referenced) | refs=5 |
| `setup_unity_catalog_governance` | `FunctionDef` | `src/project_a/pyspark_interview_project/unity_catalog.py:633` | ✅ Works (referenced) | refs=3 |
| `migrate_to_unity_catalog` | `FunctionDef` | `src/project_a/pyspark_interview_project/unity_catalog.py:674` | ⚠️ Risky (only definition found) | refs=1 |
| `get_last_processed_batch` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/checkpoint.py:19` | ⚠️ Risky (only definition found) | refs=1 |
| `set_last_processed_batch` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/checkpoint.py:45` | ⚠️ Risky (only definition found) | refs=1 |
| `get_watermark` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/checkpoint.py:85` | ✅ Works (referenced) | refs=33 |
| `set_watermark` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/checkpoint.py:113` | ✅ Works (referenced) | refs=14 |
| `put_metric` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/cloudwatch_metrics.py:15` | ✅ Works (referenced) | refs=39 |
| `emit_job_success` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/cloudwatch_metrics.py:54` | ✅ Works (referenced) | refs=6 |
| `emit_job_failure` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/cloudwatch_metrics.py:93` | ✅ Works (referenced) | refs=6 |
| `emit_data_quality_metric` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/cloudwatch_metrics.py:113` | ✅ Works (referenced) | refs=2 |
| `load_conf` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/config.py:15` | ✅ Works (referenced) | refs=249 |
| `_overlay_env_vars` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/config.py:47` | ✅ Works (referenced) | refs=2 |
| `_resolve_vars` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/config.py:50` | ✅ Works (referenced) | refs=4 |
| `_resolve_template_variables` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/config.py:69` | ✅ Works (referenced) | refs=2 |
| `replace_var` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/config.py:73` | ✅ Works (referenced) | refs=12 |
| `_validate_config` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/config.py:94` | ✅ Works (referenced) | refs=2 |
| `_get_nested_value` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/config.py:114` | ✅ Works (referenced) | refs=2 |
| `_resolve_env_vars` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/config_loader.py:23` | ✅ Works (referenced) | refs=2 |
| `replace_env` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/config_loader.py:26` | ✅ Works (referenced) | refs=4 |
| `_resolve_config_vars` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/config_loader.py:33` | ✅ Works (referenced) | refs=2 |
| `replace_var` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/config_loader.py:36` | ✅ Works (referenced) | refs=12 |
| `_resolve_value` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/config_loader.py:55` | ✅ Works (referenced) | refs=11 |
| `_resolve_secrets` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/config_loader.py:70` | ✅ Works (referenced) | refs=16 |
| `load_config_from_s3` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/config_loader.py:85` | ✅ Works (referenced) | refs=2 |
| `load_config_resolved` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/config_loader.py:126` | ✅ Works (referenced) | refs=107 |
| `get_config_value` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/config_loader.py:169` | ⚠️ Risky (only definition found) | refs=1 |
| `load_schema_contract` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/contracts.py:18` | ⚠️ Risky (only definition found) | refs=1 |
| `contract_to_struct_type` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/contracts.py:38` | ✅ Works (referenced) | refs=2 |
| `align_to_schema` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/contracts.py:59` | ✅ Works (referenced) | refs=2 |
| `validate_and_quarantine` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/contracts.py:139` | ⚠️ Risky (only definition found) | refs=1 |
| `_py_type_mapping` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/contracts.py:214` | ✅ Works (referenced) | refs=2 |
| `add_metadata_columns` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/contracts.py:267` | ✅ Works (referenced) | refs=13 |
| `run_dq_suite` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/dq_utils.py:16` | ✅ Works (referenced) | refs=7 |
| `validate_not_null` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/dq_utils.py:70` | ✅ Works (referenced) | refs=6 |
| `validate_unique` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/dq_utils.py:76` | ✅ Works (referenced) | refs=6 |
| `generate_dq_report` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/dq_utils.py:83` | ✅ Works (referenced) | refs=4 |
| `ErrorLaneHandler` | `ClassDef` | `src/project_a/pyspark_interview_project/utils/error_lanes.py:18` | ✅ Works (referenced) | refs=8 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/error_lanes.py:26` | ✅ Works (referenced) | refs=162 |
| `get_error_lane_path` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/error_lanes.py:36` | ✅ Works (referenced) | refs=7 |
| `quarantine_bad_rows` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/error_lanes.py:65` | ✅ Works (referenced) | refs=2 |
| `quarantine_schema_violations` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/error_lanes.py:135` | ✅ Works (referenced) | refs=2 |
| `add_row_id` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/error_lanes.py:201` | ✅ Works (referenced) | refs=2 |
| `create_error_lane_metadata` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/error_lanes.py:220` | ✅ Works (referenced) | refs=2 |
| `read_delta` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/io.py:14` | ✅ Works (referenced) | refs=55 |
| `write_delta` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/io.py:44` | ✅ Works (referenced) | refs=43 |
| `_normalize_path` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/io.py:96` | ✅ Works (referenced) | refs=2 |
| `ensure_directory_exists` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/io.py:118` | ⚠️ Risky (only definition found) | refs=1 |
| `setup_json_logging` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/logging.py:11` | ✅ Works (referenced) | refs=54 |
| `JSONFormatter` | `ClassDef` | `src/project_a/pyspark_interview_project/utils/logging.py:23` | ✅ Works (referenced) | refs=4 |
| `format` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/logging.py:24` | ✅ Works (referenced) | refs=982 |
| `get_trace_id` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/logging.py:84` | ✅ Works (referenced) | refs=24 |
| `log_with_trace` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/logging.py:89` | ✅ Works (referenced) | refs=9 |
| `emit_metric` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/metrics.py:15` | ✅ Works (referenced) | refs=28 |
| `track_job_start` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/metrics.py:40` | ✅ Works (referenced) | refs=11 |
| `track_job_complete` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/metrics.py:48` | ✅ Works (referenced) | refs=15 |
| `track_records_processed` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/metrics.py:58` | ✅ Works (referenced) | refs=11 |
| `track_dq_check` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/metrics.py:67` | ✅ Works (referenced) | refs=3 |
| `resolve_path` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/path_resolver.py:14` | ✅ Works (referenced) | refs=20 |
| `resolve_lake_path` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/path_resolver.py:85` | ✅ Works (referenced) | refs=3 |
| `bronze_path` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/path_resolver.py:99` | ✅ Works (referenced) | refs=136 |
| `silver_path` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/path_resolver.py:114` | ✅ Works (referenced) | refs=126 |
| `gold_path` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/path_resolver.py:128` | ✅ Works (referenced) | refs=153 |
| `get_salt` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/pii_utils.py:16` | ✅ Works (referenced) | refs=2 |
| `mask_email` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/pii_utils.py:21` | ✅ Works (referenced) | refs=12 |
| `mask_phone` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/pii_utils.py:45` | ✅ Works (referenced) | refs=12 |
| `hash_value` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/pii_utils.py:70` | ✅ Works (referenced) | refs=9 |
| `mask_name` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/pii_utils.py:97` | ✅ Works (referenced) | refs=10 |
| `apply_pii_masking` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/pii_utils.py:119` | ✅ Works (referenced) | refs=6 |
| `write_run_audit` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/run_audit.py:20` | ✅ Works (referenced) | refs=14 |
| `read_run_audit` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/run_audit.py:104` | ✅ Works (referenced) | refs=4 |
| `SafeDeltaWriter` | `ClassDef` | `src/project_a/pyspark_interview_project/utils/safe_writer.py:18` | ✅ Works (referenced) | refs=19 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/safe_writer.py:30` | ✅ Works (referenced) | refs=162 |
| `write_with_merge` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/safe_writer.py:33` | ✅ Works (referenced) | refs=18 |
| `write_partition_overwrite` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/safe_writer.py:126` | ✅ Works (referenced) | refs=2 |
| `write_append` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/safe_writer.py:220` | ✅ Works (referenced) | refs=3 |
| `validate_and_log_metrics` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/safe_writer.py:292` | ⚠️ Risky (only definition found) | refs=1 |
| `write_delta_safe` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/safe_writer.py:331` | ⚠️ Risky (only definition found) | refs=1 |
| `SchemaValidator` | `ClassDef` | `src/project_a/pyspark_interview_project/utils/schema_validator.py:26` | ✅ Works (referenced) | refs=36 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/schema_validator.py:29` | ✅ Works (referenced) | refs=162 |
| `_load_schema` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/schema_validator.py:39` | ✅ Works (referenced) | refs=2 |
| `validate_required_fields` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/schema_validator.py:47` | ✅ Works (referenced) | refs=2 |
| `handle_schema_drift` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/schema_validator.py:66` | ✅ Works (referenced) | refs=2 |
| `validate_data_quality` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/schema_validator.py:113` | ✅ Works (referenced) | refs=8 |
| `validate_and_prepare` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/schema_validator.py:158` | ⚠️ Risky (only definition found) | refs=1 |
| `validate_schema` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/schema_validator.py:194` | ✅ Works (referenced) | refs=34 |
| `log_schema_drift` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/schema_validator.py:273` | ✅ Works (referenced) | refs=2 |
| `validate_bronze_ingestion` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/schema_validator.py:309` | ✅ Works (referenced) | refs=7 |
| `get_secret_from_manager` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/secrets.py:25` | ✅ Works (referenced) | refs=10 |
| `get_parameter_from_ssm` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/secrets.py:72` | ⚠️ Risky (only definition found) | refs=1 |
| `_get_secret_from_env` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/secrets.py:110` | ✅ Works (referenced) | refs=4 |
| `get_snowflake_credentials` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/secrets.py:158` | ✅ Works (referenced) | refs=21 |
| `get_redshift_credentials` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/secrets.py:181` | ✅ Works (referenced) | refs=8 |
| `build_spark` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/spark_session.py:9` | ✅ Works (referenced) | refs=141 |
| `StateStore` | `ClassDef` | `src/project_a/pyspark_interview_project/utils/state_store.py:26` | ✅ Works (referenced) | refs=6 |
| `__init__` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/state_store.py:33` | ✅ Works (referenced) | refs=162 |
| `_get_key` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/state_store.py:55` | ✅ Works (referenced) | refs=3 |
| `get_watermark` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/state_store.py:59` | ✅ Works (referenced) | refs=33 |
| `set_watermark` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/state_store.py:105` | ✅ Works (referenced) | refs=14 |
| `get_checkpoint` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/state_store.py:143` | ✅ Works (referenced) | refs=4 |
| `set_checkpoint` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/state_store.py:176` | ⚠️ Risky (only definition found) | refs=1 |
| `get_state_store` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/state_store.py:212` | ✅ Works (referenced) | refs=11 |
| `_path` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/watermark.py:11` | ✅ Works (referenced) | refs=3086 |
| `load_watermark` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/watermark.py:16` | ✅ Works (referenced) | refs=3 |
| `save_watermark` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/watermark.py:28` | ✅ Works (referenced) | refs=3 |
| `get_watermark` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/watermark_utils.py:19` | ✅ Works (referenced) | refs=33 |
| `upsert_watermark` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/watermark_utils.py:68` | ✅ Works (referenced) | refs=8 |
| `get_latest_timestamp_from_df` | `FunctionDef` | `src/project_a/pyspark_interview_project/utils/watermark_utils.py:146` | ✅ Works (referenced) | refs=16 |
| `ValidateOutput` | `ClassDef` | `src/project_a/pyspark_interview_project/validate.py:14` | ✅ Works (referenced) | refs=5 |
| `validate_parquet` | `FunctionDef` | `src/project_a/pyspark_interview_project/validate.py:15` | ✅ Works (referenced) | refs=3 |
| `validate_avro` | `FunctionDef` | `src/project_a/pyspark_interview_project/validate.py:26` | ✅ Works (referenced) | refs=3 |
| `validate_json` | `FunctionDef` | `src/project_a/pyspark_interview_project/validate.py:36` | ✅ Works (referenced) | refs=3 |
| `Permission` | `ClassDef` | `src/project_a/security/access_control.py:21` | ✅ Works (referenced) | refs=17 |
| `DataSensitivity` | `ClassDef` | `src/project_a/security/access_control.py:28` | ✅ Works (referenced) | refs=27 |
| `User` | `ClassDef` | `src/project_a/security/access_control.py:36` | ✅ Works (referenced) | refs=65 |
| `DatasetAccessRule` | `ClassDef` | `src/project_a/security/access_control.py:51` | ✅ Works (referenced) | refs=4 |
| `UserManager` | `ClassDef` | `src/project_a/security/access_control.py:64` | ✅ Works (referenced) | refs=6 |
| `__init__` | `FunctionDef` | `src/project_a/security/access_control.py:67` | ✅ Works (referenced) | refs=162 |
| `_load_users` | `FunctionDef` | `src/project_a/security/access_control.py:74` | ✅ Works (referenced) | refs=2 |
| `save_users` | `FunctionDef` | `src/project_a/security/access_control.py:97` | ✅ Works (referenced) | refs=3 |
| `create_user` | `FunctionDef` | `src/project_a/security/access_control.py:117` | ✅ Works (referenced) | refs=3 |
| `authenticate_user` | `FunctionDef` | `src/project_a/security/access_control.py:139` | ✅ Works (referenced) | refs=3 |
| `get_user` | `FunctionDef` | `src/project_a/security/access_control.py:149` | ✅ Works (referenced) | refs=7 |
| `AccessControlManager` | `ClassDef` | `src/project_a/security/access_control.py:154` | ✅ Works (referenced) | refs=6 |
| `__init__` | `FunctionDef` | `src/project_a/security/access_control.py:157` | ✅ Works (referenced) | refs=162 |
| `_load_rules` | `FunctionDef` | `src/project_a/security/access_control.py:164` | ✅ Works (referenced) | refs=2 |
| `save_rules` | `FunctionDef` | `src/project_a/security/access_control.py:184` | ✅ Works (referenced) | refs=2 |
| `set_dataset_access_rule` | `FunctionDef` | `src/project_a/security/access_control.py:203` | ✅ Works (referenced) | refs=3 |
| `check_permission` | `FunctionDef` | `src/project_a/security/access_control.py:232` | ✅ Works (referenced) | refs=4 |
| `get_row_level_filter` | `FunctionDef` | `src/project_a/security/access_control.py:254` | ✅ Works (referenced) | refs=2 |
| `get_column_masking` | `FunctionDef` | `src/project_a/security/access_control.py:271` | ✅ Works (referenced) | refs=2 |
| `DataMasker` | `ClassDef` | `src/project_a/security/access_control.py:289` | ✅ Works (referenced) | refs=4 |
| `__init__` | `FunctionDef` | `src/project_a/security/access_control.py:292` | ✅ Works (referenced) | refs=162 |
| `mask_dataframe` | `FunctionDef` | `src/project_a/security/access_control.py:304` | ✅ Works (referenced) | refs=2 |
| `_mask_email` | `FunctionDef` | `src/project_a/security/access_control.py:321` | ✅ Works (referenced) | refs=5 |
| `_mask_phone` | `FunctionDef` | `src/project_a/security/access_control.py:339` | ✅ Works (referenced) | refs=5 |
| `_mask_ssn` | `FunctionDef` | `src/project_a/security/access_control.py:349` | ✅ Works (referenced) | refs=4 |
| `_mask_credit_card` | `FunctionDef` | `src/project_a/security/access_control.py:359` | ✅ Works (referenced) | refs=2 |
| `_mask_partial` | `FunctionDef` | `src/project_a/security/access_control.py:369` | ✅ Works (referenced) | refs=2 |
| `_mask_hash` | `FunctionDef` | `src/project_a/security/access_control.py:381` | ✅ Works (referenced) | refs=2 |
| `_mask_nullify` | `FunctionDef` | `src/project_a/security/access_control.py:388` | ✅ Works (referenced) | refs=2 |
| `SecureDataFrame` | `ClassDef` | `src/project_a/security/access_control.py:393` | ✅ Works (referenced) | refs=2 |
| `__init__` | `FunctionDef` | `src/project_a/security/access_control.py:396` | ✅ Works (referenced) | refs=162 |
| `check_access` | `FunctionDef` | `src/project_a/security/access_control.py:411` | ✅ Works (referenced) | refs=2 |
| `get_secure_dataframe` | `FunctionDef` | `src/project_a/security/access_control.py:415` | ✅ Works (referenced) | refs=3 |
| `_apply_row_filter` | `FunctionDef` | `src/project_a/security/access_control.py:442` | ✅ Works (referenced) | refs=2 |
| `get_user_manager` | `FunctionDef` | `src/project_a/security/access_control.py:470` | ✅ Works (referenced) | refs=6 |
| `get_access_control_manager` | `FunctionDef` | `src/project_a/security/access_control.py:482` | ✅ Works (referenced) | refs=6 |
| `get_data_masker` | `FunctionDef` | `src/project_a/security/access_control.py:494` | ✅ Works (referenced) | refs=2 |
| `create_user` | `FunctionDef` | `src/project_a/security/access_control.py:502` | ✅ Works (referenced) | refs=3 |
| `authenticate_user` | `FunctionDef` | `src/project_a/security/access_control.py:510` | ✅ Works (referenced) | refs=3 |
| `set_dataset_access_rule` | `FunctionDef` | `src/project_a/security/access_control.py:516` | ✅ Works (referenced) | refs=3 |
| `check_permission` | `FunctionDef` | `src/project_a/security/access_control.py:540` | ✅ Works (referenced) | refs=4 |
| `get_secure_dataframe` | `FunctionDef` | `src/project_a/security/access_control.py:546` | ✅ Works (referenced) | refs=3 |
| `TestResult` | `ClassDef` | `src/project_a/testing/framework.py:21` | ✅ Works (referenced) | refs=11 |
| `TestCaseResult` | `ClassDef` | `src/project_a/testing/framework.py:29` | ✅ Works (referenced) | refs=7 |
| `TestSuiteResult` | `ClassDef` | `src/project_a/testing/framework.py:40` | ✅ Works (referenced) | refs=5 |
| `DataTestFramework` | `ClassDef` | `src/project_a/testing/framework.py:49` | ✅ Works (referenced) | refs=8 |
| `__init__` | `FunctionDef` | `src/project_a/testing/framework.py:52` | ✅ Works (referenced) | refs=162 |
| `run_test_suite` | `FunctionDef` | `src/project_a/testing/framework.py:58` | ✅ Works (referenced) | refs=3 |
| `_save_test_results` | `FunctionDef` | `src/project_a/testing/framework.py:132` | ✅ Works (referenced) | refs=2 |
| `assert_dataframe_equal` | `FunctionDef` | `src/project_a/testing/framework.py:156` | ✅ Works (referenced) | refs=5 |
| `assert_dataframe_schema` | `FunctionDef` | `src/project_a/testing/framework.py:177` | ✅ Works (referenced) | refs=4 |
| `assert_dataframe_count` | `FunctionDef` | `src/project_a/testing/framework.py:209` | ✅ Works (referenced) | refs=4 |
| `assert_no_nulls_in_column` | `FunctionDef` | `src/project_a/testing/framework.py:222` | ✅ Works (referenced) | refs=4 |
| `assert_unique_values` | `FunctionDef` | `src/project_a/testing/framework.py:234` | ✅ Works (referenced) | refs=4 |
| `assert_column_range` | `FunctionDef` | `src/project_a/testing/framework.py:251` | ✅ Works (referenced) | refs=4 |
| `IntegrationTestRunner` | `ClassDef` | `src/project_a/testing/framework.py:278` | ✅ Works (referenced) | refs=4 |
| `__init__` | `FunctionDef` | `src/project_a/testing/framework.py:281` | ✅ Works (referenced) | refs=162 |
| `run_transformation_tests` | `FunctionDef` | `src/project_a/testing/framework.py:285` | ✅ Works (referenced) | refs=3 |
| `run_pipeline_tests` | `FunctionDef` | `src/project_a/testing/framework.py:310` | ⚠️ Risky (only definition found) | refs=1 |
| `QualityTestGenerator` | `ClassDef` | `src/project_a/testing/framework.py:351` | ✅ Works (referenced) | refs=2 |
| `__init__` | `FunctionDef` | `src/project_a/testing/framework.py:354` | ✅ Works (referenced) | refs=162 |
| `generate_tests_from_profile` | `FunctionDef` | `src/project_a/testing/framework.py:358` | ✅ Works (referenced) | refs=3 |
| `test_schema` | `FunctionDef` | `src/project_a/testing/framework.py:365` | ✅ Works (referenced) | refs=17 |
| `test_count` | `FunctionDef` | `src/project_a/testing/framework.py:374` | ✅ Works (referenced) | refs=4 |
| `make_test` | `FunctionDef` | `src/project_a/testing/framework.py:383` | ✅ Works (referenced) | refs=6 |
| `test` | `FunctionDef` | `src/project_a/testing/framework.py:384` | ✅ Works (referenced) | refs=1476 |
| `make_test` | `FunctionDef` | `src/project_a/testing/framework.py:393` | ✅ Works (referenced) | refs=6 |
| `test` | `FunctionDef` | `src/project_a/testing/framework.py:394` | ✅ Works (referenced) | refs=1476 |
| `make_test` | `FunctionDef` | `src/project_a/testing/framework.py:406` | ✅ Works (referenced) | refs=6 |
| `test` | `FunctionDef` | `src/project_a/testing/framework.py:407` | ✅ Works (referenced) | refs=1476 |
| `get_test_framework` | `FunctionDef` | `src/project_a/testing/framework.py:420` | ✅ Works (referenced) | refs=13 |
| `run_test_suite` | `FunctionDef` | `src/project_a/testing/framework.py:431` | ✅ Works (referenced) | refs=3 |
| `assert_dataframe_equal` | `FunctionDef` | `src/project_a/testing/framework.py:437` | ✅ Works (referenced) | refs=5 |
| `assert_dataframe_schema` | `FunctionDef` | `src/project_a/testing/framework.py:445` | ✅ Works (referenced) | refs=4 |
| `assert_dataframe_count` | `FunctionDef` | `src/project_a/testing/framework.py:451` | ✅ Works (referenced) | refs=4 |
| `assert_no_nulls_in_column` | `FunctionDef` | `src/project_a/testing/framework.py:457` | ✅ Works (referenced) | refs=4 |
| `assert_unique_values` | `FunctionDef` | `src/project_a/testing/framework.py:463` | ✅ Works (referenced) | refs=4 |
| `assert_column_range` | `FunctionDef` | `src/project_a/testing/framework.py:469` | ✅ Works (referenced) | refs=4 |
| `run_transformation_tests` | `FunctionDef` | `src/project_a/testing/framework.py:475` | ✅ Works (referenced) | refs=3 |
| `generate_tests_from_profile` | `FunctionDef` | `src/project_a/testing/framework.py:484` | ✅ Works (referenced) | refs=3 |
| `bronze_to_silver_complete` | `FunctionDef` | `src/project_a/transform/__init__.py:13` | ✅ Works (referenced) | refs=5 |
| `silver_to_gold_complete` | `FunctionDef` | `src/project_a/transform/__init__.py:18` | ✅ Works (referenced) | refs=5 |
| `put_metric` | `FunctionDef` | `src/project_a/utils/cloudwatch_metrics.py:15` | ✅ Works (referenced) | refs=39 |
| `emit_job_success` | `FunctionDef` | `src/project_a/utils/cloudwatch_metrics.py:54` | ✅ Works (referenced) | refs=6 |
| `emit_job_failure` | `FunctionDef` | `src/project_a/utils/cloudwatch_metrics.py:93` | ✅ Works (referenced) | refs=6 |
| `emit_data_quality_metric` | `FunctionDef` | `src/project_a/utils/cloudwatch_metrics.py:113` | ✅ Works (referenced) | refs=2 |
| `_resolve_value` | `FunctionDef` | `src/project_a/utils/config.py:23` | ✅ Works (referenced) | refs=11 |
| `replace_secret` | `FunctionDef` | `src/project_a/utils/config.py:29` | ✅ Works (referenced) | refs=6 |
| `replace_var` | `FunctionDef` | `src/project_a/utils/config.py:39` | ✅ Works (referenced) | refs=12 |
| `_resolve_secrets` | `FunctionDef` | `src/project_a/utils/config.py:59` | ✅ Works (referenced) | refs=16 |
| `load_config` | `FunctionDef` | `src/project_a/utils/config.py:71` | ✅ Works (referenced) | refs=155 |
| `load_conf` | `FunctionDef` | `src/project_a/utils/config.py:109` | ✅ Works (referenced) | refs=249 |
| `load_config_resolved` | `FunctionDef` | `src/project_a/utils/config.py:123` | ✅ Works (referenced) | refs=107 |
| `TableContract` | `ClassDef` | `src/project_a/utils/contracts.py:23` | ✅ Works (referenced) | refs=30 |
| `from_json` | `FunctionDef` | `src/project_a/utils/contracts.py:32` | ✅ Works (referenced) | refs=19 |
| `load_contract` | `FunctionDef` | `src/project_a/utils/contracts.py:42` | ✅ Works (referenced) | refs=9 |
| `validate_schema` | `FunctionDef` | `src/project_a/utils/contracts.py:66` | ✅ Works (referenced) | refs=34 |
| `enforce_not_null` | `FunctionDef` | `src/project_a/utils/contracts.py:82` | ✅ Works (referenced) | refs=7 |
| `validate_primary_key` | `FunctionDef` | `src/project_a/utils/contracts.py:102` | ✅ Works (referenced) | refs=5 |
| `validate_dataframe_schema` | `FunctionDef` | `src/project_a/utils/contracts.py:122` | ✅ Works (referenced) | refs=4 |
| `ErrorLaneHandler` | `ClassDef` | `src/project_a/utils/error_lanes.py:18` | ✅ Works (referenced) | refs=8 |
| `__init__` | `FunctionDef` | `src/project_a/utils/error_lanes.py:26` | ✅ Works (referenced) | refs=162 |
| `get_error_lane_path` | `FunctionDef` | `src/project_a/utils/error_lanes.py:43` | ✅ Works (referenced) | refs=7 |
| `quarantine_bad_rows` | `FunctionDef` | `src/project_a/utils/error_lanes.py:68` | ✅ Works (referenced) | refs=2 |
| `quarantine_schema_violations` | `FunctionDef` | `src/project_a/utils/error_lanes.py:132` | ✅ Works (referenced) | refs=2 |
| `add_row_id` | `FunctionDef` | `src/project_a/utils/error_lanes.py:194` | ✅ Works (referenced) | refs=2 |
| `create_error_lane_metadata` | `FunctionDef` | `src/project_a/utils/error_lanes.py:210` | ✅ Works (referenced) | refs=2 |
| `setup_json_logging` | `FunctionDef` | `src/project_a/utils/logging.py:11` | ✅ Works (referenced) | refs=54 |
| `JSONFormatter` | `ClassDef` | `src/project_a/utils/logging.py:23` | ✅ Works (referenced) | refs=4 |
| `format` | `FunctionDef` | `src/project_a/utils/logging.py:24` | ✅ Works (referenced) | refs=982 |
| `get_trace_id` | `FunctionDef` | `src/project_a/utils/logging.py:84` | ✅ Works (referenced) | refs=24 |
| `log_with_trace` | `FunctionDef` | `src/project_a/utils/logging.py:89` | ✅ Works (referenced) | refs=9 |
| `resolve_data_path` | `FunctionDef` | `src/project_a/utils/path_resolver.py:23` | ✅ Works (referenced) | refs=39 |
| `resolve_source_file_path` | `FunctionDef` | `src/project_a/utils/path_resolver.py:84` | ✅ Works (referenced) | refs=24 |
| `is_local_environment` | `FunctionDef` | `src/project_a/utils/path_resolver.py:140` | ✅ Works (referenced) | refs=2 |
| `get_kafka_bootstrap_servers` | `FunctionDef` | `src/project_a/utils/path_resolver.py:154` | ✅ Works (referenced) | refs=2 |
| `write_run_audit` | `FunctionDef` | `src/project_a/utils/run_audit.py:20` | ✅ Works (referenced) | refs=14 |
| `read_run_audit` | `FunctionDef` | `src/project_a/utils/run_audit.py:104` | ✅ Works (referenced) | refs=4 |
| `SchemaValidator` | `ClassDef` | `src/project_a/utils/schema_validator.py:24` | ✅ Works (referenced) | refs=36 |
| `validate_columns` | `FunctionDef` | `src/project_a/utils/schema_validator.py:72` | ✅ Works (referenced) | refs=3 |
| `validate_schema` | `FunctionDef` | `src/project_a/utils/schema_validator.py:98` | ✅ Works (referenced) | refs=34 |
| `validate_primary_key` | `FunctionDef` | `src/project_a/utils/schema_validator.py:147` | ✅ Works (referenced) | refs=5 |
| `get_schema` | `FunctionDef` | `src/project_a/utils/schema_validator.py:180` | ✅ Works (referenced) | refs=32 |
| `apply_schema` | `FunctionDef` | `src/project_a/utils/schema_validator.py:193` | ✅ Works (referenced) | refs=3 |
| `get_spark_session` | `FunctionDef` | `src/project_a/utils/spark.py:15` | ✅ Works (referenced) | refs=37 |
| `_patched_spark_session_init` | `FunctionDef` | `src/project_a/utils/spark_session.py:24` | ✅ Works (referenced) | refs=2 |
| `_append_extension` | `FunctionDef` | `src/project_a/utils/spark_session.py:76` | ✅ Works (referenced) | refs=4 |
| `_merge_packages` | `FunctionDef` | `src/project_a/utils/spark_session.py:85` | ✅ Works (referenced) | refs=4 |
| `build_spark` | `FunctionDef` | `src/project_a/utils/spark_session.py:100` | ✅ Works (referenced) | refs=141 |
| `get_spark` | `FunctionDef` | `src/project_a/utils/spark_session.py:290` | ✅ Works (referenced) | refs=61 |
| `count_dirs` | `FunctionDef` | `test_pipeline.py:164` | ✅ Works (referenced) | refs=4 |
| `spark` | `FunctionDef` | `tests/conftest.py:16` | ✅ Works (referenced) | refs=3266 |
| `sample_customers_data` | `FunctionDef` | `tests/conftest.py:42` | ⚠️ Risky (only definition found) | refs=1 |
| `sample_orders_data` | `FunctionDef` | `tests/conftest.py:94` | ✅ Works (referenced) | refs=4 |
| `temp_dir` | `FunctionDef` | `tests/conftest.py:104` | ✅ Works (referenced) | refs=32 |
| `setup_logging` | `FunctionDef` | `tests/conftest.py:110` | ⚠️ Risky (only definition found) | refs=1 |
| `test_delta_write_and_read` | `FunctionDef` | `tests/delta_test.py:4` | ⚠️ Risky (only definition found) | refs=1 |
| `fetch_secret` | `FunctionDef` | `tests/dev_secret_probe.py:13` | ✅ Works (referenced) | refs=2 |
| `spark` | `FunctionDef` | `tests/test_bronze_to_silver.py:17` | ✅ Works (referenced) | refs=3266 |
| `sample_customers_df` | `FunctionDef` | `tests/test_bronze_to_silver.py:23` | ✅ Works (referenced) | refs=5 |
| `sample_orders_df` | `FunctionDef` | `tests/test_bronze_to_silver.py:95` | ✅ Works (referenced) | refs=4 |
| `test_transform_customers_bronze_to_silver` | `FunctionDef` | `tests/test_bronze_to_silver.py:120` | ⚠️ Risky (only definition found) | refs=1 |
| `test_transform_orders_bronze_to_silver` | `FunctionDef` | `tests/test_bronze_to_silver.py:137` | ⚠️ Risky (only definition found) | refs=1 |
| `test_transform_products_bronze_to_silver` | `FunctionDef` | `tests/test_bronze_to_silver.py:158` | ⚠️ Risky (only definition found) | refs=1 |
| `mock_spark` | `FunctionDef` | `tests/test_bronze_to_silver_behavior.py:14` | ✅ Works (referenced) | refs=52 |
| `sample_bronze_behavior_data` | `FunctionDef` | `tests/test_bronze_to_silver_behavior.py:20` | ✅ Works (referenced) | refs=5 |
| `mock_config` | `FunctionDef` | `tests/test_bronze_to_silver_behavior.py:85` | ✅ Works (referenced) | refs=84 |
| `test_transform_bronze_to_silver_behavior_success` | `FunctionDef` | `tests/test_bronze_to_silver_behavior.py:101` | ⚠️ Risky (only definition found) | refs=1 |
| `test_transform_handles_missing_bronze_data` | `FunctionDef` | `tests/test_bronze_to_silver_behavior.py:145` | ⚠️ Risky (only definition found) | refs=1 |
| `test_transform_with_ge_failure` | `FunctionDef` | `tests/test_bronze_to_silver_behavior.py:164` | ⚠️ Risky (only definition found) | refs=1 |
| `spark` | `FunctionDef` | `tests/test_bronze_to_silver_orders.py:14` | ✅ Works (referenced) | refs=3266 |
| `test_orders_null_order_id_filtered` | `FunctionDef` | `tests/test_bronze_to_silver_orders.py:19` | ⚠️ Risky (only definition found) | refs=1 |
| `test_orders_dq_gate_passes` | `FunctionDef` | `tests/test_bronze_to_silver_orders.py:52` | ⚠️ Risky (only definition found) | refs=1 |
| `test_orders_dq_gate_fails_on_null` | `FunctionDef` | `tests/test_bronze_to_silver_orders.py:87` | ⚠️ Risky (only definition found) | refs=1 |
| `mock_config` | `FunctionDef` | `tests/test_collect_run_summary.py:11` | ✅ Works (referenced) | refs=84 |
| `test_collect_run_summary_success` | `FunctionDef` | `tests/test_collect_run_summary.py:23` | ⚠️ Risky (only definition found) | refs=1 |
| `test_collect_run_summary_handles_errors` | `FunctionDef` | `tests/test_collect_run_summary.py:56` | ⚠️ Risky (only definition found) | refs=1 |
| `sample_config_file` | `FunctionDef` | `tests/test_config.py:13` | ✅ Works (referenced) | refs=3 |
| `test_load_conf_from_file` | `FunctionDef` | `tests/test_config.py:22` | ⚠️ Risky (only definition found) | refs=1 |
| `test_load_conf_file_not_found` | `FunctionDef` | `tests/test_config.py:31` | ⚠️ Risky (only definition found) | refs=1 |
| `get_config_files` | `FunctionDef` | `tests/test_config_validation.py:15` | ✅ Works (referenced) | refs=4 |
| `test_config_loads_without_error` | `FunctionDef` | `tests/test_config_validation.py:22` | ⚠️ Risky (only definition found) | refs=1 |
| `test_config_has_lake_bucket` | `FunctionDef` | `tests/test_config_validation.py:32` | ⚠️ Risky (only definition found) | refs=1 |
| `test_config_has_region` | `FunctionDef` | `tests/test_config_validation.py:45` | ⚠️ Risky (only definition found) | refs=1 |
| `test_local_config_has_all_paths` | `FunctionDef` | `tests/test_config_validation.py:55` | ⚠️ Risky (only definition found) | refs=1 |
| `test_local_config_has_ingestion_settings` | `FunctionDef` | `tests/test_config_validation.py:70` | ⚠️ Risky (only definition found) | refs=1 |
| `test_config_schema_validity` | `FunctionDef` | `tests/test_config_validation.py:86` | ⚠️ Risky (only definition found) | refs=1 |
| `test_prod_config_secrets_references` | `FunctionDef` | `tests/test_config_validation.py:98` | ⚠️ Risky (only definition found) | refs=1 |
| `test_config_spark_settings` | `FunctionDef` | `tests/test_config_validation.py:116` | ⚠️ Risky (only definition found) | refs=1 |
| `test_returns_schema` | `FunctionDef` | `tests/test_contracts.py:6` | ⚠️ Risky (only definition found) | refs=1 |
| `spark` | `FunctionDef` | `tests/test_contracts_customers.py:12` | ✅ Works (referenced) | refs=3266 |
| `test_customers_contract` | `FunctionDef` | `tests/test_contracts_customers.py:17` | ✅ Works (referenced) | refs=3 |
| `test_customers_contract_missing_required` | `FunctionDef` | `tests/test_contracts_customers.py:38` | ⚠️ Risky (only definition found) | refs=1 |
| `test_customers_contract_null_filtering` | `FunctionDef` | `tests/test_contracts_customers.py:56` | ⚠️ Risky (only definition found) | refs=1 |
| `test_all_dags_import_fast` | `FunctionDef` | `tests/test_dag_imports.py:16` | ⚠️ Risky (only definition found) | refs=1 |
| `test_dag_syntax_validation` | `FunctionDef` | `tests/test_dag_imports.py:46` | ⚠️ Risky (only definition found) | refs=1 |
| `test_dag_configuration` | `FunctionDef` | `tests/test_dag_imports.py:68` | ⚠️ Risky (only definition found) | refs=1 |
| `test_assert_non_null` | `FunctionDef` | `tests/test_dq_checks.py:9` | ⚠️ Risky (only definition found) | refs=1 |
| `test_assert_unique` | `FunctionDef` | `tests/test_dq_checks.py:19` | ⚠️ Risky (only definition found) | refs=1 |
| `test_dq_yaml_schema_validation` | `FunctionDef` | `tests/test_dq_policies.py:11` | ⚠️ Risky (only definition found) | refs=1 |
| `test_dq_policy_naming_convention` | `FunctionDef` | `tests/test_dq_policies.py:62` | ⚠️ Risky (only definition found) | refs=1 |
| `test_dq_rule_configuration` | `FunctionDef` | `tests/test_dq_policies.py:84` | ⚠️ Risky (only definition found) | refs=1 |
| `test_dq_table_name_consistency` | `FunctionDef` | `tests/test_dq_policies.py:139` | ⚠️ Risky (only definition found) | refs=1 |
| `test_dq_yaml_syntax` | `FunctionDef` | `tests/test_dq_policies.py:184` | ⚠️ Risky (only definition found) | refs=1 |
| `test_extract_customers` | `FunctionDef` | `tests/test_extract.py:8` | ⚠️ Risky (only definition found) | refs=1 |
| `test_extract_products` | `FunctionDef` | `tests/test_extract.py:15` | ⚠️ Risky (only definition found) | refs=1 |
| `test_extract_orders_json` | `FunctionDef` | `tests/test_extract.py:26` | ⚠️ Risky (only definition found) | refs=1 |
| `test_extract_returns` | `FunctionDef` | `tests/test_extract.py:38` | ⚠️ Risky (only definition found) | refs=1 |
| `test_extract_exchange_rates` | `FunctionDef` | `tests/test_extract.py:44` | ⚠️ Risky (only definition found) | refs=1 |
| `test_extract_inventory_snapshots` | `FunctionDef` | `tests/test_extract.py:51` | ⚠️ Risky (only definition found) | refs=1 |
| `spark_session` | `FunctionDef` | `tests/test_fx_transform.py:18` | ✅ Works (referenced) | refs=177 |
| `sample_fx_data` | `FunctionDef` | `tests/test_fx_transform.py:32` | ✅ Works (referenced) | refs=8 |
| `test_deduplicate_fx_rates` | `FunctionDef` | `tests/test_fx_transform.py:56` | ⚠️ Risky (only definition found) | refs=1 |
| `test_add_rate_categories` | `FunctionDef` | `tests/test_fx_transform.py:79` | ⚠️ Risky (only definition found) | refs=1 |
| `test_validate_fx_rates_valid_data` | `FunctionDef` | `tests/test_fx_transform.py:93` | ⚠️ Risky (only definition found) | refs=1 |
| `test_validate_fx_rates_null_values` | `FunctionDef` | `tests/test_fx_transform.py:99` | ⚠️ Risky (only definition found) | refs=1 |
| `test_validate_fx_rates_negative_rates` | `FunctionDef` | `tests/test_fx_transform.py:122` | ⚠️ Risky (only definition found) | refs=1 |
| `test_validate_fx_rates_duplicates` | `FunctionDef` | `tests/test_fx_transform.py:143` | ⚠️ Risky (only definition found) | refs=1 |
| `test_glue_database_names` | `FunctionDef` | `tests/test_glue_catalog_contract.py:14` | ⚠️ Risky (only definition found) | refs=1 |
| `test_s3_path_alignment` | `FunctionDef` | `tests/test_glue_catalog_contract.py:27` | ⚠️ Risky (only definition found) | refs=1 |
| `test_table_naming_convention` | `FunctionDef` | `tests/test_glue_catalog_contract.py:43` | ⚠️ Risky (only definition found) | refs=1 |
| `test_checkpoint_path_consistency` | `FunctionDef` | `tests/test_glue_catalog_contract.py:84` | ⚠️ Risky (only definition found) | refs=1 |
| `test_airflow_variable_consistency` | `FunctionDef` | `tests/test_glue_catalog_contract.py:104` | ⚠️ Risky (only definition found) | refs=1 |
| `test_config_file_alignment` | `FunctionDef` | `tests/test_glue_catalog_contract.py:123` | ⚠️ Risky (only definition found) | refs=1 |
| `spark` | `FunctionDef` | `tests/test_gold_contract.py:24` | ✅ Works (referenced) | refs=3266 |
| `schema_validator` | `FunctionDef` | `tests/test_gold_contract.py:42` | ✅ Works (referenced) | refs=29 |
| `fact_orders_contract` | `FunctionDef` | `tests/test_gold_contract.py:48` | ✅ Works (referenced) | refs=3 |
| `test_bronze_schema_validation` | `FunctionDef` | `tests/test_gold_contract.py:72` | ⚠️ Risky (only definition found) | refs=1 |
| `test_silver_schema_compatibility` | `FunctionDef` | `tests/test_gold_contract.py:84` | ⚠️ Risky (only definition found) | refs=1 |
| `test_gold_schema_contract` | `FunctionDef` | `tests/test_gold_contract.py:143` | ⚠️ Risky (only definition found) | refs=1 |
| `test_schema_contract_file_not_found` | `FunctionDef` | `tests/test_gold_contract.py:195` | ⚠️ Risky (only definition found) | refs=1 |
| `test_ge_runner_init` | `FunctionDef` | `tests/test_great_expectations_runner.py:12` | ✅ Works (referenced) | refs=3 |
| `test_ge_runner_init_context_success` | `FunctionDef` | `tests/test_great_expectations_runner.py:21` | ⚠️ Risky (only definition found) | refs=1 |
| `test_ge_runner_init_context_missing` | `FunctionDef` | `tests/test_great_expectations_runner.py:37` | ⚠️ Risky (only definition found) | refs=1 |
| `test_ge_runner_run_checkpoint_no_context` | `FunctionDef` | `tests/test_great_expectations_runner.py:51` | ⚠️ Risky (only definition found) | refs=1 |
| `test_ge_runner_run_checkpoint_success` | `FunctionDef` | `tests/test_great_expectations_runner.py:63` | ⚠️ Risky (only definition found) | refs=1 |
| `test_ge_runner_run_checkpoint_failure` | `FunctionDef` | `tests/test_great_expectations_runner.py:78` | ⚠️ Risky (only definition found) | refs=1 |
| `test_run_dq_checkpoint_convenience` | `FunctionDef` | `tests/test_great_expectations_runner.py:91` | ⚠️ Risky (only definition found) | refs=1 |
| `test_config_loader` | `FunctionDef` | `tests/test_integration.py:25` | ✅ Works (referenced) | refs=2 |
| `test_spark_session` | `FunctionDef` | `tests/test_integration.py:40` | ✅ Works (referenced) | refs=2 |
| `test_enterprise_platform` | `FunctionDef` | `tests/test_integration.py:53` | ✅ Works (referenced) | refs=2 |
| `test_data_quality` | `FunctionDef` | `tests/test_integration.py:70` | ✅ Works (referenced) | refs=4 |
| `test_etl_pipeline` | `FunctionDef` | `tests/test_integration.py:84` | ✅ Works (referenced) | refs=2 |
| `test_streaming_import` | `FunctionDef` | `tests/test_integration.py:105` | ✅ Works (referenced) | refs=2 |
| `main` | `FunctionDef` | `tests/test_integration.py:120` | ✅ Works (referenced) | refs=480 |
| `ComprehensiveIntegrationTest` | `ClassDef` | `tests/test_integration_comprehensive.py:34` | ✅ Works (referenced) | refs=2 |
| `__init__` | `FunctionDef` | `tests/test_integration_comprehensive.py:37` | ✅ Works (referenced) | refs=162 |
| `setup` | `FunctionDef` | `tests/test_integration_comprehensive.py:45` | ✅ Works (referenced) | refs=136 |
| `_create_test_directories` | `FunctionDef` | `tests/test_integration_comprehensive.py:66` | ✅ Works (referenced) | refs=2 |
| `_create_sample_data` | `FunctionDef` | `tests/test_integration_comprehensive.py:82` | ✅ Works (referenced) | refs=2 |
| `_save_sample_data` | `FunctionDef` | `tests/test_integration_comprehensive.py:489` | ✅ Works (referenced) | refs=7 |
| `test_full_etl_pipeline` | `FunctionDef` | `tests/test_integration_comprehensive.py:494` | ✅ Works (referenced) | refs=2 |
| `_count_files_in_layer` | `FunctionDef` | `tests/test_integration_comprehensive.py:531` | ✅ Works (referenced) | refs=4 |
| `test_data_quality_suite` | `FunctionDef` | `tests/test_integration_comprehensive.py:543` | ✅ Works (referenced) | refs=2 |
| `test_data_contracts` | `FunctionDef` | `tests/test_integration_comprehensive.py:573` | ✅ Works (referenced) | refs=2 |
| `test_disaster_recovery` | `FunctionDef` | `tests/test_integration_comprehensive.py:604` | ✅ Works (referenced) | refs=2 |
| `test_performance_optimization` | `FunctionDef` | `tests/test_integration_comprehensive.py:662` | ✅ Works (referenced) | refs=3 |
| `test_metrics_integration` | `FunctionDef` | `tests/test_integration_comprehensive.py:696` | ✅ Works (referenced) | refs=2 |
| `test_streaming_integration` | `FunctionDef` | `tests/test_integration_comprehensive.py:738` | ✅ Works (referenced) | refs=2 |
| `test_incremental_loading` | `FunctionDef` | `tests/test_integration_comprehensive.py:778` | ✅ Works (referenced) | refs=2 |
| `run_all_tests` | `FunctionDef` | `tests/test_integration_comprehensive.py:806` | ✅ Works (referenced) | refs=2 |
| `cleanup` | `FunctionDef` | `tests/test_integration_comprehensive.py:847` | ✅ Works (referenced) | refs=29 |
| `main` | `FunctionDef` | `tests/test_integration_comprehensive.py:855` | ✅ Works (referenced) | refs=480 |
| `mock_config` | `FunctionDef` | `tests/test_lineage_emitter.py:18` | ✅ Works (referenced) | refs=84 |
| `test_emit_lineage_event_success` | `FunctionDef` | `tests/test_lineage_emitter.py:24` | ⚠️ Risky (only definition found) | refs=1 |
| `test_emit_lineage_event_disabled` | `FunctionDef` | `tests/test_lineage_emitter.py:40` | ⚠️ Risky (only definition found) | refs=1 |
| `test_emit_lineage_event_no_url` | `FunctionDef` | `tests/test_lineage_emitter.py:51` | ⚠️ Risky (only definition found) | refs=1 |
| `test_emit_start` | `FunctionDef` | `tests/test_lineage_emitter.py:63` | ⚠️ Risky (only definition found) | refs=1 |
| `test_emit_complete` | `FunctionDef` | `tests/test_lineage_emitter.py:78` | ⚠️ Risky (only definition found) | refs=1 |
| `test_emit_fail` | `FunctionDef` | `tests/test_lineage_emitter.py:94` | ⚠️ Risky (only definition found) | refs=1 |
| `test_write_parquet` | `FunctionDef` | `tests/test_load.py:6` | ⚠️ Risky (only definition found) | refs=1 |
| `test_write_avro` | `FunctionDef` | `tests/test_load.py:13` | ⚠️ Risky (only definition found) | refs=1 |
| `test_write_json` | `FunctionDef` | `tests/test_load.py:20` | ⚠️ Risky (only definition found) | refs=1 |
| `mock_config` | `FunctionDef` | `tests/test_metrics_collector.py:13` | ✅ Works (referenced) | refs=84 |
| `test_emit_rowcount_cloudwatch` | `FunctionDef` | `tests/test_metrics_collector.py:22` | ⚠️ Risky (only definition found) | refs=1 |
| `test_emit_rowcount_local` | `FunctionDef` | `tests/test_metrics_collector.py:33` | ⚠️ Risky (only definition found) | refs=1 |
| `test_emit_duration` | `FunctionDef` | `tests/test_metrics_collector.py:42` | ⚠️ Risky (only definition found) | refs=1 |
| `test_emit_metrics` | `FunctionDef` | `tests/test_metrics_collector.py:53` | ⚠️ Risky (only definition found) | refs=1 |
| `test_project_a_imports` | `FunctionDef` | `tests/test_module_imports.py:14` | ⚠️ Risky (only definition found) | refs=1 |
| `test_pipeline_imports` | `FunctionDef` | `tests/test_module_imports.py:24` | ⚠️ Risky (only definition found) | refs=1 |
| `test_utils_imports` | `FunctionDef` | `tests/test_module_imports.py:35` | ⚠️ Risky (only definition found) | refs=1 |
| `test_jobs_imports` | `FunctionDef` | `tests/test_module_imports.py:47` | ⚠️ Risky (only definition found) | refs=1 |
| `test_no_src_prefix_imports` | `FunctionDef` | `tests/test_module_imports.py:59` | ⚠️ Risky (only definition found) | refs=1 |
| `TestMetricsCollection` | `ClassDef` | `tests/test_monitoring.py:28` | ⚠️ Risky (only definition found) | refs=1 |
| `test_track_job_execution_success` | `FunctionDef` | `tests/test_monitoring.py:31` | ⚠️ Risky (only definition found) | refs=1 |
| `successful_job` | `FunctionDef` | `tests/test_monitoring.py:35` | ✅ Works (referenced) | refs=2 |
| `test_track_job_execution_failure` | `FunctionDef` | `tests/test_monitoring.py:50` | ⚠️ Risky (only definition found) | refs=1 |
| `failing_job` | `FunctionDef` | `tests/test_monitoring.py:54` | ✅ Works (referenced) | refs=2 |
| `test_track_stage_duration` | `FunctionDef` | `tests/test_monitoring.py:69` | ✅ Works (referenced) | refs=2 |
| `test_track_stage_duration_with_error` | `FunctionDef` | `tests/test_monitoring.py:78` | ⚠️ Risky (only definition found) | refs=1 |
| `test_record_dq_check_passed` | `FunctionDef` | `tests/test_monitoring.py:91` | ⚠️ Risky (only definition found) | refs=1 |
| `test_record_dq_check_failed_with_violations` | `FunctionDef` | `tests/test_monitoring.py:101` | ⚠️ Risky (only definition found) | refs=1 |
| `test_record_records_processed` | `FunctionDef` | `tests/test_monitoring.py:114` | ⚠️ Risky (only definition found) | refs=1 |
| `test_record_records_failed` | `FunctionDef` | `tests/test_monitoring.py:123` | ⚠️ Risky (only definition found) | refs=1 |
| `test_record_delta_table_metrics` | `FunctionDef` | `tests/test_monitoring.py:131` | ⚠️ Risky (only definition found) | refs=1 |
| `test_record_delta_write` | `FunctionDef` | `tests/test_monitoring.py:145` | ⚠️ Risky (only definition found) | refs=1 |
| `test_record_schema_drift` | `FunctionDef` | `tests/test_monitoring.py:154` | ⚠️ Risky (only definition found) | refs=1 |
| `test_record_error` | `FunctionDef` | `tests/test_monitoring.py:162` | ⚠️ Risky (only definition found) | refs=1 |
| `test_get_metrics_text_format` | `FunctionDef` | `tests/test_monitoring.py:171` | ⚠️ Risky (only definition found) | refs=1 |
| `test_multiple_metrics_recording` | `FunctionDef` | `tests/test_monitoring.py:181` | ⚠️ Risky (only definition found) | refs=1 |
| `test_metrics_with_special_characters` | `FunctionDef` | `tests/test_monitoring.py:196` | ⚠️ Risky (only definition found) | refs=1 |
| `spark` | `FunctionDef` | `tests/test_orders_transform.py:20` | ✅ Works (referenced) | refs=3266 |
| `test_orders_basic` | `FunctionDef` | `tests/test_orders_transform.py:30` | ⚠️ Risky (only definition found) | refs=1 |
| `test_orders_with_nested_payment` | `FunctionDef` | `tests/test_orders_transform.py:85` | ⚠️ Risky (only definition found) | refs=1 |
| `test_orders_filters_invalid_data` | `FunctionDef` | `tests/test_orders_transform.py:124` | ⚠️ Risky (only definition found) | refs=1 |
| `test_orders_deduplication` | `FunctionDef` | `tests/test_orders_transform.py:188` | ⚠️ Risky (only definition found) | refs=1 |
| `mock_spark_session` | `FunctionDef` | `tests/test_performance_optimization.py:29` | ✅ Works (referenced) | refs=37 |
| `sample_dataframe` | `FunctionDef` | `tests/test_performance_optimization.py:45` | ✅ Works (referenced) | refs=27 |
| `test_config` | `FunctionDef` | `tests/test_performance_optimization.py:73` | ✅ Works (referenced) | refs=31 |
| `TestCacheManager` | `ClassDef` | `tests/test_performance_optimization.py:88` | ⚠️ Risky (only definition found) | refs=1 |
| `test_smart_cache_auto_strategy` | `FunctionDef` | `tests/test_performance_optimization.py:91` | ⚠️ Risky (only definition found) | refs=1 |
| `test_smart_cache_memory_strategy` | `FunctionDef` | `tests/test_performance_optimization.py:107` | ⚠️ Risky (only definition found) | refs=1 |
| `test_cache_hit_ratio` | `FunctionDef` | `tests/test_performance_optimization.py:116` | ⚠️ Risky (only definition found) | refs=1 |
| `test_unpersist_dataset` | `FunctionDef` | `tests/test_performance_optimization.py:127` | ⚠️ Risky (only definition found) | refs=1 |
| `test_clear_all_caches` | `FunctionDef` | `tests/test_performance_optimization.py:139` | ⚠️ Risky (only definition found) | refs=1 |
| `TestPerformanceBenchmark` | `ClassDef` | `tests/test_performance_optimization.py:154` | ⚠️ Risky (only definition found) | refs=1 |
| `test_benchmark_dataset_basic_operations` | `FunctionDef` | `tests/test_performance_optimization.py:157` | ⚠️ Risky (only definition found) | refs=1 |
| `test_benchmark_dataset_custom_operations` | `FunctionDef` | `tests/test_performance_optimization.py:170` | ⚠️ Risky (only definition found) | refs=1 |
| `test_benchmark_dataset_error_handling` | `FunctionDef` | `tests/test_performance_optimization.py:180` | ⚠️ Risky (only definition found) | refs=1 |
| `test_export_benchmark_results` | `FunctionDef` | `tests/test_performance_optimization.py:193` | ⚠️ Risky (only definition found) | refs=1 |
| `TestPerformanceOptimizer` | `ClassDef` | `tests/test_performance_optimization.py:215` | ✅ Works (referenced) | refs=2 |
| `test_optimize_returns_raw` | `FunctionDef` | `tests/test_performance_optimization.py:218` | ⚠️ Risky (only definition found) | refs=1 |
| `test_optimize_shuffle_partitions` | `FunctionDef` | `tests/test_performance_optimization.py:244` | ⚠️ Risky (only definition found) | refs=1 |
| `test_optimize_file_compaction` | `FunctionDef` | `tests/test_performance_optimization.py:261` | ⚠️ Risky (only definition found) | refs=1 |
| `test_run_full_optimization_pipeline` | `FunctionDef` | `tests/test_performance_optimization.py:278` | ⚠️ Risky (only definition found) | refs=1 |
| `test_get_performance_summary` | `FunctionDef` | `tests/test_performance_optimization.py:298` | ⚠️ Risky (only definition found) | refs=1 |
| `TestPerformanceOptimizerIntegration` | `ClassDef` | `tests/test_performance_optimization.py:332` | ⚠️ Risky (only definition found) | refs=1 |
| `test_create_performance_optimizer` | `FunctionDef` | `tests/test_performance_optimization.py:335` | ⚠️ Risky (only definition found) | refs=1 |
| `test_returns_raw_scale_benchmark` | `FunctionDef` | `tests/test_performance_optimization.py:343` | ⚠️ Risky (only definition found) | refs=1 |
| `test_caching_strategy_selection` | `FunctionDef` | `tests/test_performance_optimization.py:381` | ⚠️ Risky (only definition found) | refs=1 |
| `test_mask_email` | `FunctionDef` | `tests/test_pii_utils.py:14` | ⚠️ Risky (only definition found) | refs=1 |
| `test_mask_phone` | `FunctionDef` | `tests/test_pii_utils.py:23` | ⚠️ Risky (only definition found) | refs=1 |
| `test_hash_value` | `FunctionDef` | `tests/test_pii_utils.py:32` | ⚠️ Risky (only definition found) | refs=1 |
| `test_mask_name` | `FunctionDef` | `tests/test_pii_utils.py:42` | ⚠️ Risky (only definition found) | refs=1 |
| `test_apply_pii_masking` | `FunctionDef` | `tests/test_pii_utils.py:50` | ⚠️ Risky (only definition found) | refs=1 |
| `TestPipelineComponents` | `ClassDef` | `tests/test_pipeline_components.py:23` | ⚠️ Risky (only definition found) | refs=1 |
| `setup_method` | `FunctionDef` | `tests/test_pipeline_components.py:26` | ⚠️ Risky (only definition found) | refs=1 |
| `teardown_method` | `FunctionDef` | `tests/test_pipeline_components.py:40` | ⚠️ Risky (only definition found) | refs=1 |
| `test_logging_setup` | `FunctionDef` | `tests/test_pipeline_components.py:46` | ⚠️ Risky (only definition found) | refs=1 |
| `test_paths_configuration` | `FunctionDef` | `tests/test_pipeline_components.py:56` | ⚠️ Risky (only definition found) | refs=1 |
| `test_bronze_to_silver_import` | `FunctionDef` | `tests/test_pipeline_components.py:63` | ⚠️ Risky (only definition found) | refs=1 |
| `test_silver_to_gold_import` | `FunctionDef` | `tests/test_pipeline_components.py:72` | ⚠️ Risky (only definition found) | refs=1 |
| `test_end_to_end` | `FunctionDef` | `tests/test_pipeline_integration.py:1` | ✅ Works (referenced) | refs=2 |
| `mock_config` | `FunctionDef` | `tests/test_publish_gold_to_snowflake.py:14` | ✅ Works (referenced) | refs=84 |
| `sample_gold_data` | `FunctionDef` | `tests/test_publish_gold_to_snowflake.py:26` | ✅ Works (referenced) | refs=7 |
| `test_load_to_snowflake_success` | `FunctionDef` | `tests/test_publish_gold_to_snowflake.py:51` | ⚠️ Risky (only definition found) | refs=1 |
| `test_load_to_snowflake_missing_data` | `FunctionDef` | `tests/test_publish_gold_to_snowflake.py:98` | ⚠️ Risky (only definition found) | refs=1 |
| `TestQualityGate` | `ClassDef` | `tests/test_quality_gate.py:15` | ⚠️ Risky (only definition found) | refs=1 |
| `spark` | `FunctionDef` | `tests/test_quality_gate.py:19` | ✅ Works (referenced) | refs=3266 |
| `quality_thresholds` | `FunctionDef` | `tests/test_quality_gate.py:27` | ✅ Works (referenced) | refs=9 |
| `test_silver_orders_quality_gate` | `FunctionDef` | `tests/test_quality_gate.py:35` | ⚠️ Risky (only definition found) | refs=1 |
| `test_silver_fx_rates_quality_gate` | `FunctionDef` | `tests/test_quality_gate.py:67` | ⚠️ Risky (only definition found) | refs=1 |
| `test_gold_revenue_quality_gate` | `FunctionDef` | `tests/test_quality_gate.py:93` | ⚠️ Risky (only definition found) | refs=1 |
| `test_critical_checks_defined` | `FunctionDef` | `tests/test_quality_gate.py:119` | ⚠️ Risky (only definition found) | refs=1 |
| `spark` | `FunctionDef` | `tests/test_safe_writer.py:24` | ✅ Works (referenced) | refs=3266 |
| `temp_dir` | `FunctionDef` | `tests/test_safe_writer.py:44` | ✅ Works (referenced) | refs=32 |
| `sample_schema` | `FunctionDef` | `tests/test_safe_writer.py:52` | ✅ Works (referenced) | refs=9 |
| `sample_data` | `FunctionDef` | `tests/test_safe_writer.py:66` | ✅ Works (referenced) | refs=59 |
| `TestSafeDeltaWriter` | `ClassDef` | `tests/test_safe_writer.py:76` | ⚠️ Risky (only definition found) | refs=1 |
| `test_writer_initialization` | `FunctionDef` | `tests/test_safe_writer.py:79` | ⚠️ Risky (only definition found) | refs=1 |
| `test_write_new_table_append` | `FunctionDef` | `tests/test_safe_writer.py:84` | ⚠️ Risky (only definition found) | refs=1 |
| `test_write_merge_upsert` | `FunctionDef` | `tests/test_safe_writer.py:102` | ⚠️ Risky (only definition found) | refs=1 |
| `test_write_with_replace_where` | `FunctionDef` | `tests/test_safe_writer.py:140` | ⚠️ Risky (only definition found) | refs=1 |
| `test_write_without_merge_keys_fails` | `FunctionDef` | `tests/test_safe_writer.py:176` | ⚠️ Risky (only definition found) | refs=1 |
| `test_write_overwrite_without_replace_where_fails` | `FunctionDef` | `tests/test_safe_writer.py:192` | ⚠️ Risky (only definition found) | refs=1 |
| `test_write_with_pre_hook` | `FunctionDef` | `tests/test_safe_writer.py:212` | ⚠️ Risky (only definition found) | refs=1 |
| `pre_hook` | `FunctionDef` | `tests/test_safe_writer.py:217` | ✅ Works (referenced) | refs=3 |
| `test_write_with_post_hook` | `FunctionDef` | `tests/test_safe_writer.py:238` | ⚠️ Risky (only definition found) | refs=1 |
| `post_hook` | `FunctionDef` | `tests/test_safe_writer.py:245` | ✅ Works (referenced) | refs=3 |
| `test_row_count_validation` | `FunctionDef` | `tests/test_safe_writer.py:260` | ⚠️ Risky (only definition found) | refs=1 |
| `test_multiple_merge_operations` | `FunctionDef` | `tests/test_safe_writer.py:274` | ⚠️ Risky (only definition found) | refs=1 |
| `TestSCD2Standardized` | `ClassDef` | `tests/test_scd2.py:18` | ⚠️ Risky (only definition found) | refs=1 |
| `spark` | `FunctionDef` | `tests/test_scd2.py:22` | ✅ Works (referenced) | refs=3266 |
| `scd2_config` | `FunctionDef` | `tests/test_scd2.py:32` | ✅ Works (referenced) | refs=24 |
| `sample_customer_data` | `FunctionDef` | `tests/test_scd2.py:46` | ✅ Works (referenced) | refs=12 |
| `updated_customer_data` | `FunctionDef` | `tests/test_scd2.py:66` | ✅ Works (referenced) | refs=3 |
| `late_arriving_data` | `FunctionDef` | `tests/test_scd2.py:86` | ✅ Works (referenced) | refs=4 |
| `test_scd2_initial_load` | `FunctionDef` | `tests/test_scd2.py:104` | ⚠️ Risky (only definition found) | refs=1 |
| `test_scd2_incremental_update` | `FunctionDef` | `tests/test_scd2.py:134` | ⚠️ Risky (only definition found) | refs=1 |
| `test_scd2_late_arriving_data` | `FunctionDef` | `tests/test_scd2.py:174` | ⚠️ Risky (only definition found) | refs=1 |
| `test_scd2_validation` | `FunctionDef` | `tests/test_scd2.py:206` | ⚠️ Risky (only definition found) | refs=1 |
| `test_scd2_no_changes` | `FunctionDef` | `tests/test_scd2.py:220` | ⚠️ Risky (only definition found) | refs=1 |
| `test_scd2_missing_columns` | `FunctionDef` | `tests/test_scd2.py:239` | ⚠️ Risky (only definition found) | refs=1 |
| `test_scd2_empty_dataframe` | `FunctionDef` | `tests/test_scd2.py:259` | ⚠️ Risky (only definition found) | refs=1 |
| `test_scd2_with_ts_column` | `FunctionDef` | `tests/test_scd2.py:280` | ⚠️ Risky (only definition found) | refs=1 |
| `test_scd2_golden_dataset` | `FunctionDef` | `tests/test_scd2.py:318` | ⚠️ Risky (only definition found) | refs=1 |
| `TestSCD2Validation` | `ClassDef` | `tests/test_scd2_validation.py:12` | ⚠️ Risky (only definition found) | refs=1 |
| `spark` | `FunctionDef` | `tests/test_scd2_validation.py:16` | ✅ Works (referenced) | refs=3266 |
| `sample_scd2_data` | `FunctionDef` | `tests/test_scd2_validation.py:29` | ✅ Works (referenced) | refs=23 |
| `test_scd2_required_columns` | `FunctionDef` | `tests/test_scd2_validation.py:51` | ⚠️ Risky (only definition found) | refs=1 |
| `test_scd2_single_current_record_per_key` | `FunctionDef` | `tests/test_scd2_validation.py:60` | ⚠️ Risky (only definition found) | refs=1 |
| `test_scd2_no_date_overlaps` | `FunctionDef` | `tests/test_scd2_validation.py:71` | ⚠️ Risky (only definition found) | refs=1 |
| `test_scd2_current_records_have_null_effective_to` | `FunctionDef` | `tests/test_scd2_validation.py:81` | ⚠️ Risky (only definition found) | refs=1 |
| `test_scd2_historical_records_have_effective_to` | `FunctionDef` | `tests/test_scd2_validation.py:89` | ⚠️ Risky (only definition found) | refs=1 |
| `test_scd2_effective_from_before_effective_to` | `FunctionDef` | `tests/test_scd2_validation.py:97` | ⚠️ Risky (only definition found) | refs=1 |
| `test_scd2_no_duplicate_records` | `FunctionDef` | `tests/test_scd2_validation.py:105` | ⚠️ Risky (only definition found) | refs=1 |
| `test_scd2_continuous_date_ranges` | `FunctionDef` | `tests/test_scd2_validation.py:115` | ⚠️ Risky (only definition found) | refs=1 |
| `test_scd2_business_key_not_null` | `FunctionDef` | `tests/test_scd2_validation.py:125` | ⚠️ Risky (only definition found) | refs=1 |
| `test_scd2_effective_from_not_null` | `FunctionDef` | `tests/test_scd2_validation.py:131` | ⚠️ Risky (only definition found) | refs=1 |
| `test_scd2_is_current_not_null` | `FunctionDef` | `tests/test_scd2_validation.py:137` | ⚠️ Risky (only definition found) | refs=1 |
| `validate_scd2_table` | `FunctionDef` | `tests/test_scd2_validation.py:144` | ✅ Works (referenced) | refs=5 |
| `spark` | `FunctionDef` | `tests/test_schema_alignment.py:29` | ✅ Works (referenced) | refs=3266 |
| `test_orders_silver_schema_defined` | `FunctionDef` | `tests/test_schema_alignment.py:45` | ⚠️ Risky (only definition found) | refs=1 |
| `test_orders_silver_total_amount_type` | `FunctionDef` | `tests/test_schema_alignment.py:61` | ⚠️ Risky (only definition found) | refs=1 |
| `test_bronze_to_silver_transformation_mock` | `FunctionDef` | `tests/test_schema_alignment.py:78` | ⚠️ Risky (only definition found) | refs=1 |
| `test_schema_validator_primary_key_null_detection` | `FunctionDef` | `tests/test_schema_alignment.py:121` | ⚠️ Risky (only definition found) | refs=1 |
| `test_schema_validator_duplicate_key_detection` | `FunctionDef` | `tests/test_schema_alignment.py:135` | ⚠️ Risky (only definition found) | refs=1 |
| `test_schema_validator_column_validation` | `FunctionDef` | `tests/test_schema_alignment.py:149` | ⚠️ Risky (only definition found) | refs=1 |
| `test_dbt_silver_orders_source_expectation` | `FunctionDef` | `tests/test_schema_alignment.py:162` | ⚠️ Risky (only definition found) | refs=1 |
| `test_customers_silver_schema_defined` | `FunctionDef` | `tests/test_schema_alignment.py:192` | ⚠️ Risky (only definition found) | refs=1 |
| `test_products_silver_schema_defined` | `FunctionDef` | `tests/test_schema_alignment.py:203` | ⚠️ Risky (only definition found) | refs=1 |
| `test_schema_apply_enforcement` | `FunctionDef` | `tests/test_schema_alignment.py:214` | ⚠️ Risky (only definition found) | refs=1 |
| `test_end_to_end_schema_flow` | `FunctionDef` | `tests/test_schema_alignment.py:251` | ⚠️ Risky (only definition found) | refs=1 |
| `expected_schema` | `FunctionDef` | `tests/test_schema_validator.py:13` | ✅ Works (referenced) | refs=75 |
| `actual_schema_match` | `FunctionDef` | `tests/test_schema_validator.py:25` | ✅ Works (referenced) | refs=3 |
| `test_validate_schema_match` | `FunctionDef` | `tests/test_schema_validator.py:36` | ⚠️ Risky (only definition found) | refs=1 |
| `test_validate_schema_missing_column` | `FunctionDef` | `tests/test_schema_validator.py:48` | ⚠️ Risky (only definition found) | refs=1 |
| `spark` | `FunctionDef` | `tests/test_schemas.py:34` | ✅ Works (referenced) | refs=3266 |
| `TestSchemaRegistry` | `ClassDef` | `tests/test_schemas.py:47` | ⚠️ Risky (only definition found) | refs=1 |
| `test_get_bronze_customers_schema` | `FunctionDef` | `tests/test_schemas.py:50` | ⚠️ Risky (only definition found) | refs=1 |
| `test_get_bronze_orders_schema` | `FunctionDef` | `tests/test_schemas.py:65` | ⚠️ Risky (only definition found) | refs=1 |
| `test_get_silver_customers_schema` | `FunctionDef` | `tests/test_schemas.py:78` | ⚠️ Risky (only definition found) | refs=1 |
| `test_get_nonexistent_schema_raises_error` | `FunctionDef` | `tests/test_schemas.py:91` | ⚠️ Risky (only definition found) | refs=1 |
| `test_bronze_customers_schema_structure` | `FunctionDef` | `tests/test_schemas.py:96` | ⚠️ Risky (only definition found) | refs=1 |
| `test_schema_drift_detection_no_drift` | `FunctionDef` | `tests/test_schemas.py:116` | ⚠️ Risky (only definition found) | refs=1 |
| `test_schema_drift_detection_missing_column` | `FunctionDef` | `tests/test_schemas.py:141` | ⚠️ Risky (only definition found) | refs=1 |
| `test_schema_drift_detection_type_mismatch` | `FunctionDef` | `tests/test_schemas.py:170` | ⚠️ Risky (only definition found) | refs=1 |
| `test_schema_drift_detection_extra_column` | `FunctionDef` | `tests/test_schemas.py:205` | ⚠️ Risky (only definition found) | refs=1 |
| `test_all_registered_schemas_are_valid` | `FunctionDef` | `tests/test_schemas.py:240` | ⚠️ Risky (only definition found) | refs=1 |
| `test_bronze_orders_schema_structure` | `FunctionDef` | `tests/test_schemas.py:253` | ⚠️ Risky (only definition found) | refs=1 |
| `test_silver_customers_scd2_fields` | `FunctionDef` | `tests/test_schemas.py:271` | ⚠️ Risky (only definition found) | refs=1 |
| `test_schema_field_ordering` | `FunctionDef` | `tests/test_schemas.py:288` | ⚠️ Risky (only definition found) | refs=1 |
| `mock_config` | `FunctionDef` | `tests/test_secrets.py:18` | ✅ Works (referenced) | refs=84 |
| `test_get_secret_from_manager_success` | `FunctionDef` | `tests/test_secrets.py:34` | ⚠️ Risky (only definition found) | refs=1 |
| `test_get_secret_from_manager_not_found` | `FunctionDef` | `tests/test_secrets.py:49` | ⚠️ Risky (only definition found) | refs=1 |
| `test_get_snowflake_credentials_from_secrets` | `FunctionDef` | `tests/test_secrets.py:65` | ⚠️ Risky (only definition found) | refs=1 |
| `test_get_snowflake_credentials_from_config` | `FunctionDef` | `tests/test_secrets.py:81` | ⚠️ Risky (only definition found) | refs=1 |
| `test_get_redshift_credentials_from_secrets` | `FunctionDef` | `tests/test_secrets.py:92` | ⚠️ Risky (only definition found) | refs=1 |
| `spark` | `FunctionDef` | `tests/test_silver_behavior_contract.py:16` | ✅ Works (referenced) | refs=3266 |
| `silver_behavior_df` | `FunctionDef` | `tests/test_silver_behavior_contract.py:25` | ✅ Works (referenced) | refs=22 |
| `test_schema_matches_contract` | `FunctionDef` | `tests/test_silver_behavior_contract.py:35` | ⚠️ Risky (only definition found) | refs=1 |
| `test_event_name_is_lowercase` | `FunctionDef` | `tests/test_silver_behavior_contract.py:61` | ⚠️ Risky (only definition found) | refs=1 |
| `test_session_id_pattern` | `FunctionDef` | `tests/test_silver_behavior_contract.py:73` | ⚠️ Risky (only definition found) | refs=1 |
| `test_no_duplicates_by_event_id` | `FunctionDef` | `tests/test_silver_behavior_contract.py:87` | ⚠️ Risky (only definition found) | refs=1 |
| `test_event_ts_is_timestamp` | `FunctionDef` | `tests/test_silver_behavior_contract.py:98` | ⚠️ Risky (only definition found) | refs=1 |
| `test_exactly_expected_columns` | `FunctionDef` | `tests/test_silver_behavior_contract.py:114` | ⚠️ Risky (only definition found) | refs=1 |
| `test_types_match_contract` | `FunctionDef` | `tests/test_silver_behavior_contract.py:127` | ⚠️ Risky (only definition found) | refs=1 |
| `test_row_count_threshold` | `FunctionDef` | `tests/test_silver_behavior_contract.py:148` | ⚠️ Risky (only definition found) | refs=1 |
| `mock_config` | `FunctionDef` | `tests/test_silver_build_customer_360.py:20` | ✅ Works (referenced) | refs=84 |
| `sample_contacts_data` | `FunctionDef` | `tests/test_silver_build_customer_360.py:33` | ✅ Works (referenced) | refs=3 |
| `sample_accounts_data` | `FunctionDef` | `tests/test_silver_build_customer_360.py:73` | ✅ Works (referenced) | refs=3 |
| `sample_orders_data` | `FunctionDef` | `tests/test_silver_build_customer_360.py:93` | ✅ Works (referenced) | refs=4 |
| `sample_behavior_data` | `FunctionDef` | `tests/test_silver_build_customer_360.py:115` | ✅ Works (referenced) | refs=3 |
| `test_build_customer_360_success` | `FunctionDef` | `tests/test_silver_build_customer_360.py:135` | ⚠️ Risky (only definition found) | refs=1 |
| `test_build_customer_360_handles_missing_data` | `FunctionDef` | `tests/test_silver_build_customer_360.py:184` | ⚠️ Risky (only definition found) | refs=1 |
| `mock_config` | `FunctionDef` | `tests/test_spark_session.py:13` | ✅ Works (referenced) | refs=84 |
| `test_build_spark_success` | `FunctionDef` | `tests/test_spark_session.py:28` | ⚠️ Risky (only definition found) | refs=1 |
| `test_build_spark_minimal_config` | `FunctionDef` | `tests/test_spark_session.py:43` | ⚠️ Risky (only definition found) | refs=1 |
| `test_select_and_filter` | `FunctionDef` | `tests/test_transform.py:29` | ⚠️ Risky (only definition found) | refs=1 |
| `test_join_examples` | `FunctionDef` | `tests/test_transform.py:55` | ⚠️ Risky (only definition found) | refs=1 |
| `test_broadcast_join_demo` | `FunctionDef` | `tests/test_transform.py:85` | ⚠️ Risky (only definition found) | refs=1 |
| `test_skew_mitigation_demo` | `FunctionDef` | `tests/test_transform.py:92` | ⚠️ Risky (only definition found) | refs=1 |
| `test_partitioning_examples` | `FunctionDef` | `tests/test_transform.py:100` | ⚠️ Risky (only definition found) | refs=1 |
| `test_window_functions_demo` | `FunctionDef` | `tests/test_transform.py:106` | ⚠️ Risky (only definition found) | refs=1 |
| `test_udf_and_cleaning` | `FunctionDef` | `tests/test_transform.py:118` | ⚠️ Risky (only definition found) | refs=1 |
| `test_sql_vs_dsl_demo` | `FunctionDef` | `tests/test_transform.py:134` | ⚠️ Risky (only definition found) | refs=1 |
| `test_normalize_currency_and_returns` | `FunctionDef` | `tests/test_transform.py:147` | ⚠️ Risky (only definition found) | refs=1 |
| `test_join_inventory_and_enrich` | `FunctionDef` | `tests/test_transform.py:181` | ⚠️ Risky (only definition found) | refs=1 |
| `test_build_fact_orders` | `FunctionDef` | `tests/test_transform.py:200` | ⚠️ Risky (only definition found) | refs=1 |
| `test_build_customers_scd2` | `FunctionDef` | `tests/test_transform.py:218` | ⚠️ Risky (only definition found) | refs=1 |
| `test_validate_parquet` | `FunctionDef` | `tests/test_validate.py:4` | ⚠️ Risky (only definition found) | refs=1 |
| `test_validate_avro` | `FunctionDef` | `tests/test_validate.py:12` | ⚠️ Risky (only definition found) | refs=1 |
| `test_validate_json` | `FunctionDef` | `tests/test_validate.py:20` | ⚠️ Risky (only definition found) | refs=1 |
| `mock_config` | `FunctionDef` | `tests/test_watermark_utils.py:19` | ✅ Works (referenced) | refs=84 |
| `test_get_watermark_first_run` | `FunctionDef` | `tests/test_watermark_utils.py:24` | ⚠️ Risky (only definition found) | refs=1 |
| `test_get_latest_timestamp_from_df` | `FunctionDef` | `tests/test_watermark_utils.py:32` | ⚠️ Risky (only definition found) | refs=1 |
| `test_upsert_watermark` | `FunctionDef` | `tests/test_watermark_utils.py:51` | ⚠️ Risky (only definition found) | refs=1 |
| `test_get_watermark_with_existing` | `FunctionDef` | `tests/test_watermark_utils.py:61` | ⚠️ Risky (only definition found) | refs=1 |
| `read_table` | `FunctionDef` | `tools/validate_aws_etl.py:50` | ✅ Works (referenced) | refs=9 |
| `print_table_report` | `FunctionDef` | `tools/validate_aws_etl.py:178` | ✅ Works (referenced) | refs=6 |
| `check_null_percentage` | `FunctionDef` | `tools/validate_aws_etl.py:258` | ✅ Works (referenced) | refs=8 |
| `check_join_integrity` | `FunctionDef` | `tools/validate_aws_etl.py:280` | ✅ Works (referenced) | refs=2 |
| `validate_silver_layer` | `FunctionDef` | `tools/validate_aws_etl.py:347` | ✅ Works (referenced) | refs=6 |
| `validate_gold_layer` | `FunctionDef` | `tools/validate_aws_etl.py:457` | ✅ Works (referenced) | refs=6 |
| `main` | `FunctionDef` | `tools/validate_aws_etl.py:569` | ✅ Works (referenced) | refs=480 |
| `read_table` | `FunctionDef` | `tools/validate_local_etl.py:42` | ✅ Works (referenced) | refs=9 |
| `print_table_report` | `FunctionDef` | `tools/validate_local_etl.py:142` | ✅ Works (referenced) | refs=6 |
| `check_null_percentage` | `FunctionDef` | `tools/validate_local_etl.py:222` | ✅ Works (referenced) | refs=8 |
| `check_join_integrity` | `FunctionDef` | `tools/validate_local_etl.py:244` | ✅ Works (referenced) | refs=2 |
| `validate_silver_layer` | `FunctionDef` | `tools/validate_local_etl.py:311` | ✅ Works (referenced) | refs=6 |
| `validate_gold_layer` | `FunctionDef` | `tools/validate_local_etl.py:423` | ✅ Works (referenced) | refs=6 |
| `main` | `FunctionDef` | `tools/validate_local_etl.py:535` | ✅ Works (referenced) | refs=480 |
