#!/usr/bin/env python3
"""
Snowflake Customers to Bronze Ingestion
Similar to snowflake_to_bronze.py but for customers table
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import uuid

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.config_loader import load_config_resolved
from pyspark_interview_project.utils.contracts import (
    load_schema_contract,
    validate_and_quarantine,
    add_metadata_columns
)
from pyspark_interview_project.utils.error_lanes import ErrorLaneHandler, add_row_id
from pyspark_interview_project.utils.state_store import get_state_store
from pyspark_interview_project.utils.secrets import get_snowflake_credentials


def main():
    """Main entry point."""
    config_path = Path("config/dev.yaml")
    if not config_path.exists():
        config_path = Path("config/prod.yaml")
    config = load_config_resolved(str(config_path))
    
    spark = build_spark(config)
    run_id = str(uuid.uuid4())
    
    try:
        lake_bucket = config["buckets"]["lake"]
        source_cfg = config.get("sources", {}).get("snowflake_customers", {})
        watermark_col = source_cfg.get("watermark_column", "last_modified_ts")
        watermark_key = source_cfg.get("watermark_state_key", "snowflake_customers_max_ts")
        
        # Read watermark
        state_store = get_state_store(config)
        last_ts = None
        watermark_str = state_store.get_watermark(watermark_key)
        if watermark_str:
            last_ts = datetime.fromisoformat(watermark_str.replace('Z', '+00:00'))
        
        # Read from Snowflake or local sample
        if config.get('env') == 'dev' and config.get('environment') == 'local':
            sample_path = config.get('paths', {}).get('snowflake_customers', 
                "data/samples/snowflake/customers.csv")
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_path)
        else:
            snowflake_config = get_snowflake_credentials(config)
            account = snowflake_config.get('account', '').replace('.snowflakecomputing.com', '')
            cond = f"{watermark_col} > '{last_ts.isoformat()}'" if last_ts else "1=1"
            query = f"SELECT * FROM CUSTOMERS WHERE {cond}"
            
            df = spark.read \
                .format("snowflake") \
                .option("sfURL", f"{account}.snowflakecomputing.com") \
                .option("sfUser", snowflake_config.get("user")) \
                .option("sfPassword", snowflake_config.get("password")) \
                .option("sfDatabase", snowflake_config.get("database", "ETL_PROJECT_DB")) \
                .option("sfSchema", snowflake_config.get("schema", "RAW")) \
                .option("sfWarehouse", snowflake_config.get("warehouse")) \
                .option("query", query) \
                .load()
        
        if df.isEmpty():
            print("⚠️  No new customer records found")
            return 0
        
        df = add_row_id(df)
        
        # Validate schema contract
        contract_path = Path("config/schema_definitions/customers_bronze.json")
        if contract_path.exists():
            contract = load_schema_contract(str(contract_path))
            lake_root = f"s3a://{lake_bucket}"
            error_handler = ErrorLaneHandler(lake_root)
            clean_df, quarantined_df, validation_results = validate_and_quarantine(
                spark, df, contract,
                error_lane_path=error_handler.get_error_lane_path("bronze", "snowflake_customers", run_id)
            )
            df = clean_df
        
        # Add metadata columns
        run_date = datetime.now().strftime("%Y-%m-%d")
        df = add_metadata_columns(df, batch_id=run_id, source_system="snowflake", run_date=run_date)
        
        # Update watermark
        if not df.isEmpty():
            max_ts = df.agg(F.max(watermark_col)).first()[0]
            if max_ts:
                state_store.set_watermark(watermark_key, max_ts.isoformat())
        
        # Write to bronze
        bronze_path = f"s3a://{lake_bucket}/bronze/snowflake/customers/"
        df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("_run_date") \
            .save(bronze_path)
        
        print(f"✅ Snowflake customers to Bronze completed: {df.count():,} records")
        return 0
        
    except Exception as e:
        print(f"❌ Job failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

