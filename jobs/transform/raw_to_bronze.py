"""
Transform raw zone data to bronze layer.

Reads from raw/, normalizes to canonical schema, writes to bronze/
"""

import argparse
import logging
from typing import Dict, Any
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp, to_date

from project_a.utils.spark_session import build_spark
from project_a.utils.config import load_conf
from project_a.schema_validator import validate_schema

logger = logging.getLogger(__name__)


def raw_to_bronze(
    spark: SparkSession,
    config: Dict[str, Any],
    source: str,
    table: str,
    execution_date: str = None
) -> DataFrame:
    """
    Transform raw zone data to bronze layer.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        source: Source name (snowflake, crm, redshift, etc.)
        table: Table name
        execution_date: Execution date (YYYY-MM-DD)
        
    Returns:
        Bronze DataFrame
    """
    if execution_date is None:
        execution_date = datetime.now().strftime("%Y-%m-%d")
    
    logger.info(f"Transforming {source}.{table} from raw to bronze")
    
    # Read from raw zone
    raw_path = f"s3://my-etl-lake-demo/raw/{source}/{table}/load_dt={execution_date}/"
    
    try:
        df = spark.read.parquet(raw_path)
    except Exception as e:
        logger.error(f"Failed to read from raw: {e}")
        raise
    
    # Load schema definition
    schema_def_path = f"config/schema_definitions/bronze/{source}_{table}_bronze.json"
    
    try:
        import json
        with open(schema_def_path) as f:
            schema_def = json.load(f)
        
        # Validate schema
        df, validation_results = validate_schema(df, schema_def, mode="allow_new", config=config)
        
        if not validation_results.get("passed"):
            logger.warning(f"Schema validation issues: {validation_results}")
    except FileNotFoundError:
        logger.warning(f"Schema definition not found: {schema_def_path}, skipping validation")
    
    # Normalize to canonical columns
    # Add standard metadata
    df = df.withColumn("record_source", lit(source)) \
           .withColumn("record_table", lit(table)) \
           .withColumn("ingest_timestamp", current_timestamp()) \
           .withColumn("_proc_date", to_date(lit(execution_date)))
    
    # Write to bronze
    bronze_path = f"s3://my-etl-lake-demo/bronze/{source}/{table}/"
    logger.info(f"Writing to bronze: {bronze_path}")
    
    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("_proc_date") \
        .save(bronze_path)
    
    record_count = df.count()
    logger.info(f"✅ Transformed {record_count:,} records to bronze")
    
    return df


def main():
    parser = argparse.ArgumentParser(description="Transform raw to bronze")
    parser.add_argument("--source", required=True, help="Source name")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--execution-date", help="Execution date")
    parser.add_argument("--config", default="config/prod.yaml", help="Config file")
    
    args = parser.parse_args()
    
    config = load_conf(args.config)
    spark = build_spark(app_name=f"raw_to_bronze_{args.source}_{args.table}", config=config)
    
    try:
        raw_to_bronze(spark, config, args.source, args.table, args.execution_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

