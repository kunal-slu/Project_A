"""
Transform raw zone data to bronze layer.

Reads from raw/, normalizes to canonical schema, writes to bronze/
"""

import argparse
import logging
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date

from project_a.pyspark_interview_project.utils.schema_validator import validate_schema
from project_a.utils.config import load_conf
from project_a.utils.spark_session import build_spark

logger = logging.getLogger(__name__)


def raw_to_bronze(
    spark: SparkSession, config: dict[str, Any], source: str, table: str, execution_date: str = None
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

    # Read from raw zone (config-driven, with safe fallback)
    paths_cfg = config.get("paths", {})
    raw_root = paths_cfg.get("raw_root") or paths_cfg.get("raw")
    if not raw_root:
        bronze_root = paths_cfg.get("bronze_root") or paths_cfg.get("bronze")
        if bronze_root and "/bronze" in bronze_root:
            raw_root = bronze_root.replace("/bronze", "/raw")
        else:
            raw_root = "data/raw"
    raw_path = f"{raw_root.rstrip('/')}/{source}/{table}/load_dt={execution_date}/"

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
    df = (
        df.withColumn("record_source", lit(source))
        .withColumn("record_table", lit(table))
        .withColumn("ingest_timestamp", current_timestamp())
        .withColumn("_proc_date", to_date(lit(execution_date)))
    )

    # Write to bronze (config-driven, with safe fallback)
    bronze_root = paths_cfg.get("bronze_root") or paths_cfg.get("bronze") or "data/bronze"
    bronze_path = f"{bronze_root.rstrip('/')}/{source}/{table}/"
    logger.info(f"Writing to bronze: {bronze_path}")

    df.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy(
        "_proc_date"
    ).save(bronze_path)

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
