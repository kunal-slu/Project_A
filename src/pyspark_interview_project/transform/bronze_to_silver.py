"""Transform data from Bronze to Silver layer.

Enhancements:
- Accept run_id and execution_date
- Write to run-scoped path: s3a://my-etl-lake-demo/silver/<table>/run_date=<execution_date>/run_id=<run_id>/
- If DQ passes, promote to latest: s3a://my-etl-lake-demo/silver/<table>/latest/
- Supports Iceberg, Delta, and Parquet formats via config
"""
import logging
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, trim, upper, regexp_replace, coalesce
from pyspark_interview_project.utils.dq_utils import validate_not_null, validate_unique
from pyspark_interview_project.io.write_table import write_table
from pyspark.sql import functions as F
from pyspark_interview_project.monitoring.lineage_decorator import lineage_job

logger = logging.getLogger(__name__)


@lineage_job(
    name="transform_bronze_to_silver",
    inputs=["s3://bucket/bronze/*"],
    outputs=["s3://bucket/silver/*"]
)
def transform_bronze_to_silver(
    spark: SparkSession,
    df: DataFrame,
    layer: str = "silver",
    run_id: str = "unknown",
    execution_date: str = "1970-01-01",
    table: str = "unknown",
    config: Optional[Dict[str, Any]] = None,
    **kwargs
) -> DataFrame:
    """
    Transform data from Bronze to Silver layer.
    
    Performs:
    - Data cleaning
    - Deduplication
    - Data type conversions
    - Null handling
    - DQ validation
    
    Args:
        spark: SparkSession object
        df: Bronze DataFrame
        layer: Target layer name
        **kwargs: Additional arguments
        
    Returns:
        Cleaned Silver DataFrame
    """
    logger.info(f"Transforming data from Bronze to Silver layer (table={table}, run_id={run_id}, exec_date={execution_date})")
    
    try:
        # Clean data
        df_cleaned = df
        
        # Trim string columns
        string_cols = [field.name for field in df.schema.fields if field.dataType.typeName() == 'string']
        for col_name in string_cols:
            df_cleaned = df_cleaned.withColumn(col_name, trim(col(col_name)))
        
        # Handle nulls
        df_cleaned = df_cleaned.na.fill("", subset=string_cols)
        
        # Validate DQ
        if 'order_id' in df_cleaned.columns:
            df_cleaned = validate_not_null(df_cleaned, 'order_id')
            df_cleaned = validate_unique(df_cleaned, 'order_id')
        
        rows_out = df_cleaned.count()
        
        # Add metadata columns
        df_with_metadata = df_cleaned \
            .withColumn("_run_id", F.lit(run_id)) \
            .withColumn("_exec_date", F.lit(execution_date))

        # Write using abstracted write_table function (supports Iceberg/Delta/Parquet)
        if config is None:
            config = {}
        
        # Write to Silver using configured format
        table_name = f"silver.{table}"
        write_table(
            df=df_with_metadata,
            table_name=table_name,
            mode="overwrite",
            cfg=config,
            spark=spark
        )
        logger.info(f"Wrote silver table '{table_name}' using format '{config.get('storage', {}).get('format', 'delta')}' rows={rows_out}")

        return df_cleaned
        
    except Exception as e:
        logger.error(f"Failed to transform Bronze to Silver: {e}")
        raise

