"""Transform data from Silver to Gold layer.

Supports Iceberg, Delta, and Parquet formats via config.
"""
import logging
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as spark_sum, count, avg
from pyspark_interview_project.io.write_table import write_table
from pyspark_interview_project.monitoring.lineage_decorator import lineage_job

logger = logging.getLogger(__name__)


@lineage_job(
    name="transform_silver_to_gold",
    inputs=["s3://bucket/silver/*"],
    outputs=["s3://bucket/gold/*"]
)
def transform_silver_to_gold(
    spark: SparkSession,
    df: DataFrame,
    table_name: str = "gold",
    config: Optional[Dict[str, Any]] = None,
    **kwargs
) -> DataFrame:
    """
    Transform data from Silver to Gold layer.
    
    Performs:
    - Business logic aggregation
    - Dimensional modeling
    - Metric calculations
    - Data enrichment
    
    Args:
        spark: SparkSession object
        df: Silver DataFrame
        table_name: Target Gold table name
        **kwargs: Additional arguments
        
    Returns:
        Gold DataFrame with business-ready metrics
    """
    logger.info(f"Transforming data from Silver to Gold layer: {table_name}")
    
    try:
        # Example: Aggregate to create gold table
        if 'order_id' in df.columns:
            df_gold = df.groupBy("customer_id") \
                .agg(
                    spark_sum("total_amount").alias("total_revenue"),
                    count("order_id").alias("order_count"),
                    avg("total_amount").alias("avg_order_value")
                )
        else:
            # For other tables, return as-is for now
            df_gold = df
        
        rows_out = df_gold.count()
        logger.info(f"Successfully transformed to Gold: {rows_out} records")
        
        # Write using abstracted write_table function (supports Iceberg/Delta/Parquet)
        if config is None:
            config = {}
        
        # Write to Gold using configured format
        full_table_name = f"gold.{table_name}"
        write_table(
            df=df_gold,
            table_name=full_table_name,
            mode="overwrite",
            cfg=config,
            spark=spark
        )
        logger.info(f"Wrote gold table '{full_table_name}' using format '{config.get('storage', {}).get('format', 'delta')}'")
        
        return df_gold
        
    except Exception as e:
        logger.error(f"Failed to transform Silver to Gold: {e}")
        raise

