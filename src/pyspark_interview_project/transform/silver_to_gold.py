"""Transform data from Silver to Gold layer."""
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as spark_sum, count, avg

logger = logging.getLogger(__name__)


def transform_silver_to_gold(
    spark: SparkSession,
    df: DataFrame,
    table_name: str = "gold",
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
        
        logger.info(f"Successfully transformed to Gold: {df_gold.count()} records")
        return df_gold
        
    except Exception as e:
        logger.error(f"Failed to transform Silver to Gold: {e}")
        raise

