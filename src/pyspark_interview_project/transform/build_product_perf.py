"""Build product performance metrics."""
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as spark_sum, count, avg, max as spark_max

logger = logging.getLogger(__name__)


def build_product_performance(
    spark: SparkSession,
    orders_df: DataFrame,
    **kwargs
) -> DataFrame:
    """
    Build product performance metrics.
    
    Metrics:
    - Total revenue per product
    - Order count per product
    - Average order value per product
    - Peak sales period
    
    Args:
        spark: SparkSession object
        orders_df: Orders DataFrame
        **kwargs: Additional arguments
        
    Returns:
        DataFrame with product performance metrics
    """
    logger.info("Building product performance metrics")
    
    try:
        # Aggregate product data
        df_perf = orders_df.groupBy("product_id") \
            .agg(
                spark_sum("total_amount").alias("total_revenue"),
                count("order_id").alias("order_count"),
                avg("total_amount").alias("avg_order_value"),
                spark_max("order_date").alias("last_order_date")
            )
        
        logger.info(f"Successfully built performance metrics for {df_perf.count()} products")
        return df_perf
        
    except Exception as e:
        logger.error(f"Failed to build product performance: {e}")
        raise

