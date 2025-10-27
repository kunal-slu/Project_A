"""Build customer segments from orders/customer data."""
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, sum as spark_sum, count

logger = logging.getLogger(__name__)


def build_customer_segments(
    spark: SparkSession,
    orders_df: DataFrame,
    **kwargs
) -> DataFrame:
    """
    Build customer segments based on order behavior.
    
    Segments:
    - High Value (>$10k total)
    - Medium Value ($5k-$10k total)
    - Low Value (<$5k total)
    
    Args:
        spark: SparkSession object
        orders_df: Orders DataFrame
        **kwargs: Additional arguments
        
    Returns:
        DataFrame with customer segments
    """
    logger.info("Building customer segments")
    
    try:
        # Aggregate customer data
        df_segments = orders_df.groupBy("customer_id") \
            .agg(
                spark_sum("total_amount").alias("total_spend"),
                count("order_id").alias("order_count")
            ) \
            .withColumn(
                "segment",
                when(col("total_spend") > 10000, "High Value")
                .when(col("total_spend") > 5000, "Medium Value")
                .otherwise("Low Value")
            )
        
        logger.info(f"Successfully built segments for {df_segments.count()} customers")
        return df_segments
        
    except Exception as e:
        logger.error(f"Failed to build customer segments: {e}")
        raise

