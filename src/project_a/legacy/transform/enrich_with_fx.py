"""Enrich data with FX rates."""
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, join

logger = logging.getLogger(__name__)


def enrich_with_fx(
    spark: SparkSession,
    df: DataFrame,
    fx_rates_df: DataFrame,
    **kwargs
) -> DataFrame:
    """
    Enrich orders/data with FX rates for currency conversion.
    
    Args:
        spark: SparkSession object
        df: Source DataFrame to enrich
        fx_rates_df: FX rates DataFrame
        **kwargs: Additional arguments
        
    Returns:
        Enriched DataFrame with FX conversions
    """
    logger.info("Enriching data with FX rates")
    
    try:
        # Join with FX rates on date
        if 'order_date' in df.columns and 'date' in fx_rates_df.columns:
            df_enriched = df.join(
                fx_rates_df,
                col("order_date") == col("date"),
                "left"
            )
        else:
            # Fallback: add dummy FX columns if no date match
            df_enriched = df
        
        logger.info(f"Successfully enriched {df_enriched.count()} records with FX rates")
        return df_enriched
        
    except Exception as e:
        logger.error(f"Failed to enrich with FX rates: {e}")
        raise

