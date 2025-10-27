"""Transform data from Bronze to Silver layer."""
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, trim, upper, regexp_replace, coalesce
from pyspark_interview_project.utils.dq import validate_not_null, validate_unique

logger = logging.getLogger(__name__)


def transform_bronze_to_silver(
    spark: SparkSession,
    df: DataFrame,
    layer: str = "silver",
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
    logger.info(f"Transforming data from Bronze to Silver layer")
    
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
        
        logger.info(f"Successfully transformed {df_cleaned.count()} records to Silver")
        return df_cleaned
        
    except Exception as e:
        logger.error(f"Failed to transform Bronze to Silver: {e}")
        raise

