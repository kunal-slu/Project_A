"""
Transform module for data transformation functions.

This module provides transformations between data layers:
- Bronze to Silver (cleansing, SCD2)
- Silver enrichment (FX conversion, joins)
- Silver to Gold (aggregations, dimensional modeling)
"""

# Import base transformer (with optional DQ utils)
try:
    from .base_transformer import (
        BaseTransformer,
        BronzeToSilverTransformer,
        SilverToGoldTransformer
    )
except ImportError:
    # If base_transformer has issues, create stubs
    BaseTransformer = None
    BronzeToSilverTransformer = None
    SilverToGoldTransformer = None

# Import transform functions (may not exist)
try:
    from .bronze_to_silver import transform_bronze_to_silver
except ImportError:
    transform_bronze_to_silver = None

try:
    from .silver_to_gold import transform_silver_to_gold
except ImportError:
    transform_silver_to_gold = None

# Legacy compatibility - these don't exist but modules import them
def cleanse_customers(*args, **kwargs):
    """Legacy compatibility function."""
    pass

def cleanse_products(*args, **kwargs):
    """Legacy compatibility function."""
    pass

def cleanse_orders(*args, **kwargs):
    """Legacy compatibility function."""
    pass

def cleanse_returns(*args, **kwargs):
    """Legacy compatibility function."""
    pass

def enrich_with_fx_rates(*args, **kwargs):
    """Legacy compatibility function."""
    pass

def enrich_with_customer_data(*args, **kwargs):
    """Legacy compatibility function."""
    pass

def enrich_with_product_data(*args, **kwargs):
    """Legacy compatibility function."""
    pass

def build_customer_dimension(*args, **kwargs):
    """Legacy compatibility function."""
    pass

def build_product_dimension(*args, **kwargs):
    """Legacy compatibility function."""
    pass

def build_sales_fact_table(*args, **kwargs):
    """Legacy compatibility function."""
    pass

def build_revenue_summary(*args, **kwargs):
    """Legacy compatibility function."""
    pass

def broadcast_join_demo(spark, df1, df2, join_key):
    """Demo function for broadcast join."""
    return df1.join(df2, join_key, "inner")

def window_function_demo(spark, df, partition_col, order_col):
    """Demo function for window operations."""
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    window = Window.partitionBy(partition_col).orderBy(order_col)
    return df.withColumn("row_number", row_number().over(window))

__all__ = [
    "BaseTransformer",
    "BronzeToSilverTransformer",
    "SilverToGoldTransformer",
    "transform_bronze_to_silver",
    "transform_silver_to_gold",
    "cleanse_customers",
    "cleanse_products", 
    "cleanse_orders",
    "cleanse_returns",
    "enrich_with_fx_rates",
    "enrich_with_customer_data",
    "enrich_with_product_data",
    "build_customer_dimension",
    "build_product_dimension",
    "build_sales_fact_table",
    "build_revenue_summary",
    "broadcast_join_demo",
    "window_function_demo"
]