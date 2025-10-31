"""
Transform module for data transformation functions.

This module provides transformations between data layers:
- Bronze to Silver (cleansing, SCD2)
- Silver enrichment (FX conversion, joins)
- Silver to Gold (aggregations, dimensional modeling)
"""

from .bronze_to_silver import (
    cleanse_customers,
    cleanse_products,
    cleanse_orders,
    cleanse_returns
)

from .silver_enrichment import (
    enrich_with_fx_rates,
    enrich_with_customer_data,
    enrich_with_product_data
)

from .silver_to_gold import (
    build_customer_dimension,
    build_product_dimension,
    build_sales_fact_table,
    build_revenue_summary
)

# Add demo functions for testing
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