"""
Gold Layer Transform Builders

Reusable functions for building Gold layer (star schema) from Silver data.
Creates dimensions, facts, and aggregated analytics tables.
"""

import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def build_dim_date(spark: SparkSession, orders_df: DataFrame) -> DataFrame:
    """
    Build date dimension table with full calendar range.
    
    Generates a date dimension covering the range from min(order_date) to max(order_date)
    in orders_df, with a minimum of 2 years (730 days) if orders_df is empty or has limited dates.
    
    Args:
        spark: SparkSession
        orders_df: Orders DataFrame with order_date column (used to determine date range)
        
    Returns:
        Date dimension DataFrame with columns: date_sk, date, year, quarter, month, day_of_week, is_weekend
    """
    logger.info("🔧 Building dim_date...")
    
    # Determine date range from orders or use default range
    try:
        # Extract min and max dates from orders
        date_col = None
        for col in ["order_date", "event_ts", "date", "created_at"]:
            if col in orders_df.columns:
                date_col = col
                break
        
        if date_col and orders_df.count() > 0:
            min_max = orders_df.agg(
                F.min(F.col(date_col)).alias("min_date"),
                F.max(F.col(date_col)).alias("max_date")
            ).collect()[0]
            
            start_date = min_max["min_date"]
            end_date = min_max["max_date"]
            
            # Ensure we have at least 2 years of dates
            from datetime import date, timedelta
            if start_date is None or end_date is None:
                end_date = date.today()
                start_date = end_date - timedelta(days=730)
            else:
                # Convert to date if timestamp
                if hasattr(start_date, 'date'):
                    start_date = start_date.date()
                if hasattr(end_date, 'date'):
                    end_date = end_date.date()
                
                # Extend range to at least 730 days
                days_diff = (end_date - start_date).days
                if days_diff < 730:
                    # Center the range around the data
                    center = start_date + timedelta(days=days_diff // 2)
                    start_date = center - timedelta(days=365)
                    end_date = center + timedelta(days=365)
        else:
            # Fallback: create 2-year range ending today
            from datetime import date, timedelta
            end_date = date.today()
            start_date = end_date - timedelta(days=730)
    except Exception as e:
        logger.warning(f"Could not determine date range from orders: {e}, using default 2-year range")
        from datetime import date, timedelta
        end_date = date.today()
        start_date = end_date - timedelta(days=730)
    
    # Generate date range using Spark SQL
    # Calculate number of days
    from datetime import date as dt_date
    if isinstance(start_date, dt_date) and isinstance(end_date, dt_date):
        num_days = (end_date - start_date).days + 1
        start_date_str = start_date.strftime("%Y-%m-%d")
    else:
        # Fallback
        num_days = 731
        start_date_str = "2023-01-01"
    
    # Create date range using Spark range and date_add
    # Cast id to INT because date_add requires INT, not BIGINT
    dim_date = (
        spark.range(0, num_days)
        .withColumn("date", F.expr(f"date_add('{start_date_str}', cast(id as int))"))
        .select("date")
        .filter(F.col("date").isNotNull())
        .withColumn("date_sk", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("is_weekend", F.when(F.col("day_of_week").isin([1, 7]), True).otherwise(False))
        .select("date_sk", "date", "year", "quarter", "month", "day_of_week", "is_weekend")
        .orderBy("date")
    )
    
    try:
        count = dim_date.count()
        logger.info(f"✅ dim_date: {count:,} rows (from {start_date_str} to {end_date})")
    except Exception as e:
        logger.warning(f"Could not count dim_date: {e}")
    
    return dim_date


def build_dim_customer(customers_silver: DataFrame, spark: Optional[SparkSession] = None) -> DataFrame:
    """
    Build customer dimension with SCD-lite support.
    
    Args:
        customers_silver: Silver customers DataFrame
        spark: Optional SparkSession (extracted from DataFrame if not provided)
        
    Returns:
        Customer dimension DataFrame
    """
    logger.info("🔧 Building dim_customer...")
    
    # Get SparkSession if not provided
    if spark is None:
        try:
            spark = customers_silver.sql_ctx.sparkSession
        except Exception:
            try:
                spark = customers_silver.sparkSession
            except Exception:
                from pyspark.sql import SparkSession
                spark = SparkSession.getActiveSession()
                if spark is None:
                    raise ValueError("Could not get SparkSession")
    
    # Handle empty DataFrame
    try:
        if customers_silver.count() == 0:
            logger.warning("customers_silver is empty, creating empty dim_customer")
            from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, BooleanType
            schema = StructType([
                StructField("customer_id", StringType(), True),
                StructField("customer_name", StringType(), True),
                StructField("primary_email", StringType(), True),
                StructField("primary_phone", StringType(), True),
                StructField("segment", StringType(), True),
                StructField("country", StringType(), True),
                StructField("industry", StringType(), True),
                StructField("customer_since", DateType(), True),
                StructField("is_active", BooleanType(), True),
            ])
            return spark.createDataFrame([], schema)
    except Exception:
        pass  # Continue if count fails
    
    # Build dimension with required columns
    dim_customer = customers_silver.select(
        F.col("customer_id"),
        F.coalesce(F.col("customer_name"), F.lit("UNKNOWN")).alias("customer_name"),
        F.col("primary_email"),
        F.col("primary_phone"),
        F.coalesce(F.col("segment"), F.lit("UNKNOWN")).alias("segment"),
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country"),
        F.col("industry"),
        F.col("customer_since"),
        F.lit(True).alias("is_active")  # Assume all customers are active
    )
    
    try:
        logger.info(f"✅ dim_customer: {dim_customer.count():,} rows")
    except Exception as e:
        logger.warning(f"Could not count dim_customer: {e}")
    
    return dim_customer


def build_dim_product(products_silver: DataFrame, spark: Optional[SparkSession] = None) -> DataFrame:
    """
    Build product dimension.
    
    Args:
        products_silver: Silver products DataFrame
        spark: Optional SparkSession
        
    Returns:
        Product dimension DataFrame
    """
    logger.info("🔧 Building dim_product...")
    
    if spark is None:
        try:
            spark = products_silver.sql_ctx.sparkSession
        except Exception:
            spark = products_silver.sparkSession
    
    # Handle empty DataFrame
    try:
        if products_silver.count() == 0:
            logger.warning("products_silver is empty, creating empty dim_product")
            from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, BooleanType
            schema = StructType([
                StructField("product_id", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("price_usd", DoubleType(), True),
                StructField("currency", StringType(), True),
                StructField("is_active", BooleanType(), True),
            ])
            return spark.createDataFrame([], schema)
    except Exception:
        pass
    
    dim_product = products_silver.select(
        F.col("product_id"),
        F.col("product_name"),
        F.col("category"),
        F.col("price_usd"),
        F.col("currency"),
        F.lit(True).alias("is_active")
    )
    
    try:
        logger.info(f"✅ dim_product: {dim_product.count():,} rows")
    except Exception as e:
        logger.warning(f"Could not count dim_product: {e}")
    
    return dim_product


def build_fact_orders(
    orders_silver: DataFrame,
    dim_customer: DataFrame,
    dim_product: DataFrame,
    dim_date: DataFrame,
    spark: Optional[SparkSession] = None
) -> DataFrame:
    """
    Build fact_orders table with dimension surrogate keys.
    
    Args:
        orders_silver: Silver orders DataFrame
        dim_customer: Customer dimension
        dim_product: Product dimension
        dim_date: Date dimension
        spark: Optional SparkSession
        
    Returns:
        Fact orders DataFrame
    """
    logger.info("🔧 Building fact_orders...")
    
    if spark is None:
        try:
            spark = orders_silver.sql_ctx.sparkSession
        except Exception:
            spark = orders_silver.sparkSession
    
    # Handle empty orders
    try:
        if orders_silver.count() == 0:
            logger.warning("orders_silver is empty, creating empty fact_orders")
            from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType
            schema = StructType([
                StructField("order_id", StringType(), True),
                StructField("customer_sk", IntegerType(), True),
                StructField("product_sk", IntegerType(), True),
                StructField("date_sk", IntegerType(), True),
                StructField("sales_amount", DoubleType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("order_date", DateType(), True),
            ])
            return spark.createDataFrame([], schema)
    except Exception:
        pass
    
    # Get sales amount (prefer amount_usd, fallback to amount_orig)
    orders = orders_silver.withColumn(
        "sales_amount",
        F.coalesce(
            F.col("amount_usd"),
            F.col("amount_orig"),
            F.lit(0.0)
        )
    )
    
    # Join with dimensions to get surrogate keys
    # Add row numbers as surrogate keys (in production, use proper SCD2 logic)
    # NOTE: Using monotonically_increasing_id() for surrogate keys is fine for demos,
    # but in production, use proper SCD2 logic with stable surrogate keys.
    dim_customer_with_sk = dim_customer.withColumn(
        "customer_sk",
        F.monotonically_increasing_id().cast("long")
    )
    
    dim_product_with_sk = dim_product.withColumn(
        "product_sk",
        F.monotonically_increasing_id().cast("long")
    )
    
    # Join orders with dimensions
    # NOTE: These are left joins, so missing dimensions will result in -1 surrogate keys.
    # This is intentional to handle orphan orders. The validation script checks for high rates of -1 values.
    fact = orders.join(
        dim_customer_with_sk.select("customer_id", "customer_sk"),
        "customer_id",
        "left"
    ).join(
        dim_product_with_sk.select("product_id", "product_sk"),
        "product_id",
        "left"
    ).join(
        dim_date.select("date", "date_sk"),
        F.col("order_date") == F.col("date"),
        "left"
    ).withColumn(
        "customer_sk",
        F.coalesce(F.col("customer_sk"), F.lit(-1))
    ).withColumn(
        "product_sk",
        F.coalesce(F.col("product_sk"), F.lit(-1))
    ).withColumn(
        "date_sk",
        F.coalesce(F.col("date_sk"), F.lit(-1))
    ).select(
        "order_id",
        "customer_sk",
        "product_sk",
        "date_sk",
        "sales_amount",
        "quantity",
        "order_date"
    )
    
    try:
        logger.info(f"✅ fact_orders: {fact.count():,} rows")
    except Exception as e:
        logger.warning(f"Could not count fact_orders: {e}")
    
    return fact


def build_customer_360(
    dim_customer: DataFrame,
    orders_silver: DataFrame,
    behavior_silver: DataFrame,
    spark: Optional[SparkSession] = None
) -> DataFrame:
    """
    Build customer 360 view with aggregated metrics.
    
    Args:
        dim_customer: Customer dimension
        orders_silver: Silver orders DataFrame
        behavior_silver: Silver behavior DataFrame
        spark: Optional SparkSession
        
    Returns:
        Customer 360 DataFrame with lifetime metrics
    """
    logger.info("🔧 Building customer_360...")
    
    if spark is None:
        try:
            spark = dim_customer.sql_ctx.sparkSession
        except Exception:
            spark = dim_customer.sparkSession
    
    # Aggregate orders by customer
    order_agg = orders_silver.groupBy("customer_id").agg(
        F.count("*").alias("total_orders"),
        F.sum("amount_usd").alias("lifetime_value_usd"),
        F.avg("amount_usd").alias("avg_order_value_usd"),
        F.max("order_date").alias("last_order_date")
    )
    
    # Aggregate behavior by customer
    behavior_agg = behavior_silver.groupBy("customer_id").agg(
        F.sum("login_count").alias("total_logins"),
        F.sum("page_views").alias("total_page_views"),
        F.sum("purchases").alias("total_purchases"),
        F.sum("revenue").alias("total_behavior_revenue")
    )
    
    # Join all sources
    customer_360 = dim_customer.join(order_agg, "customer_id", "left") \
                               .join(behavior_agg, "customer_id", "left")
    
    # Fill nulls
    customer_360 = customer_360.fillna({
        "total_orders": 0,
        "lifetime_value_usd": 0.0,
        "avg_order_value_usd": 0.0,
        "total_logins": 0,
        "total_page_views": 0,
        "total_purchases": 0,
        "total_behavior_revenue": 0.0
    })
    
    try:
        logger.info(f"✅ customer_360: {customer_360.count():,} rows")
    except Exception as e:
        logger.warning(f"Could not count customer_360: {e}")
    
    return customer_360


def build_product_performance(
    dim_product: DataFrame,
    orders_silver: DataFrame,
    spark: Optional[SparkSession] = None
) -> DataFrame:
    """
    Build product performance analytics table.
    
    Args:
        dim_product: Product dimension
        orders_silver: Silver orders DataFrame
        spark: Optional SparkSession
        
    Returns:
        Product performance DataFrame
    """
    logger.info("🔧 Building product_performance...")
    
    if spark is None:
        try:
            spark = dim_product.sql_ctx.sparkSession
        except Exception:
            spark = dim_product.sparkSession
    
    # Aggregate orders by product
    product_agg = orders_silver.groupBy("product_id").agg(
        F.count("*").alias("total_orders"),
        F.sum("amount_usd").alias("total_revenue_usd"),
        F.avg("amount_usd").alias("avg_order_value_usd"),
        F.sum("quantity").alias("total_quantity_sold"),
        F.max("order_date").alias("last_sale_date")
    )
    
    # Join with dimension
    product_performance = dim_product.join(product_agg, "product_id", "left")
    
    # Fill nulls
    product_performance = product_performance.fillna({
        "total_orders": 0,
        "total_revenue_usd": 0.0,
        "avg_order_value_usd": 0.0,
        "total_quantity_sold": 0,
    })
    
    try:
        logger.info(f"✅ product_performance: {product_performance.count():,} rows")
    except Exception as e:
        logger.warning(f"Could not count product_performance: {e}")
    
    return product_performance

