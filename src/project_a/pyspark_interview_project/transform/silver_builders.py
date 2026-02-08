"""
Silver Layer Transform Builders

Reusable functions for building Silver layer tables from Bronze data.
Handles joins, aggregations, and data quality transformations.
"""

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def build_customers_silver(
    df_customers: DataFrame,
    df_accounts: DataFrame,
    df_contacts: DataFrame,
    df_opps: DataFrame,
    df_behavior: DataFrame,
) -> DataFrame:
    """
    Build silver customers table with joins and aggregations.

    Uses Snowflake customers as primary source, enriched with:
    - CRM account information (if available)
    - Latest contact information
    - Aggregated behavior metrics
    """
    logger.info("🔧 Building silver.customers...")

    # Use Snowflake customers as primary source (has proper CUST-XXXXX IDs)
    # Map Snowflake columns: first_name+last_name -> customer_name, customer_segment -> segment, registration_date -> customer_since
    customers_base = df_customers.select(
        F.col("customer_id"),
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("customer_name"),
        F.coalesce(F.col("customer_segment"), F.lit("UNKNOWN")).alias("segment"),
        F.coalesce(
            F.to_date("registration_date", "yyyy-MM-dd"),
            F.to_date("registration_date"),
            F.current_date(),
        ).alias("customer_since"),
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country"),
        F.lit("UNKNOWN").alias("industry"),  # Snowflake customers don't have industry
        F.coalesce(F.col("lifetime_value"), F.lit(0.0)).alias(
            "annual_revenue"
        ),  # Use lifetime_value as proxy
    ).filter(F.col("customer_id").isNotNull())

    # Optionally enrich with CRM account data (if customer_id matches)
    # Clean accounts - map actual CSV columns to expected schema
    df_accounts.select(
        F.col("Id").alias("account_id"),  # Keep as account_id to avoid confusion
        F.trim(F.col("Name")).alias("account_name"),
        F.coalesce(F.col("CustomerSegment"), F.lit("UNKNOWN")).alias("crm_segment"),
        F.coalesce(
            F.to_date("CreatedDate", "yyyy-MM-dd"), F.to_date("CreatedDate"), F.current_date()
        ).alias("crm_created_date"),
        F.coalesce(F.col("BillingCountry"), F.lit("UNKNOWN")).alias("crm_country"),
        F.col("Industry").alias("crm_industry"),
        F.coalesce(F.col("AnnualRevenue"), F.lit(0.0)).alias("crm_annual_revenue"),
    ).filter(F.col("account_id").isNotNull())

    # Get latest contact per customer
    # Use date column for sorting (safer than timestamp for date-only strings)
    # Convert date strings to date type first, then use for window ordering
    last_contact = (
        df_contacts.withColumn(
            "contact_date",
            F.coalesce(
                F.to_date("LastModifiedDate", "yyyy-MM-dd"),
                F.to_date("LastModifiedDate"),
                F.to_date("CreatedDate", "yyyy-MM-dd"),
                F.to_date("CreatedDate"),
                F.current_date(),
            ),
        )
        .filter(F.col("contact_date").isNotNull() & F.col("AccountId").isNotNull())
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("AccountId").orderBy(F.col("contact_date").desc())
            ),
        )
        .filter(F.col("rn") == 1)
        .select(
            F.col("AccountId").alias("customer_id"),
            F.col("Email").alias("primary_email"),
            F.col("Phone").alias("primary_phone"),
        )
    )

    # Aggregate behavior metrics - derive from event_name and conversion_value
    behavior_agg = df_behavior.groupBy("customer_id").agg(
        F.count("*").alias("behavior_events_24m"),
        F.sum(F.when(F.col("event_name") == "login", 1).otherwise(0)).alias("total_logins_24m"),
        F.sum(F.when(F.col("event_name") == "page_view", 1).otherwise(0)).alias(
            "total_page_views_24m"
        ),
        F.sum(F.when(F.col("event_name") == "purchase", 1).otherwise(0)).alias(
            "total_purchases_24m"
        ),
        F.sum(F.coalesce(F.col("conversion_value"), F.lit(0.0))).alias("total_revenue_24m"),
    )

    # Join all sources - start with Snowflake customers as base
    customers = customers_base.join(last_contact, "customer_id", "left").join(
        behavior_agg, "customer_id", "left"
    )

    # Null handling
    customers = customers.fillna(
        {
            "segment": "UNKNOWN",
            "country": "UNKNOWN",
            "behavior_events_24m": 0,
            "total_logins_24m": 0,
            "total_page_views_24m": 0,
            "total_purchases_24m": 0,
            "total_revenue_24m": 0.0,
        }
    )

    try:
        logger.info(f"✅ silver.customers: {customers.count():,} rows")
    except Exception as e:
        logger.warning(f"Could not count customers: {e}")

    return customers


def build_orders_silver(
    df_orders: DataFrame, df_customers: DataFrame, df_products: DataFrame, df_fx: DataFrame
) -> DataFrame:
    """
    Build silver orders table with FX normalization.

    Joins orders with customers, products, and FX rates to:
    - Enrich with customer/product information
    - Normalize amounts to USD using FX rates
    """
    logger.info("🔧 Building silver.orders...")

    # Handle empty DataFrame case
    try:
        if df_orders.count() == 0:
            logger.warning("⚠️  df_orders is empty, creating empty DataFrame with expected schema")
            from pyspark.sql.types import (
                DateType,
                DoubleType,
                IntegerType,
                StringType,
                StructField,
                StructType,
            )

            empty_schema = StructType(
                [
                    StructField("order_id", StringType(), True),
                    StructField("customer_id", StringType(), True),
                    StructField("product_id", StringType(), True),
                    StructField("product_name", StringType(), True),
                    StructField("category", StringType(), True),
                    StructField("order_date", DateType(), True),
                    StructField("amount_orig", DoubleType(), True),
                    StructField("currency", StringType(), True),
                    StructField("amount_usd", DoubleType(), True),
                    StructField("quantity", IntegerType(), True),
                ]
            )
            return df_orders.sparkSession.createDataFrame([], empty_schema)
    except Exception:
        pass  # Continue if count fails

    # Map order columns correctly from CSV
    # CSV structure: order_id, customer_id, product_id, currency, total_amount, quantity, order_date, order_timestamp, status, ...
    # Ensure we select by column name (not position) to avoid misalignment
    orders = df_orders.select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("product_id"),
        # Parse order_date: try order_date column first, fallback to order_timestamp
        # Use regexp to check if order_date looks like a date (YYYY-MM-DD format)
        F.when(
            F.regexp_extract(F.col("order_date"), r"^\d{4}-\d{2}-\d{2}", 0) != "",
            F.to_date("order_date", "yyyy-MM-dd"),
        )
        .when(
            F.regexp_extract(F.col("order_timestamp"), r"^\d{4}-\d{2}-\d{2}", 0) != "",
            F.to_date(F.substring("order_timestamp", 1, 10), "yyyy-MM-dd"),
        )
        .otherwise(F.current_date())
        .cast("date")
        .alias("order_date"),
        # Map total_amount correctly (ensure it's a double, not 0)
        F.coalesce(F.col("total_amount").cast("double"), F.lit(0.0)).alias("amount_orig"),
        # Map currency correctly (should be EUR, GBP, USD, not status)
        F.coalesce(F.col("currency"), F.lit("USD")).alias("currency"),
        # Keep status separate if it exists
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("order_status"),
        F.coalesce(F.col("quantity").cast("int"), F.lit(1)).alias("quantity"),
    ).filter(
        F.col("order_date").isNotNull()
        & F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & (F.col("amount_orig") > 0.0)  # Filter out zero-amount orders
    )  # Filter out rows with invalid dates, missing keys, or zero amounts

    # Join with products to get product_name and category
    products = df_products.select(
        F.col("product_id"),
        F.col("product_name"),
        F.col("category"),
        F.col("price_usd").alias("price"),
    )

    orders = orders.join(products, "product_id", "left")

    # Join with FX rates for currency conversion (if FX data is available)
    # fx_rates_silver has: trade_date, base_ccy, counter_ccy, rate
    # We need to join on: base_ccy = orders.currency, counter_ccy = 'USD', trade_date = orders.order_date
    try:
        fx_count = df_fx.count()
    except Exception:
        fx_count = 0

    if fx_count > 0:
        # Prepare FX rates: filter for USD conversions and rename for join
        # df_fx from bronze has: quote_ccy (not counter_ccy), base_ccy, rate, date/trade_date
        fx_rates = df_fx.select(
            F.coalesce(F.to_date("trade_date"), F.to_date("date")).alias("fx_date"),
            F.coalesce(F.col("base_ccy"), F.col("base_currency")).alias("fx_base_currency"),
            F.coalesce(F.col("quote_ccy"), F.col("target_currency")).alias("fx_counter_currency"),
            F.coalesce(F.col("rate"), F.col("exchange_rate")).alias("fx_rate"),
        ).filter(
            F.col("fx_counter_currency") == "USD"  # Only USD conversion rates
        )

        # Join orders with FX rates
        # Match: orders.currency = fx_rates.base_ccy AND orders.order_date = fx_rates.trade_date
        orders = (
            orders.join(
                fx_rates,
                (F.col("currency") == F.col("fx_base_currency"))
                & (F.col("order_date") == F.col("fx_date")),
                "left",
            )
            .withColumn(
                "amount_usd",
                F.when(
                    F.col("currency") == "USD",
                    F.col("amount_orig"),  # Already in USD
                )
                .when(
                    F.col("fx_rate").isNotNull(),
                    F.col("amount_orig") * F.col("fx_rate"),  # Convert using FX rate
                )
                .otherwise(
                    F.col("amount_orig")  # Fallback: assume USD if FX rate not found
                ),
            )
            .select(
                "order_id",
                "customer_id",
                "product_id",
                "product_name",
                "category",
                "order_date",
                "amount_orig",
                "currency",
                "amount_usd",
                "quantity",
            )
        )
    else:
        # No FX data - assume all amounts are in USD
        orders = orders.withColumn("amount_usd", F.col("amount_orig")).select(
            "order_id",
            "customer_id",
            "product_id",
            "product_name",
            "category",
            "order_date",
            "amount_orig",
            "currency",
            "amount_usd",
            "quantity",
        )

    try:
        logger.info(f"✅ silver.orders: {orders.count():,} rows")
    except Exception as e:
        logger.warning(f"Could not count orders: {e}")

    return orders


def build_products_silver(df_products: DataFrame) -> DataFrame:
    """Build silver products table."""
    logger.info("🔧 Building silver.products...")

    # CSV structure: product_id, product_name, category, price_usd, currency, description, brand, sku, ...
    # Ensure we select by column name to avoid misalignment
    products = df_products.select(
        F.col("product_id"),
        F.col("product_name"),
        # Ensure category is a string (not numeric)
        F.coalesce(F.col("category").cast("string"), F.lit("UNKNOWN")).alias("category"),
        # Map price_usd correctly (ensure it's a double, not 0)
        F.coalesce(F.col("price_usd").cast("double"), F.lit(0.0)).alias("price_usd"),
        # Use currency from source if available, otherwise default to USD
        F.coalesce(F.col("currency"), F.lit("USD")).alias("currency"),
    ).filter(
        F.col("product_id").isNotNull()
        & (F.col("price_usd") > 0.0)  # Filter out zero-price products
    )

    try:
        logger.info(f"✅ silver.products: {products.count():,} rows")
    except Exception as e:
        logger.warning(f"Could not count products: {e}")

    return products


def build_behavior_silver(df_behavior: DataFrame) -> DataFrame:
    """Build silver behavior table with aggregated metrics."""
    logger.info("🔧 Building silver.customer_behavior...")

    # Derive metrics from event_name and conversion_value
    behavior = (
        df_behavior.withColumn("event_date", F.to_date(F.col("event_timestamp")))
        .withColumn("login_count", F.when(F.col("event_name") == "login", 1).otherwise(0))
        .withColumn("page_views", F.when(F.col("event_name") == "page_view", 1).otherwise(0))
        .withColumn("purchases", F.when(F.col("event_name") == "purchase", 1).otherwise(0))
        .withColumn("revenue", F.coalesce(F.col("conversion_value"), F.lit(0.0)))
        .groupBy("customer_id", "event_date")
        .agg(
            F.sum("login_count").alias("login_count"),
            F.sum("page_views").alias("page_views"),
            F.sum("purchases").alias("purchases"),
            F.sum("revenue").alias("revenue"),
            F.avg("duration_seconds").alias("session_duration_minutes"),
        )
        .withColumn("session_duration_minutes", F.col("session_duration_minutes") / 60.0)
    )

    try:
        logger.info(f"✅ silver.customer_behavior: {behavior.count():,} rows")
    except Exception as e:
        logger.warning(f"Could not count behavior: {e}")

    return behavior
