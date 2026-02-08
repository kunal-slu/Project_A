"""
Bronze layer schemas - explicit schemas for all source data.
No schema inference in production.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# CRM Schemas
CRM_ACCOUNTS_SCHEMA = StructType(
    [
        StructField("account_id", StringType(), nullable=False),
        StructField("account_name", StringType(), nullable=True),
        StructField("segment", StringType(), nullable=True),
        StructField("created_date", StringType(), nullable=True),  # Will convert to date
        StructField("country", StringType(), nullable=True),
        StructField("industry", StringType(), nullable=True),
        StructField("annual_revenue", DoubleType(), nullable=True),
    ]
)

CRM_CONTACTS_SCHEMA = StructType(
    [
        StructField("contact_id", StringType(), nullable=False),
        StructField("account_id", StringType(), nullable=True),
        StructField("contact_email", StringType(), nullable=True),
        StructField("contact_phone", StringType(), nullable=True),
        StructField("first_name", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True),
        StructField("last_contact_ts", StringType(), nullable=True),  # Will convert to timestamp
    ]
)

CRM_OPPORTUNITIES_SCHEMA = StructType(
    [
        StructField("opportunity_id", StringType(), nullable=False),
        StructField("account_id", StringType(), nullable=True),
        StructField("opportunity_name", StringType(), nullable=True),
        StructField("stage", StringType(), nullable=True),
        StructField("amount", DoubleType(), nullable=True),
        StructField("close_date", StringType(), nullable=True),  # Will convert to date
    ]
)

# Redshift Behavior Schema
REDSHIFT_BEHAVIOR_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), nullable=False),
        StructField("event_date", StringType(), nullable=True),  # Will convert to date
        StructField("login_count", IntegerType(), nullable=True),
        StructField("page_views", IntegerType(), nullable=True),
        StructField("session_duration_minutes", DoubleType(), nullable=True),
        StructField("purchases", IntegerType(), nullable=True),
        StructField("revenue", DoubleType(), nullable=True),
    ]
)

# Snowflake Schemas
SNOWFLAKE_CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), nullable=False),
        StructField("customer_name", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
        StructField("phone", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("created_date", StringType(), nullable=True),
    ]
)

SNOWFLAKE_ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("order_date", StringType(), nullable=True),
        StructField("amount", DoubleType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
    ]
)

SNOWFLAKE_PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), nullable=False),
        StructField("product_name", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("price", DoubleType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
    ]
)

# FX Rates Schema (supports both CSV and JSON formats)
FX_RATES_SCHEMA = StructType(
    [
        StructField("date", StringType(), nullable=True),  # JSON format
        StructField("trade_date", StringType(), nullable=True),  # CSV format
        StructField("base_ccy", StringType(), nullable=True),  # JSON format
        StructField("base_currency", StringType(), nullable=True),  # CSV format
        StructField("quote_ccy", StringType(), nullable=True),  # JSON format
        StructField("target_currency", StringType(), nullable=True),  # CSV format
        StructField("rate", DoubleType(), nullable=True),  # JSON format
        StructField("exchange_rate", DoubleType(), nullable=True),  # CSV format
        StructField("source", StringType(), nullable=True),  # Optional metadata
        StructField("bid_rate", DoubleType(), nullable=True),  # Optional
        StructField("ask_rate", DoubleType(), nullable=True),  # Optional
        StructField("mid_rate", DoubleType(), nullable=True),  # Optional
    ]
)

# Kafka Events Schema
KAFKA_EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("order_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=True),
        StructField("event_ts", StringType(), nullable=True),  # Will convert to timestamp
        StructField("status", StringType(), nullable=True),
        StructField("metadata", StringType(), nullable=True),
    ]
)
