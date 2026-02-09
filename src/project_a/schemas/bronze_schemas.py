"""
Bronze layer schemas - explicit schemas matching ACTUAL source data headers.

These schemas match the real CSV files in aws/data/samples/.
Transformation jobs will normalize these to Silver schemas.
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# CRM Schemas - match actual CSV headers
CRM_ACCOUNTS_SCHEMA = StructType(
    [
        StructField("Id", StringType(), nullable=True),  # account_id (nullable in raw)
        StructField("Name", StringType(), nullable=True),  # account_name
        StructField("Phone", StringType(), nullable=True),
        StructField("Website", StringType(), nullable=True),
        StructField("Industry", StringType(), nullable=True),
        StructField("AnnualRevenue", DoubleType(), nullable=True),
        StructField("NumberOfEmployees", IntegerType(), nullable=True),
        StructField("BillingStreet", StringType(), nullable=True),
        StructField("BillingCity", StringType(), nullable=True),
        StructField("BillingState", StringType(), nullable=True),
        StructField("BillingPostalCode", StringType(), nullable=True),
        StructField("BillingCountry", StringType(), nullable=True),
        StructField("Rating", StringType(), nullable=True),
        StructField("Type", StringType(), nullable=True),
        StructField("AccountSource", StringType(), nullable=True),
        StructField("AccountNumber", StringType(), nullable=True),
        StructField("Site", StringType(), nullable=True),
        StructField("Description", StringType(), nullable=True),
        StructField("Ownership", StringType(), nullable=True),
        StructField("ParentId", StringType(), nullable=True),
        StructField("TickerSymbol", StringType(), nullable=True),
        StructField("YearStarted", DoubleType(), nullable=True),
        StructField("CreatedDate", StringType(), nullable=True),
        StructField("LastModifiedDate", StringType(), nullable=True),
        StructField("OwnerId", StringType(), nullable=True),
        StructField("CustomerSegment", StringType(), nullable=True),  # segment
        StructField("GeographicRegion", StringType(), nullable=True),
        StructField("AccountStatus", StringType(), nullable=True),
        StructField("LastActivityDate", StringType(), nullable=True),
    ]
)

CRM_CONTACTS_SCHEMA = StructType(
    [
        StructField("Id", StringType(), nullable=True),  # contact_id (nullable in raw)
        StructField("LastName", StringType(), nullable=True),
        StructField("AccountId", StringType(), nullable=True),  # account_id
        StructField("FirstName", StringType(), nullable=True),
        StructField("Email", StringType(), nullable=True),  # contact_email
        StructField("Phone", StringType(), nullable=True),  # contact_phone
        StructField("MobilePhone", StringType(), nullable=True),
        StructField("Title", StringType(), nullable=True),
        StructField("Department", StringType(), nullable=True),
        StructField("LeadSource", StringType(), nullable=True),
        StructField("MailingStreet", StringType(), nullable=True),
        StructField("MailingCity", StringType(), nullable=True),
        StructField("MailingState", StringType(), nullable=True),
        StructField("MailingPostalCode", StringType(), nullable=True),
        StructField("MailingCountry", StringType(), nullable=True),
        StructField("DoNotCall", BooleanType(), nullable=True),
        StructField("HasOptedOutOfEmail", BooleanType(), nullable=True),
        StructField("Description", StringType(), nullable=True),
        StructField("CreatedDate", StringType(), nullable=True),
        StructField("LastModifiedDate", StringType(), nullable=True),  # last_contact_ts
        StructField("OwnerId", StringType(), nullable=True),
        StructField("ContactRole", StringType(), nullable=True),
        StructField("ContactLevel", StringType(), nullable=True),
        StructField("EngagementScore", DoubleType(), nullable=True),
    ]
)

CRM_OPPORTUNITIES_SCHEMA = StructType(
    [
        StructField("Id", StringType(), nullable=True),  # opportunity_id (nullable in raw)
        StructField("Name", StringType(), nullable=True),  # opportunity_name
        StructField("AccountId", StringType(), nullable=True),  # account_id
        StructField("StageName", StringType(), nullable=True),  # stage
        StructField("CloseDate", StringType(), nullable=True),  # close_date
        StructField("Amount", DoubleType(), nullable=True),  # amount
        StructField("Probability", IntegerType(), nullable=True),
        StructField("LeadSource", StringType(), nullable=True),
        StructField("Type", StringType(), nullable=True),
        StructField("NextStep", StringType(), nullable=True),
        StructField("Description", StringType(), nullable=True),
        StructField("ForecastCategory", StringType(), nullable=True),
        StructField("IsClosed", BooleanType(), nullable=True),
        StructField("IsWon", BooleanType(), nullable=True),
        StructField("CreatedDate", StringType(), nullable=True),
        StructField("LastModifiedDate", StringType(), nullable=True),
        StructField("OwnerId", StringType(), nullable=True),
        StructField("DealSize", StringType(), nullable=True),
        StructField("SalesCycle", IntegerType(), nullable=True),
        StructField("ProductInterest", StringType(), nullable=True),
        StructField("Budget", StringType(), nullable=True),
        StructField("Timeline", StringType(), nullable=True),
    ]
)

# Redshift Behavior Schema - match actual CSV headers
REDSHIFT_BEHAVIOR_SCHEMA = StructType(
    [
        StructField("behavior_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("event_name", StringType(), nullable=True),
        StructField("event_timestamp", StringType(), nullable=True),  # event_date
        StructField("session_id", StringType(), nullable=True),
        StructField("page_url", StringType(), nullable=True),
        StructField("referrer", StringType(), nullable=True),
        StructField("device_type", StringType(), nullable=True),
        StructField("browser", StringType(), nullable=True),
        StructField("os", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("user_agent", StringType(), nullable=True),
        StructField("ip_address", StringType(), nullable=True),
        StructField("duration_seconds", IntegerType(), nullable=True),  # session_duration_minutes
        StructField("conversion_value", DoubleType(), nullable=True),  # revenue
        StructField("utm_source", StringType(), nullable=True),
        StructField("utm_medium", StringType(), nullable=True),
        StructField("utm_campaign", StringType(), nullable=True),
        StructField("behavior_segment", StringType(), nullable=True),
        StructField("conversion_probability", DoubleType(), nullable=True),
        StructField("journey_stage", StringType(), nullable=True),
        StructField("engagement_score", DoubleType(), nullable=True),
        StructField("session_quality", StringType(), nullable=True),
    ]
)

# Snowflake Schemas - match actual CSV headers
SNOWFLAKE_CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), nullable=True),
        StructField("first_name", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
        StructField("phone", StringType(), nullable=True),
        StructField("address", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("zip_code", StringType(), nullable=True),
        StructField("registration_date", StringType(), nullable=True),  # created_date
        StructField("gender", StringType(), nullable=True),
        StructField("age", IntegerType(), nullable=True),
        StructField("customer_segment", StringType(), nullable=True),
        StructField("lifetime_value", DoubleType(), nullable=True),
        StructField("last_purchase_date", StringType(), nullable=True),
        StructField("total_orders", IntegerType(), nullable=True),
        StructField("preferred_channel", StringType(), nullable=True),
        StructField("created_at", StringType(), nullable=True),
        StructField("updated_at", StringType(), nullable=True),
        StructField("source_system", StringType(), nullable=True),
        StructField("ingestion_timestamp", StringType(), nullable=True),
        StructField("estimated_clv", DoubleType(), nullable=True),
    ]
)

SNOWFLAKE_ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("order_date", StringType(), nullable=True),
        StructField("order_timestamp", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DoubleType(), nullable=True),
        StructField("total_amount", DoubleType(), nullable=True),  # amount
        StructField("currency", StringType(), nullable=True),
        StructField("payment_method", StringType(), nullable=True),
        StructField("shipping_method", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("op", StringType(), nullable=True),
        StructField("is_deleted", BooleanType(), nullable=True),
        StructField("shipping_address", StringType(), nullable=True),
        StructField("billing_address", StringType(), nullable=True),
        StructField("discount_percent", DoubleType(), nullable=True),
        StructField("tax_amount", DoubleType(), nullable=True),
        StructField("shipping_cost", DoubleType(), nullable=True),
        StructField("promo_code", StringType(), nullable=True),
        StructField("sales_rep_id", StringType(), nullable=True),
        StructField("channel", StringType(), nullable=True),
        StructField("created_at", StringType(), nullable=True),
        StructField("updated_at", StringType(), nullable=True),
        StructField("source_system", StringType(), nullable=True),
        StructField("ingestion_timestamp", StringType(), nullable=True),
        StructField("order_month", IntegerType(), nullable=True),
        StructField("order_quarter", IntegerType(), nullable=True),
        StructField("order_year", IntegerType(), nullable=True),
        StructField("order_segment", StringType(), nullable=True),
        StructField("customer_order_count", IntegerType(), nullable=True),
        StructField("fulfillment_days", IntegerType(), nullable=True),
    ]
)

SNOWFLAKE_PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), nullable=True),
        StructField("sku", StringType(), nullable=True),
        StructField("product_name", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("subcategory", StringType(), nullable=True),
        StructField("brand", StringType(), nullable=True),
        StructField("price_usd", DoubleType(), nullable=True),  # price
        StructField("cost_usd", DoubleType(), nullable=True),
        StructField("weight_kg", DoubleType(), nullable=True),
        StructField("dimensions", StringType(), nullable=True),
        StructField("color", StringType(), nullable=True),
        StructField("size", StringType(), nullable=True),
        StructField("active", BooleanType(), nullable=True),
        StructField("stock_quantity", IntegerType(), nullable=True),
        StructField("reorder_level", IntegerType(), nullable=True),
        StructField("supplier_id", StringType(), nullable=True),
        StructField("created_at", StringType(), nullable=True),
        StructField("updated_at", StringType(), nullable=True),
        StructField("source_system", StringType(), nullable=True),
        StructField("ingestion_timestamp", StringType(), nullable=True),
        StructField("rating", DoubleType(), nullable=True),
        StructField("review_count", IntegerType(), nullable=True),
        StructField("in_stock", BooleanType(), nullable=True),
    ]
)

# FX Rates Schema (supports both CSV and JSON formats)
# Note: JSON uses base_ccy/quote_ccy/rate, but we normalize to base_currency/target_currency/exchange_rate
FX_RATES_SCHEMA = StructType(
    [
        # Accept both formats during read
        StructField("date", StringType(), nullable=True),  # JSON format
        StructField("trade_date", StringType(), nullable=True),  # CSV format
        StructField("base_ccy", StringType(), nullable=True),  # JSON format (will be normalized)
        StructField("base_currency", StringType(), nullable=True),  # CSV format (target)
        StructField("quote_ccy", StringType(), nullable=True),  # JSON format (will be normalized)
        StructField("counter_ccy", StringType(), nullable=True),  # JSON alternative field
        StructField("target_currency", StringType(), nullable=True),  # CSV format (target)
        StructField("rate", DoubleType(), nullable=True),  # JSON format (will be normalized)
        StructField("exchange_rate", DoubleType(), nullable=True),  # CSV format (target)
        StructField("source", StringType(), nullable=True),  # Optional metadata
        StructField("record_source", StringType(), nullable=True),  # Normalized source identifier
        StructField(
            "ingest_timestamp", StringType(), nullable=True
        ),  # Will be converted to timestamp
        StructField("bid_rate", DoubleType(), nullable=True),  # Optional
        StructField("ask_rate", DoubleType(), nullable=True),  # Optional
        StructField("mid_rate", DoubleType(), nullable=True),  # Optional
    ]
)

# Normalized Bronze FX schema (post-read normalization used in DQ)
FX_RATES_BRONZE_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("trade_date", DateType(), nullable=True),
        StructField("base_ccy", StringType(), nullable=True),
        StructField("base_currency", StringType(), nullable=True),
        StructField("quote_ccy", StringType(), nullable=True),
        StructField("counter_ccy", StringType(), nullable=True),
        StructField("target_currency", StringType(), nullable=True),
        StructField("rate", DoubleType(), nullable=True),
        StructField("exchange_rate", DoubleType(), nullable=True),
        StructField("source", StringType(), nullable=True),
        StructField("record_source", StringType(), nullable=True),
        StructField("ingest_timestamp", TimestampType(), nullable=True),
        StructField("bid_rate", DoubleType(), nullable=True),
        StructField("ask_rate", DoubleType(), nullable=True),
        StructField("mid_rate", DoubleType(), nullable=True),
        StructField("fx_rate", DoubleType(), nullable=True),
        StructField("fx_mid_rate", DoubleType(), nullable=True),
        StructField("fx_bid_rate", DoubleType(), nullable=True),
        StructField("fx_ask_rate", DoubleType(), nullable=True),
    ]
)

# Kafka Events Schema - match actual CSV headers
KAFKA_EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=True),
        StructField("topic", StringType(), nullable=True),
        StructField("partition", IntegerType(), nullable=True),
        StructField("offset", IntegerType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),  # event_ts
        StructField("key", StringType(), nullable=True),
        StructField("value", StringType(), nullable=True),  # JSON string with event data
        StructField("headers", StringType(), nullable=True),  # JSON string
    ]
)
