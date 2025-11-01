-- DDL for all tables in the data platform
-- Supports: Redshift, Snowflake, Athena (via Glue Catalog)

-- ============================================================================
-- BRONZE LAYER (Raw landing, normalized)
-- ============================================================================

-- Bronze: Customer Behavior Events
CREATE TABLE IF NOT EXISTS bronze.customer_behavior (
    event_id VARCHAR(255) NOT NULL,
    customer_id VARCHAR(255),
    event_name VARCHAR(100),
    event_ts TIMESTAMP,
    session_id VARCHAR(100),
    page_url VARCHAR(500),
    referrer VARCHAR(500),
    device_type VARCHAR(50),
    country VARCHAR(2),
    -- Metadata
    record_source VARCHAR(50),
    ingest_timestamp TIMESTAMP,
    _proc_date DATE
)
PARTITIONED BY (_proc_date)
STORED AS PARQUET
LOCATION 's3://my-etl-lake-demo/bronze/customer_behavior/';

-- Bronze: CRM Accounts
CREATE TABLE IF NOT EXISTS bronze.crm_accounts (
    account_id VARCHAR(255) NOT NULL,
    account_name VARCHAR(255),
    account_type VARCHAR(50),
    industry VARCHAR(100),
    billing_street VARCHAR(255),
    billing_city VARCHAR(100),
    billing_state VARCHAR(50),
    billing_country VARCHAR(50),
    billing_postal_code VARCHAR(20),
    phone VARCHAR(50),
    website VARCHAR(255),
    annual_revenue DECIMAL(18,2),
    number_of_employees INT,
    rating VARCHAR(50),
    account_source VARCHAR(100),
    created_date TIMESTAMP,
    last_modified_date TIMESTAMP,
    owner_id VARCHAR(255),
    -- Metadata
    record_source VARCHAR(50),
    ingest_timestamp TIMESTAMP,
    _proc_date DATE
)
PARTITIONED BY (_proc_date)
STORED AS PARQUET
LOCATION 's3://my-etl-lake-demo/bronze/crm_accounts/';

-- Bronze: CRM Contacts
CREATE TABLE IF NOT EXISTS bronze.crm_contacts (
    contact_id VARCHAR(255) NOT NULL,
    account_id VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    phone VARCHAR(50),
    mobile_phone VARCHAR(50),
    title VARCHAR(100),
    department VARCHAR(100),
    lead_source VARCHAR(100),
    mailing_street VARCHAR(255),
    mailing_city VARCHAR(100),
    mailing_state VARCHAR(50),
    mailing_postal_code VARCHAR(20),
    mailing_country VARCHAR(50),
    email_opt_out BOOLEAN,
    do_not_call BOOLEAN,
    created_date TIMESTAMP,
    last_modified_date TIMESTAMP,
    owner_id VARCHAR(255),
    -- Metadata
    record_source VARCHAR(50),
    ingest_timestamp TIMESTAMP,
    _proc_date DATE
)
PARTITIONED BY (_proc_date)
STORED AS PARQUET
LOCATION 's3://my-etl-lake-demo/bronze/crm_contacts/';

-- Bronze: Snowflake Orders
CREATE TABLE IF NOT EXISTS bronze.snowflake_orders (
    order_id VARCHAR(255) NOT NULL,
    customer_id VARCHAR(255),
    product_id VARCHAR(255),
    order_date DATE,
    order_amount DECIMAL(18,2),
    quantity INT,
    currency VARCHAR(3),
    status VARCHAR(50),
    ship_date DATE,
    -- Metadata
    record_source VARCHAR(50),
    ingest_timestamp TIMESTAMP,
    _proc_date DATE
)
PARTITIONED BY (_proc_date)
STORED AS PARQUET
LOCATION 's3://my-etl-lake-demo/bronze/snowflake_orders/';

-- ============================================================================
-- SILVER LAYER (Business-cleaned, PKs/UKs enforced)
-- ============================================================================

-- Silver: Dimension - Customer
CREATE TABLE IF NOT EXISTS silver.dim_customer (
    customer_id VARCHAR(255) NOT NULL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    account_id VARCHAR(255),
    account_name VARCHAR(255),
    industry VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    created_date TIMESTAMP,
    last_modified_date TIMESTAMP,
    -- Metadata
    _processing_ts TIMESTAMP,
    _run_id VARCHAR(100),
    _proc_date DATE
)
PARTITIONED BY (_proc_date)
STORED AS DELTA
LOCATION 's3://my-etl-lake-demo/silver/dim_customer/';

-- Silver: Dimension - Account
CREATE TABLE IF NOT EXISTS silver.dim_account (
    account_id VARCHAR(255) NOT NULL PRIMARY KEY,
    account_name VARCHAR(255) NOT NULL,
    account_type VARCHAR(50),
    industry VARCHAR(100),
    billing_city VARCHAR(100),
    billing_state VARCHAR(50),
    billing_country VARCHAR(50),
    annual_revenue DECIMAL(18,2),
    number_of_employees INT,
    rating VARCHAR(50),
    created_date TIMESTAMP,
    last_modified_date TIMESTAMP,
    -- Metadata
    _processing_ts TIMESTAMP,
    _run_id VARCHAR(100),
    _proc_date DATE
)
PARTITIONED BY (_proc_date)
STORED AS DELTA
LOCATION 's3://my-etl-lake-demo/silver/dim_account/';

-- Silver: Dimension - Product
CREATE TABLE IF NOT EXISTS silver.dim_product (
    product_id VARCHAR(255) NOT NULL PRIMARY KEY,
    product_name VARCHAR(255),
    sku VARCHAR(100),
    category VARCHAR(100),
    brand VARCHAR(100),
    price_usd DECIMAL(10,2),
    cost_usd DECIMAL(10,2),
    created_date TIMESTAMP,
    last_modified_date TIMESTAMP,
    -- Metadata
    _processing_ts TIMESTAMP,
    _run_id VARCHAR(100),
    _proc_date DATE
)
PARTITIONED BY (_proc_date)
STORED AS DELTA
LOCATION 's3://my-etl-lake-demo/silver/dim_product/';

-- Silver: Fact - Orders
CREATE TABLE IF NOT EXISTS silver.fact_orders (
    order_id VARCHAR(255) NOT NULL PRIMARY KEY,
    customer_id VARCHAR(255) NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    order_date DATE NOT NULL,
    order_amount DECIMAL(18,2),
    quantity INT,
    currency VARCHAR(3),
    amount_usd DECIMAL(18,2),  -- FX converted
    status VARCHAR(50),
    ship_date DATE,
    -- Metadata
    _processing_ts TIMESTAMP,
    _run_id VARCHAR(100),
    _proc_date DATE
)
PARTITIONED BY (order_date)
STORED AS DELTA
LOCATION 's3://my-etl-lake-demo/silver/fact_orders/';

-- Silver: Fact - Events
CREATE TABLE IF NOT EXISTS silver.fact_events (
    event_id VARCHAR(255) NOT NULL PRIMARY KEY,
    customer_id VARCHAR(255),
    event_name VARCHAR(100),
    event_ts TIMESTAMP NOT NULL,
    session_id VARCHAR(100),
    page_url VARCHAR(500),
    referrer VARCHAR(500),
    device_type VARCHAR(50),
    country VARCHAR(2),
    -- Metadata
    _processing_ts TIMESTAMP,
    _run_id VARCHAR(100),
    _proc_date DATE
)
PARTITIONED BY (_proc_date)
STORED AS DELTA
LOCATION 's3://my-etl-lake-demo/silver/fact_events/';

-- Silver: Bridge - Customer Account
CREATE TABLE IF NOT EXISTS silver.bridge_customer_account (
    customer_id VARCHAR(255) NOT NULL,
    account_id VARCHAR(255) NOT NULL,
    relationship_type VARCHAR(50),  -- Primary, Secondary, etc.
    start_date DATE,
    end_date DATE,
    is_active BOOLEAN,
    PRIMARY KEY (customer_id, account_id)
)
STORED AS DELTA
LOCATION 's3://my-etl-lake-demo/silver/bridge_customer_account/';

-- ============================================================================
-- GOLD LAYER (Analytics-ready, joined, aggregated)
-- ============================================================================

-- Gold: Fact - Customer 360
CREATE TABLE IF NOT EXISTS gold.fact_customer_360 (
    customer_id VARCHAR(255) NOT NULL PRIMARY KEY,
    -- Customer attributes
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    -- Account attributes
    account_id VARCHAR(255),
    account_name VARCHAR(255),
    industry VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    -- Metrics
    lifetime_value_usd DECIMAL(18,2),
    total_orders INT,
    last_order_date DATE,
    total_events INT,
    last_event_ts TIMESTAMP,
    -- Metadata
    _load_ts TIMESTAMP,
    _proc_date DATE
)
PARTITIONED BY (_proc_date)
STORED AS DELTA
LOCATION 's3://my-etl-lake-demo/gold/fact_customer_360/';

-- Gold: Fact - Orders Daily
CREATE TABLE IF NOT EXISTS gold.fact_orders_daily (
    order_date DATE NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    -- Metrics
    order_count INT,
    total_quantity INT,
    revenue_usd DECIMAL(18,2),
    avg_order_value_usd DECIMAL(18,2),
    max_order_value_usd DECIMAL(18,2),
    min_order_value_usd DECIMAL(18,2),
    unique_customers INT,
    -- Metadata
    _load_ts TIMESTAMP,
    PRIMARY KEY (order_date, product_id)
)
PARTITIONED BY (order_date)
STORED AS DELTA
LOCATION 's3://my-etl-lake-demo/gold/fact_orders_daily/';

-- Gold: Fact - Marketing Events
CREATE TABLE IF NOT EXISTS gold.fact_marketing_events (
    event_date DATE NOT NULL,
    event_name VARCHAR(100) NOT NULL,
    customer_id VARCHAR(255),
    -- Metrics
    event_count INT,
    unique_sessions INT,
    unique_customers INT,
    -- Dimensions
    device_type VARCHAR(50),
    country VARCHAR(2),
    -- Metadata
    _load_ts TIMESTAMP,
    PRIMARY KEY (event_date, event_name, customer_id)
)
PARTITIONED BY (event_date)
STORED AS DELTA
LOCATION 's3://my-etl-lake-demo/gold/fact_marketing_events/';

-- ============================================================================
-- Redshift Tables (for reporting)
-- ============================================================================

-- Redshift: Customer 360 (synced from Gold)
CREATE TABLE IF NOT EXISTS redshift_analytics.gold_customer_360 (
    customer_id VARCHAR(255) NOT NULL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    account_id VARCHAR(255),
    account_name VARCHAR(255),
    industry VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    lifetime_value_usd DECIMAL(18,2),
    total_orders INT,
    last_order_date DATE,
    total_events INT,
    last_event_ts TIMESTAMP,
    _load_ts TIMESTAMP
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (account_id, last_order_date);

-- ============================================================================
-- Snowflake Tables (for analytics)
-- ============================================================================

-- Snowflake: Customer 360 (synced from Gold)
CREATE TABLE IF NOT EXISTS analytics.fact_customer_360 (
    customer_id VARCHAR(255) NOT NULL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),  -- PII masked in Silver→Gold
    phone VARCHAR(50),   -- PII masked
    account_id VARCHAR(255),
    account_name VARCHAR(255),
    industry VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    lifetime_value_usd DECIMAL(18,2),
    total_orders INT,
    last_order_date DATE,
    total_events INT,
    last_event_ts TIMESTAMP,
    _load_ts TIMESTAMP
)
CLUSTER BY (account_id, last_order_date);

