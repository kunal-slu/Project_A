"""
modeling.py
Utilities for dimensional modeling: date dimension, surrogate keys, and SCD helpers.
"""


from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    date_format,
    dayofmonth,
    expr,
    lit,
    month,
    quarter,
    sha2,
    weekofyear,
    year,
)


def add_surrogate_key(
    df: DataFrame, key_columns: list[str], surrogate_key_column: str
) -> DataFrame:
    """
    Add a deterministic surrogate key using SHA-256 over concatenated natural keys.

    Args:
        df: Input DataFrame
        key_columns: List of natural key column names
        surrogate_key_column: Name of the new surrogate key column
    """
    return df.withColumn(
        surrogate_key_column,
        sha2(concat_ws("||", *[col(c).cast("string") for c in key_columns]), 256),
    )


def build_dim_date(spark: SparkSession, start_date: str, end_date: str) -> DataFrame:
    """
    Build a standard date dimension between start_date and end_date (inclusive).

    Args:
        spark: SparkSession
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
    """
    df = spark.sql(
        f"""
        SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS date
        """
    )
    out = (
        df.withColumn("date_key", date_format(col("date"), "yyyyMMdd"))
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("day", dayofmonth(col("date")))
        .withColumn("week_of_year", weekofyear(col("date")))
        .withColumn("quarter", quarter(col("date")))
        .withColumn("year_month", date_format(col("date"), "yyyyMM"))
        .withColumn(
            "is_weekend",
            expr("case when dayofweek(date) in (1,7) then true else false end"),
        )
    )
    return out


def build_dim_products_scd2_base(products: DataFrame) -> DataFrame:
    """
    Build a simple SCD2-like structure for products using launch_date as effective_from.
    This is a base implementation; in production, merge actual change logs.
    """
    out = products
    if "launch_date" in out.columns:
        out = out.withColumn("effective_from", col("launch_date"))
    else:
        out = out.withColumn("effective_from", lit(None).cast("date"))
    return out.withColumn("effective_to", lit(None).cast("date")).withColumn(
        "is_current", lit(True)
    )


# --- Snowflake schema helpers ---


def build_dim_category(products: DataFrame) -> DataFrame:
    """
    Create a category dimension with surrogate keys.
    """
    dim = (
        products.select("category").dropna().dropDuplicates(["category"])
        if "category" in products.columns
        else products.sparkSession.createDataFrame([], products.schema)
    )
    if dim.columns:
        return add_surrogate_key(dim, ["category"], "category_sk")
    return dim


def build_dim_brand(products: DataFrame) -> DataFrame:
    """
    Create a brand dimension with surrogate keys.
    """
    dim = (
        products.select("brand").dropna().dropDuplicates(["brand"])
        if "brand" in products.columns
        else products.sparkSession.createDataFrame([], products.schema)
    )
    if dim.columns:
        return add_surrogate_key(dim, ["brand"], "brand_sk")
    return dim


def normalize_dim_products(
    products_sk: DataFrame, dim_category: DataFrame, dim_brand: DataFrame
) -> DataFrame:
    """
    Normalize product dimension to reference category and brand dimensions.
    Adds category_sk and brand_sk and retains original natural keys for traceability.
    """
    out = products_sk
    if "category" in out.columns and {"category"}.issubset(set(dim_category.columns)):
        out = out.join(dim_category, ["category"], "left")
    if "brand" in out.columns and {"brand"}.issubset(set(dim_brand.columns)):
        out = out.join(dim_brand, ["brand"], "left")
    return out


def build_dim_geography(customers: DataFrame) -> DataFrame:
    """
    Create a geography dimension (city, state, country) with surrogate keys.
    """
    cols = [c for c in ["city", "state", "country"] if c in customers.columns]
    if not cols:
        return customers.sparkSession.createDataFrame([], customers.schema)
    dim = customers.select(*cols).dropDuplicates(cols)
    return add_surrogate_key(dim, cols, "geography_sk")


def normalize_dim_customers(customers_sk: DataFrame, dim_geography: DataFrame) -> DataFrame:
    """
    Normalize customers to reference geography dimension via geography_sk.
    """
    cols = [c for c in ["city", "state", "country"] if c in customers_sk.columns]
    if not cols:
        return customers_sk
    return customers_sk.join(dim_geography.select(*cols, "geography_sk"), cols, "left")
