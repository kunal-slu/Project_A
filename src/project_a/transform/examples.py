"""Lightweight Spark transformation examples used by tests and demos."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def select_and_filter(df):
    """Filter to records with email and keep a minimal customer projection."""
    return df.filter(F.col("email").isNotNull()).select("customer_id", "first_name", "state")


def join_examples(customers, products, orders):
    """Show a simple orders -> customers -> products join chain."""
    return orders.join(customers, "customer_id", "left").join(products, "product_id", "left")


def broadcast_join_demo(customers, products):
    """Show broadcast join usage for small dimension tables."""
    return customers.join(
        F.broadcast(products), customers.customer_id == products.product_id, "inner"
    )


def skew_mitigation_demo(customers):
    """Add a salt column that can be used for skew mitigation patterns."""
    return customers.withColumn("salt", (F.crc32("customer_id") % F.lit(10)).cast("int"))


def partitioning_examples(df):
    """Small partitioning example for local execution/tests."""
    return df.repartition(2)


def window_functions_demo(df):
    """Running total by customer over order date."""
    window_spec = Window.partitionBy("customer_id").orderBy("order_date")
    return df.withColumn("running_total", F.sum("quantity").over(window_spec))


def udf_examples(df):
    """Column-expression examples that mimic UDF-style enrichments."""
    return df.withColumn(
        "email_domain_py", F.regexp_extract(F.col("email"), r"@(.+)$", 1)
    ).withColumn("age_bucket", (F.col("age") / F.lit(10)).cast("int"))


def data_cleaning_examples(df):
    """Basic null-filling and string cleansing examples."""
    return df.withColumn(
        "email", F.coalesce(F.col("email"), F.lit("unknown@example.com"))
    ).withColumn(
        "product_name_clean", F.regexp_replace(F.col("product_name"), r"[^A-Za-z0-9 ]", "")
    )


def sql_vs_dsl_demo(spark, df):
    """Return equivalent SQL and DataFrame DSL aggregates."""
    df.createOrReplaceTempView("orders_tmp")
    sql_res = spark.sql(
        "SELECT customer_id, SUM(quantity) AS total_qty FROM orders_tmp GROUP BY customer_id HAVING total_qty > 10"
    )
    dsl_res = (
        df.groupBy("customer_id")
        .agg(F.sum("quantity").alias("total_qty"))
        .filter(F.col("total_qty") > 10)
    )
    return sql_res, dsl_res


def normalize_currency(orders, rates):
    """Normalize order amounts to USD using provided currency rates."""
    joined = orders.join(rates, "currency", "left")
    return joined.withColumn(
        "total_amount_usd",
        F.when(F.col("currency") == "USD", F.col("total_amount")).otherwise(
            F.col("total_amount") * F.coalesce(F.col("usd_rate"), F.lit(1.0))
        ),
    )


def join_returns(orders, returns):
    """Annotate orders with return flags."""
    return (
        orders.join(
            returns.withColumn("_returned", F.lit(True)),
            "order_id",
            "left",
        )
        .withColumn("is_returned", F.coalesce(F.col("_returned"), F.lit(False)))
        .drop("_returned")
    )


def enrich_products(products):
    """Add derived tag arrays and price band bucketing."""
    return products.withColumn(
        "tags_array", F.split(F.coalesce(F.col("tags"), F.lit("")), ",")
    ).withColumn(
        "price_band",
        F.when(F.col("price") < 50, F.lit("low"))
        .when(F.col("price") < 200, F.lit("mid"))
        .otherwise(F.lit("high")),
    )


def join_inventory(products, inventory):
    """Join product facts to inventory data."""
    return products.join(inventory, "product_id", "left")


def build_fact_orders(df):
    """Construct minimal fact fields used in tests."""
    return df.withColumn("revenue", F.col("price") * F.col("quantity")).withColumn(
        "order_ym", F.date_format(F.to_timestamp("order_date"), "yyyyMM")
    )


def build_customers_scd2(customers, changes):
    """Build a simple SCD2-style current view for customers."""
    effective_from_col = F.coalesce(F.to_timestamp(F.col("effective_from")), F.current_timestamp())
    changes_norm = changes.select(
        "customer_id",
        F.col("new_address").alias("address"),
        effective_from_col.alias("effective_from"),
    )
    joined = customers.drop("address").join(changes_norm, "customer_id", "left")
    return (
        joined.withColumn(
            "effective_from", F.coalesce(F.col("effective_from"), F.current_timestamp())
        )
        .withColumn("effective_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )


__all__ = [
    "select_and_filter",
    "join_examples",
    "broadcast_join_demo",
    "skew_mitigation_demo",
    "partitioning_examples",
    "window_functions_demo",
    "udf_examples",
    "data_cleaning_examples",
    "sql_vs_dsl_demo",
    "normalize_currency",
    "join_returns",
    "enrich_products",
    "join_inventory",
    "build_fact_orders",
    "build_customers_scd2",
]
