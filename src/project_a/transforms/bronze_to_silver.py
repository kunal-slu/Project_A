"""Simple Bronze->Silver transform helpers used in tests."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _require_columns(df: DataFrame, required: list[str], table_name: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{table_name}: Missing required columns: {missing}")


def _validate_row_drop(before_count: int, after_count: int, table_name: str, max_row_drop_ratio: float) -> None:
    if before_count <= 0:
        return
    dropped = before_count - after_count
    dropped_ratio = dropped / before_count
    if dropped_ratio > max_row_drop_ratio:
        raise ValueError(
            f"{table_name}: Unexpected row drop detected (before={before_count}, "
            f"after={after_count}, dropped_ratio={dropped_ratio:.2%}, "
            f"max_allowed={max_row_drop_ratio:.2%})"
        )


def transform_customers_bronze_to_silver(
    spark,
    df: DataFrame,
    *,
    strict: bool = False,
    max_row_drop_ratio: float = 0.50,
) -> DataFrame:
    _require_columns(df, ["customer_id", "age"], "customers_bronze")
    input_count = df.count()

    if strict and input_count == 0:
        raise ValueError("customers_bronze: Empty input DataFrame")

    if strict:
        null_pk_count = df.filter(F.col("customer_id").isNull()).count()
        if null_pk_count > 0:
            raise ValueError(f"customers_bronze: Found {null_pk_count} null primary keys (customer_id)")

    result = (
        df.filter(F.col("customer_id").isNotNull())
        .withColumn("age", F.col("age").cast("int"))
        .dropDuplicates(["customer_id"])
    )

    if strict:
        output_count = result.count()
        if output_count == 0:
            raise ValueError("customers_silver: No rows produced after transformation")
        _validate_row_drop(input_count, output_count, "customers_silver", max_row_drop_ratio)

    return result


def transform_orders_bronze_to_silver(
    spark,
    df: DataFrame,
    *,
    strict: bool = False,
    max_row_drop_ratio: float = 0.50,
) -> DataFrame:
    _require_columns(df, ["order_id", "customer_id", "quantity", "total_amount"], "orders_bronze")
    input_count = df.count()

    if strict and input_count == 0:
        raise ValueError("orders_bronze: Empty input DataFrame")

    if strict:
        null_order_id_count = df.filter(F.col("order_id").isNull()).count()
        if null_order_id_count > 0:
            raise ValueError(
                f"orders_bronze: Found {null_order_id_count} null primary keys (order_id)"
            )

        null_customer_id_count = df.filter(F.col("customer_id").isNull()).count()
        if null_customer_id_count > 0:
            raise ValueError(
                f"orders_bronze: Found {null_customer_id_count} null foreign keys (customer_id)"
            )

        negative_amount_count = df.filter(F.col("total_amount").cast("double") < F.lit(0.0)).count()
        if negative_amount_count > 0:
            raise ValueError(
                f"orders_bronze: Found {negative_amount_count} rows with negative total_amount"
            )

    out = df

    # Normalize nested payment struct to flat columns when available.
    if "payment" in out.columns:
        if "payment_method" not in out.columns:
            out = out.withColumn("payment_method", F.col("payment.method"))
        if "status" not in out.columns:
            out = out.withColumn("status", F.col("payment.status"))

    if "unit_price" not in out.columns:
        out = out.withColumn(
            "unit_price",
            F.when(
                (F.col("quantity").cast("double") != 0) & F.col("quantity").isNotNull(),
                F.col("total_amount").cast("double") / F.col("quantity").cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )

    result = (
        out.withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("total_amount") >= F.lit(0.0))
        .dropDuplicates(["order_id"])
    )

    if strict:
        output_count = result.count()
        if output_count == 0:
            raise ValueError("orders_silver: No rows produced after transformation")
        _validate_row_drop(input_count, output_count, "orders_silver", max_row_drop_ratio)

    return result


def transform_products_bronze_to_silver(
    spark,
    df: DataFrame,
    *,
    strict: bool = False,
    max_row_drop_ratio: float = 0.50,
) -> DataFrame:
    _require_columns(df, ["product_id", "product_name", "price", "stock_quantity"], "products_bronze")
    input_count = df.count()

    if strict and input_count == 0:
        raise ValueError("products_bronze: Empty input DataFrame")

    if strict:
        null_pk_count = df.filter(F.col("product_id").isNull()).count()
        if null_pk_count > 0:
            raise ValueError(f"products_bronze: Found {null_pk_count} null primary keys (product_id)")

    result = (
        df.filter(F.col("product_name").isNotNull())
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("stock_quantity", F.col("stock_quantity").cast("int"))
        .dropDuplicates(["product_id"])
    )

    if strict:
        output_count = result.count()
        if output_count == 0:
            raise ValueError("products_silver: No rows produced after transformation")
        _validate_row_drop(input_count, output_count, "products_silver", max_row_drop_ratio)

    return result
