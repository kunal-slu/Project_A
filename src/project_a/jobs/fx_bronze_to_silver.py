"""FX Bronze → Silver helpers used in tests and local workflows."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def deduplicate_fx_rates(spark, df: DataFrame) -> DataFrame:
    """Keep latest FX rate per (ccy, as_of_date, base_currency) by ingestion timestamp."""
    if df.rdd.isEmpty():
        return df

    window = Window.partitionBy("ccy", "as_of_date", "base_currency").orderBy(
        F.to_timestamp("ingestion_timestamp").desc_nulls_last()
    )
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def add_rate_categories(spark, df: DataFrame) -> DataFrame:
    """Assign a rate category for demo analytics (major/minor/exotic)."""
    majors = {"USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD"}
    minors = {"SEK", "NOK", "DKK", "SGD", "HKD"}

    return df.withColumn(
        "rate_category",
        F.when(F.col("ccy").isin(list(majors)), F.lit("major"))
        .when(F.col("ccy").isin(list(minors)), F.lit("minor"))
        .otherwise(F.lit("exotic")),
    )


def validate_fx_rates(df: DataFrame) -> bool:
    """Basic FX rate validation for tests."""
    required = ["ccy", "rate_to_base", "as_of_date", "base_currency", "ingestion_timestamp"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        return False

    nulls = (
        df.filter(
            F.col("ccy").isNull()
            | F.col("rate_to_base").isNull()
            | F.col("as_of_date").isNull()
            | F.col("base_currency").isNull()
            | F.col("ingestion_timestamp").isNull()
        )
        .limit(1)
        .count()
    )
    if nulls > 0:
        return False

    invalid_rates = df.filter(F.col("rate_to_base") <= F.lit(0)).limit(1).count()
    if invalid_rates > 0:
        return False

    dupes = (
        df.groupBy("ccy", "as_of_date").count().filter(F.col("count") > F.lit(1)).limit(1).count()
    )
    return dupes == 0
