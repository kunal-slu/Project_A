from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Adjust fields to your real schema if different
returns_raw_schema = StructType(
    [
        StructField("return_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("qty", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("return_date", DateType(), True),
        StructField("ingest_ts", TimestampType(), True),
        # add more fields here if present
    ]
)
