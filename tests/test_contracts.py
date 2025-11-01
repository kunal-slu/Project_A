from pyspark.sql import SparkSession


def test_returns_schema(spark: SparkSession):
    df = spark.read.option("header", True).csv("tests/data/returns_raw.csv")
    assert "return_id" in df.columns










