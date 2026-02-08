from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession


def test_returns_schema(spark: SparkSession):
    if isinstance(spark, MagicMock):
        pytest.skip("Spark unavailable in current environment")
    df = spark.read.option("header", True).csv("tests/data/returns_raw.csv")
    assert "return_id" in df.columns
