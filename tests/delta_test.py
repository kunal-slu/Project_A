from pyspark.sql import SparkSession


def test_delta_write_and_read(spark: SparkSession):
    # Skip gracefully if delta source is not available in this environment
    try:
        df = spark.createDataFrame([(1, "foo")], ["id", "value"])
        path = "/tmp/test-delta"
        df.write.format("delta").mode("overwrite").save(path)
        out = spark.read.format("delta").load(path)
        assert out.count() == 1
    except Exception as e:
        import pytest

        if "DATA_SOURCE_NOT_FOUND" in str(e) or "delta.DefaultSource" in str(e):
            pytest.skip("Delta Lake not available in current Spark runtime")
        raise
