import os

from pyspark_interview_project.load import (write_avro, write_json,
                                            write_parquet)


def test_write_parquet(spark, tmp_path):
    df = spark.createDataFrame([(1, "TX")], ["customer_id", "state"])
    p = str(tmp_path / "pq")
    write_parquet(df, p)
    assert os.path.isdir(p)


def test_write_avro(spark, tmp_path):
    df = spark.createDataFrame([(1, "A")], ["id", "val"])
    p = str(tmp_path / "avro")
    write_avro(df, p)
    assert os.path.isdir(p)


def test_write_json(spark, tmp_path):
    df = spark.createDataFrame([(1, "A")], ["id", "val"])
    p = str(tmp_path / "json")
    write_json(df, p)
    assert os.path.isdir(p)
