
from project_a.validate import ValidateOutput


def test_validate_parquet(spark, tmp_path):
    df = spark.createDataFrame([(1, "A")], ["id", "val"])
    out = tmp_path / "pq"
    df.write.parquet(str(out))
    v = ValidateOutput()
    v.validate_parquet(spark, str(out))


def test_validate_avro(spark, tmp_path):
    df = spark.createDataFrame([(1, "A")], ["id", "val"])
    out = tmp_path / "avro"
    df.write.format("avro").save(str(out))
    v = ValidateOutput()
    v.validate_avro(spark, str(out))


def test_validate_json(spark, tmp_path):
    df = spark.createDataFrame([(1, "A")], ["id", "val"])
    out = tmp_path / "json"
    df.write.json(str(out))
    v = ValidateOutput()
    v.validate_json(spark, str(out))
