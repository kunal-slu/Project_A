"""Simple load helpers used by tests and legacy scripts."""


def write_parquet(df, path: str) -> None:
    df.write.mode("overwrite").parquet(path)


def write_avro(df, path: str) -> None:
    df.write.mode("overwrite").format("avro").save(path)


def write_json(df, path: str) -> None:
    df.write.mode("overwrite").json(path)

