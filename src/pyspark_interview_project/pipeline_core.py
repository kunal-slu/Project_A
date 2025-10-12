from typing import Dict
from pyspark.sql import DataFrame
from .io.path_resolver import resolve
from .dq.runner import run_dq

CFG = {}  # Global config, set by main


def write_delta(df: DataFrame, logical_path: str, mode="append"):
    return df.write.format("delta").mode(mode).save(resolve(logical_path, CFG))


def run_pipeline(spark, cfg: Dict, run_id: str):
    global CFG
    CFG = cfg
    
    # Extract
    customers = spark.read.option("header", True).csv(
        resolve("lake://bronze/customers_raw", CFG)
    )
    
    # Validate
    dq = run_dq(customers, key_cols=["customer_id"], required_cols=["first_name"])
    if dq.critical_fail:
        raise RuntimeError(f"DQ failed: {dq.issues}")

    # Transform (simple example)
    clean = customers.dropDuplicates(["customer_id"])

    # Load
    write_delta(clean, "lake://silver/customers_clean", mode="overwrite")
    # Similar steps for products, orders, returns...












