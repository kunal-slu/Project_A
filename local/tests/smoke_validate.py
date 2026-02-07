#!/usr/bin/env python3
import os
import sys
import json

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(ROOT, 'src'))

from project_a.utils import load_config, get_spark_session
from project_a.load import read_delta


def main():
    cfg_path = os.environ.get("CONFIG_PATH", "config/config-dev.yaml")
    cfg = load_config(cfg_path)
    spark = get_spark_session(cfg)
    try:
        gold_fact = os.path.join(cfg["output"].get("gold_path", "data/lakehouse/gold"), "fact_orders")
        df = read_delta(spark, gold_fact)
        count = df.limit(10).count()
        print(json.dumps({"fact_sample_count": count}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()


