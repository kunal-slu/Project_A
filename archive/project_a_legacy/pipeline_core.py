
from pyspark.sql import DataFrame

from .dq.runner import run_yaml_policy
from .io.path_resolver import resolve

CFG = {}  # Global config, set by main


def write_delta(df: DataFrame, logical_path: str, mode="append"):
    return df.write.format("delta").mode(mode).save(resolve(logical_path, CFG))


def run_pipeline(spark, cfg: dict, run_id: str):
    global CFG
    CFG = cfg

    # Extract
    customers = spark.read.option("header", True).csv(resolve("lake://bronze/customers_raw", CFG))

    # Validate
    dq = run_yaml_policy(customers, key_cols=["customer_id"], required_cols=["first_name"])
    if dq.critical_fail:
        raise RuntimeError(f"DQ failed: {dq.issues}")

    # Transform (simple example)
    clean = customers.dropDuplicates(["customer_id"])

    # Load
    write_delta(clean, "lake://silver/customers_clean", mode="overwrite")
    # Similar steps for products, orders, returns...


def main():
    """
    Main entry point for the pipeline core module.
    This function orchestrates the entire ETL pipeline execution.
    """
    import logging

    from .utils.config import load_config
    from .utils.spark_session import build_spark

    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Load configuration
        config = load_config()

        # Build Spark session
        spark = build_spark(config)

        # Run pipeline
        run_pipeline(spark, config, "main_pipeline")

        logger.info("Pipeline execution completed successfully")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise

    finally:
        if "spark" in locals():
            spark.stop()


if __name__ == "__main__":
    main()
