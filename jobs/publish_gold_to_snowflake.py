"""Publish customer_360 gold table to Snowflake (test-friendly shim)."""

from __future__ import annotations

import os

from project_a.monitoring.lineage_emitter import emit_complete, emit_fail, emit_start
from project_a.monitoring.metrics_collector import emit_metrics
from project_a.utils.secrets import get_snowflake_credentials


def load_customer_360_to_snowflake(spark, config: dict) -> None:
    gold_base = config.get("data_lake", {}).get("gold_path", "")
    source_path = f"{gold_base}/customer_360"

    emit_start(
        "publish_gold_to_snowflake",
        [{"name": source_path}],
        [{"name": "snowflake://ANALYTICS.PUBLIC.CUSTOMER_360"}],
        config,
    )

    try:
        local_source = source_path
        if source_path.startswith("file://"):
            local_source = source_path.replace("file://", "", 1)
        if local_source.startswith("/") and not os.path.exists(local_source):
            raise FileNotFoundError(f"Gold path not found: {source_path}")

        df = spark.read.format("delta").load(source_path)
        creds = get_snowflake_credentials(config)
        if not creds:
            raise ValueError("Missing Snowflake credentials")

        (
            df.write.format("snowflake")
            .options(**creds)
            .option("dbtable", "CUSTOMER_360")
            .mode("overwrite")
            .save()
        )

        emit_metrics(
            job_name="publish_gold_to_snowflake",
            rows_in=df.count(),
            rows_out=df.count(),
            duration_seconds=0.0,
            dq_status="pass",
            config=config,
        )
        emit_complete(
            "publish_gold_to_snowflake",
            [{"name": source_path}],
            [{"name": "snowflake://ANALYTICS.PUBLIC.CUSTOMER_360"}],
            config,
            {"rows": df.count()},
        )
    except Exception as exc:
        emit_fail(
            "publish_gold_to_snowflake",
            [{"name": source_path}],
            [{"name": "snowflake://ANALYTICS.PUBLIC.CUSTOMER_360"}],
            config,
            str(exc),
        )
        raise
