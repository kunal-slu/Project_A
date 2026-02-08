"""
Kafka Streaming Fitness Validator

Validates that streaming data is suitable for Spark Structured Streaming:
- Monotonically increasing timestamps
- Proper event schema
- Mixed event types
- Session ID consistency
- High cardinality
- Late events handling
- Out-of-order events
"""

import logging
from datetime import timedelta
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class KafkaStreamingValidator:
    """Validate Kafka streaming data fitness."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.issues: list[dict[str, Any]] = []

    def validate_streaming_fitness(
        self,
        df: DataFrame,
        timestamp_col: str = "event_timestamp",
        event_type_col: str = "event_type",
        session_col: str = "session_id",
    ) -> dict[str, Any]:
        """Comprehensive streaming fitness validation."""
        logger.info("🔍 Validating Kafka streaming fitness...")

        result = {
            "valid": True,
            "timestamp_monotonic": False,
            "event_type_diversity": {},
            "session_consistency": {},
            "cardinality": {},
            "late_events": {},
            "out_of_order": {},
            "issues": [],
        }

        # Check timestamp monotonicity
        timestamp_result = self._check_timestamp_monotonicity(df, timestamp_col)
        result["timestamp_monotonic"] = timestamp_result["monotonic"]
        if not timestamp_result["monotonic"]:
            result["valid"] = False
            result["issues"].append("Timestamps are not monotonically increasing")

        # Check event type diversity
        event_diversity = self._check_event_type_diversity(df, event_type_col)
        result["event_type_diversity"] = event_diversity
        if event_diversity.get("unique_types", 0) < 3:
            result["issues"].append("Low event type diversity")

        # Check session consistency
        session_result = self._check_session_consistency(df, session_col, timestamp_col)
        result["session_consistency"] = session_result

        # Check cardinality
        cardinality = self._check_cardinality(df)
        result["cardinality"] = cardinality

        # Check for late events (events older than 1 hour from latest)
        late_events = self._check_late_events(df, timestamp_col)
        result["late_events"] = late_events

        # Check for out-of-order events
        out_of_order = self._check_out_of_order(df, timestamp_col, session_col)
        result["out_of_order"] = out_of_order

        if result["issues"]:
            self.issues.append(result)
            logger.warning(f"⚠️ Streaming fitness issues: {result['issues']}")
        else:
            logger.info("✅ Streaming data is fit for Kafka/Structured Streaming")

        return result

    def _check_timestamp_monotonicity(self, df: DataFrame, timestamp_col: str) -> dict[str, Any]:
        """Check if timestamps are monotonically increasing."""
        from pyspark.sql.window import Window

        window = Window.orderBy(timestamp_col)
        df_with_prev = df.withColumn("prev_timestamp", F.lag(timestamp_col).over(window))

        # Count rows where current < previous
        violations = df_with_prev.filter(
            (F.col(timestamp_col) < F.col("prev_timestamp")) & F.col("prev_timestamp").isNotNull()
        ).count()

        total_rows = df.count()
        violation_pct = (violations / total_rows * 100) if total_rows > 0 else 0

        return {
            "monotonic": violations == 0,
            "violations": violations,
            "violation_percentage": round(violation_pct, 2),
        }

    def _check_event_type_diversity(self, df: DataFrame, event_type_col: str) -> dict[str, Any]:
        """Check event type diversity."""
        if event_type_col not in df.columns:
            return {"unique_types": 0, "distribution": {}}

        event_counts = df.groupBy(event_type_col).count().collect()
        distribution = {row[event_type_col]: row["count"] for row in event_counts}

        return {"unique_types": len(distribution), "distribution": distribution}

    def _check_session_consistency(
        self, df: DataFrame, session_col: str, timestamp_col: str
    ) -> dict[str, Any]:
        """Check session ID consistency."""
        if session_col not in df.columns:
            return {"valid": False, "issue": "session_id column not found"}

        # Check for sessions with gaps (potential issues)
        from pyspark.sql.window import Window

        window = Window.partitionBy(session_col).orderBy(timestamp_col)
        df_with_gaps = df.withColumn(
            "time_diff",
            F.col(timestamp_col).cast("long") - F.lag(timestamp_col).over(window).cast("long"),
        )

        # Sessions with gaps > 1 hour
        large_gaps = df_with_gaps.filter(
            (F.col("time_diff") > 3600) & F.col("time_diff").isNotNull()
        ).count()

        return {
            "valid": large_gaps == 0,
            "sessions_with_large_gaps": large_gaps,
            "total_sessions": df.select(session_col).distinct().count(),
        }

    def _check_cardinality(self, df: DataFrame) -> dict[str, Any]:
        """Check data cardinality metrics."""
        total_rows = df.count()

        # Check unique customers
        customer_cardinality = 0
        if "customer_id" in df.columns:
            customer_cardinality = df.select("customer_id").distinct().count()

        # Check unique sessions
        session_cardinality = 0
        if "session_id" in df.columns:
            session_cardinality = df.select("session_id").distinct().count()

        return {
            "total_rows": total_rows,
            "unique_customers": customer_cardinality,
            "unique_sessions": session_cardinality,
            "rows_per_customer": round(total_rows / customer_cardinality, 2)
            if customer_cardinality > 0
            else 0,
            "rows_per_session": round(total_rows / session_cardinality, 2)
            if session_cardinality > 0
            else 0,
        }

    def _check_late_events(self, df: DataFrame, timestamp_col: str) -> dict[str, Any]:
        """Check for late events (events older than 1 hour from latest)."""
        if df.count() == 0:
            return {"late_events_count": 0, "latest_timestamp": None}

        latest_ts = df.agg(F.max(timestamp_col).alias("max_ts")).collect()[0]["max_ts"]

        if latest_ts is None:
            return {"late_events_count": 0, "latest_timestamp": None}

        # Events older than 1 hour from latest
        one_hour_ago = latest_ts - timedelta(hours=1)
        late_events = df.filter(F.col(timestamp_col) < F.lit(one_hour_ago)).count()

        return {
            "late_events_count": late_events,
            "latest_timestamp": latest_ts.isoformat()
            if hasattr(latest_ts, "isoformat")
            else str(latest_ts),
            "threshold_hours": 1,
        }

    def _check_out_of_order(
        self, df: DataFrame, timestamp_col: str, session_col: str
    ) -> dict[str, Any]:
        """Check for out-of-order events within sessions."""
        if session_col not in df.columns:
            return {"out_of_order_count": 0, "sessions_affected": 0}

        from pyspark.sql.window import Window

        window = Window.partitionBy(session_col).orderBy(timestamp_col)
        df_with_order = df.withColumn("prev_ts", F.lag(timestamp_col).over(window))

        # Events where current < previous within same session
        out_of_order = df_with_order.filter(
            (F.col(timestamp_col) < F.col("prev_ts")) & F.col("prev_ts").isNotNull()
        )

        out_of_order_count = out_of_order.count()
        sessions_affected = (
            out_of_order.select(session_col).distinct().count() if out_of_order_count > 0 else 0
        )

        return {"out_of_order_count": out_of_order_count, "sessions_affected": sessions_affected}

    def get_all_issues(self) -> list[dict[str, Any]]:
        """Get all detected streaming fitness issues."""
        return self.issues
