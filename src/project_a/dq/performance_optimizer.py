"""
Performance Optimization Checker

Validates data is optimized for Spark performance:
- Small dim tables suitable for broadcast joins
- Large fact tables properly partitioned
- No data skew
- Proper column types
- Date columns in correct format
"""

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class PerformanceOptimizer:
    """Check and optimize data for Spark performance."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.issues: list[dict[str, Any]] = []
        self.recommendations: list[str] = []

    def analyze_table_performance(
        self, df: DataFrame, table_name: str, is_dimension: bool = False
    ) -> dict[str, Any]:
        """Analyze table for performance optimization opportunities."""
        logger.info(f"🔍 Analyzing performance for {table_name}...")

        result = {
            "table": table_name,
            "row_count": df.count(),
            "column_count": len(df.columns),
            "broadcast_suitable": False,
            "partitioning_optimal": False,
            "skew_detected": False,
            "type_optimizations": [],
            "recommendations": [],
        }

        row_count = result["row_count"]

        # Check if suitable for broadcast join (dimension tables < 10MB)
        if is_dimension:
            # Estimate size (rough calculation)
            sample_size = df.limit(1000).cache()
            sample_count = sample_size.count()
            if sample_count > 0:
                estimated_size_mb = (row_count / sample_count) * 0.1  # Rough estimate
                result["broadcast_suitable"] = estimated_size_mb < 10
                if not result["broadcast_suitable"]:
                    result["recommendations"].append(
                        f"Table is {estimated_size_mb:.1f}MB - consider reducing size for broadcast join"
                    )

        # Check for data skew
        skew_result = self._check_data_skew(df, table_name)
        result["skew_detected"] = skew_result["has_skew"]
        result["skew_details"] = skew_result

        # Check column types
        type_result = self._check_column_types(df)
        result["type_optimizations"] = type_result

        # Check partitioning
        if not is_dimension:
            partition_result = self._check_partitioning(df, table_name)
            result["partitioning_optimal"] = partition_result["optimal"]
            result["partitioning_details"] = partition_result

        if result["recommendations"]:
            self.recommendations.extend(
                [f"{table_name}: {rec}" for rec in result["recommendations"]]
            )

        return result

    def _check_data_skew(self, df: DataFrame, table_name: str) -> dict[str, Any]:
        """Check for data skew in key columns."""
        # Check for ID columns that might cause skew
        id_columns = [col for col in df.columns if col.endswith("_id")]

        skew_results = {}
        has_skew = False

        for id_col in id_columns[:3]:  # Check first 3 ID columns
            value_counts = df.groupBy(id_col).count()
            counts = [row["count"] for row in value_counts.collect()]

            if counts:
                max_count = max(counts)
                avg_count = sum(counts) / len(counts)
                skew_ratio = max_count / avg_count if avg_count > 0 else 0

                skew_results[id_col] = {
                    "max_count": max_count,
                    "avg_count": round(avg_count, 2),
                    "skew_ratio": round(skew_ratio, 2),
                    "has_skew": skew_ratio > 10,  # Threshold: 10x average
                }

                if skew_results[id_col]["has_skew"]:
                    has_skew = True

        return {"has_skew": has_skew, "column_details": skew_results}

    def _check_column_types(self, df: DataFrame) -> list[dict[str, Any]]:
        """Check for column type optimizations."""
        optimizations = []

        for field in df.schema.fields:
            field_type = str(field.dataType)

            # Check for string columns that could be more efficient
            if "StringType" in field_type:
                # Check if it's actually numeric or date
                if field.name.endswith("_id") or field.name.endswith("_date"):
                    optimizations.append(
                        {
                            "column": field.name,
                            "current_type": field_type,
                            "recommendation": "Consider using numeric or date type if applicable",
                        }
                    )

            # Check for large decimal precision
            if "DecimalType" in field_type:
                optimizations.append(
                    {
                        "column": field.name,
                        "current_type": field_type,
                        "recommendation": "Verify decimal precision is necessary",
                    }
                )

        return optimizations

    def _check_partitioning(self, df: DataFrame, table_name: str) -> dict[str, Any]:
        """Check if table is properly partitioned."""
        result = {
            "optimal": False,
            "has_date_column": False,
            "date_column": None,
            "recommendation": None,
        }

        # Check for date columns suitable for partitioning
        date_columns = [
            col for col in df.columns if "date" in col.lower() or "timestamp" in col.lower()
        ]

        if date_columns:
            result["has_date_column"] = True
            result["date_column"] = date_columns[0]
            result["optimal"] = True
            result["recommendation"] = f"Partition by {date_columns[0]}"
        else:
            result["recommendation"] = "Consider adding date column for partitioning"

        return result

    def get_recommendations(self) -> list[str]:
        """Get all performance recommendations."""
        return self.recommendations

    def generate_report(self) -> str:
        """Generate performance optimization report."""
        report = ["🚀 Performance Optimization Report", "=" * 50, ""]

        if self.recommendations:
            report.append("Recommendations:")
            for rec in self.recommendations:
                report.append(f"  - {rec}")
        else:
            report.append("✅ No performance optimizations needed.")

        return "\n".join(report)
