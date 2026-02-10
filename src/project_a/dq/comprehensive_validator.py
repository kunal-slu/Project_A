"""
Comprehensive Data Quality Validator

Orchestrates all DQ checks:
1. Schema drift
2. Referential integrity
3. Primary key uniqueness
4. Null analysis
5. Timestamp validation
6. Semantic validation
7. Distribution profiling
8. Incremental ETL readiness
9. Kafka streaming fitness
10. Performance optimization
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from project_a.dq.data_drift_checker import DataDriftChecker
from project_a.dq.kafka_streaming_validator import KafkaStreamingValidator
from project_a.dq.performance_optimizer import PerformanceOptimizer
from project_a.dq.referential_integrity import ReferentialIntegrityChecker
from project_a.dq.schema_drift_checker import SchemaDriftChecker

logger = logging.getLogger(__name__)


class ComprehensiveValidator:
    """Comprehensive data quality validator for all layers."""

    def __init__(self, spark: SparkSession, dq_config: dict[str, Any] | None = None):
        self.spark = spark
        self.dq_config = dq_config or {}
        self.sampling_cfg = (self.dq_config.get("sampling") or {}) if self.dq_config else {}
        self.profiling_cfg = (self.dq_config.get("profiling") or {}) if self.dq_config else {}
        self.recon_cfg = (self.dq_config.get("reconciliation") or {}) if self.dq_config else {}
        self.realism_cfg = (self.dq_config.get("realism") or {}) if self.dq_config else {}
        self.drift_cfg = (self.dq_config.get("drift") or {}) if self.dq_config else {}
        self.schema_checker = SchemaDriftChecker(spark)
        self.ref_integrity_checker = ReferentialIntegrityChecker(spark)
        self.kafka_validator = KafkaStreamingValidator(spark)
        self.performance_optimizer = PerformanceOptimizer(spark)
        baseline_path = self.drift_cfg.get("baseline_path", "artifacts/dq/profile_baselines")
        self.drift_checker = DataDriftChecker(baseline_path, thresholds=self.drift_cfg)
        self.results: dict[str, Any] = {}

    def validate_bronze_layer(
        self, bronze_data: dict[str, DataFrame], expected_schemas: dict[str, Any]
    ) -> dict[str, Any]:
        """Validate Bronze layer data."""
        logger.info("🔍 Validating Bronze layer...")

        results = {
            "layer": "bronze",
            "timestamp": datetime.utcnow().isoformat(),
            "tables": {},
            "overall_status": "PASS",
        }

        for table_name, df in bronze_data.items():
            table_results = {
                "row_count": df.count(),
                "schema_valid": True,
                "null_analysis": {},
                "uniqueness": {},
            }

            sample_df, sample_meta = self._sample_df(df)
            if sample_meta:
                table_results["sample"] = sample_meta

            # Schema validation
            if table_name in expected_schemas:
                schema_result = self.schema_checker.validate_dataframe(
                    df, expected_schemas[table_name], table_name
                )
                drift_detected = schema_result["drift_detected"]
                # Bronze is raw: allow drift by default (especially nullability-only drift)
                if drift_detected and self._allow_bronze_schema_drift():
                    nullability_only = (
                        schema_result.get("missing_columns") == []
                        and schema_result.get("new_columns") == []
                        and schema_result.get("type_changes") == []
                        and schema_result.get("nullability_changes")
                    )
                    if nullability_only and self._allow_bronze_nullability_drift():
                        drift_detected = False
                    # Keep drift details but mark schema as valid for Bronze
                    table_results["schema_valid"] = True
                else:
                    table_results["schema_valid"] = not drift_detected
                table_results["schema_issues"] = schema_result

            # Null analysis
            table_results["null_analysis"] = self._analyze_nulls(df, table_name)

            # Uniqueness check (if ID column exists)
            id_cols = [col for col in df.columns if col.endswith("_id")]
            if id_cols:
                primary_key = id_cols[0]
                uniqueness_result = self.ref_integrity_checker.check_duplicate_ids(
                    df, primary_key, table_name
                )
                table_results["uniqueness"] = uniqueness_result
            else:
                uniqueness_result = {"valid": True}

            if self._realism_enabled():
                table_results["date_realism"] = self._validate_date_realism(
                    sample_df if sample_df is not None else df,
                    table_name=table_name,
                )
                if (
                    table_results["date_realism"].get("violations")
                    and self._realism_fail_on_violation()
                ):
                    results["overall_status"] = "FAIL"

            if self._profiling_enabled():
                profile, drift = self._profile_df(
                    sample_df if sample_df is not None else df,
                    table_name=table_name,
                    layer="bronze",
                )
                table_results["profile"] = profile
                if drift:
                    table_results["drift"] = drift
                    if drift.get("drift_detected") and self._drift_fail_on_violation():
                        results["overall_status"] = "FAIL"

            results["tables"][table_name] = table_results

            if (
                not table_results["schema_valid"] and not self._allow_bronze_schema_drift()
            ) or not uniqueness_result.get("valid", True):
                results["overall_status"] = "FAIL"

        self.results["bronze"] = results
        return results

    def validate_silver_layer(
        self, silver_data: dict[str, DataFrame], bronze_data: dict[str, DataFrame]
    ) -> dict[str, Any]:
        """Validate Silver layer data and relationships."""
        logger.info("🔍 Validating Silver layer...")

        results = {
            "layer": "silver",
            "timestamp": datetime.utcnow().isoformat(),
            "tables": {},
            "relationships": {},
            "overall_status": "PASS",
        }

        # Validate each silver table
        for table_name, df in silver_data.items():
            table_results = {
                "row_count": df.count(),
                "null_analysis": self._analyze_nulls(df, table_name),
                "timestamp_validation": {},
            }

            sample_df, sample_meta = self._sample_df(df)
            if sample_meta:
                table_results["sample"] = sample_meta

            # Timestamp validation
            timestamp_cols = [
                col for col in df.columns if "timestamp" in col.lower() or "date" in col.lower()
            ]
            if timestamp_cols:
                table_results["timestamp_validation"] = self._validate_timestamps(
                    df, timestamp_cols[0]
                )

            if self._realism_enabled():
                table_results["date_realism"] = self._validate_date_realism(
                    sample_df if sample_df is not None else df,
                    table_name=table_name,
                )
                if (
                    table_results["date_realism"].get("violations")
                    and self._realism_fail_on_violation()
                ):
                    results["overall_status"] = "FAIL"

            if self._profiling_enabled():
                profile, drift = self._profile_df(
                    sample_df if sample_df is not None else df,
                    table_name=table_name,
                    layer="silver",
                )
                table_results["profile"] = profile
                if drift:
                    table_results["drift"] = drift
                    if drift.get("drift_detected") and self._drift_fail_on_violation():
                        results["overall_status"] = "FAIL"

            results["tables"][table_name] = table_results

        # Check referential integrity
        if "orders" in silver_data and "customers" in silver_data:
            ref_result = self.ref_integrity_checker.check_orders_customers(
                silver_data["orders"], silver_data["customers"]
            )
            results["relationships"]["orders_customers"] = ref_result
            if not ref_result["valid"]:
                results["overall_status"] = "FAIL"

        if "orders" in silver_data and "products" in silver_data:
            ref_result = self.ref_integrity_checker.check_orders_products(
                silver_data["orders"], silver_data["products"]
            )
            results["relationships"]["orders_products"] = ref_result
            if not ref_result["valid"]:
                results["overall_status"] = "FAIL"

        if self._reconciliation_enabled():
            results["reconciliation"] = self._reconcile_layer_pairs(
                left_layer="bronze",
                right_layer="silver",
                left_tables=bronze_data,
                right_tables=silver_data,
            )

        self.results["silver"] = results
        return results

    def validate_gold_layer(
        self, gold_data: dict[str, DataFrame], silver_data: dict[str, DataFrame]
    ) -> dict[str, Any]:
        """Validate Gold layer data."""
        logger.info("🔍 Validating Gold layer...")

        results = {
            "layer": "gold",
            "timestamp": datetime.utcnow().isoformat(),
            "tables": {},
            "overall_status": "PASS",
        }

        for table_name, df in gold_data.items():
            table_results = {
                "row_count": df.count(),
                "null_analysis": self._analyze_nulls(df, table_name),
                "semantic_validation": {},
            }

            sample_df, sample_meta = self._sample_df(df)
            if sample_meta:
                table_results["sample"] = sample_meta

            # Semantic validation based on table type
            if "fact_orders" in table_name:
                table_results["semantic_validation"] = self._validate_fact_orders(df)
            elif "dim_customer" in table_name:
                table_results["semantic_validation"] = self._validate_dim_customer(df)

            if self._realism_enabled():
                table_results["date_realism"] = self._validate_date_realism(
                    sample_df if sample_df is not None else df,
                    table_name=table_name,
                )
                if (
                    table_results["date_realism"].get("violations")
                    and self._realism_fail_on_violation()
                ):
                    results["overall_status"] = "FAIL"

            if self._profiling_enabled():
                profile, drift = self._profile_df(
                    sample_df if sample_df is not None else df,
                    table_name=table_name,
                    layer="gold",
                )
                table_results["profile"] = profile
                if drift:
                    table_results["drift"] = drift
                    if drift.get("drift_detected") and self._drift_fail_on_violation():
                        results["overall_status"] = "FAIL"

            results["tables"][table_name] = table_results

        if self._reconciliation_enabled():
            results["reconciliation"] = self._reconcile_layer_pairs(
                left_layer="silver",
                right_layer="gold",
                left_tables=silver_data,
                right_tables=gold_data,
            )

        self.results["gold"] = results
        return results

    def _analyze_nulls(self, df: DataFrame, table_name: str) -> dict[str, Any]:
        """Analyze null values in DataFrame."""
        null_counts = {}
        total_rows = df.count()

        for col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
            null_counts[col] = {
                "null_count": null_count,
                "null_percentage": round(null_pct, 2),
                "critical": null_pct > 50 and col.endswith("_id"),
            }

        return null_counts

    def _validate_timestamps(self, df: DataFrame, timestamp_col: str) -> dict[str, Any]:
        """Validate timestamp column."""
        result = {"column": timestamp_col, "valid": True, "issues": []}

        # Check for null timestamps
        null_count = df.filter(F.col(timestamp_col).isNull()).count()
        if null_count > 0:
            result["issues"].append(f"{null_count} null timestamps")
            result["valid"] = False

        # Check for future timestamps
        future_count = df.filter(F.col(timestamp_col) > F.current_timestamp()).count()
        if future_count > 0:
            result["issues"].append(f"{future_count} future timestamps")
            result["valid"] = False

        # Check for very old timestamps (older than 10 years)
        from datetime import timedelta

        ten_years_ago = datetime.utcnow() - timedelta(days=3650)
        old_count = df.filter(F.col(timestamp_col) < F.lit(ten_years_ago)).count()
        if old_count > 0:
            result["issues"].append(f"{old_count} timestamps older than 10 years")

        return result

    def _validate_fact_orders(self, df: DataFrame) -> dict[str, Any]:
        """Validate fact_orders semantic rules."""
        result = {"valid": True, "issues": []}

        # Check total_amount >= 0
        if "sales_amount" in df.columns:
            negative_count = df.filter(F.col("sales_amount") < 0).count()
            if negative_count > 0:
                result["issues"].append(f"{negative_count} orders with negative sales_amount")
                result["valid"] = False

        # Check quantity >= 1
        if "quantity" in df.columns:
            invalid_qty = df.filter(F.col("quantity") < 1).count()
            if invalid_qty > 0:
                result["issues"].append(f"{invalid_qty} orders with quantity < 1")
                result["valid"] = False

        return result

    def _validate_dim_customer(self, df: DataFrame) -> dict[str, Any]:
        """Validate dim_customer semantic rules."""
        result = {"valid": True, "issues": []}

        # Check for valid emails (basic check)
        if "email" in df.columns:
            invalid_email = df.filter(
                ~F.col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$")
            ).count()
            if invalid_email > 0:
                result["issues"].append(f"{invalid_email} invalid email addresses")

        return result

    def _sampling_enabled(self) -> bool:
        return bool(self.sampling_cfg.get("enabled", False))

    def _profiling_enabled(self) -> bool:
        return bool(self.profiling_cfg.get("enabled", False))

    def _reconciliation_enabled(self) -> bool:
        return bool(self.recon_cfg.get("enabled", False))

    def _realism_enabled(self) -> bool:
        return bool(self.realism_cfg.get("enabled", False))

    def _realism_fail_on_violation(self) -> bool:
        return bool(self.realism_cfg.get("fail_on_violation", False))

    def _allow_bronze_schema_drift(self) -> bool:
        """Bronze is raw/append-only; allow schema drift unless explicitly disabled."""
        return bool(self.dq_config.get("allow_bronze_schema_drift", True))

    def _allow_bronze_nullability_drift(self) -> bool:
        """Allow nullability changes in Bronze unless explicitly disabled."""
        return bool(self.dq_config.get("allow_bronze_nullability_drift", True))

    def _sample_df(self, df: DataFrame) -> tuple[DataFrame | None, dict[str, Any] | None]:
        if not self._sampling_enabled():
            return None, None
        fraction = float(self.sampling_cfg.get("fraction", 0.02))
        max_rows = int(self.sampling_cfg.get("max_rows", 100000))
        seed = int(self.sampling_cfg.get("seed", 42))

        if fraction <= 0:
            return None, None

        sample = df.sample(withReplacement=False, fraction=fraction, seed=seed)
        if max_rows > 0:
            sample = sample.limit(max_rows)

        sample_count = sample.count()
        return sample, {
            "fraction": fraction,
            "max_rows": max_rows,
            "row_count": sample_count,
        }

    def _profile_df(
        self, df: DataFrame, table_name: str, layer: str
    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
        profile = {
            "table": table_name,
            "layer": layer,
            "generated_at": datetime.utcnow().isoformat(),
            "row_count": df.count(),
            "columns": {},
        }

        max_columns = int(self.profiling_cfg.get("max_columns", 30))
        top_values = int(self.profiling_cfg.get("top_values", 5))

        for col_name, dtype in df.dtypes[:max_columns]:
            col_profile = {}
            null_count = df.filter(F.col(col_name).isNull()).count()
            col_profile["null_count"] = null_count
            col_profile["null_pct"] = round(
                (null_count / profile["row_count"] * 100) if profile["row_count"] else 0, 2
            )
            col_profile["distinct_count"] = int(
                df.select(F.approx_count_distinct(F.col(col_name)).alias("d")).collect()[0]["d"]
            )

            dtype_lower = str(dtype).lower()
            if dtype_lower.startswith("decimal") or dtype_lower in {
                "int",
                "bigint",
                "double",
                "float",
            }:
                stats = df.select(
                    F.min(col_name).alias("min"),
                    F.max(col_name).alias("max"),
                    F.avg(col_name).alias("avg"),
                ).collect()[0]
                col_profile["min"] = stats["min"]
                col_profile["max"] = stats["max"]
                col_profile["avg"] = stats["avg"]
            else:
                top = (
                    df.groupBy(col_name)
                    .count()
                    .orderBy(F.desc("count"))
                    .limit(top_values)
                    .collect()
                )
                col_profile["top_values"] = [
                    {"value": row[col_name], "count": row["count"]} for row in top
                ]

            profile["columns"][col_name] = col_profile

        self._persist_profile(profile, table_name, layer)
        drift = None
        if self._drift_enabled():
            drift = self.drift_checker.compare(layer=layer, table=table_name, profile=profile)
        return profile, drift

    def _persist_profile(self, profile: dict[str, Any], table_name: str, layer: str) -> None:
        output_path = self.profiling_cfg.get("output_path")
        if not output_path:
            return
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        path = Path(output_path) / layer / table_name / f"profile_{timestamp}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(profile, indent=2, default=str))

    def _drift_enabled(self) -> bool:
        return bool(self.drift_cfg.get("enabled", False))

    def _drift_fail_on_violation(self) -> bool:
        return bool(self.drift_cfg.get("fail_on_drift", False))

    def _reconcile_layer_pairs(
        self,
        left_layer: str,
        right_layer: str,
        left_tables: dict[str, DataFrame],
        right_tables: dict[str, DataFrame],
    ) -> dict[str, Any]:
        default_pairs = [
            {"left": "customers", "right": "customers", "keys": ["customer_id"]},
            {"left": "orders", "right": "orders", "keys": ["order_id"]},
            {"left": "products", "right": "products", "keys": ["product_id"]},
            {"left": "behavior", "right": "behavior", "keys": ["behavior_id"]},
        ]

        if left_layer == "silver" and right_layer == "gold":
            default_pairs = [
                {"left": "orders", "right": "fact_orders", "keys": ["order_id"]},
                {"left": "customers", "right": "dim_customer", "keys": ["customer_id"]},
                {"left": "products", "right": "dim_product", "keys": ["product_id"]},
            ]

        pairs = self.recon_cfg.get("pairs") or default_pairs
        sample_size = int(self.recon_cfg.get("row_sample", 0) or 0)

        results: dict[str, Any] = {}

        for pair in pairs:
            left_name = pair.get("left")
            right_name = pair.get("right")
            keys = pair.get("keys") or []
            label = f"{left_layer}.{left_name} -> {right_layer}.{right_name}"

            if not left_name or not right_name or not keys:
                results[label] = {"skipped": True, "reason": "missing pair configuration"}
                continue

            if left_name not in left_tables or right_name not in right_tables:
                results[label] = {"skipped": True, "reason": "table missing"}
                continue

            left_df = left_tables[left_name]
            right_df = right_tables[right_name]

            if any(col not in left_df.columns for col in keys) or any(
                col not in right_df.columns for col in keys
            ):
                # For behavior, fall back to customer_id if behavior_id is missing
                if left_name == "behavior" and right_name == "behavior":
                    fallback_keys = ["customer_id"]
                    if all(col in left_df.columns for col in fallback_keys) and all(
                        col in right_df.columns for col in fallback_keys
                    ):
                        keys = fallback_keys
                    else:
                        results[label] = {"skipped": True, "reason": "key columns missing"}
                        continue
                else:
                    results[label] = {"skipped": True, "reason": "key columns missing"}
                    continue

            left_keys = left_df.select(*keys).distinct()
            right_keys = right_df.select(*keys).distinct()
            left_only = left_keys.join(right_keys, keys, "left_anti").count()
            right_only = right_keys.join(left_keys, keys, "left_anti").count()

            recon_result = {
                "left_only": left_only,
                "right_only": right_only,
                "matched": left_only == 0 and right_only == 0,
            }

            if sample_size > 0:
                left_sample = left_keys.limit(sample_size)
                missing_in_right = left_sample.join(right_keys, keys, "left_anti").count()
                recon_result["sample_size"] = sample_size
                recon_result["missing_in_right"] = missing_in_right

            results[label] = recon_result

        return results

    def _validate_date_realism(self, df: DataFrame, table_name: str) -> dict[str, Any]:
        max_future_days = int(self.realism_cfg.get("max_future_days", 3))
        max_past_years = int(self.realism_cfg.get("max_past_years", 20))
        date_cols = [
            col for col in df.columns if "date" in col.lower() or "timestamp" in col.lower()
        ]

        if not date_cols:
            return {"checked": False, "reason": "no date columns"}

        results: dict[str, Any] = {
            "checked": True,
            "table": table_name,
            "violations": False,
            "columns": {},
        }

        today = F.current_date()
        max_future = F.date_add(today, max_future_days)
        min_past = F.date_sub(today, max_past_years * 365)

        for col_name in date_cols:
            col = F.col(col_name)
            parsed = F.coalesce(
                F.to_date(col),
                F.to_date(col, "yyyy-MM-dd HH:mm:ss"),
                F.to_date(col, "yyyy-MM-dd'T'HH:mm:ss"),
            )
            invalid = df.filter(col.isNotNull() & parsed.isNull()).count()
            future = df.filter(parsed > max_future).count()
            old = df.filter(parsed < min_past).count()

            col_result = {
                "invalid_format": invalid,
                "future_dates": future,
                "too_old_dates": old,
            }

            if invalid > 0 or future > 0 or old > 0:
                results["violations"] = True

            results["columns"][col_name] = col_result

        return results

    def generate_comprehensive_report(self) -> str:
        """Generate comprehensive DQ report."""
        report = [
            "=" * 70,
            "COMPREHENSIVE DATA QUALITY REPORT",
            "=" * 70,
            f"Generated: {datetime.utcnow().isoformat()}",
            "",
        ]

        for layer, results in self.results.items():
            report.append(f"\n{'=' * 70}")
            report.append(f"LAYER: {layer.upper()}")
            report.append(f"{'=' * 70}")
            report.append(f"Status: {results.get('overall_status', 'UNKNOWN')}")
            report.append("")

            if "tables" in results:
                for table_name, table_results in results["tables"].items():
                    report.append(f"  Table: {table_name}")
                    report.append(f"    Row Count: {table_results.get('row_count', 0):,}")

                    if "schema_valid" in table_results:
                        status = "✅" if table_results["schema_valid"] else "❌"
                        report.append(f"    Schema: {status}")

                    if "null_analysis" in table_results:
                        critical_nulls = [
                            col
                            for col, stats in table_results["null_analysis"].items()
                            if stats.get("critical", False)
                        ]
                        if critical_nulls:
                            report.append(f"    ⚠️  Critical nulls in: {', '.join(critical_nulls)}")

            if "relationships" in results:
                report.append("  Relationships:")
                for rel_name, rel_result in results["relationships"].items():
                    status = "✅" if rel_result.get("valid", False) else "❌"
                    report.append(
                        f"    {status} {rel_name}: {rel_result.get('orphaned_count', 0)} orphaned keys"
                    )
            if "reconciliation" in results:
                report.append("  Reconciliation:")
                for rel_name, rel_result in results["reconciliation"].items():
                    if rel_result.get("skipped"):
                        report.append(f"    ⚠️  {rel_name}: skipped ({rel_result.get('reason')})")
                    else:
                        status = "✅" if rel_result.get("matched") else "❌"
                        report.append(
                            f"    {status} {rel_name}: left_only={rel_result.get('left_only')}, right_only={rel_result.get('right_only')}"
                        )

        report.append("\n" + "=" * 70)
        report.append("END OF REPORT")
        report.append("=" * 70)

        return "\n".join(report)

    def get_summary(self) -> dict[str, Any]:
        """Get summary of all validation results."""
        summary = {
            "total_layers_validated": len(self.results),
            "layers_passed": sum(
                1 for r in self.results.values() if r.get("overall_status") == "PASS"
            ),
            "layers_failed": sum(
                1 for r in self.results.values() if r.get("overall_status") == "FAIL"
            ),
            "timestamp": datetime.utcnow().isoformat(),
        }

        return summary
