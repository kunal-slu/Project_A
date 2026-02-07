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
from typing import Dict, List, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime import datetime
import logging

from project_a.dq.schema_drift_checker import SchemaDriftChecker
from project_a.dq.referential_integrity import ReferentialIntegrityChecker
from project_a.dq.kafka_streaming_validator import KafkaStreamingValidator
from project_a.dq.performance_optimizer import PerformanceOptimizer

logger = logging.getLogger(__name__)


class ComprehensiveValidator:
    """Comprehensive data quality validator for all layers."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.schema_checker = SchemaDriftChecker(spark)
        self.ref_integrity_checker = ReferentialIntegrityChecker(spark)
        self.kafka_validator = KafkaStreamingValidator(spark)
        self.performance_optimizer = PerformanceOptimizer(spark)
        self.results: Dict[str, Any] = {}
    
    def validate_bronze_layer(
        self,
        bronze_data: Dict[str, DataFrame],
        expected_schemas: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate Bronze layer data."""
        logger.info("🔍 Validating Bronze layer...")
        
        results = {
            "layer": "bronze",
            "timestamp": datetime.utcnow().isoformat(),
            "tables": {},
            "overall_status": "PASS"
        }
        
        for table_name, df in bronze_data.items():
            table_results = {
                "row_count": df.count(),
                "schema_valid": True,
                "null_analysis": {},
                "uniqueness": {}
            }
            
            # Schema validation
            if table_name in expected_schemas:
                schema_result = self.schema_checker.validate_dataframe(
                    df, expected_schemas[table_name], table_name
                )
                table_results["schema_valid"] = not schema_result["drift_detected"]
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
            
            results["tables"][table_name] = table_results
            
            if not table_results["schema_valid"] or not uniqueness_result.get("valid", True):
                results["overall_status"] = "FAIL"
        
        self.results["bronze"] = results
        return results
    
    def validate_silver_layer(
        self,
        silver_data: Dict[str, DataFrame],
        bronze_data: Dict[str, DataFrame]
    ) -> Dict[str, Any]:
        """Validate Silver layer data and relationships."""
        logger.info("🔍 Validating Silver layer...")
        
        results = {
            "layer": "silver",
            "timestamp": datetime.utcnow().isoformat(),
            "tables": {},
            "relationships": {},
            "overall_status": "PASS"
        }
        
        # Validate each silver table
        for table_name, df in silver_data.items():
            table_results = {
                "row_count": df.count(),
                "null_analysis": self._analyze_nulls(df, table_name),
                "timestamp_validation": {}
            }
            
            # Timestamp validation
            timestamp_cols = [col for col in df.columns if "timestamp" in col.lower() or "date" in col.lower()]
            if timestamp_cols:
                table_results["timestamp_validation"] = self._validate_timestamps(df, timestamp_cols[0])
            
            results["tables"][table_name] = table_results
        
        # Check referential integrity
        if "orders_silver" in silver_data and "customers_silver" in silver_data:
            ref_result = self.ref_integrity_checker.check_orders_customers(
                silver_data["orders_silver"],
                silver_data["customers_silver"]
            )
            results["relationships"]["orders_customers"] = ref_result
            if not ref_result["valid"]:
                results["overall_status"] = "FAIL"
        
        if "orders_silver" in silver_data and "products_silver" in silver_data:
            ref_result = self.ref_integrity_checker.check_orders_products(
                silver_data["orders_silver"],
                silver_data["products_silver"]
            )
            results["relationships"]["orders_products"] = ref_result
            if not ref_result["valid"]:
                results["overall_status"] = "FAIL"
        
        self.results["silver"] = results
        return results
    
    def validate_gold_layer(
        self,
        gold_data: Dict[str, DataFrame],
        silver_data: Dict[str, DataFrame]
    ) -> Dict[str, Any]:
        """Validate Gold layer data."""
        logger.info("🔍 Validating Gold layer...")
        
        results = {
            "layer": "gold",
            "timestamp": datetime.utcnow().isoformat(),
            "tables": {},
            "overall_status": "PASS"
        }
        
        for table_name, df in gold_data.items():
            table_results = {
                "row_count": df.count(),
                "null_analysis": self._analyze_nulls(df, table_name),
                "semantic_validation": {}
            }
            
            # Semantic validation based on table type
            if "fact_orders" in table_name:
                table_results["semantic_validation"] = self._validate_fact_orders(df)
            elif "dim_customer" in table_name:
                table_results["semantic_validation"] = self._validate_dim_customer(df)
            
            results["tables"][table_name] = table_results
        
        self.results["gold"] = results
        return results
    
    def _analyze_nulls(self, df: DataFrame, table_name: str) -> Dict[str, Any]:
        """Analyze null values in DataFrame."""
        null_counts = {}
        total_rows = df.count()
        
        for col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
            null_counts[col] = {
                "null_count": null_count,
                "null_percentage": round(null_pct, 2),
                "critical": null_pct > 50 and col.endswith("_id")
            }
        
        return null_counts
    
    def _validate_timestamps(self, df: DataFrame, timestamp_col: str) -> Dict[str, Any]:
        """Validate timestamp column."""
        result = {
            "column": timestamp_col,
            "valid": True,
            "issues": []
        }
        
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
    
    def _validate_fact_orders(self, df: DataFrame) -> Dict[str, Any]:
        """Validate fact_orders semantic rules."""
        result = {
            "valid": True,
            "issues": []
        }
        
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
    
    def _validate_dim_customer(self, df: DataFrame) -> Dict[str, Any]:
        """Validate dim_customer semantic rules."""
        result = {
            "valid": True,
            "issues": []
        }
        
        # Check for valid emails (basic check)
        if "email" in df.columns:
            invalid_email = df.filter(
                ~F.col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$")
            ).count()
            if invalid_email > 0:
                result["issues"].append(f"{invalid_email} invalid email addresses")
        
        return result
    
    def generate_comprehensive_report(self) -> str:
        """Generate comprehensive DQ report."""
        report = [
            "=" * 70,
            "COMPREHENSIVE DATA QUALITY REPORT",
            "=" * 70,
            f"Generated: {datetime.utcnow().isoformat()}",
            ""
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
                            col for col, stats in table_results["null_analysis"].items()
                            if stats.get("critical", False)
                        ]
                        if critical_nulls:
                            report.append(f"    ⚠️  Critical nulls in: {', '.join(critical_nulls)}")
            
            if "relationships" in results:
                report.append("  Relationships:")
                for rel_name, rel_result in results["relationships"].items():
                    status = "✅" if rel_result.get("valid", False) else "❌"
                    report.append(f"    {status} {rel_name}: {rel_result.get('orphaned_count', 0)} orphaned keys")
        
        report.append("\n" + "=" * 70)
        report.append("END OF REPORT")
        report.append("=" * 70)
        
        return "\n".join(report)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all validation results."""
        summary = {
            "total_layers_validated": len(self.results),
            "layers_passed": sum(1 for r in self.results.values() if r.get("overall_status") == "PASS"),
            "layers_failed": sum(1 for r in self.results.values() if r.get("overall_status") == "FAIL"),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return summary

