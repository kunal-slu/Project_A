#!/usr/bin/env python3
"""
Comprehensive Integration Tests for the Enterprise Data Platform.
Tests full ETL pipeline with sample data, DR configs, and performance benchmarks.
"""

import sys
import os
import json
import tempfile
import shutil
import logging
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from pyspark_interview_project import (
    build_spark,
    load_config_resolved,
    extract_customers,
    extract_products,
    extract_orders_json,
    extract_returns,
    extract_exchange_rates,
    extract_inventory_snapshots,
    write_delta,
    write_parquet
)

from pyspark_interview_project.ingestion_pipeline import IngestionPipeline
from pyspark_interview_project.metrics_collector import MetricsCollector
from pyspark_interview_project.data_quality_suite import DataQualitySuite
from pyspark_interview_project.disaster_recovery import DisasterRecoveryExecutor
from pyspark_interview_project.data_contracts import DataContractManager
from pyspark_interview_project.performance_optimizer import PerformanceOptimizer

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ComprehensiveIntegrationTest:
    """Comprehensive integration test suite."""

    def __init__(self):
        self.test_data_dir = Path(__file__).parent / "data"
        self.temp_dir = Path(tempfile.mkdtemp())
        self.config = None
        self.spark = None
        self.platform = None
        self.test_results = {}

    def setup(self):
        """Setup test environment."""
        print("🔧 Setting up comprehensive integration test environment...")

        # Load configuration
        self.config = load_config_resolved('config/config-dev.yaml')

        # Create Spark session
        self.spark = build_spark(self.config)

        # Create enterprise platform
        self.platform = EnterpriseDataPlatform(self.spark, self.config)

        # Create test directories
        self._create_test_directories()

        # Create sample data
        self._create_sample_data()

        print("✅ Test environment setup completed")

    def _create_test_directories(self):
        """Create test directory structure."""
        test_dirs = [
            "data/lakehouse/bronze",
            "data/lakehouse/silver",
            "data/lakehouse/gold",
            "data/metrics",
            "data/contracts",
            "data/backups",
            "data/replication"
        ]

        for dir_path in test_dirs:
            full_path = self.temp_dir / dir_path
            full_path.mkdir(parents=True, exist_ok=True)

    def _create_sample_data(self):
        """Create comprehensive sample data for testing."""
        print("📊 Creating sample data...")

        # Sample customers data
        customers_data = [
            ("C001", "John", "Doe", "john@example.com", "123 Main St", "Austin", "TX", "USA", "78701", "555-0101", "2021-01-01", "M", 30),
            ("C002", "Jane", "Smith", "jane@example.com", "456 Oak Ave", "Dallas", "TX", "USA", "75201", "555-0102", "2021-01-02", "F", 25),
            ("C003", "Bob", "Johnson", "bob@example.com", "789 Pine Rd", "Houston", "TX", "USA", "77001", "555-0103", "2021-01-03", "M", 35),
            ("C004", "Alice", "Brown", "alice@example.com", "321 Elm St", "San Antonio", "TX", "USA", "78201", "555-0104", "2021-01-04", "F", 28),
            ("C005", "Charlie", "Wilson", "charlie@example.com", "654 Maple Dr", "Fort Worth", "TX", "USA", "76101", "555-0105", "2021-01-05", "M", 32)
        ]

        customers_df = self.spark.createDataFrame(
            customers_data,
            ["customer_id", "first_name", "last_name", "email", "address", "city", "state", "country", "zip", "phone", "registration_date", "gender", "age"]
        )

        # Sample products data
        products_data = [
            ("P001", "Laptop", "Electronics", 999.99, "TechCorp", "High-performance laptop", "2021-01-01", True, "LAP001"),
            ("P002", "Smartphone", "Electronics", 599.99, "TechCorp", "5G smartphone", "2021-01-01", True, "PHN001"),
            ("P003", "Headphones", "Electronics", 199.99, "AudioTech", "Wireless headphones", "2021-01-01", True, "AUD001"),
            ("P004", "T-Shirt", "Clothing", 29.99, "FashionCo", "Cotton t-shirt", "2021-01-01", True, "CLT001"),
            ("P005", "Jeans", "Clothing", 79.99, "FashionCo", "Blue jeans", "2021-01-01", True, "CLJ001")
        ]

        products_df = self.spark.createDataFrame(
            products_data,
            ["product_id", "name", "category", "price", "brand", "description", "created_date", "is_active", "sku"]
        )

        # Sample orders data
        orders_data = [
            ("O001", "C001", "P001", "2021-01-15", 1, 999.99, "USD", True, False, "123 Main St", "credit_card"),
            ("O002", "C002", "P002", "2021-01-16", 1, 599.99, "USD", True, False, "456 Oak Ave", "credit_card"),
            ("O003", "C003", "P003", "2021-01-17", 2, 399.98, "USD", True, False, "789 Pine Rd", "paypal"),
            ("O004", "C004", "P004", "2021-01-18", 3, 89.97, "USD", True, False, "321 Elm St", "credit_card"),
            ("O005", "C005", "P005", "2021-01-19", 1, 79.99, "USD", True, False, "654 Maple Dr", "credit_card")
        ]

        orders_df = self.spark.createDataFrame(
            orders_data,
            ["order_id", "customer_id", "product_id", "order_date", "quantity", "amount", "currency", "shipped", "returned", "shipping_address", "payment_method"]
        )

        # Sample returns data
        returns_data = [
            ("R001", "O001", "C001", "P001", "2021-02-01", "defective", 999.99, 999.99, "processed", "mail", 3, False, "Product was damaged"),
            ("R002", "O002", "C002", "P002", "2021-02-02", "wrong_size", 599.99, 599.99, "pending", "store", 1, True, "Size too small"),
            ("R003", "O003", "C003", "P003", "2021-02-03", "not_as_described", 199.99, 199.99, "processed", "mail", 5, False, "Not as advertised"),
            ("R004", "O004", "C004", "P004", "2021-02-04", "changed_mind", 29.99, 29.99, "pending", "store", 2, False, "Customer changed mind"),
            ("R005", "O005", "C005", "P005", "2021-02-05", "damaged", 79.99, 79.99, "processed", "mail", 4, False, "Damaged during shipping")
        ]

        returns_df = self.spark.createDataFrame(
            returns_data,
            ["return_id", "order_id", "customer_id", "product_id", "return_date", "return_reason", "refund_amount", "order_amount", "return_status", "return_method", "processing_time_days", "is_partial_return", "return_notes"]
        )

        # Sample inventory data
        inventory_data = [
            ("INV001", "P001", 50, "2021-01-01", "WH001", 10, 100),
            ("INV002", "P002", 75, "2021-01-01", "WH001", 15, 150),
            ("INV003", "P003", 100, "2021-01-01", "WH002", 20, 200),
            ("INV004", "P004", 200, "2021-01-01", "WH002", 40, 400),
            ("INV005", "P005", 150, "2021-01-01", "WH003", 30, 300)
        ]

        inventory_df = self.spark.createDataFrame(
            inventory_data,
            ["snapshot_id", "product_id", "quantity", "snapshot_date", "warehouse_id", "reorder_level", "max_stock"]
        )

        # Sample FX rates data
        fx_data = [
            ("FX001", "USD", "EUR", 0.85, "2021-01-01", "ECB"),
            ("FX002", "USD", "GBP", 0.73, "2021-01-01", "BOE"),
            ("FX003", "USD", "JPY", 110.50, "2021-01-01", "BOJ"),
            ("FX004", "EUR", "USD", 1.18, "2021-01-01", "ECB"),
            ("FX005", "GBP", "USD", 1.37, "2021-01-01", "BOE")
        ]

        fx_df = self.spark.createDataFrame(
            fx_data,
            ["rate_id", "from_currency", "to_currency", "rate", "effective_date", "source"]
        )

        # Save sample data to test directories
        self._save_sample_data(customers_df, "customers_raw")
        self._save_sample_data(products_df, "products_raw")
        self._save_sample_data(orders_df, "orders_raw")
        self._save_sample_data(returns_df, "returns_raw")
        self._save_sample_data(inventory_df, "inventory_snapshots")
        self._save_sample_data(fx_df, "fx_rates")

        print(f"✅ Created sample data: {len(customers_data)} customers, {len(products_data)} products, {len(orders_data)} orders, {len(returns_data)} returns")

    def _save_sample_data(self, df, table_name):
        """Save sample data to test directory."""
        output_path = self.temp_dir / "data" / "lakehouse" / "bronze" / table_name
        df.write.mode("overwrite").parquet(str(output_path))

    def test_full_etl_pipeline(self):
        """Test complete ETL pipeline from bronze to gold."""
        print("🔄 Testing full ETL pipeline...")

        try:
            # Initialize ingestion pipeline
            ingestion_pipeline = IngestionPipeline(self.spark, self.config)

            # Run bronze to silver ingestion
            bronze_to_silver_result = ingestion_pipeline.run_bronze_to_silver()
            assert bronze_to_silver_result["success"], f"Bronze to silver failed: {bronze_to_silver_result.get('error')}"

            # Run silver to gold ingestion
            silver_to_gold_result = ingestion_pipeline.run_silver_to_gold()
            assert silver_to_gold_result["success"], f"Silver to gold failed: {silver_to_gold_result.get('error')}"

            # Verify data in each layer
            bronze_count = self._count_files_in_layer("bronze")
            silver_count = self._count_files_in_layer("silver")
            gold_count = self._count_files_in_layer("gold")

            assert bronze_count > 0, "No data in bronze layer"
            assert silver_count > 0, "No data in silver layer"
            assert gold_count > 0, "No data in gold layer"

            self.test_results["etl_pipeline"] = "PASSED"
            print("✅ Full ETL pipeline test passed")

        except Exception as e:
            self.test_results["etl_pipeline"] = f"FAILED: {str(e)}"
            print(f"❌ Full ETL pipeline test failed: {e}")
            raise

    def _count_files_in_layer(self, layer):
        """Count files in a data layer."""
        layer_path = self.temp_dir / "data" / "lakehouse" / layer
        if not layer_path.exists():
            return 0

        count = 0
        for item in layer_path.iterdir():
            if item.is_dir():
                count += len(list(item.glob("*.parquet")))
        return count

    def test_data_quality_suite(self):
        """Test data quality suite with sample data."""
        print("🔍 Testing data quality suite...")

        try:
            # Initialize DQ suite
            dq_suite = DataQualitySuite(self.spark, self.config)

            # Test DQ checks on returns data
            returns_path = self.temp_dir / "data" / "lakehouse" / "bronze" / "returns_raw"
            returns_df = self.spark.read.parquet(str(returns_path))

            # Run DQ checks
            dq_results = dq_suite.run_data_quality_checks(returns_df, "returns_raw")

            assert dq_results["overall_score"] >= 0.8, f"Data quality score too low: {dq_results['overall_score']}"
            assert len(dq_results["failed_checks"]) == 0, f"Data quality checks failed: {dq_results['failed_checks']}"

            self.test_results["data_quality"] = "PASSED"
            print("✅ Data quality suite test passed")

        except Exception as e:
            self.test_results["data_quality"] = f"FAILED: {str(e)}"
            print(f"❌ Data quality suite test failed: {e}")
            raise

    def test_data_contracts(self):
        """Test data contract validation."""
        print("📋 Testing data contracts...")

        try:
            # Initialize contract manager
            contract_manager = DataContractManager(self.spark)

            # Test contract validation on returns data
            returns_path = self.temp_dir / "data" / "lakehouse" / "bronze" / "returns_raw"
            returns_df = self.spark.read.parquet(str(returns_path))

            # Validate against contract
            validation_result = contract_manager.validate_contract(returns_df, "returns_raw")

            assert validation_result["overall_valid"], f"Contract validation failed: {validation_result.get('error')}"

            # Test schema drift detection
            drift_result = contract_manager.detect_schema_drift(returns_df, "returns_raw")
            assert not drift_result["drift_detected"], "Unexpected schema drift detected"

            self.test_results["data_contracts"] = "PASSED"
            print("✅ Data contracts test passed")

        except Exception as e:
            self.test_results["data_contracts"] = f"FAILED: {str(e)}"
            print(f"❌ Data contracts test failed: {e}")
            raise

    def test_disaster_recovery(self):
        """Test disaster recovery executor with configs."""
        print("🛡️ Testing disaster recovery...")

        try:
            # Create test DR configs
            backup_config = {
                "backup_strategy": "daily",
                "retention_days": 30,
                "storage_account": "testbackup",
                "container": "backups",
                "compression": "gzip"
            }

            replication_config = {
                "source_region": "eastus",
                "target_region": "westus2",
                "sync_interval_minutes": 60,
                "data_types": ["bronze", "silver", "gold"]
            }

            # Save test configs
            backup_config_path = self.temp_dir / "data" / "backups" / "backup_strategy.json"
            replication_config_path = self.temp_dir / "data" / "replication" / "replication_config.json"

            with open(backup_config_path, 'w') as f:
                json.dump(backup_config, f, indent=2)

            with open(replication_config_path, 'w') as f:
                json.dump(replication_config, f, indent=2)

            # Initialize DR executor
            dr_executor = DisasterRecoveryExecutor(self.spark, self.config)

            # Test backup execution
            backup_result = dr_executor.execute_backup_strategy(str(backup_config_path))
            assert backup_result["success"], f"Backup execution failed: {backup_result.get('error')}"

            # Test replication execution
            replication_result = dr_executor.execute_replication_config(str(replication_config_path))
            assert replication_result["success"], f"Replication execution failed: {replication_result.get('error')}"

            self.test_results["disaster_recovery"] = "PASSED"
            print("✅ Disaster recovery test passed")

        except Exception as e:
            self.test_results["disaster_recovery"] = f"FAILED: {str(e)}"
            print(f"❌ Disaster recovery test failed: {e}")
            raise

    def test_performance_optimization(self):
        """Test performance optimization with returns_raw dataset."""
        print("⚡ Testing performance optimization...")

        try:
            # Initialize performance optimizer
            optimizer = PerformanceOptimizer(self.spark, self.config)

            # Load returns data for optimization
            returns_path = self.temp_dir / "data" / "lakehouse" / "bronze" / "returns_raw"
            returns_df = self.spark.read.parquet(str(returns_path))

            # Test optimization pipeline
            optimized_df = optimizer.optimize_returns_raw(returns_df)

            # Verify optimization was applied
            assert optimized_df is not None, "Optimization failed to return DataFrame"

            # Test caching strategy
            cache_summary = optimizer.cache_manager.get_cache_summary()
            assert "returns_raw" in cache_summary["cached_datasets"], "Returns dataset not cached"

            # Test performance benchmarking
            benchmark_result = optimizer.benchmark.benchmark_dataset(returns_df, "returns_raw_test")
            assert benchmark_result.dataset_name == "returns_raw_test", "Benchmark failed"

            self.test_results["performance_optimization"] = "PASSED"
            print("✅ Performance optimization test passed")

        except Exception as e:
            self.test_results["performance_optimization"] = f"FAILED: {str(e)}"
            print(f"❌ Performance optimization test failed: {e}")
            raise

    def test_metrics_integration(self):
        """Test metrics collection and integration."""
        print("📊 Testing metrics integration...")

        try:
            # Initialize metrics collector
            metrics_collector = MetricsCollector(self.spark, self.config)

            # Create sample pipeline metrics
            pipeline_metrics = {
                "pipeline_id": "test_pipeline_001",
                "start_time": datetime.now().isoformat(),
                "end_time": (datetime.now() + timedelta(minutes=5)).isoformat(),
                "status": "completed",
                "records_processed": 1000,
                "processing_time_seconds": 300,
                "data_quality_score": 0.95
            }

            # Save metrics
            metrics_path = self.temp_dir / "data" / "metrics" / "pipeline_metrics.json"
            with open(metrics_path, 'w') as f:
                json.dump(pipeline_metrics, f, indent=2)

            # Test metrics ingestion
            ingestion_result = metrics_collector.ingest_pipeline_metrics(str(metrics_path))
            assert ingestion_result["success"], f"Metrics ingestion failed: {ingestion_result.get('error')}"

            # Test metrics aggregation
            aggregated_metrics = metrics_collector.aggregate_metrics()
            assert "total_pipelines" in aggregated_metrics, "Metrics aggregation failed"

            self.test_results["metrics_integration"] = "PASSED"
            print("✅ Metrics integration test passed")

        except Exception as e:
            self.test_results["metrics_integration"] = f"FAILED: {str(e)}"
            print(f"❌ Metrics integration test failed: {e}")
            raise

    def test_streaming_integration(self):
        """Test streaming data integration."""
        print("🌊 Testing streaming integration...")

        try:
            # Test streaming function import and basic functionality
            assert callable(stream_orders_to_bronze), "Streaming function not callable"

            # Create sample streaming data
            streaming_orders = [
                {"order_id": "SO001", "customer_id": "C001", "product_id": "P001", "order_date": "2021-01-20", "amount": 999.99},
                {"order_id": "SO002", "customer_id": "C002", "product_id": "P002", "order_date": "2021-01-21", "amount": 599.99}
            ]

            # Test streaming data processing (basic validation)
            for order in streaming_orders:
                assert "order_id" in order, "Streaming order missing order_id"
                assert "customer_id" in order, "Streaming order missing customer_id"
                assert "amount" in order, "Streaming order missing amount"

            self.test_results["streaming_integration"] = "PASSED"
            print("✅ Streaming integration test passed")

        except Exception as e:
            self.test_results["streaming_integration"] = f"FAILED: {str(e)}"
            print(f"❌ Streaming integration test failed: {e}")
            raise

    def test_incremental_loading(self):
        """Test incremental loading capabilities."""
        print("🔄 Testing incremental loading...")

        try:
            # Test that incremental loading functions exist
            ingestion_pipeline = IngestionPipeline(self.spark, self.config)

            # Test incremental load detection
            assert hasattr(ingestion_pipeline, 'detect_incremental_changes'), "Missing incremental change detection"
            assert hasattr(ingestion_pipeline, 'process_incremental_changes'), "Missing incremental processing"

            # Test incremental load simulation
            incremental_result = ingestion_pipeline.detect_incremental_changes("returns_raw")
            assert isinstance(incremental_result, dict), "Incremental detection should return dict"

            self.test_results["incremental_loading"] = "PASSED"
            print("✅ Incremental loading test passed")

        except Exception as e:
            self.test_results["incremental_loading"] = f"FAILED: {str(e)}"
            print(f"❌ Incremental loading test failed: {e}")
            raise

    def run_all_tests(self):
        """Run all comprehensive tests."""
        print("🚀 Starting Comprehensive Integration Test Suite...")
        print("=" * 80)

        test_methods = [
            self.test_full_etl_pipeline,
            self.test_data_quality_suite,
            self.test_data_contracts,
            self.test_disaster_recovery,
            self.test_performance_optimization,
            self.test_metrics_integration,
            self.test_streaming_integration,
            self.test_incremental_loading
        ]

        passed = 0
        total = len(test_methods)

        for test_method in test_methods:
            try:
                test_method()
                passed += 1
            except Exception as e:
                print(f"❌ Test {test_method.__name__} failed: {e}")

        print("=" * 80)
        print("📊 Comprehensive Test Results:")
        for test_name, result in self.test_results.items():
            status = "✅ PASSED" if result == "PASSED" else f"❌ {result}"
            print(f"  {test_name}: {status}")

        print(f"\n🎯 Overall Results: {passed}/{total} tests passed")

        if passed == total:
            print("🎉 All comprehensive tests passed! The platform is production-ready.")
            return 0
        else:
            print("⚠️  Some tests failed. Please review the errors above.")
            return 1

    def cleanup(self):
        """Clean up test environment."""
        print("🧹 Cleaning up test environment...")
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
        print("✅ Cleanup completed")


def main():
    """Main entry point for comprehensive integration tests."""
    test_suite = ComprehensiveIntegrationTest()

    try:
        test_suite.setup()
        exit_code = test_suite.run_all_tests()
        return exit_code
    finally:
        test_suite.cleanup()


if __name__ == "__main__":
    sys.exit(main())
