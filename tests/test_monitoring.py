"""
Unit tests for monitoring/metrics module.
"""
import pytest
import time
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from project_a.monitoring.metrics import (
    track_job_execution,
    track_stage_duration,
    record_dq_check,
    record_records_processed,
    record_records_failed,
    record_delta_table_metrics,
    record_delta_write,
    record_schema_drift,
    record_error,
    get_metrics_text,
    registry,
)


class TestMetricsCollection:
    """Test suite for metrics collection."""

    def test_track_job_execution_success(self):
        """Test tracking successful job execution."""
        @track_job_execution("test_job", "test")
        def successful_job():
            time.sleep(0.1)
            return "success"
        
        result = successful_job()
        assert result == "success"
        
        # Verify metrics were recorded
        metrics = get_metrics_text().decode('utf-8')
        assert 'etl_job_executions_total{environment="test",job_name="test_job",status="success"}' in metrics
        assert 'etl_job_duration_seconds' in metrics

    def test_track_job_execution_failure(self):
        """Test tracking failed job execution."""
        @track_job_execution("test_job_fail", "test")
        def failing_job():
            time.sleep(0.1)
            raise ValueError("Test error")
        
        with pytest.raises(ValueError, match="Test error"):
            failing_job()
        
        # Verify error metrics were recorded
        metrics = get_metrics_text().decode('utf-8')
        assert 'etl_job_executions_total{environment="test",job_name="test_job_fail",status="failure"}' in metrics
        assert 'etl_errors_total' in metrics

    def test_track_stage_duration(self):
        """Test tracking stage duration."""
        with track_stage_duration("test_pipeline", "extract", "test"):
            time.sleep(0.05)
        
        metrics = get_metrics_text().decode('utf-8')
        assert 'etl_job_duration_seconds' in metrics
        assert 'stage="extract"' in metrics

    def test_track_stage_duration_with_error(self):
        """Test tracking stage duration when error occurs."""
        try:
            with track_stage_duration("test_pipeline_error", "transform", "test"):
                time.sleep(0.05)
                raise RuntimeError("Stage failed")
        except RuntimeError:
            pass
        
        metrics = get_metrics_text().decode('utf-8')
        assert 'etl_errors_total' in metrics
        assert 'stage="transform"' in metrics

    def test_record_dq_check_passed(self):
        """Test recording passed DQ check."""
        record_dq_check("test_table", "null_check", passed=True)
        
        metrics = get_metrics_text().decode('utf-8')
        assert 'etl_dq_checks_total{check_type="null_check",status="passed",table="test_table"}' in metrics

    def test_record_dq_check_failed_with_violations(self):
        """Test recording failed DQ check with violations."""
        record_dq_check("test_table", "range_check", passed=False, violations=10, severity="critical")
        
        metrics = get_metrics_text().decode('utf-8')
        assert 'etl_dq_checks_total{check_type="range_check",status="failed",table="test_table"}' in metrics
        assert 'etl_dq_violations_total' in metrics

    def test_record_records_processed(self):
        """Test recording processed records."""
        record_records_processed("test_job", "transform", "customers", 1000)
        
        metrics = get_metrics_text().decode('utf-8')
        assert 'etl_records_processed_total' in metrics
        assert 'table="customers"' in metrics
        assert 'stage="transform"' in metrics

    def test_record_records_failed(self):
        """Test recording failed records."""
        record_records_failed("test_job", "transform", "orders", 5, "ValidationError")
        
        metrics = get_metrics_text().decode('utf-8')
        assert 'etl_records_failed_total' in metrics
        assert 'error_type="ValidationError"' in metrics

    def test_record_delta_table_metrics(self):
        """Test recording Delta Lake table metrics."""
        metrics_data = {
            "size_bytes": 1024 * 1024 * 100,  # 100MB
            "version_count": 15,
            "file_count": 25
        }
        record_delta_table_metrics("customers", "bronze", metrics_data)
        
        metrics = get_metrics_text().decode('utf-8')
        assert 'delta_table_size_bytes{layer="bronze",table="customers"}' in metrics
        assert 'delta_table_versions{layer="bronze",table="customers"}' in metrics
        assert 'delta_table_files{layer="bronze",table="customers"}' in metrics

    def test_record_delta_write(self):
        """Test recording Delta write operation."""
        record_delta_write("orders", "merge", "silver", 2.5)
        
        metrics = get_metrics_text().decode('utf-8')
        assert 'delta_write_duration_seconds' in metrics
        assert 'operation="merge"' in metrics
        assert 'layer="silver"' in metrics

    def test_record_schema_drift(self):
        """Test recording schema drift."""
        record_schema_drift("products", "column_added")
        
        metrics = get_metrics_text().decode('utf-8')
        assert 'etl_schema_drift_detected_total' in metrics
        assert 'drift_type="column_added"' in metrics

    def test_record_error(self):
        """Test recording errors."""
        record_error("etl_pipeline", "load", "ConnectionError", "critical")
        
        metrics = get_metrics_text().decode('utf-8')
        assert 'etl_errors_total' in metrics
        assert 'error_type="ConnectionError"' in metrics
        assert 'severity="critical"' in metrics

    def test_get_metrics_text_format(self):
        """Test metrics text format is valid Prometheus format."""
        metrics = get_metrics_text()
        assert isinstance(metrics, bytes)
        
        # Decode and verify contains some expected content
        metrics_text = metrics.decode('utf-8')
        assert 'TYPE' in metrics_text  # Should have TYPE declarations
        assert 'HELP' in metrics_text  # Should have HELP descriptions

    def test_multiple_metrics_recording(self):
        """Test recording multiple metrics in sequence."""
        # Record various metrics
        record_records_processed("batch_job", "extract", "users", 500)
        record_records_processed("batch_job", "transform", "users", 490)
        record_records_failed("batch_job", "transform", "users", 10, "ParseError")
        record_dq_check("users", "completeness", True)
        
        metrics = get_metrics_text().decode('utf-8')
        
        # Verify all metrics are present
        assert 'etl_records_processed_total' in metrics
        assert 'etl_records_failed_total' in metrics
        assert 'etl_dq_checks_total' in metrics

    def test_metrics_with_special_characters(self):
        """Test metrics with special characters in labels."""
        # Should handle tables/jobs with underscores, hyphens
        record_records_processed("my-etl-job_v2", "extract", "user_events", 1000)
        
        metrics = get_metrics_text().decode('utf-8')
        assert 'user_events' in metrics


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

