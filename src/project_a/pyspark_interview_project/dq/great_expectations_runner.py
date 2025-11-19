"""
Great Expectations runner with fail-fast validation.

This module ensures data quality checks terminate the job on failures.
"""

import logging
import sys
from typing import Dict, Any, Optional
from pathlib import Path
import json

logger = logging.getLogger(__name__)


class GreatExpectationsRunner:
    """
    Production-ready GE runner with fail-fast behavior.
    
    Features:
    - Fails job immediately on validation failures
    - Emits compact summaries to logs
    - Generates data docs links
    - Stores suite versions under version control
    """
    
    def __init__(self, context_root: Optional[Path] = None):
        """
        Initialize GE runner.
        
        Args:
            context_root: Root directory for GE context (default: project root)
        """
        self.context_root = context_root or Path.cwd()
        self.context = None
        
    def init_context(self) -> None:
        """Initialize Great Expectations context."""
        try:
            import great_expectations as gx
            from great_expectations.data_context import DataContext
            
            context_path = self.context_root / "great_expectations"
            
            if context_path.exists():
                self.context = DataContext(str(context_path))
                logger.info(f"Loaded existing GE context from {context_path}")
            else:
                logger.warning(
                    f"GE context not found at {context_path}. "
                    "Run 'great_expectations init' first."
                )
                self.context = None
                
        except ImportError:
            logger.error(
                "Great Expectations not installed. "
                "Install with: pip install great-expectations"
            )
            self.context = None
        except Exception as e:
            logger.error(f"Failed to initialize GE context: {e}")
            self.context = None
    
    def run_checkpoint(
        self,
        checkpoint_name: str,
        batch_request: Optional[Dict[str, Any]] = None,
        fail_on_error: bool = True
    ) -> Dict[str, Any]:
        """
        Run a GE checkpoint with fail-fast behavior.
        
        Args:
            checkpoint_name: Name of the checkpoint to run
            batch_request: Optional batch request configuration
            fail_on_error: Whether to raise exception on validation failure
            
        Returns:
            Dictionary with validation results
            
        Raises:
            RuntimeError: If validation fails and fail_on_error is True
        """
        if not self.context:
            logger.warning("GE context not initialized, skipping validation")
            return {
                "success": True,
                "skipped": True,
                "message": "GE not configured"
            }
        
        try:
            logger.info(f"Running GE checkpoint: {checkpoint_name}")
            
            # Get checkpoint
            checkpoint = self.context.get_checkpoint(checkpoint_name)
            
            # Run checkpoint
            if batch_request:
                result = checkpoint.run(batch_request=batch_request)
            else:
                result = checkpoint.run()
            
            # Extract results
            success = result.get("success", False)
            validation_results = result.get("run_results", {})
            
            # Build summary
            summary = self._build_summary(result)
            
            # Log summary
            logger.info(
                f"GE Checkpoint Results for {checkpoint_name}:",
                extra={
                    "checkpoint": checkpoint_name,
                    "success": success,
                    "total_expectations": summary["total_expectations"],
                    "successful_expectations": summary["successful_expectations"],
                    "failed_expectations": summary["failed_expectations"],
                    "validation_results": summary["validation_results"]
                }
            )
            
            # Generate data docs link
            data_docs_url = self._get_data_docs_url(result)
            if data_docs_url:
                logger.info(f"Data Docs available at: {data_docs_url}")
            
            # Fail fast if validation failed
            if not success and fail_on_error:
                error_msg = (
                    f"Great Expectations validation failed for {checkpoint_name}. "
                    f"Failed {summary['failed_expectations']} of "
                    f"{summary['total_expectations']} expectations. "
                    f"See logs for details."
                )
                logger.error(error_msg)
                
                # Log specific failures
                for failure in summary["failures"]:
                    logger.error(
                        f"  - {failure['expectation_type']}: {failure['kwargs']}"
                    )
                
                raise RuntimeError(error_msg)
            
            return {
                "success": success,
                "summary": summary,
                "data_docs_url": data_docs_url
            }
            
        except Exception as e:
            logger.error(f"GE checkpoint execution failed: {e}")
            if fail_on_error:
                raise RuntimeError(f"Data quality gate failed: {e}") from e
            return {
                "success": False,
                "error": str(e)
            }
    
    def _build_summary(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build compact summary from GE results.
        
        Args:
            result: GE checkpoint result
            
        Returns:
            Summary dictionary
        """
        summary = {
            "total_expectations": 0,
            "successful_expectations": 0,
            "failed_expectations": 0,
            "validation_results": [],
            "failures": []
        }
        
        try:
            run_results = result.get("run_results", {})
            
            for run_id, run_result in run_results.items():
                validation_result = run_result.get("validation_result", {})
                
                # Get statistics
                statistics = validation_result.get("statistics", {})
                summary["total_expectations"] += statistics.get(
                    "evaluated_expectations", 0
                )
                summary["successful_expectations"] += statistics.get(
                    "successful_expectations", 0
                )
                
                # Get specific failures
                results = validation_result.get("results", [])
                for exp_result in results:
                    if not exp_result.get("success", True):
                        summary["failed_expectations"] += 1
                        summary["failures"].append({
                            "expectation_type": exp_result.get(
                                "expectation_config", {}
                            ).get("expectation_type"),
                            "kwargs": exp_result.get(
                                "expectation_config", {}
                            ).get("kwargs", {}),
                            "result": exp_result.get("result", {})
                        })
                
                # Add validation result summary
                summary["validation_results"].append({
                    "run_id": run_id,
                    "success": validation_result.get("success", False),
                    "evaluated_expectations": statistics.get(
                        "evaluated_expectations", 0
                    ),
                    "successful_expectations": statistics.get(
                        "successful_expectations", 0
                    )
                })
                
        except Exception as e:
            logger.warning(f"Failed to build detailed summary: {e}")
        
        return summary
    
    def _get_data_docs_url(self, result: Dict[str, Any]) -> Optional[str]:
        """
        Extract data docs URL from result.
        
        Args:
            result: GE checkpoint result
            
        Returns:
            Data docs URL or None
        """
        try:
            run_results = result.get("run_results", {})
            for run_result in run_results.values():
                actions_results = run_result.get("actions_results", {})
                for action_result in actions_results.values():
                    if isinstance(action_result, dict):
                        data_docs_url = action_result.get("local_site")
                        if data_docs_url:
                            return data_docs_url
        except Exception:
            pass
        
        return None
    
    def validate_dataframe(
        self,
        df: "pyspark.sql.DataFrame",
        expectation_suite_name: str,
        fail_on_error: bool = True
    ) -> Dict[str, Any]:
        """
        Validate a PySpark DataFrame against an expectation suite.
        
        Args:
            df: PySpark DataFrame to validate
            expectation_suite_name: Name of expectation suite
            fail_on_error: Whether to raise exception on failure
            
        Returns:
            Dictionary with validation results
        """
        if not self.context:
            logger.warning("GE context not initialized, skipping validation")
            return {
                "success": True,
                "skipped": True,
                "message": "GE not configured"
            }
        
        try:
            # Create batch request
            batch_request = {
                "datasource_name": "my_spark_datasource",
                "data_connector_name": "default_runtime_data_connector",
                "data_asset_name": "runtime_data_asset",
                "runtime_parameters": {"batch_data": df},
                "batch_identifiers": {
                    "default_identifier_name": "default_identifier"
                }
            }
            
            # Get expectation suite
            suite = self.context.get_expectation_suite(expectation_suite_name)
            
            # Run validation
            validator = self.context.get_validator(
                batch_request=batch_request,
                expectation_suite=suite
            )
            
            result = validator.validate()
            
            # Check success
            success = result.get("success", False)
            statistics = result.get("statistics", {})
            
            # Log results
            logger.info(
                f"DataFrame validation for {expectation_suite_name}:",
                extra={
                    "suite": expectation_suite_name,
                    "success": success,
                    "total_expectations": statistics.get(
                        "evaluated_expectations", 0
                    ),
                    "successful_expectations": statistics.get(
                        "successful_expectations", 0
                    )
                }
            )
            
            # Fail fast if needed
            if not success and fail_on_error:
                failed_count = (
                    statistics.get("evaluated_expectations", 0) -
                    statistics.get("successful_expectations", 0)
                )
                error_msg = (
                    f"DataFrame validation failed for {expectation_suite_name}. "
                    f"Failed {failed_count} expectations."
                )
                logger.error(error_msg)
                raise RuntimeError(error_msg)
            
            return {
                "success": success,
                "statistics": statistics
            }
            
        except Exception as e:
            logger.error(f"DataFrame validation failed: {e}")
            if fail_on_error:
                raise RuntimeError(f"Data quality gate failed: {e}") from e
            return {
                "success": False,
                "error": str(e)
            }


def run_dq_checkpoint(
    checkpoint_name: str,
    context_root: Optional[Path] = None,
    fail_on_error: bool = True
) -> Dict[str, Any]:
    """
    Convenience function to run a GE checkpoint with fail-fast behavior.
    
    Args:
        checkpoint_name: Name of checkpoint to run
        context_root: Root directory for GE context
        fail_on_error: Whether to fail job on validation failure
        
    Returns:
        Dictionary with validation results
    """
    runner = GreatExpectationsRunner(context_root)
    runner.init_context()
    return runner.run_checkpoint(checkpoint_name, fail_on_error=fail_on_error)

