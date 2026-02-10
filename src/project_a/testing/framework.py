"""
Testing Framework for Project_A

Comprehensive testing framework for data pipelines, transformations, and quality.
"""

import json
import logging
import warnings
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

import pandas as pd

warnings.filterwarnings("ignore")


class TestResult(Enum):
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ERROR = "error"


@dataclass
class TestCaseResult:
    """Result of a single test case"""

    test_name: str
    result: TestResult
    duration: float
    error_message: str | None = None
    details: dict[str, Any] | None = None
    timestamp: datetime = None


@dataclass
class TestSuiteResult:
    """Result of a test suite"""

    suite_name: str
    test_cases: list[TestCaseResult]
    timestamp: datetime
    duration: float
    summary: dict[str, int]  # counts by result type


class DataTestFramework:
    """Main testing framework for data pipelines"""

    def __init__(self, results_path: str = "tests/results"):
        self.results_path = Path(results_path)
        self.results_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
        self.test_suites = []

    def run_test_suite(self, suite_name: str, test_functions: list[Callable]) -> TestSuiteResult:
        """Run a suite of tests"""
        start_time = datetime.utcnow()
        results = []

        for test_func in test_functions:
            try:
                test_start = datetime.utcnow()
                test_result = test_func()
                duration = (datetime.utcnow() - test_start).total_seconds()

                if test_result is True:
                    result = TestCaseResult(
                        test_name=test_func.__name__, result=TestResult.PASSED, duration=duration
                    )
                elif isinstance(test_result, dict) and "passed" in test_result:
                    if test_result["passed"]:
                        result = TestCaseResult(
                            test_name=test_func.__name__,
                            result=TestResult.PASSED,
                            duration=duration,
                            details=test_result,
                        )
                    else:
                        result = TestCaseResult(
                            test_name=test_func.__name__,
                            result=TestResult.FAILED,
                            duration=duration,
                            error_message=test_result.get("error", "Test failed"),
                            details=test_result,
                        )
                else:
                    result = TestCaseResult(
                        test_name=test_func.__name__,
                        result=TestResult.FAILED,
                        duration=duration,
                        error_message=str(test_result) if test_result else "Test failed",
                    )

                results.append(result)

            except Exception as e:
                duration = (datetime.utcnow() - test_start).total_seconds()
                results.append(
                    TestCaseResult(
                        test_name=test_func.__name__,
                        result=TestResult.ERROR,
                        duration=duration,
                        error_message=str(e),
                    )
                )

        # Calculate summary
        summary = {
            "total": len(results),
            "passed": len([r for r in results if r.result == TestResult.PASSED]),
            "failed": len([r for r in results if r.result == TestResult.FAILED]),
            "error": len([r for r in results if r.result == TestResult.ERROR]),
            "skipped": len([r for r in results if r.result == TestResult.SKIPPED]),
        }

        suite_result = TestSuiteResult(
            suite_name=suite_name,
            test_cases=results,
            timestamp=start_time,
            duration=(datetime.utcnow() - start_time).total_seconds(),
            summary=summary,
        )

        # Save results
        self._save_test_results(suite_result)

        return suite_result

    def _save_test_results(self, suite_result: TestSuiteResult):
        """Save test results to file"""
        filename = f"test_results_{suite_result.suite_name}_{suite_result.timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        filepath = self.results_path / filename

        result_dict = {
            "suite_name": suite_result.suite_name,
            "timestamp": suite_result.timestamp.isoformat(),
            "duration": suite_result.duration,
            "summary": suite_result.summary,
            "test_cases": [
                {
                    "test_name": tc.test_name,
                    "result": tc.result.value,
                    "duration": tc.duration,
                    "error_message": tc.error_message,
                    "details": tc.details,
                }
                for tc in suite_result.test_cases
            ],
        }

        with open(filepath, "w") as f:
            json.dump(result_dict, f, indent=2, default=str)

    def assert_dataframe_equal(
        self,
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        ignore_index: bool = True,
        check_dtype: bool = False,
    ) -> dict[str, Any]:
        """Assert that two DataFrames are equal"""
        try:
            if ignore_index:
                df1 = df1.reset_index(drop=True)
                df2 = df2.reset_index(drop=True)

            if not check_dtype:
                # Convert dtypes to string for comparison
                for col in df1.columns:
                    if col in df2.columns:
                        df1[col] = df1[col].astype(str)
                        df2[col] = df2[col].astype(str)

            pd.testing.assert_frame_equal(df1, df2)
            return {"passed": True, "message": "DataFrames are equal"}
        except AssertionError as e:
            return {"passed": False, "error": str(e), "message": "DataFrames are not equal"}

    def assert_dataframe_schema(
        self, df: pd.DataFrame, expected_schema: dict[str, str]
    ) -> dict[str, Any]:
        """Assert that DataFrame has expected schema"""
        try:
            missing_cols = set(expected_schema.keys()) - set(df.columns)
            if missing_cols:
                return {
                    "passed": False,
                    "error": f"Missing columns: {missing_cols}",
                    "missing_columns": list(missing_cols),
                }

            type_mismatches = []
            for col, expected_type in expected_schema.items():
                actual_type = str(df[col].dtype)
                if expected_type != actual_type:
                    type_mismatches.append(
                        {"column": col, "expected_type": expected_type, "actual_type": actual_type}
                    )

            if type_mismatches:
                return {
                    "passed": False,
                    "error": "Type mismatches found",
                    "type_mismatches": type_mismatches,
                }

            return {"passed": True, "message": "Schema matches"}
        except Exception as e:
            return {"passed": False, "error": str(e)}

    def assert_dataframe_count(self, df: pd.DataFrame, expected_count: int) -> dict[str, Any]:
        """Assert that DataFrame has expected number of rows"""
        actual_count = len(df)
        if actual_count == expected_count:
            return {"passed": True, "message": f"Count matches: {actual_count}"}
        else:
            return {
                "passed": False,
                "error": f"Count mismatch: expected {expected_count}, got {actual_count}",
                "expected_count": expected_count,
                "actual_count": actual_count,
            }

    def assert_no_nulls_in_column(self, df: pd.DataFrame, column: str) -> dict[str, Any]:
        """Assert that a column has no null values"""
        null_count = df[column].isna().sum()
        if null_count == 0:
            return {"passed": True, "message": f"No nulls in column {column}"}
        else:
            return {
                "passed": False,
                "error": f"Null values found in column {column}: {null_count}",
                "null_count": int(null_count),
            }

    def assert_unique_values(self, df: pd.DataFrame, column: str) -> dict[str, Any]:
        """Assert that a column has unique values"""
        total_count = len(df)
        unique_count = df[column].nunique()

        if total_count == unique_count:
            return {"passed": True, "message": f"All values in {column} are unique"}
        else:
            duplicate_count = total_count - unique_count
            return {
                "passed": False,
                "error": f"Duplicate values found in {column}: {duplicate_count}",
                "duplicate_count": int(duplicate_count),
                "total_count": int(total_count),
                "unique_count": int(unique_count),
            }

    def assert_column_range(
        self, df: pd.DataFrame, column: str, min_val: float, max_val: float
    ) -> dict[str, Any]:
        """Assert that column values are within specified range"""
        series = df[column]
        if pd.api.types.is_numeric_dtype(series):
            min_actual = series.min()
            max_actual = series.max()

            if min_actual >= min_val and max_actual <= max_val:
                return {
                    "passed": True,
                    "message": f"Column {column} values are within range [{min_val}, {max_val}]",
                    "actual_range": [float(min_actual), float(max_actual)],
                }
            else:
                return {
                    "passed": False,
                    "error": f"Column {column} values out of range [{min_val}, {max_val}]",
                    "actual_range": [float(min_actual), float(max_actual)],
                    "expected_range": [min_val, max_val],
                }
        else:
            return {"passed": False, "error": f"Column {column} is not numeric"}


class IntegrationTestRunner:
    """Runs integration tests for data pipelines"""

    def __init__(self, framework: DataTestFramework):
        self.framework = framework
        self.logger = logging.getLogger(__name__)

    def run_transformation_tests(
        self, transformation_func: Callable, input_data: pd.DataFrame, expected_output: pd.DataFrame
    ) -> dict[str, Any]:
        """Run tests for a transformation function"""
        try:
            # Apply transformation
            actual_output = transformation_func(input_data)

            # Compare with expected output
            comparison_result = self.framework.assert_dataframe_equal(
                actual_output, expected_output
            )

            return {
                "passed": comparison_result["passed"],
                "transformation_func": transformation_func.__name__,
                "input_shape": input_data.shape,
                "output_shape": actual_output.shape,
                "comparison_result": comparison_result,
            }
        except Exception as e:
            return {
                "passed": False,
                "error": str(e),
                "transformation_func": transformation_func.__name__,
            }

    def run_pipeline_tests(
        self, pipeline_func: Callable, test_scenarios: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Run tests for a pipeline with multiple scenarios"""
        results = []

        for scenario in test_scenarios:
            try:
                # Prepare input data
                input_data = scenario["input"]
                expected_output = scenario["expected"]

                # Run pipeline
                actual_output = pipeline_func(input_data)

                # Compare with expected
                comparison_result = self.framework.assert_dataframe_equal(
                    actual_output,
                    expected_output,
                    ignore_index=scenario.get("ignore_index", True),
                    check_dtype=scenario.get("check_dtype", False),
                )

                result = {
                    "scenario_name": scenario.get("name", "unnamed"),
                    "passed": comparison_result["passed"],
                    "input_shape": input_data.shape
                    if hasattr(input_data, "shape")
                    else len(input_data),
                    "output_shape": actual_output.shape
                    if hasattr(actual_output, "shape")
                    else len(actual_output),
                    "comparison_result": comparison_result,
                }

                results.append(result)

            except Exception as e:
                results.append(
                    {
                        "scenario_name": scenario.get("name", "unnamed"),
                        "passed": False,
                        "error": str(e),
                    }
                )

        return results


class QualityTestGenerator:
    """Generates quality tests based on data profiling"""

    def __init__(self, framework: DataTestFramework):
        self.framework = framework
        self.logger = logging.getLogger(__name__)

    def generate_tests_from_profile(self, df: pd.DataFrame, dataset_name: str) -> list[Callable]:
        """Generate test functions based on data profiling"""
        tests = []

        # Generate schema tests
        schema = {col: str(df[col].dtype) for col in df.columns}

        def test_schema():
            return self.framework.assert_dataframe_schema(df, schema)

        test_schema.__name__ = f"test_{dataset_name}_schema"
        tests.append(test_schema)

        # Generate count tests
        expected_count = len(df)

        def test_count():
            return self.framework.assert_dataframe_count(df, expected_count)

        test_count.__name__ = f"test_{dataset_name}_count"
        tests.append(test_count)

        # Generate tests for non-null columns
        for col in df.columns:
            if df[col].isna().sum() == 0:  # Column has no nulls

                def make_test(col_name):
                    def test():
                        return self.framework.assert_no_nulls_in_column(df, col_name)

                    test.__name__ = f"test_{dataset_name}_{col_name}_no_nulls"
                    return test

                tests.append(make_test(col))

        # Generate tests for unique columns (if they appear to be unique)
        for col in df.columns:
            if df[col].nunique() == len(df):  # Column appears to be unique

                def make_test(col_name):
                    def test():
                        return self.framework.assert_unique_values(df, col_name)

                    test.__name__ = f"test_{dataset_name}_{col_name}_unique"
                    return test

                tests.append(make_test(col))

        # Generate range tests for numeric columns
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                min_val = float(df[col].min())
                max_val = float(df[col].max())

                def make_test(col_name, min_v, max_v):
                    def test():
                        return self.framework.assert_column_range(df, col_name, min_v, max_v)

                    test.__name__ = f"test_{dataset_name}_{col_name}_range"
                    return test

                tests.append(make_test(col, min_val, max_val))

        return tests


# Global instance
_test_framework = None


def get_test_framework() -> DataTestFramework:
    """Get the global test framework instance"""
    global _test_framework
    if _test_framework is None:
        from ..config_loader import load_config_resolved

        config = load_config_resolved("local/config/local.yaml")
        results_path = config.get("paths", {}).get("test_results_root", "tests/results")
        _test_framework = DataTestFramework(results_path)
    return _test_framework


def run_test_suite(suite_name: str, test_functions: list[Callable]) -> TestSuiteResult:
    """Run a suite of tests"""
    framework = get_test_framework()
    return framework.run_test_suite(suite_name, test_functions)


def assert_dataframe_equal(
    df1: pd.DataFrame, df2: pd.DataFrame, ignore_index: bool = True, check_dtype: bool = False
) -> dict[str, Any]:
    """Assert that two DataFrames are equal"""
    framework = get_test_framework()
    return framework.assert_dataframe_equal(df1, df2, ignore_index, check_dtype)


def assert_dataframe_schema(df: pd.DataFrame, expected_schema: dict[str, str]) -> dict[str, Any]:
    """Assert that DataFrame has expected schema"""
    framework = get_test_framework()
    return framework.assert_dataframe_schema(df, expected_schema)


def assert_dataframe_count(df: pd.DataFrame, expected_count: int) -> dict[str, Any]:
    """Assert that DataFrame has expected number of rows"""
    framework = get_test_framework()
    return framework.assert_dataframe_count(df, expected_count)


def assert_no_nulls_in_column(df: pd.DataFrame, column: str) -> dict[str, Any]:
    """Assert that a column has no null values"""
    framework = get_test_framework()
    return framework.assert_no_nulls_in_column(df, column)


def assert_unique_values(df: pd.DataFrame, column: str) -> dict[str, Any]:
    """Assert that a column has unique values"""
    framework = get_test_framework()
    return framework.assert_unique_values(df, column)


def assert_column_range(
    df: pd.DataFrame, column: str, min_val: float, max_val: float
) -> dict[str, Any]:
    """Assert that column values are within specified range"""
    framework = get_test_framework()
    return framework.assert_column_range(df, column, min_val, max_val)


def run_transformation_tests(
    transformation_func: Callable, input_data: pd.DataFrame, expected_output: pd.DataFrame
) -> dict[str, Any]:
    """Run tests for a transformation function"""
    framework = get_test_framework()
    runner = IntegrationTestRunner(framework)
    return runner.run_transformation_tests(transformation_func, input_data, expected_output)


def generate_tests_from_profile(df: pd.DataFrame, dataset_name: str) -> list[Callable]:
    """Generate test functions based on data profiling"""
    framework = get_test_framework()
    generator = QualityTestGenerator(framework)
    return generator.generate_tests_from_profile(df, dataset_name)
