"""
Test DAG imports for enterprise-grade validation.
"""

import importlib
import logging
import os
import pathlib
import pkgutil
import time

import pytest

logger = logging.getLogger(__name__)


def test_all_dags_import_fast():
    """Test that all DAGs can be imported quickly."""
    start_time = time.time()
    dags_dir = pathlib.Path("airflow/dags")

    if not dags_dir.exists():
        pytest.skip("Airflow DAGs directory not found")

    imported_dags = []
    failed_imports = []

    for mod in pkgutil.iter_modules([str(dags_dir)]):
        try:
            importlib.import_module(f"airflow.dags.{mod.name}")
            imported_dags.append(mod.name)
            logger.info(f"✅ DAG imported successfully: {mod.name}")
        except Exception as e:
            failed_imports.append((mod.name, str(e)))
            logger.error(f"❌ DAG import failed: {mod.name} - {e}")

    elapsed_time = time.time() - start_time

    # Assertions
    assert len(failed_imports) == 0, f"Failed to import DAGs: {failed_imports}"
    max_import_seconds = float(os.getenv("DAG_IMPORT_MAX_SECONDS", "8.0"))
    assert elapsed_time < max_import_seconds, (
        f"DAG import took too long: {elapsed_time:.2f}s "
        f"(threshold={max_import_seconds:.2f}s)"
    )
    assert len(imported_dags) > 0, "No DAGs were imported"

    logger.info(f"✅ All {len(imported_dags)} DAGs imported in {elapsed_time:.2f}s")


def test_dag_syntax_validation():
    """Test DAG syntax validation."""
    dags_dir = pathlib.Path("airflow/dags")

    if not dags_dir.exists():
        pytest.skip("Airflow DAGs directory not found")

    for dag_file in dags_dir.glob("*.py"):
        try:
            with open(dag_file) as f:
                content = f.read()

            # Basic syntax check
            compile(content, str(dag_file), "exec")
            logger.info(f"✅ DAG syntax valid: {dag_file.name}")

        except SyntaxError as e:
            pytest.fail(f"DAG syntax error in {dag_file.name}: {e}")
        except Exception as e:
            pytest.fail(f"Error reading DAG {dag_file.name}: {e}")


def test_dag_configuration():
    """Test DAG configuration consistency."""
    dags_dir = pathlib.Path("airflow/dags")

    if not dags_dir.exists():
        pytest.skip("Airflow DAGs directory not found")

    required_tags = ["project_a"]

    for dag_file in dags_dir.glob("*.py"):
        try:
            with open(dag_file) as f:
                content = f.read()

            # Check for required tags
            for tag in required_tags:
                assert tag in content, f"DAG {dag_file.name} missing required tag: {tag}"

            # Check for proper timeout configuration
            assert "dagrun_timeout" in content, f"DAG {dag_file.name} missing dagrun_timeout"

            logger.info(f"✅ DAG configuration valid: {dag_file.name}")

        except Exception as e:
            pytest.fail(f"DAG configuration error in {dag_file.name}: {e}")
