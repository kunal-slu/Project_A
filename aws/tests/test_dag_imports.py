"""
Test that all Airflow DAGs can be imported without errors.

This prevents deploying broken DAGs to MWAA.
"""
import pytest
import os
import sys
from pathlib import Path

# Add aws to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_import_all_dags():
    """Import all DAGs to catch syntax and import errors."""
    dags_dir = Path(__file__).parent.parent / "dags"
    
    dag_files = []
    for file in dags_dir.rglob("*.py"):
        if not file.name.startswith("_"):
            dag_files.append(file)
    
    assert len(dag_files) > 0, "No DAG files found"
    
    for dag_file in dag_files:
        try:
            # Import the module
            module_path = str(dag_file)
            if module_path.endswith(".py"):
                module_path = module_path[:-3]
            
            module_path = module_path.replace("/", ".").replace("\\", ".")
            
            # Skip if not a valid module path
            if "__pycache__" in module_path or ".pyc" in module_path:
                continue
                
            __import__(module_path)
            print(f"✅ Successfully imported: {dag_file.name}")
            
        except Exception as e:
            pytest.fail(f"Failed to import {dag_file}: {e}")


def test_dag_files_exist():
    """Verify critical DAG files exist."""
    dags_dir = Path(__file__).parent.parent / "dags"
    
    required_dags = [
        "daily_batch_pipeline_dag.py",
        "production_etl_dag.py",
    ]
    
    for dag in required_dags:
        dag_path = dags_dir / dag
        assert dag_path.exists(), f"Required DAG not found: {dag}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

