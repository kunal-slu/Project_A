"""
Test DAG imports to ensure all Airflow DAGs can be loaded without errors.
"""

import pytest
import sys
from pathlib import Path

# Add dags directory to path
dags_dir = Path(__file__).parent.parent / "dags"
sys.path.insert(0, str(dags_dir))


def test_main_etl_dag_import():
    """Test that main_etl_dag can be imported."""
    try:
        import main_etl_dag
        assert hasattr(main_etl_dag, 'dag')
        assert main_etl_dag.dag.dag_id == 'main_etl_dag'
    except Exception as e:
        pytest.fail(f"Failed to import main_etl_dag: {e}")


def test_daily_pipeline_dag_import():
    """Test that daily_pipeline can be imported."""
    try:
        import daily_pipeline
        assert hasattr(daily_pipeline, 'dag')
        assert daily_pipeline.dag.dag_id == 'daily_pipeline'
    except Exception as e:
        pytest.fail(f"Failed to import daily_pipeline: {e}")


def test_returns_pipeline_dag_import():
    """Test that returns_pipeline_dag can be imported."""
    try:
        import returns_pipeline_dag
        assert hasattr(returns_pipeline_dag, 'dag')
        assert returns_pipeline_dag.dag.dag_id == 'returns_pipeline'
    except Exception as e:
        pytest.fail(f"Failed to import returns_pipeline_dag: {e}")


def test_catalog_and_dq_dag_import():
    """Test that catalog_and_dq_dag can be imported."""
    try:
        import catalog_and_dq_dag
        assert hasattr(catalog_and_dq_dag, 'dag')
        assert catalog_and_dq_dag.dag.dag_id == 'catalog_and_dq'
    except Exception as e:
        pytest.fail(f"Failed to import catalog_and_dq_dag: {e}")


def test_all_dags_have_required_variables():
    """Test that all DAGs have standardized Airflow variables."""
    required_vars = [
        'EMR_APP_ID',
        'EMR_JOB_ROLE_ARN', 
        'GLUE_DB_SILVER',
        'GLUE_DB_GOLD',
        'S3_LAKE_BUCKET',
        'S3_CHECKPOINT_PATH'
    ]
    
    dags = [
        'main_etl_dag',
        'daily_pipeline', 
        'returns_pipeline_dag',
        'catalog_and_dq_dag'
    ]
    
    for dag_name in dags:
        try:
            dag_module = __import__(dag_name)
            for var in required_vars:
                assert hasattr(dag_module, var), f"{dag_name} missing {var}"
        except ImportError as e:
            pytest.fail(f"Failed to import {dag_name}: {e}")


def test_dag_default_args():
    """Test that all DAGs have proper default arguments."""
    dags = [
        ('main_etl_dag', 'main_etl_dag'),
        ('daily_pipeline', 'daily_pipeline'),
        ('returns_pipeline_dag', 'returns_pipeline'),
        ('catalog_and_dq_dag', 'catalog_and_dq')
    ]
    
    for module_name, dag_name in dags:
        try:
            dag_module = __import__(module_name)
            dag = dag_module.dag
            
            # Check required default args
            assert 'owner' in dag.default_args
            assert 'depends_on_past' in dag.default_args
            assert 'retries' in dag.default_args
            assert 'retry_delay' in dag.default_args
            
            # Check values
            assert dag.default_args['depends_on_past'] is False
            assert dag.default_args['retries'] >= 1
            
        except ImportError as e:
            pytest.fail(f"Failed to import {module_name}: {e}")
