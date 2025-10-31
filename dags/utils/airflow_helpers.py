"""
Utility functions for Airflow DAGs.
"""

from airflow.models import Variable
from airflow.utils.context import Context
import logging

logger = logging.getLogger(__name__)


def get_variable(key: str, default_var=None):
    """
    Get Airflow variable with fallback to default.
    
    Args:
        key: Variable key
        default_var: Default value if variable not found
        
    Returns:
        Variable value or default
    """
    try:
        return Variable.get(key)
    except Exception:
        return default_var


def get_retry_delay(retry_number: int, base_delay: int = 300) -> int:
    """
    Calculate exponential backoff delay.
    
    Args:
        retry_number: Current retry attempt (0-indexed)
        base_delay: Base delay in seconds
        
    Returns:
        Delay in seconds
    """
    return base_delay * (2 ** retry_number)


def xcom_push_result(context: Context, key: str, value):
    """
    Push result to XCom.
    
    Args:
        context: Airflow context
        key: XCom key
        value: Value to push
    """
    context["ti"].xcom_push(key=key, value=value)


def xcom_pull_result(context: Context, key: str, task_ids: str = None):
    """
    Pull result from XCom.
    
    Args:
        context: Airflow context
        key: XCom key
        task_ids: Optional task ID to pull from
        
    Returns:
        XCom value
    """
    if task_ids:
        return context["ti"].xcom_pull(task_ids=task_ids, key=key)
    return context["ti"].xcom_pull(key=key)

