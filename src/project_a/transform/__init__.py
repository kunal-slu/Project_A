"""
Transform modules for Project A.

Bronze → Silver → Gold transformations.

Note: The actual transformation logic is in jobs/transform/bronze_to_silver.py
and jobs/transform/silver_to_gold.py. This module is kept for backward compatibility.
"""

# Import from canonical job files
try:
    from jobs.transform.bronze_to_silver import bronze_to_silver_complete
    from jobs.transform.silver_to_gold import silver_to_gold_complete
    __all__ = ["bronze_to_silver_complete", "silver_to_gold_complete"]
except ImportError:
    # Fallback if jobs/transform is not in path
    __all__ = []

