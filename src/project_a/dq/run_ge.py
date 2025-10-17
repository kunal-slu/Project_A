"""
Great Expectations checkpoint runner for enterprise data quality.
"""
import logging
from typing import Dict, Any, Optional
from great_expectations.data_context import get_context
from great_expectations.checkpoint import SimpleCheckpoint

logger = logging.getLogger(__name__)

def run_checkpoint(checkpoint_name: str, context_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Run Great Expectations checkpoint.
    
    Args:
        checkpoint_name: Name of the checkpoint to run
        context_path: Optional path to GE context
    
    Returns:
        dict: Checkpoint results
    """
    try:
        if context_path:
            ctx = get_context(context_root_dir=context_path)
        else:
            ctx = get_context()
        
        checkpoint = SimpleCheckpoint(name=checkpoint_name, data_context=ctx)
        result = checkpoint.run()
        
        if not result["success"]:
            logger.error(f"GE checkpoint failed: {checkpoint_name}")
            logger.error(f"Validation results: {result}")
            raise SystemExit(f"Data quality check failed: {checkpoint_name}")
        
        logger.info(f"GE checkpoint passed: {checkpoint_name}")
        return result
        
    except Exception as e:
        logger.error(f"GE checkpoint execution failed: {e}")
        raise

def run_contract_validation(contract_path: str, data_path: str) -> Dict[str, Any]:
    """
    Run contract-based validation using Great Expectations.
    
    Args:
        contract_path: Path to contract YAML file
        data_path: Path to data to validate
    
    Returns:
        dict: Validation results
    """
    # TODO: Implement contract-based validation
    # This would parse the contract YAML and create expectations dynamically
    logger.info(f"Contract validation: {contract_path} -> {data_path}")
    return {"success": True, "contract": contract_path}
