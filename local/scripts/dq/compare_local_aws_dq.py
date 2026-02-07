#!/usr/bin/env python3
"""
Compare Data Quality Results: Local vs AWS

Runs DQ checks on both environments and compares results.
"""
import sys
import subprocess
import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent


def run_dq_check(env: str, layer: str = "all") -> dict:
    """Run DQ check and parse results."""
    logger.info(f"Running DQ check for {env} environment...")
    
    script_path = PROJECT_ROOT / "local/scripts/dq/check_data_quality.py"
    config_path = PROJECT_ROOT / f"{env}/config/{'local' if env == 'local' else 'dev'}.yaml"
    
    cmd = [
        sys.executable,
        str(script_path),
        "--env", env,
        "--config", str(config_path),
        "--layer", layer
    ]
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT
    )
    
    if result.returncode != 0:
        logger.error(f"DQ check failed for {env}: {result.stderr}")
        return {}
    
    # Parse output (simplified - in production would use structured output)
    return {
        "env": env,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "returncode": result.returncode
    }


def compare_results(local_result: dict, aws_result: dict):
    """Compare local and AWS DQ results."""
    logger.info("")
    logger.info("=" * 80)
    logger.info("LOCAL vs AWS DATA QUALITY COMPARISON")
    logger.info("=" * 80)
    logger.info("")
    
    # Extract key metrics from output (simplified parsing)
    logger.info("Local Results:")
    logger.info(local_result.get("stdout", "")[:500])
    logger.info("")
    logger.info("AWS Results:")
    logger.info(aws_result.get("stdout", "")[:500])
    logger.info("")
    logger.info("=" * 80)
    logger.info("✅ Comparison complete - review outputs above")
    logger.info("=" * 80)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Compare DQ results between local and AWS")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "all"], default="all")
    args = parser.parse_args()
    
    logger.info("=" * 80)
    logger.info("DATA QUALITY COMPARISON: LOCAL vs AWS")
    logger.info("=" * 80)
    logger.info("")
    
    # Run local check
    local_result = run_dq_check("local", args.layer)
    
    # Run AWS check (if AWS credentials available)
    try:
        aws_result = run_dq_check("aws", args.layer)
        compare_results(local_result, aws_result)
    except Exception as e:
        logger.warning(f"Could not run AWS check: {e}")
        logger.info("")
        logger.info("Local Results:")
        logger.info(local_result.get("stdout", ""))


if __name__ == "__main__":
    main()

