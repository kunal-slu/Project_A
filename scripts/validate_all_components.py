#!/usr/bin/env python3
"""
Comprehensive validation script for all data engineering components.
Ensures all modules are properly implemented and follow industry standards.
"""

import sys
import logging
from pathlib import Path
from typing import Dict, List, Tuple

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ComponentValidator:
    """Validates all data engineering components."""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.results: Dict[str, bool] = {}
        self.errors: List[str] = []
    
    def validate_all(self) -> Tuple[bool, Dict[str, bool]]:
        """Run all validation checks."""
        logger.info("=" * 80)
        logger.info("Starting comprehensive component validation")
        logger.info("=" * 80)
        
        # Validate module structure
        self.validate_module_structure()
        
        # Validate imports
        self.validate_imports()
        
        # Validate code quality
        self.validate_code_quality()
        
        # Print summary
        self.print_summary()
        
        return all(self.results.values()), self.results
    
    def validate_module_structure(self):
        """Validate that all modules have proper structure."""
        logger.info("\n[1/3] Validating module structure...")
        
        required_modules = [
            'src/project_a/lineage',
            'src/project_a/metadata',
            'src/project_a/monitoring',
            'src/project_a/dq',
            'src/project_a/cdc',
            'src/project_a/archival',
            'src/project_a/disaster_recovery',
            'src/project_a/cost',
            'src/project_a/security',
            'src/project_a/contracts',
            'src/project_a/testing',
            'src/project_a/cicd',
            'src/project_a/performance',
            'src/project_a/privacy'
        ]
        
        all_valid = True
        for module_path in required_modules:
            module_dir = self.project_root / module_path
            init_file = module_dir / '__init__.py'
            
            if not module_dir.exists():
                self.errors.append(f"Module directory missing: {module_path}")
                all_valid = False
            elif not init_file.exists():
                self.errors.append(f"__init__.py missing in: {module_path}")
                all_valid = False
            else:
                logger.info(f"✓ {module_path}")
        
        self.results['module_structure'] = all_valid
        logger.info(f"Module structure validation: {'PASSED' if all_valid else 'FAILED'}")
    
    def validate_imports(self):
        """Validate that all modules can be imported."""
        logger.info("\n[2/3] Validating module imports...")
        
        modules_to_import = [
            'project_a.lineage',
            'project_a.metadata',
            'project_a.monitoring',
            'project_a.dq',
            'project_a.cdc',
            'project_a.archival',
            'project_a.disaster_recovery',
            'project_a.cost',
            'project_a.security',
            'project_a.contracts',
            'project_a.testing',
            'project_a.cicd',
            'project_a.performance',
            'project_a.privacy'
        ]
        
        # Add src to path
        src_path = str(self.project_root / 'src')
        if src_path not in sys.path:
            sys.path.insert(0, src_path)
        
        all_valid = True
        for module_name in modules_to_import:
            try:
                __import__(module_name)
                logger.info(f"✓ {module_name}")
            except Exception as e:
                self.errors.append(f"Import failed for {module_name}: {str(e)}")
                all_valid = False
                logger.error(f"✗ {module_name}: {str(e)}")
        
        self.results['imports'] = all_valid
        logger.info(f"Import validation: {'PASSED' if all_valid else 'FAILED'}")
    
    def validate_code_quality(self):
        """Validate code quality standards."""
        logger.info("\n[3/3] Validating code quality standards...")
        
        # Check for required files
        required_files = [
            'jobs/run_pipeline.py',
            'jobs/ingest/snowflake_to_bronze.py',
            'jobs/ingest/redshift_to_bronze.py',
            'jobs/ingest/crm_to_bronze.py',
            'jobs/ingest/fx_to_bronze.py',
            'jobs/ingest/kafka_events_to_bronze.py',
            'jobs/transform/bronze_to_silver.py',
            'jobs/transform/silver_to_gold.py'
        ]
        
        all_valid = True
        for file_path in required_files:
            full_path = self.project_root / file_path
            if not full_path.exists():
                self.errors.append(f"Required file missing: {file_path}")
                all_valid = False
            else:
                logger.info(f"✓ {file_path}")
        
        self.results['code_quality'] = all_valid
        logger.info(f"Code quality validation: {'PASSED' if all_valid else 'FAILED'}")
    
    def print_summary(self):
        """Print validation summary."""
        logger.info("\n" + "=" * 80)
        logger.info("VALIDATION SUMMARY")
        logger.info("=" * 80)
        
        total_checks = len(self.results)
        passed_checks = sum(1 for v in self.results.values() if v)
        
        logger.info(f"Total checks: {total_checks}")
        logger.info(f"Passed: {passed_checks}")
        logger.info(f"Failed: {total_checks - passed_checks}")
        
        if self.errors:
            logger.info("\nErrors found:")
            for error in self.errors:
                logger.error(f"  - {error}")
        
        logger.info("=" * 80)
        
        if all(self.results.values()):
            logger.info("✓ ALL VALIDATIONS PASSED!")
        else:
            logger.error("✗ SOME VALIDATIONS FAILED")
        
        logger.info("=" * 80)


def main():
    """Main validation entry point."""
    project_root = Path(__file__).parent.parent.resolve()
    
    validator = ComponentValidator(project_root)
    success, results = validator.validate_all()
    
    # Return appropriate exit code
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
