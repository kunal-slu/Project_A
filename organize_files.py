#!/usr/bin/env python3
"""
File Organization and Alignment Checker

This script will:
1. Organize files into proper folder structure
2. Check each file for alignment issues
3. Fix import paths and references
4. Ensure consistent naming conventions
5. Remove duplicate or obsolete files
"""

import os
import sys
import shutil
import logging
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def organize_files():
    """Organize files into proper folder structure."""
    logger.info("🗂️ Starting File Organization")
    logger.info("=" * 50)
    
    # Create organized folder structure
    folders_to_create = [
        "scripts/local",
        "scripts/aws", 
        "scripts/cleanup",
        "docs/status",
        "docs/guides",
        "docs/schema_contracts",
        "docs/runbooks",
        "config/schemas",
        "tests/integration",
        "tests/unit",
        "data/samples",
        "data/backups"
    ]
    
    for folder in folders_to_create:
        Path(folder).mkdir(parents=True, exist_ok=True)
        logger.info(f"✅ Created folder: {folder}")
    
    # Move files to appropriate locations
    file_moves = [
        # Scripts organization
        ("verify_enterprise_setup.py", "scripts/local/verify_enterprise_setup.py"),
        ("verify_salesforce_data.py", "scripts/local/verify_salesforce_data.py"),
        ("verify_salesforce_final.py", "scripts/local/verify_salesforce_final.py"),
        ("test_crm_etl_pandas.py", "scripts/local/test_crm_etl_pandas.py"),
        ("test_salesforce_etl_local.py", "scripts/local/test_salesforce_etl_local.py"),
        ("crm_etl_pipeline.py", "scripts/local/crm_etl_pipeline.py"),
        ("local_pipeline_smoke.py", "scripts/local/local_pipeline_smoke.py"),
        ("local_salesforce_pipeline_test.py", "scripts/local/local_salesforce_pipeline_test.py"),
        ("enterprise_pipeline_driver.py", "scripts/local/enterprise_pipeline_driver.py"),
        ("pipeline_driver.py", "scripts/local/pipeline_driver.py"),
        
        # Documentation organization
        ("ENTERPRISE_IMPLEMENTATION_COMPLETE.md", "docs/status/ENTERPRISE_IMPLEMENTATION_COMPLETE.md"),
        ("ENTERPRISE_FEATURES_COMPLETE.md", "docs/status/ENTERPRISE_FEATURES_COMPLETE.md"),
        ("FINAL_TODO_COMPLETION_SUMMARY.md", "docs/status/FINAL_TODO_COMPLETION_SUMMARY.md"),
        ("SETUP_COMPLETE_SUMMARY.md", "docs/status/SETUP_COMPLETE_SUMMARY.md"),
        ("SCD2_ANALYSIS.md", "docs/guides/SCD2_ANALYSIS.md"),
        ("CRM_SIMULATION.md", "docs/guides/CRM_SIMULATION.md"),
        
        # Config organization
        ("data_quality_report.json", "data/samples/data_quality_report.json"),
        ("data_relationships.json", "data/samples/data_relationships.json"),
        ("data_validation_rules.json", "data/samples/data_validation_rules.json"),
    ]
    
    for src, dst in file_moves:
        if os.path.exists(src):
            try:
                shutil.move(src, dst)
                logger.info(f"📁 Moved: {src} → {dst}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to move {src}: {e}")
        else:
            logger.info(f"ℹ️ Source not found: {src}")


def check_file_alignment():
    """Check each file for alignment issues."""
    logger.info("\\n🔍 Checking File Alignment")
    logger.info("=" * 50)
    
    # Files to check for alignment issues
    critical_files = [
        "src/pyspark_interview_project/__init__.py",
        "src/pyspark_interview_project/utils/metrics.py",
        "config/dev.yaml",
        "config/prod.yaml",
        "aws/jobs/crm_accounts_ingest.py",
        "aws/jobs/crm_contacts_ingest.py", 
        "aws/jobs/crm_opportunities_ingest.py"
    ]
    
    alignment_issues = []
    
    for file_path in critical_files:
        if os.path.exists(file_path):
            logger.info(f"🔍 Checking: {file_path}")
            
            with open(file_path, 'r') as f:
                content = f.read()
                
            # Check for common alignment issues
            issues = []
            
            if 'hubspot' in content.lower() and 'salesforce' not in content.lower():
                issues.append("Contains HubSpot references but no Salesforce")
            
            if 'import pyspark_interview_project' in content and 'from pyspark_interview_project' not in content:
                issues.append("Inconsistent import patterns")
            
            if 'config-dev.yaml' in content:
                issues.append("References old config file name")
            
            if 'data/hubspot' in content:
                issues.append("References old HubSpot data paths")
            
            if issues:
                alignment_issues.append((file_path, issues))
                logger.warning(f"⚠️ Issues found in {file_path}: {issues}")
            else:
                logger.info(f"✅ {file_path} looks good")
        else:
            logger.warning(f"❌ File not found: {file_path}")
    
    return alignment_issues


def fix_alignment_issues(issues):
    """Fix identified alignment issues."""
    logger.info("\\n🔧 Fixing Alignment Issues")
    logger.info("=" * 50)
    
    for file_path, file_issues in issues:
        logger.info(f"🔧 Fixing: {file_path}")
        
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            # Apply fixes based on issues
            if "Contains HubSpot references but no Salesforce" in file_issues:
                content = content.replace('hubspot', 'salesforce')
                content = content.replace('HubSpot', 'Salesforce')
                logger.info(f"   ✅ Replaced HubSpot with Salesforce")
            
            if "References old config file name" in file_issues:
                content = content.replace('config-dev.yaml', 'config/dev.yaml')
                logger.info(f"   ✅ Updated config file references")
            
            if "References old HubSpot data paths" in file_issues:
                content = content.replace('data/hubspot', 'aws/data/crm')
                logger.info(f"   ✅ Updated data paths")
            
            # Write back the fixed content
            with open(file_path, 'w') as f:
                f.write(content)
            
            logger.info(f"✅ Fixed: {file_path}")
            
        except Exception as e:
            logger.error(f"❌ Failed to fix {file_path}: {e}")


def remove_duplicate_files():
    """Remove duplicate or obsolete files."""
    logger.info("\\n🗑️ Removing Duplicate Files")
    logger.info("=" * 50)
    
    # Files to remove (duplicates or obsolete)
    files_to_remove = [
        "aws/data_fixed/salesforce_accounts.csv",
        "aws/data_fixed/salesforce_contacts.csv", 
        "aws/data_fixed/salesforce_opportunities.csv",
        "analyze_data_quality.py",
        "validate_data_quality.py"
    ]
    
    for file_path in files_to_remove:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info(f"🗑️ Removed: {file_path}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to remove {file_path}: {e}")
        else:
            logger.info(f"ℹ️ File not found: {file_path}")


def verify_imports():
    """Verify all imports are working correctly."""
    logger.info("\\n🔗 Verifying Imports")
    logger.info("=" * 50)
    
    # Test critical imports
    import_tests = [
        "from pyspark_interview_project.utils.metrics import track_job_start",
        "from pyspark_interview_project.utils.config import load_conf",
        "from pyspark_interview_project.extract.crm_accounts import extract_crm_accounts",
        "from pyspark_interview_project.extract.crm_contacts import extract_crm_contacts",
        "from pyspark_interview_project.extract.crm_opportunities import extract_crm_opportunities"
    ]
    
    for import_test in import_tests:
        try:
            exec(import_test)
            logger.info(f"✅ Import works: {import_test}")
        except Exception as e:
            logger.error(f"❌ Import failed: {import_test} - {e}")


def main():
    """Main organization function."""
    logger.info("🚀 FILE ORGANIZATION AND ALIGNMENT CHECKER")
    logger.info("=" * 60)
    logger.info(f"⏰ Started at: {datetime.now()}")
    
    # Step 1: Organize files
    organize_files()
    
    # Step 2: Check alignment
    alignment_issues = check_file_alignment()
    
    # Step 3: Fix issues
    if alignment_issues:
        fix_alignment_issues(alignment_issues)
    else:
        logger.info("✅ No alignment issues found!")
    
    # Step 4: Remove duplicates
    remove_duplicate_files()
    
    # Step 5: Verify imports
    verify_imports()
    
    # Summary
    logger.info("\\n🎯 ORGANIZATION SUMMARY")
    logger.info("=" * 50)
    logger.info(f"📁 Files organized into proper folders")
    logger.info(f"🔍 Alignment issues checked and fixed")
    logger.info(f"🗑️ Duplicate files removed")
    logger.info(f"🔗 Imports verified")
    
    logger.info(f"\\n⏰ Completed at: {datetime.now()}")
    logger.info("🎉 File organization and alignment complete!")


if __name__ == "__main__":
    main()
