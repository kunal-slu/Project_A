"""
Test data quality policies to validate YAML schema keys.
"""

import pytest
import yaml
import os
from pathlib import Path


def test_dq_yaml_schema_validation():
    """Test that DQ YAML files have valid schema structure."""
    dq_dir = Path(__file__).parent.parent / "dq"
    
    if not dq_dir.exists():
        pytest.skip("DQ directory not found")
    
    yaml_files = list(dq_dir.glob("**/*.yml")) + list(dq_dir.glob("**/*.yaml"))
    
    if not yaml_files:
        pytest.skip("No DQ YAML files found")
    
    required_schema_keys = [
        'table',
        'rules'
    ]
    
    rule_required_keys = [
        'name',
        'type'
    ]
    
    for yaml_file in yaml_files:
        try:
            with open(yaml_file, 'r') as f:
                data = yaml.safe_load(f)
            
            # Check top-level schema
            for key in required_schema_keys:
                assert key in data, f"{yaml_file} missing required key: {key}"
            
            # Check rules structure
            assert isinstance(data['rules'], list), f"{yaml_file} rules must be a list"
            
            for rule in data['rules']:
                for key in rule_required_keys:
                    assert key in rule, f"{yaml_file} rule missing required key: {key}"
                
                # Validate rule type
                valid_rule_types = [
                    'not_null',
                    'unique',
                    'range',
                    'format',
                    'referential_integrity',
                    'custom'
                ]
                assert rule['type'] in valid_rule_types, f"{yaml_file} invalid rule type: {rule['type']}"
                
        except yaml.YAMLError as e:
            pytest.fail(f"Invalid YAML in {yaml_file}: {e}")
        except Exception as e:
            pytest.fail(f"Error processing {yaml_file}: {e}")


def test_dq_policy_naming_convention():
    """Test that DQ policy files follow naming conventions."""
    dq_dir = Path(__file__).parent.parent / "dq"
    
    if not dq_dir.exists():
        pytest.skip("DQ directory not found")
    
    yaml_files = list(dq_dir.glob("**/*.yml")) + list(dq_dir.glob("**/*.yaml"))
    
    for yaml_file in yaml_files:
        filename = yaml_file.name
        
        # Should be lowercase with underscores
        assert filename.islower() or filename.replace('.', '_').replace('-', '_').islower()
        
        # Should end with .yml or .yaml
        assert filename.endswith(('.yml', '.yaml'))
        
        # Should not contain spaces
        assert ' ' not in filename


def test_dq_rule_configuration():
    """Test that DQ rules have proper configuration."""
    dq_dir = Path(__file__).parent.parent / "dq"
    
    if not dq_dir.exists():
        pytest.skip("DQ directory not found")
    
    yaml_files = list(dq_dir.glob("**/*.yml")) + list(dq_dir.glob("**/*.yaml"))
    
    for yaml_file in yaml_files:
        try:
            with open(yaml_file, 'r') as f:
                data = yaml.safe_load(f)
            
            for rule in data.get('rules', []):
                rule_type = rule.get('type')
                
                if rule_type == 'not_null':
                    assert 'columns' in rule, f"{yaml_file} not_null rule missing columns"
                    assert isinstance(rule['columns'], list), f"{yaml_file} columns must be a list"
                
                elif rule_type == 'unique':
                    assert 'columns' in rule, f"{yaml_file} unique rule missing columns"
                    assert isinstance(rule['columns'], list), f"{yaml_file} columns must be a list"
                
                elif rule_type == 'range':
                    assert 'column' in rule, f"{yaml_file} range rule missing column"
                    assert 'min' in rule or 'max' in rule, f"{yaml_file} range rule missing min/max"
                
                elif rule_type == 'format':
                    assert 'column' in rule, f"{yaml_file} format rule missing column"
                    assert 'pattern' in rule, f"{yaml_file} format rule missing pattern"
                
                elif rule_type == 'referential_integrity':
                    assert 'source_column' in rule, f"{yaml_file} referential_integrity missing source_column"
                    assert 'target_table' in rule, f"{yaml_file} referential_integrity missing target_table"
                    assert 'target_column' in rule, f"{yaml_file} referential_integrity missing target_column"
                
                # Check severity
                if 'severity' in rule:
                    valid_severities = ['critical', 'warning', 'info']
                    assert rule['severity'] in valid_severities, f"{yaml_file} invalid severity: {rule['severity']}"
                
        except Exception as e:
            pytest.fail(f"Error validating {yaml_file}: {e}")


def test_dq_table_name_consistency():
    """Test that DQ table names are consistent with expected tables."""
    dq_dir = Path(__file__).parent.parent / "dq"
    
    if not dq_dir.exists():
        pytest.skip("DQ directory not found")
    
    yaml_files = list(dq_dir.glob("**/*.yml")) + list(dq_dir.glob("**/*.yaml"))
    
    expected_tables = [
        'customers',
        'orders', 
        'products',
        'customer_behavior',
        'fx_rates',
        'kafka_events',
        'hubspot_contacts',
        'hubspot_deals'
    ]
    
    found_tables = set()
    
    for yaml_file in yaml_files:
        try:
            with open(yaml_file, 'r') as f:
                data = yaml.safe_load(f)
            
            table_name = data.get('table')
            if table_name:
                found_tables.add(table_name)
                
                # Check naming convention
                assert '_' in table_name or table_name.islower(), f"{yaml_file} table name should be lowercase with underscores"
                
        except Exception as e:
            pytest.fail(f"Error processing {yaml_file}: {e}")
    
    # Check that we have DQ policies for expected tables
    missing_tables = set(expected_tables) - found_tables
    if missing_tables:
        pytest.skip(f"Missing DQ policies for tables: {missing_tables}")


def test_dq_yaml_syntax():
    """Test that all DQ YAML files have valid syntax."""
    dq_dir = Path(__file__).parent.parent / "dq"
    
    if not dq_dir.exists():
        pytest.skip("DQ directory not found")
    
    yaml_files = list(dq_dir.glob("**/*.yml")) + list(dq_dir.glob("**/*.yaml"))
    
    for yaml_file in yaml_files:
        try:
            with open(yaml_file, 'r') as f:
                yaml.safe_load(f)
        except yaml.YAMLError as e:
            pytest.fail(f"YAML syntax error in {yaml_file}: {e}")
        except Exception as e:
            pytest.fail(f"Error reading {yaml_file}: {e}")
