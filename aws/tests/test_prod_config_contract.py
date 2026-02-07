"""
Test that production configuration has all required fields.
"""
import pytest
import yaml
from pathlib import Path


def test_prod_config_exists():
    """Verify prod.yaml exists."""
    config_path = Path(__file__).parent.parent / "config" / "config-prod.yaml"
    assert config_path.exists(), "config-prod.yaml not found"


def test_prod_config_has_required_fields():
    """Test that prod.yaml has all required fields."""
    config_path = Path(__file__).parent.parent / "config" / "config-prod.yaml"
    
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    # Required top-level keys
    required_keys = [
        "spark",
        "lake",
        "data_sources",
    ]
    
    for key in required_keys:
        assert key in config, f"Missing required key: {key}"
    
    # Required lake configuration
    assert "lake" in config
    lake_config = config["lake"]
    
    lake_required = ["root", "checkpoint"]
    for key in lake_required:
        assert key in lake_config, f"Missing lake.{key}"
    
    # Required data sources
    assert "data_sources" in config
    assert len(config["data_sources"]) > 0, "No data sources configured"
    
    print("✅ Production config has all required fields")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

