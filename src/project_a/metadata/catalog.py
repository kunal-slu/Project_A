"""
Metadata Catalog for Project_A

Manages dataset schemas, business definitions, and metadata across the data lake.
"""

import hashlib
import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass
class DatasetMetadata:
    """Metadata for a single dataset"""

    name: str
    location: str
    format: str
    schema_hash: str
    columns: list[dict[str, str]]
    record_count: int
    created_at: datetime
    updated_at: datetime
    owner: str
    description: str
    tags: list[str]
    business_definition: str
    pii_fields: list[str]
    data_quality_score: float


class MetadataCatalog:
    """Centralized metadata catalog"""

    def __init__(self, catalog_path: str = "data/catalog"):
        self.catalog_path = Path(catalog_path)
        self.catalog_path.mkdir(parents=True, exist_ok=True)
        self.datasets_path = self.catalog_path / "datasets"
        self.datasets_path.mkdir(exist_ok=True)
        self.logger = logging.getLogger(__name__)

    def register_dataset(
        self,
        name: str,
        location: str,
        format: str,
        df_schema: list[dict[str, str]],  # List of {'name': str, 'type': str}
        record_count: int,
        owner: str,
        description: str,
        business_definition: str,
        tags: list[str] = None,
        pii_fields: list[str] = None,
    ) -> str:
        """Register a new dataset in the catalog"""
        # Calculate schema hash
        schema_str = json.dumps(df_schema, sort_keys=True)
        schema_hash = hashlib.sha256(schema_str.encode()).hexdigest()

        # Create metadata object
        metadata = DatasetMetadata(
            name=name,
            location=location,
            format=format,
            schema_hash=schema_hash,
            columns=df_schema,
            record_count=record_count,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            owner=owner,
            description=description,
            business_definition=business_definition,
            tags=tags or [],
            pii_fields=pii_fields or [],
            data_quality_score=100.0,  # Initial perfect score
        )

        # Save to catalog
        self._save_metadata(metadata)

        self.logger.info(f"Dataset registered: {name}")
        return metadata.name

    def _save_metadata(self, metadata: DatasetMetadata):
        """Save metadata to file"""
        filename = f"{metadata.name.replace('/', '_').replace('.', '_')}.json"
        filepath = self.datasets_path / filename

        with open(filepath, "w") as f:
            json.dump(asdict(metadata), f, default=str, indent=2)

    def get_dataset(self, name: str) -> DatasetMetadata | None:
        """Get metadata for a dataset"""
        filename = f"{name.replace('/', '_').replace('.', '_')}.json"
        filepath = self.datasets_path / filename

        if filepath.exists():
            with open(filepath) as f:
                data = json.load(f)
                # Convert string dates back to datetime
                data["created_at"] = datetime.fromisoformat(data["created_at"])
                data["updated_at"] = datetime.fromisoformat(data["updated_at"])
                return DatasetMetadata(**data)
        return None

    def update_dataset_stats(
        self, name: str, record_count: int = None, data_quality_score: float = None
    ):
        """Update dataset statistics"""
        metadata = self.get_dataset(name)
        if not metadata:
            raise ValueError(f"Dataset {name} not found in catalog")

        if record_count is not None:
            metadata.record_count = record_count
        if data_quality_score is not None:
            metadata.data_quality_score = data_quality_score
        metadata.updated_at = datetime.utcnow()

        self._save_metadata(metadata)

    def search_datasets(self, query: str = "", tags: list[str] = None) -> list[DatasetMetadata]:
        """Search datasets by name, description, or tags"""
        results = []

        for file_path in self.datasets_path.glob("*.json"):
            with open(file_path) as f:
                data = json.load(f)
                data["created_at"] = datetime.fromisoformat(data["created_at"])
                data["updated_at"] = datetime.fromisoformat(data["updated_at"])
                metadata = DatasetMetadata(**data)

                # Match query in name, description, or business definition
                matches_query = (
                    query.lower() in metadata.name.lower()
                    or query.lower() in metadata.description.lower()
                    or query.lower() in metadata.business_definition.lower()
                )

                # Match tags if specified
                matches_tags = True
                if tags:
                    matches_tags = any(tag in metadata.tags for tag in tags)

                if matches_query and matches_tags:
                    results.append(metadata)

        return results

    def get_schema_compatibility(
        self, dataset_name: str, proposed_schema: list[dict[str, str]]
    ) -> dict[str, Any]:
        """Check if a proposed schema is compatible with existing dataset"""
        existing_metadata = self.get_dataset(dataset_name)
        if not existing_metadata:
            return {"compatible": True, "changes": "New dataset"}

        # Calculate proposed schema hash
        proposed_schema_str = json.dumps(proposed_schema, sort_keys=True)
        proposed_hash = hashlib.sha256(proposed_schema_str.encode()).hexdigest()

        if proposed_hash == existing_metadata.schema_hash:
            return {"compatible": True, "changes": "No schema changes"}

        # Compare schemas to identify differences
        existing_cols = {col["name"]: col["type"] for col in existing_metadata.columns}
        proposed_cols = {col["name"]: col["type"] for col in proposed_schema}

        added_cols = set(proposed_cols.keys()) - set(existing_cols.keys())
        removed_cols = set(existing_cols.keys()) - set(proposed_cols.keys())
        changed_cols = {
            name
            for name in existing_cols.keys() & proposed_cols.keys()
            if existing_cols[name] != proposed_cols[name]
        }

        return {
            "compatible": len(removed_cols) == 0,  # Schema evolution: only additions allowed
            "changes": {
                "added": list(added_cols),
                "removed": list(removed_cols),
                "changed": list(changed_cols),
            },
        }


class MetadataExtractor:
    """Extracts metadata from DataFrames and files"""

    @staticmethod
    def extract_from_dataframe(df, name: str) -> list[dict[str, str]]:
        """Extract schema from a Spark or Pandas DataFrame"""
        columns = []
        try:
            # Try Spark DataFrame first
            for field in df.schema.fields:
                columns.append(
                    {"name": field.name, "type": str(field.dataType), "nullable": field.nullable}
                )
        except AttributeError:
            # Fallback to Pandas DataFrame
            for col_name in df.columns:
                columns.append(
                    {"name": col_name, "type": str(df[col_name].dtype), "nullable": True}
                )
        return columns

    @staticmethod
    def extract_from_csv(file_path: str) -> list[dict[str, str]]:
        """Extract schema from CSV file"""
        df = pd.read_csv(file_path, nrows=100)  # Read sample to infer types
        columns = []
        for col_name in df.columns:
            columns.append({"name": col_name, "type": str(df[col_name].dtype), "nullable": True})
        return columns


# Global catalog instance
_catalog = None


def get_catalog() -> MetadataCatalog:
    """Get the global metadata catalog instance"""
    global _catalog
    if _catalog is None:
        from ..config_loader import load_config_resolved

        config = load_config_resolved("local/config/local.yaml")
        catalog_path = config.get("paths", {}).get("catalog_root", "data/catalog")
        _catalog = MetadataCatalog(catalog_path)
    return _catalog


def register_dataset(
    name: str,
    location: str,
    format: str,
    df_schema: list[dict[str, str]],
    record_count: int,
    owner: str,
    description: str,
    business_definition: str,
    tags: list[str] = None,
    pii_fields: list[str] = None,
) -> str:
    """Register a dataset in the catalog"""
    catalog = get_catalog()
    return catalog.register_dataset(
        name,
        location,
        format,
        df_schema,
        record_count,
        owner,
        description,
        business_definition,
        tags,
        pii_fields,
    )


def get_dataset_metadata(name: str) -> DatasetMetadata | None:
    """Get metadata for a dataset"""
    catalog = get_catalog()
    return catalog.get_dataset(name)


def search_datasets(query: str = "", tags: list[str] = None) -> list[DatasetMetadata]:
    """Search datasets in the catalog"""
    catalog = get_catalog()
    return catalog.search_datasets(query, tags)
