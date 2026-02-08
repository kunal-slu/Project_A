"""Metadata Management Module"""

from .catalog import (
    DatasetMetadata,
    MetadataCatalog,
    MetadataExtractor,
    get_catalog,
    get_dataset_metadata,
    register_dataset,
    search_datasets,
)

__all__ = [
    "MetadataCatalog",
    "MetadataExtractor",
    "DatasetMetadata",
    "get_catalog",
    "register_dataset",
    "get_dataset_metadata",
    "search_datasets",
]
