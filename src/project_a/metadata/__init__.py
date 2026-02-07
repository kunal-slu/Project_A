"""Metadata Management Module"""
from .catalog import (
    MetadataCatalog,
    MetadataExtractor,
    DatasetMetadata,
    get_catalog,
    register_dataset,
    get_dataset_metadata,
    search_datasets
)

__all__ = [
    'MetadataCatalog',
    'MetadataExtractor',
    'DatasetMetadata',
    'get_catalog',
    'register_dataset',
    'get_dataset_metadata',
    'search_datasets'
]
