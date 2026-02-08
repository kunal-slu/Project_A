"""Minimal Unity Catalog compatibility stubs."""


class UnityCatalogManager:
    def __init__(self, spark, catalog_name="main"):
        self.spark = spark
        self.catalog_name = catalog_name

    def setup_unity_catalog_governance(self):
        return None

    def list_catalogs(self):
        return [self.catalog_name]

