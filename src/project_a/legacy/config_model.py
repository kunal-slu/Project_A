"""
Configuration validation using Pydantic models.
Prevents silent path/key mistakes in config/*.yaml files.
"""

from pydantic import BaseModel, Field
from typing import Optional, Dict, Any


class IO(BaseModel):
    """Input/Output configuration."""
    format: str = Field(default="csv", description="Default file format")
    compression: Optional[str] = Field(default=None, description="Compression type")


class PathSet(BaseModel):
    """Set of table paths for a data layer."""
    customers: str = Field(..., description="Customers table path")
    orders: str = Field(..., description="Orders table path")
    products: Optional[str] = Field(default=None, description="Products table path")


class Paths(BaseModel):
    """All data layer paths."""
    bronze: PathSet = Field(..., description="Bronze layer paths")
    silver: PathSet = Field(..., description="Silver layer paths")
    gold: Optional[PathSet] = Field(default=None, description="Gold layer paths")


class SparkConfig(BaseModel):
    """Spark configuration."""
    shuffle_partitions: int = Field(default=400, description="Number of shuffle partitions")
    enable_aqe: bool = Field(default=True, description="Enable Adaptive Query Execution")
    driver_memory: Optional[str] = Field(default=None, description="Driver memory")
    executor_memory: Optional[str] = Field(default=None, description="Executor memory")


class DataQuality(BaseModel):
    """Data quality configuration."""
    enabled: bool = Field(default=True, description="Enable data quality checks")
    null_threshold: float = Field(default=0.1, description="Maximum null percentage allowed")
    duplicate_threshold: float = Field(default=0.05, description="Maximum duplicate percentage allowed")


class AppConfig(BaseModel):
    """Main application configuration."""
    app_name: str = Field(default="project_a", description="Application name")
    cloud: str = Field(default="local", description="Cloud provider (local, aws, azure)")
    io: IO = Field(default_factory=IO, description="IO configuration")
    paths: Paths = Field(..., description="Data paths configuration")
    spark: SparkConfig = Field(default_factory=SparkConfig, description="Spark configuration")
    dq: DataQuality = Field(default_factory=DataQuality, description="Data quality configuration")
    
    # Allow additional fields for flexibility
    class Config:
        extra = "allow"
    
    def validate_paths(self) -> None:
        """Validate that all paths are properly configured."""
        if not self.paths.bronze.customers:
            raise ValueError("Bronze customers path is required")
        if not self.paths.bronze.orders:
            raise ValueError("Bronze orders path is required")
        if not self.paths.silver.customers:
            raise ValueError("Silver customers path is required")
        if not self.paths.silver.orders:
            raise ValueError("Silver orders path is required")
    
    def get_spark_config(self) -> Dict[str, Any]:
        """Get Spark configuration as dictionary."""
        return {
            "spark.sql.shuffle.partitions": self.spark.shuffle_partitions,
            "spark.sql.adaptive.enabled": str(self.spark.enable_aqe).lower(),
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.autoBroadcastJoinThreshold": 64 * 1024 * 1024,
        }
