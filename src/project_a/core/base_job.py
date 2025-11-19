"""
Base Job Abstract Class

All ETL jobs inherit from BaseJob to ensure consistent:
- Configuration loading
- SparkSession management
- Error handling
- Logging
- Metrics collection
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from project_a.core.config import ProjectConfig
from project_a.core.context import JobContext

logger = logging.getLogger(__name__)


class BaseJob(ABC):
    """
    Abstract base class for all ETL jobs.
    
    Provides:
    - Configuration management
    - SparkSession context
    - Standardized error handling
    - Logging setup
    """
    
    def __init__(self, config: ProjectConfig):
        """
        Initialize job.
        
        Args:
            config: ProjectConfig instance
        """
        self.config = config
        self.ctx: Optional[JobContext] = None
    
    @abstractmethod
    def run(self, ctx: JobContext) -> Dict[str, Any]:
        """
        Execute the job logic.
        
        Args:
            ctx: JobContext with SparkSession
        
        Returns:
            Dictionary with job results (e.g., row counts, paths)
        """
        pass
    
    def execute(self) -> Dict[str, Any]:
        """
        Execute the job with context management.
        
        Returns:
            Dictionary with job results
        """
        logger.info(f"Starting job: {self.__class__.__name__}")
        
        try:
            with JobContext(self.config, app_name=self.__class__.__name__) as ctx:
                self.ctx = ctx
                result = self.run(ctx)
                logger.info(f"Job completed successfully: {self.__class__.__name__}")
                return result
        except Exception as e:
            logger.error(f"Job failed: {self.__class__.__name__}", exc_info=True)
            raise
    
    @property
    def spark(self) -> SparkSession:
        """Get SparkSession from context."""
        if self.ctx is None or self.ctx.spark is None:
            raise RuntimeError("JobContext not initialized. Call execute() first.")
        return self.ctx.spark

