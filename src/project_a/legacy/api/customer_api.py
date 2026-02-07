"""
FastAPI service to expose customer_360 data.

Provides REST API endpoints for analytics teams to query Gold layer data.
"""

import os
import sys
import logging
from pathlib import Path
from typing import Optional, List
from datetime import datetime

# Add project root
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

try:
    from fastapi import FastAPI, HTTPException, Query
    from fastapi.responses import JSONResponse
    from pydantic import BaseModel
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False
    FastAPI = None

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)

# Initialize FastAPI app
if FASTAPI_AVAILABLE:
    app = FastAPI(
        title="Customer 360 API",
        description="REST API for querying customer analytics data",
        version="1.0.0"
    )
else:
    app = None


class CustomerResponse(BaseModel):
    """Customer 360 response model."""
    customer_id: str
    customer_name: str
    total_orders: int
    total_revenue: float
    last_order_date: Optional[str]
    customer_segment: Optional[str]


if FASTAPI_AVAILABLE and app:
    @app.get("/health")
    def health_check():
        """Health check endpoint."""
        return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}
    
    @app.get("/customer/{customer_id}", response_model=CustomerResponse)
    def get_customer(customer_id: str):
        """
        Get customer 360 data by customer ID.
        
        Args:
            customer_id: Customer ID
            
        Returns:
            Customer 360 record
        """
        try:
            # Initialize Spark (lazy initialization)
            spark = _get_spark_session()
            
            # Load customer_360 table
            config = _load_config()
            gold_path = config.get("data_lake", {}).get("gold_path", "data/lakehouse_delta/gold")
            customer_df = spark.read.format("delta").load(f"{gold_path}/customer_360")
            
            # Filter by customer_id
            customer = customer_df.filter(col("customer_id") == customer_id).first()
            
            if customer is None:
                raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found")
            
            return CustomerResponse(
                customer_id=customer["customer_id"],
                customer_name=customer.get("customer_name", ""),
                total_orders=customer.get("total_orders", 0),
                total_revenue=customer.get("total_revenue", 0.0),
                last_order_date=customer.get("last_order_date"),
                customer_segment=customer.get("customer_segment")
            )
            
        except Exception as e:
            logger.error(f"Error fetching customer {customer_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/customers", response_model=List[CustomerResponse])
    def list_customers(
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        segment: Optional[str] = None
    ):
        """
        List customers with pagination and filtering.
        
        Args:
            limit: Maximum number of records
            offset: Offset for pagination
            segment: Filter by customer segment
            
        Returns:
            List of customer records
        """
        try:
            spark = _get_spark_session()
            config = _load_config()
            gold_path = config.get("data_lake", {}).get("gold_path", "data/lakehouse_delta/gold")
            customer_df = spark.read.format("delta").load(f"{gold_path}/customer_360")
            
            # Apply filters
            if segment:
                customer_df = customer_df.filter(col("customer_segment") == segment)
            
            # Pagination
            customers = customer_df.limit(limit).offset(offset).collect()
            
            return [
                CustomerResponse(
                    customer_id=c["customer_id"],
                    customer_name=c.get("customer_name", ""),
                    total_orders=c.get("total_orders", 0),
                    total_revenue=c.get("total_revenue", 0.0),
                    last_order_date=c.get("last_order_date"),
                    customer_segment=c.get("customer_segment")
                )
                for c in customers
            ]
            
        except Exception as e:
            logger.error(f"Error listing customers: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/stats")
    def get_stats():
        """Get aggregated statistics."""
        try:
            spark = _get_spark_session()
            config = _load_config()
            gold_path = config.get("data_lake", {}).get("gold_path", "data/lakehouse_delta/gold")
            customer_df = spark.read.format("delta").load(f"{gold_path}/customer_360")
            
            total_customers = customer_df.count()
            total_revenue = customer_df.agg({"total_revenue": "sum"}).collect()[0][0] or 0
            total_orders = customer_df.agg({"total_orders": "sum"}).collect()[0][0] or 0
            
            return {
                "total_customers": total_customers,
                "total_revenue": float(total_revenue),
                "total_orders": total_orders,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error fetching stats: {e}")
            raise HTTPException(status_code=500, detail=str(e))


# Global Spark session (lazy initialization)
_spark_session = None


def _get_spark_session() -> SparkSession:
    """Get or create Spark session."""
    global _spark_session
    
    if _spark_session is None:
        from project_a.utils.spark_session import build_spark
        from project_a.utils.config import load_conf
        
        config = load_conf()
        _spark_session = build_spark(app_name="customer_api", config=config)
    
    return _spark_session


def _load_config():
    """Load configuration."""
    from project_a.utils.config import load_conf
    return load_conf()


if __name__ == "__main__":
    import uvicorn
    
    if not FASTAPI_AVAILABLE:
        logger.error("FastAPI not installed. Install with: pip install fastapi uvicorn")
        sys.exit(1)
    
    # Run API server
    port = int(os.getenv("API_PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)

