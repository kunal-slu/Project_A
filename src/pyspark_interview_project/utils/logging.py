"""
Structured logging utilities with trace ID support.
"""

import json
import logging
import uuid
from typing import Any, Dict, Optional
from datetime import datetime


def setup_json_logging(level: str = "INFO", include_trace_id: bool = True) -> logging.Logger:
    """
    Setup structured JSON logging.
    
    Args:
        level: Logging level
        include_trace_id: Whether to include trace ID in logs
        
    Returns:
        Configured logger
    """
    class JSONFormatter(logging.Formatter):
        def format(self, record):
            log_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }
            
            if include_trace_id and hasattr(record, 'trace_id'):
                log_entry["trace_id"] = record.trace_id
            
            if record.exc_info:
                log_entry["exception"] = self.formatException(record.exc_info)
            
            # Add any extra fields
            for key, value in record.__dict__.items():
                if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 
                              'filename', 'module', 'lineno', 'funcName', 'created', 
                              'msecs', 'relativeCreated', 'thread', 'threadName', 
                              'processName', 'process', 'getMessage', 'exc_info', 
                              'exc_text', 'stack_info', 'trace_id']:
                    log_entry[key] = value
            
            return json.dumps(log_entry)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add console handler with JSON formatter
    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    root_logger.addHandler(handler)
    
    return root_logger


def get_trace_id() -> str:
    """Generate a unique trace ID."""
    return str(uuid.uuid4())


def log_with_trace(logger: logging.Logger, level: str, message: str, trace_id: str, **kwargs) -> None:
    """
    Log message with trace ID.
    
    Args:
        logger: Logger instance
        level: Log level
        message: Log message
        trace_id: Trace ID
        **kwargs: Additional fields
    """
    extra = {"trace_id": trace_id, **kwargs}
    getattr(logger, level.lower())(message, extra=extra)
