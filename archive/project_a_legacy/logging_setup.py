import json
import logging
import os
import sys
import time
import uuid
from logging import LogRecord


class JsonFormatter(logging.Formatter):
    def format(self, record: LogRecord) -> str:
        base = {
            "ts": int(time.time() * 1000),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "file": record.pathname,
            "line": record.lineno,
            "func": record.funcName,
            "corr_id": getattr(record, "corr_id", None) or os.getenv("CORRELATION_ID"),
            "run_id": os.getenv("RUN_ID"),
            "env": os.getenv("APP_ENV", "dev"),
        }
        if record.exc_info:
            base["exc"] = self.formatException(record.exc_info)
        return json.dumps(base, ensure_ascii=False)


def get_logger(name: str = "app", level: str | int = None) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(level or os.getenv("LOG_LEVEL", "INFO"))
    return logger


def new_correlation_id() -> str:
    cid = str(uuid.uuid4())
    os.environ["CORRELATION_ID"] = cid
    return cid
