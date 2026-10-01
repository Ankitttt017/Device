"""
Industrial logging utility for QUAD Gateway.
Provides structured, timestamped console and file logging.
Guarantees no secret leakage and clear industrial event traceability.
"""
import logging
import os
import sys
from typing import Optional


def setup_logger(
    name: str = "quad_gateway",
    log_level: str = "INFO",
    log_file: Optional[str] = None
) -> logging.Logger:
    """
    Configures and returns a centralized logger for the edge gateway.
    """
    logger = logging.getLogger(name)
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(numeric_level)

    # Avoid duplicate handlers if setup is called multiple times
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(numeric_level)
    logger.addHandler(console_handler)
    logger.propagate = False

    # Optional file handler (Rotating to prevent filling up disk/SD cards)
    if log_file:
        from logging.handlers import RotatingFileHandler
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB per file
            backupCount=5,              # Keep 5 backups
            encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(numeric_level)
        logger.addHandler(file_handler)

    return logger
