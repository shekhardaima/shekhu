"""
Logging configuration utilities for the data processing pipeline.
"""
import logging
import sys
from typing import Optional


def setup_logger(
    name: str,
    level: int = logging.INFO,
    format_string: Optional[str] = None,
    handler: Optional[logging.Handler] = None
) -> logging.Logger:
    """
    Set up a logger with consistent configuration.
    
    Args:
        name: Logger name (typically __name__)
        level: Logging level (default: INFO)
        format_string: Custom format string
        handler: Custom handler (default: StreamHandler to stdout)
    
    Returns:
        Configured logger instance
    """
    if format_string is None:
        format_string = "%(asctime)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s"
    
    if handler is None:
        handler = logging.StreamHandler(sys.stdout)
    
    # Configure the logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Remove existing handlers to avoid duplicates
    for existing_handler in logger.handlers[:]:
        logger.removeHandler(existing_handler)
    
    # Set up formatter and handler
    formatter = logging.Formatter(format_string)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger


def get_default_logger(name: str) -> logging.Logger:
    """
    Get a logger with default configuration.
    
    Args:
        name: Logger name (typically __name__)
    
    Returns:
        Logger with default configuration
    """
    return setup_logger(name)