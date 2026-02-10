"""
logger.py

This module provides a reusable logger setup for the ETL pipeline.

Goals:
- Log both to console (for local debugging) and to file (for auditing/history).
- Avoid duplicate handlers when the logger is imported multiple times.
- Use a consistent log format for easy troubleshooting.

In a real production system, this could be replaced with:
- JSON logs (structured logs)
- Log aggregation (Datadog, ELK, Splunk, CloudWatch, etc.)
"""

import logging
import os


def setup_logger(log_dir="logs", log_name="etl.log"):
    """
    Creates and configures the application logger.

    Parameters
    ----------
    log_dir : str
        Directory where log files will be stored (default: "logs").
    log_name : str
        Log file name (default: "etl.log").

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """

    # Ensure log directory exists.
    os.makedirs(log_dir, exist_ok=True)

    log_path = os.path.join(log_dir, log_name)

    # Create/retrieve logger.
    logger = logging.getLogger("sales_etl")

    # Global log level.
    logger.setLevel(logging.INFO)

    # Avoid duplicated logs:
    # In Jupyter or repeated imports, handlers can be added multiple times.
    # This check ensures handlers are added only once.
    if logger.handlers:
        return logger

    # Standard format:
    # timestamp | level | message
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    # -------------------------------------------------------------------------
    # File handler: persistent logs
    # -------------------------------------------------------------------------
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    # -------------------------------------------------------------------------
    # Console handler: visible logs during execution
    # -------------------------------------------------------------------------
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    # Register handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
