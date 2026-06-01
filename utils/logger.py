# utils/logger.py
# =========================================================================
# SIMPLE LOGGER FOR SEEING LOGS AS PIPELINE FAILS U CAN GET LOGS IN TERMINAL
# =========================================================================

import logging
import sys


def get_logger(name: str):
    """
    Creates and returns a simple logger.

    Format:
    TIME | LEVEL | MESSAGE
    """

    logger = logging.getLogger(name)

    # Avoid duplicate logs if logger already exists
    if logger.hasHandlers():
        return logger

    # Set log level
    logger.setLevel(logging.INFO)

    # Create console handler (prints logs in terminal)
    handler = logging.StreamHandler(sys.stdout)

    # Simple readable format
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger