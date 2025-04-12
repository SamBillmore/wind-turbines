"""Custom logger."""

import logging
import sys


def get_logger(logger_name: str) -> logging.Logger:
    """
    Define custom logging.
    :param logger_name: Name of the Logger
    :return: Logger
    """
    # Configure logging
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
        level=logging.INFO,
    )

    # Create Custom Logger
    return logging.getLogger(logger_name)
