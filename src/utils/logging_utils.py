"""
Structured logging setup using structlog.

Call configure_logging() once at application entry point.
Call get_logger(__name__) in any module to obtain a bound logger.

Without configure_logging(), structlog uses its default (unformatted)
output — acceptable for unit tests and library use.
"""
from __future__ import annotations

import logging
import sys

import structlog


def configure_logging(log_level: str = "INFO") -> None:
    """
    Configure structlog with a human-readable console renderer.

    Sets up the stdlib logging bridge so that structlog messages flow
    through the standard logging system, making it easy to redirect
    output to files or external sinks in later phases.

    Args:
        log_level: One of DEBUG, INFO, WARNING, ERROR. Case-insensitive.
    """
    level = getattr(logging, log_level.upper(), logging.INFO)

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
    )

    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """
    Return a structlog bound logger for the given module name.

    Example:
        logger = get_logger(__name__)
        logger.info("event happened", key="value")
    """
    return structlog.get_logger(name)
