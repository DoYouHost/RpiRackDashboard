"""Centralized logging configuration for RpiRackDashboard.

Call setup_logging() once at program startup (entry points only).
Library modules should never call this — they use logging.getLogger(__name__).
"""

import logging
import os

_FORMAT = "%(asctime)s  %(levelname)-8s  %(name)-16s  %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"


def setup_logging(level: int = logging.INFO) -> None:
    """Configure root logger with console handler and standard format.

    The LOG_LEVEL environment variable overrides the passed level.
    Paho-MQTT internal debug noise is suppressed at WARNING.

    Args:
        level: Default logging level. Overridden by LOG_LEVEL env var.
    """
    env_level = getattr(logging, os.getenv("LOG_LEVEL", "").upper(), None)
    if isinstance(env_level, int):
        level = env_level

    logging.basicConfig(level=level, format=_FORMAT, datefmt=_DATEFMT)
    logging.getLogger("paho.mqtt").setLevel(logging.WARNING)
    logging.getLogger("ha_mqtt_discoverable").setLevel(logging.WARNING)
