import logging
from pathlib import Path


def get_project_root() -> Path:
    return Path(__file__).parent.parent


def setup_logger(tag):
    logger = logging.getLogger(tag)
    logger.setLevel(logging.DEBUG)

    handler: logging.StreamHandler = logging.StreamHandler()
    formatter: logging.Formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger