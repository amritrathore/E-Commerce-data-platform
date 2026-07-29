import logging
from pathlib import Path
from core.config_loader import ConfigLoader


def get_logger(name: str) -> logging.Logger:
    """
    Creates and returns a configured application logger.

    All modules write to the same log file.
    """

    config = ConfigLoader()
    log_config = config.get_logging_config()

    logger = logging.getLogger(name)

    log_level = getattr(
        logging,
        log_config.get("level", "INFO").upper()
    )

    logger.setLevel(log_level)

    # Prevent adding duplicate handlers
    if logger.handlers:
        return logger

    log_path = Path(log_config.get("path", "logs"))
    log_file = log_path / log_config.get("file", "etl.log")

    log_path.mkdir(
        parents=True, 
        exist_ok=True
    )

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # File Handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger