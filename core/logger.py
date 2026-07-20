import logging
from pathlib import Path
from config_loader import ConfigLoader


def get_logger(name: str) -> logging.Logger:
    """
    Returns a configured logger
    """

    config = ConfigLoader("D:/Project/E-Commerce-data-platform/E-Commerce-data-platform/config/config.yaml")
    log_config = config.get_logging_config()

    log_path = Path(log_config["path"])
    log_file = log_path / log_config["file"]

    log_path.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)

    if logger.hasHandlers():
        return logger
    
    logger.setLevel(getattr(logging, log_config["level"].upper()))

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