import logging

from core.logger import get_logger


def test_logger_created():
    logger = get_logger(__name__)

    assert logger is not None
    assert isinstance(logger, logging.Logger)


def test_logger_level():
    logger = get_logger(__name__)

    assert logger.level == logging.INFO


def test_logger_has_handlers():
    logger = get_logger(__name__)

    assert len(logger.handlers) > 0
