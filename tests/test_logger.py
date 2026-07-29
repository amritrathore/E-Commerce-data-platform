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


def test_logger_no_duplicate_handlers():

    logger1 = get_logger("test_logger")
    logger2 = get_logger("test_logger")

    assert len(logger1.handlers) == 2
    assert len(logger2.handlers) == 2
