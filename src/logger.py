import logging
import sys

def get_logger(name="pipeline", level="INFO"):
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # avoid duplicate logs

    logger.setLevel(level.upper())

    handler = logging.StreamHandler(sys.stdout)
    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(handler)

    return logger