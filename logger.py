import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

def _configure_logger() -> logging.Logger:
    logger = logging.getLogger("whisper")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    log_path = Path(os.getenv("WHISPER_LOG_FILE", "/logs/whisper.log"))
    log_path.parent.mkdir(parents=True, exist_ok=True)

    max_bytes = int(os.getenv("WHISPER_LOG_MAX_BYTES", 10 * 1024 * 1024))
    backup_count = int(os.getenv("WHISPER_LOG_BACKUP_COUNT", 9))

    formatter = logging.Formatter("[%(asctime)s] %(message)s", "%Y-%m-%d %H:%M:%S")

    rotating_handler = RotatingFileHandler(
        log_path,
        maxBytes=max_bytes,
        backupCount=backup_count,
    )
    rotating_handler.setFormatter(formatter)
    logger.addHandler(rotating_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logger.propagate = False
    return logger


LOGGER = _configure_logger()


def log(msg: str) -> None:
    LOGGER.info(msg)
