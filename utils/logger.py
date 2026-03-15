"""Structured logging setup with Rich console + rotating file handlers."""
from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path

from rich.logging import RichHandler

_LOG_DIR = Path("logs")
_LOG_DIR.mkdir(exist_ok=True)

_FMT = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"

_configured: set[str] = set()


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Return a named logger. Configures handlers the first time it is called."""
    logger = logging.getLogger(name)
    if name in _configured:
        return logger

    logger.setLevel(level)
    logger.propagate = False

    # ── Rich console handler ──────────────────────────────────────────
    console_handler = RichHandler(
        rich_tracebacks=True,
        markup=True,
        show_path=False,
    )
    console_handler.setLevel(level)
    logger.addHandler(console_handler)

    # ── INFO+ rotating file ───────────────────────────────────────────
    file_handler = logging.handlers.RotatingFileHandler(
        _LOG_DIR / "migration.log",
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(_FMT, datefmt=_DATE_FMT))
    logger.addHandler(file_handler)

    # ── ERROR+ error log ─────────────────────────────────────────────
    error_handler = logging.handlers.RotatingFileHandler(
        _LOG_DIR / "errors.log",
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter(_FMT, datefmt=_DATE_FMT))
    logger.addHandler(error_handler)

    _configured.add(name)
    return logger
