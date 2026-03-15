"""Application configuration loaded from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class AppConfig:
    """Central configuration object; all values resolved from env vars at import time."""

    batch_size: int = field(
        default_factory=lambda: int(os.getenv("BATCH_SIZE", "1000"))
    )
    max_workers: int = field(
        default_factory=lambda: int(os.getenv("MAX_WORKERS", "4"))
    )
    log_level: str = field(
        default_factory=lambda: os.getenv("LOG_LEVEL", "INFO").upper()
    )
    db_path: Path = field(
        default_factory=lambda: Path(
            os.getenv("DB_PATH", "storage/control_db.sqlite")
        )
    )

    def __post_init__(self) -> None:
        # Coerce string → Path when loaded from env
        if not isinstance(self.db_path, Path):
            self.db_path = Path(self.db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)


# Module-level singleton
config = AppConfig()
