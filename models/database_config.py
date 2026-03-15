"""Database connection configuration dataclass."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class DatabaseConfig:
    """Holds all parameters needed to connect to a database engine."""

    engine: str          # mysql | mssql | postgresql
    host: str
    port: int
    username: str
    password: str
    database: str | None = None
    driver: str | None = None   # for ODBC, e.g. "ODBC Driver 17 for SQL Server"

    def mask(self) -> "DatabaseConfig":
        """Return a copy with the password masked (for logging)."""
        return DatabaseConfig(
            engine=self.engine,
            host=self.host,
            port=self.port,
            username=self.username,
            password="****",
            database=self.database,
            driver=self.driver,
        )

    def __repr__(self) -> str:
        return (
            f"DatabaseConfig(engine={self.engine!r}, host={self.host!r}, "
            f"port={self.port}, username={self.username!r}, "
            f"database={self.database!r})"
        )
