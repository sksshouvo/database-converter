"""Abstract base class for all database connectors."""
from __future__ import annotations

from abc import ABC, abstractmethod

from sqlalchemy.engine import Engine


class BaseConnector(ABC):
    """Common interface for every database engine connector.

    Subclasses implement engine-specific connection URLs and metadata queries
    while consumers always interact with this consistent API.
    """

    @abstractmethod
    def connect(self) -> Engine:
        """Create and return a SQLAlchemy :class:`Engine` for this database."""
        ...

    @abstractmethod
    def list_databases(self) -> list[str]:
        """Return the names of all user databases visible to the configured user."""
        ...

    @abstractmethod
    def get_table_names(self, database: str) -> list[str]:
        """Return all table names in *database*."""
        ...

    @abstractmethod
    def get_table_schema(self, database: str, table: str) -> dict:
        """Return column metadata for *table* in *database*.

        Returns a dict with keys:
            columns  – list of {name, type, nullable, primary_key, default}
            primary_keys – list[str]
            foreign_keys – list of {column, ref_table, ref_column}
            indexes – list of {name, columns, unique}
        """
        ...

    @abstractmethod
    def get_row_count(self, table: str) -> int:
        """Return the approximate row count for *table* in the configured database."""
        ...

    def test_connection(self) -> tuple[bool, str]:
        """Attempt a connection and return ``(True, '')`` or ``(False, error_msg)``."""
        try:
            engine = self.connect()
            with engine.connect():
                pass
            return True, ""
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)
