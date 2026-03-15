"""Factory for creating database connector instances."""
from __future__ import annotations

from models.database_config import DatabaseConfig
from core.connectors.base_connector import BaseConnector


class UnsupportedEngineError(ValueError):
    """Raised when an unsupported database engine is requested."""


class ConnectorFactory:
    """Creates the appropriate :class:`BaseConnector` for a given engine."""

    _REGISTRY: dict[str, type] = {}

    @classmethod
    def _get_registry(cls) -> dict[str, type]:
        if not cls._REGISTRY:
            from core.connectors.mysql_connector import MySQLConnector
            from core.connectors.mssql_connector import MSSQLConnector
            from core.connectors.postgresql_connector import PostgreSQLConnector

            cls._REGISTRY = {
                "mysql": MySQLConnector,
                "mssql": MSSQLConnector,
                "postgresql": PostgreSQLConnector,
                "postgres": PostgreSQLConnector,  # alias
            }
        return cls._REGISTRY

    @classmethod
    def create(cls, config: DatabaseConfig) -> BaseConnector:
        """Instantiate and return the connector for *config.engine*.

        Args:
            config: DatabaseConfig describing the target engine and credentials.

        Returns:
            A concrete :class:`BaseConnector` subclass instance.

        Raises:
            UnsupportedEngineError: If *config.engine* is not in the registry.
        """
        registry = cls._get_registry()
        engine_key = config.engine.lower()
        connector_cls = registry.get(engine_key)
        if connector_cls is None:
            supported = ", ".join(sorted(set(registry.keys())))
            raise UnsupportedEngineError(
                f"Engine '{config.engine}' is not supported. "
                f"Supported engines: {supported}"
            )
        return connector_cls(config)

    @classmethod
    def supported_engines(cls) -> list[str]:
        """Return the list of canonical engine names."""
        return ["mysql", "mssql", "postgresql"]
