"""Connection validator."""
from __future__ import annotations

from models.database_config import DatabaseConfig
from core.connectors.connector_factory import ConnectorFactory
from utils.logger import get_logger

logger = get_logger(__name__)


def validate_connection(config: DatabaseConfig) -> tuple[bool, str]:
    """Attempt to connect to the database described by *config*.

    Returns:
        ``(True, '')`` on success or ``(False, error_message)`` on failure.
    """
    try:
        connector = ConnectorFactory.create(config)
        ok, error = connector.test_connection()
        if ok:
            logger.info(
                f"Connection validated: {config.engine}://{config.host}:{config.port}"
                f"/{config.database or '(no db)'}"
            )
        else:
            logger.warning(f"Connection failed for {config.host}: {error}")
        return ok, error
    except Exception as exc:  # noqa: BLE001
        logger.error(f"Unexpected error validating connection: {exc}")
        return False, str(exc)
