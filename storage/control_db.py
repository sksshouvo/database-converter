"""SQLite control database initialisation and session management."""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from models.migration_job import Base
from utils.config import config
from utils.logger import get_logger

logger = get_logger(__name__)

_engine: Engine | None = None
_SessionFactory: sessionmaker | None = None


def _get_engine() -> Engine:
    """Return the singleton SQLAlchemy engine, creating it if necessary."""
    global _engine
    if _engine is None:
        db_url = f"sqlite:///{config.db_path}"
        _engine = create_engine(
            db_url,
            connect_args={"check_same_thread": False},
            echo=False,
        )
        # Enable WAL mode for better concurrency
        @event.listens_for(_engine, "connect")
        def _set_wal(dbapi_conn, _connection_record):  # type: ignore[override]
            dbapi_conn.execute("PRAGMA journal_mode=WAL")
            dbapi_conn.execute("PRAGMA foreign_keys=ON")

        logger.info(f"Control DB engine created at [bold]{config.db_path}[/bold]")
    return _engine


def init_db() -> None:
    """Create all tables in the control DB if they don't exist yet."""
    engine = _get_engine()
    Base.metadata.create_all(engine)
    logger.info("Control DB schema initialised.")


def get_session_factory() -> sessionmaker:
    """Return the singleton session factory."""
    global _SessionFactory
    if _SessionFactory is None:
        _SessionFactory = sessionmaker(bind=_get_engine(), expire_on_commit=False)
    return _SessionFactory


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Context manager that yields a :class:`Session` and auto-commits / rolls back."""
    factory = get_session_factory()
    session: Session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
