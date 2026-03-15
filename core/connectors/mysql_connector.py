"""MySQL database connector using pymysql + SQLAlchemy."""
from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from models.database_config import DatabaseConfig
from core.connectors.base_connector import BaseConnector
from utils.logger import get_logger

logger = get_logger(__name__)


class MySQLConnector(BaseConnector):
    """Connects to a MySQL / MariaDB instance."""

    def __init__(self, config: DatabaseConfig) -> None:
        self.config = config
        self._engine: Engine | None = None

    # ── Connection ────────────────────────────────────────────────────

    def connect(self) -> Engine:
        if self._engine is None:
            url = (
                f"mysql+pymysql://{self.config.username}:{self.config.password}"
                f"@{self.config.host}:{self.config.port}"
                f"/{self.config.database or ''}"
                f"?charset=utf8mb4"
            )
            self._engine = create_engine(url, pool_pre_ping=True)
            logger.debug(f"MySQL engine created for {self.config.host}:{self.config.port}")
        return self._engine

    # ── Discovery ─────────────────────────────────────────────────────

    def list_databases(self) -> list[str]:
        engine = self.connect()
        with engine.connect() as conn:
            rows = conn.execute(text("SHOW DATABASES")).fetchall()
        system_dbs = {"information_schema", "performance_schema", "mysql", "sys"}
        return [r[0] for r in rows if r[0] not in system_dbs]

    def get_table_names(self, database: str) -> list[str]:
        engine = self.connect()
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT TABLE_NAME FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = :db AND TABLE_TYPE = 'BASE TABLE'"
                ),
                {"db": database},
            ).fetchall()
        return [r[0] for r in rows]

    # ── Schema ────────────────────────────────────────────────────────

    def get_table_schema(self, database: str, table: str) -> dict:
        engine = self.connect()
        with engine.connect() as conn:
            # Columns
            col_rows = conn.execute(
                text(
                    "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, "
                    "COLUMN_DEFAULT, EXTRA "
                    "FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA = :db AND TABLE_NAME = :tbl "
                    "ORDER BY ORDINAL_POSITION"
                ),
                {"db": database, "tbl": table},
            ).fetchall()

            columns = []
            primary_keys = []
            for row in col_rows:
                name, col_type, nullable, key, default, extra = row
                is_pk = key == "PRI"
                if is_pk:
                    primary_keys.append(name)
                columns.append(
                    {
                        "name": name,
                        "type": col_type,
                        "nullable": nullable == "YES",
                        "primary_key": is_pk,
                        "default": default,
                        "extra": extra,
                    }
                )

            # Foreign keys
            fk_rows = conn.execute(
                text(
                    "SELECT kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_NAME, "
                    "kcu.REFERENCED_COLUMN_NAME "
                    "FROM information_schema.KEY_COLUMN_USAGE kcu "
                    "JOIN information_schema.REFERENTIAL_CONSTRAINTS rc "
                    "  ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME "
                    "  AND kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA "
                    "WHERE kcu.TABLE_SCHEMA = :db AND kcu.TABLE_NAME = :tbl "
                    "  AND kcu.REFERENCED_TABLE_NAME IS NOT NULL"
                ),
                {"db": database, "tbl": table},
            ).fetchall()

            foreign_keys = [
                {"column": r[0], "ref_table": r[1], "ref_column": r[2]}
                for r in fk_rows
            ]

            # Indexes
            idx_rows = conn.execute(
                text(
                    "SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE "
                    "FROM information_schema.STATISTICS "
                    "WHERE TABLE_SCHEMA = :db AND TABLE_NAME = :tbl "
                    "ORDER BY INDEX_NAME, SEQ_IN_INDEX"
                ),
                {"db": database, "tbl": table},
            ).fetchall()

            idx_map: dict[str, dict] = {}
            for idx_name, col_name, non_unique in idx_rows:
                if idx_name == "PRIMARY":
                    continue
                if idx_name not in idx_map:
                    idx_map[idx_name] = {"name": idx_name, "columns": [], "unique": not non_unique}
                idx_map[idx_name]["columns"].append(col_name)

        return {
            "columns": columns,
            "primary_keys": primary_keys,
            "foreign_keys": foreign_keys,
            "indexes": list(idx_map.values()),
        }

    # ── Stats ─────────────────────────────────────────────────────────

    def get_row_count(self, table: str) -> int:
        engine = self.connect()
        database = self.config.database or ""
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT TABLE_ROWS FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = :db AND TABLE_NAME = :tbl"
                ),
                {"db": database, "tbl": table},
            ).fetchone()
        # information_schema.TABLE_ROWS is an estimate; fall back to COUNT(*)
        if row and row[0] is not None:
            return int(row[0])
        with engine.connect() as conn:
            row = conn.execute(text(f"SELECT COUNT(*) FROM `{database}`.`{table}`")).fetchone()
        return int(row[0]) if row else 0
