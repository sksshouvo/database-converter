"""PostgreSQL database connector using psycopg2 + SQLAlchemy."""
from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from models.database_config import DatabaseConfig
from core.connectors.base_connector import BaseConnector
from utils.logger import get_logger

logger = get_logger(__name__)


class PostgreSQLConnector(BaseConnector):
    """Connects to a PostgreSQL instance via psycopg2."""

    def __init__(self, config: DatabaseConfig) -> None:
        self.config = config
        self._engine: Engine | None = None

    # ── Connection ────────────────────────────────────────────────────

    def connect(self) -> Engine:
        if self._engine is None:
            db_part = self.config.database or "postgres"
            url = (
                f"postgresql+psycopg2://{self.config.username}:{self.config.password}"
                f"@{self.config.host}:{self.config.port}/{db_part}"
            )
            self._engine = create_engine(url, pool_pre_ping=True)
            logger.debug(f"PostgreSQL engine created for {self.config.host}:{self.config.port}")
        return self._engine

    # ── Discovery ─────────────────────────────────────────────────────

    def list_databases(self) -> list[str]:
        engine = self.connect()
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT datname FROM pg_database "
                    "WHERE datistemplate = false AND datname NOT IN ('postgres') "
                    "ORDER BY datname"
                )
            ).fetchall()
        return [r[0] for r in rows]

    def get_table_names(self, database: str) -> list[str]:
        engine = self.connect()
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
                    "ORDER BY table_name"
                )
            ).fetchall()
        return [r[0] for r in rows]

    # ── Schema ────────────────────────────────────────────────────────

    def get_table_schema(self, database: str, table: str) -> dict:
        engine = self.connect()
        with engine.connect() as conn:
            # Columns
            col_rows = conn.execute(
                text(
                    "SELECT c.column_name, c.data_type, c.character_maximum_length, "
                    "c.numeric_precision, c.numeric_scale, c.is_nullable, "
                    "c.column_default, "
                    "(c.column_default LIKE 'nextval(%') AS is_sequence "
                    "FROM information_schema.columns c "
                    "WHERE c.table_schema = 'public' AND c.table_name = :tbl "
                    "ORDER BY c.ordinal_position"
                ),
                {"tbl": table},
            ).fetchall()

            # Primary keys
            pk_rows = conn.execute(
                text(
                    "SELECT kcu.column_name "
                    "FROM information_schema.table_constraints tc "
                    "JOIN information_schema.key_column_usage kcu "
                    "  ON tc.constraint_name = kcu.constraint_name "
                    "  AND tc.table_schema = kcu.table_schema "
                    "WHERE tc.table_schema = 'public' "
                    "  AND tc.table_name = :tbl "
                    "  AND tc.constraint_type = 'PRIMARY KEY'"
                ),
                {"tbl": table},
            ).fetchall()

            primary_keys = [r[0] for r in pk_rows]

            columns = []
            for row in col_rows:
                name, data_type, max_len, precision, scale, nullable, default, is_seq = row
                # Build display type
                if data_type in ("character varying", "character"):
                    display_type = f"{data_type}({max_len})" if max_len else data_type
                elif data_type == "numeric" and precision:
                    display_type = f"numeric({precision},{scale or 0})"
                else:
                    display_type = data_type

                columns.append(
                    {
                        "name": name,
                        "type": display_type,
                        "nullable": nullable == "YES",
                        "primary_key": name in primary_keys,
                        "default": default,
                        "is_sequence": bool(is_seq),
                    }
                )

            # Foreign keys
            fk_rows = conn.execute(
                text(
                    "SELECT kcu.column_name, ccu.table_name AS ref_table, "
                    "ccu.column_name AS ref_column "
                    "FROM information_schema.table_constraints tc "
                    "JOIN information_schema.key_column_usage kcu "
                    "  ON tc.constraint_name = kcu.constraint_name "
                    "  AND tc.table_schema = kcu.table_schema "
                    "JOIN information_schema.referential_constraints rc "
                    "  ON tc.constraint_name = rc.constraint_name "
                    "  AND tc.table_schema = rc.constraint_schema "
                    "JOIN information_schema.constraint_column_usage ccu "
                    "  ON rc.unique_constraint_name = ccu.constraint_name "
                    "  AND rc.unique_constraint_schema = ccu.constraint_schema "
                    "WHERE tc.constraint_type = 'FOREIGN KEY' "
                    "  AND tc.table_schema = 'public' "
                    "  AND tc.table_name = :tbl"
                ),
                {"tbl": table},
            ).fetchall()

            foreign_keys = [
                {"column": r[0], "ref_table": r[1], "ref_column": r[2]}
                for r in fk_rows
            ]

            # Indexes
            idx_rows = conn.execute(
                text(
                    "SELECT i.relname AS idx_name, a.attname AS col_name, ix.indisunique "
                    "FROM pg_class t "
                    "JOIN pg_index ix ON t.oid = ix.indrelid "
                    "JOIN pg_class i ON i.oid = ix.indexrelid "
                    "JOIN pg_attribute a ON a.attrelid = t.oid "
                    "  AND a.attnum = ANY(ix.indkey) "
                    "WHERE t.relname = :tbl AND NOT ix.indisprimary "
                    "ORDER BY i.relname"
                ),
                {"tbl": table},
            ).fetchall()

            idx_map: dict[str, dict] = {}
            for idx_name, col_name, is_unique in idx_rows:
                if idx_name not in idx_map:
                    idx_map[idx_name] = {"name": idx_name, "columns": [], "unique": bool(is_unique)}
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
        with engine.connect() as conn:
            # pg_stat_user_tables gives a fast estimate
            row = conn.execute(
                text(
                    "SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = :tbl"
                ),
                {"tbl": table},
            ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0
