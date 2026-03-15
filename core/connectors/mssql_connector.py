"""MSSQL (SQL Server) database connector using pyodbc + SQLAlchemy."""
from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from models.database_config import DatabaseConfig
from core.connectors.base_connector import BaseConnector
from utils.logger import get_logger

logger = get_logger(__name__)

_DEFAULT_DRIVER = "ODBC Driver 17 for SQL Server"


class MSSQLConnector(BaseConnector):
    """Connects to a Microsoft SQL Server instance via ODBC."""

    def __init__(self, config: DatabaseConfig) -> None:
        self.config = config
        self._engine: Engine | None = None

    # ── Connection ────────────────────────────────────────────────────

    def connect(self) -> Engine:
        if self._engine is None:
            driver = (self.config.driver or _DEFAULT_DRIVER).replace(" ", "+")
            db_part = f"/{self.config.database}" if self.config.database else ""
            url = (
                f"mssql+pyodbc://{self.config.username}:{self.config.password}"
                f"@{self.config.host}:{self.config.port}{db_part}"
                f"?driver={driver}&TrustServerCertificate=yes"
            )
            self._engine = create_engine(url, pool_pre_ping=True)
            logger.debug(f"MSSQL engine created for {self.config.host}:{self.config.port}")
        return self._engine

    # ── Discovery ─────────────────────────────────────────────────────

    def list_databases(self) -> list[str]:
        engine = self.connect()
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT name FROM sys.databases "
                    "WHERE name NOT IN ('master','tempdb','model','msdb') "
                    "ORDER BY name"
                )
            ).fetchall()
        return [r[0] for r in rows]

    def get_table_names(self, database: str) -> list[str]:
        engine = self.connect()
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    f"SELECT TABLE_NAME FROM [{database}].information_schema.TABLES "
                    "WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME"
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
                    f"SELECT c.name, tp.name AS type_name, c.max_length, c.precision, "
                    f"c.scale, c.is_nullable, c.is_identity, "
                    f"OBJECT_DEFINITION(c.default_object_id) AS col_default "
                    f"FROM [{database}].sys.columns c "
                    f"JOIN [{database}].sys.types tp ON c.user_type_id = tp.user_type_id "
                    f"JOIN [{database}].sys.tables t ON c.object_id = t.object_id "
                    f"WHERE t.name = :tbl ORDER BY c.column_id"
                ),
                {"tbl": table},
            ).fetchall()

            # Primary keys
            pk_rows = conn.execute(
                text(
                    f"SELECT c.name FROM [{database}].sys.indexes i "
                    f"JOIN [{database}].sys.index_columns ic ON i.object_id = ic.object_id "
                    f"  AND i.index_id = ic.index_id "
                    f"JOIN [{database}].sys.columns c ON ic.object_id = c.object_id "
                    f"  AND ic.column_id = c.column_id "
                    f"JOIN [{database}].sys.tables t ON i.object_id = t.object_id "
                    f"WHERE i.is_primary_key = 1 AND t.name = :tbl"
                ),
                {"tbl": table},
            ).fetchall()

            primary_keys = [r[0] for r in pk_rows]

            columns = []
            for row in col_rows:
                name, type_name, max_len, precision, scale, is_nullable, is_identity, default = row
                # Build a display type string
                if type_name in ("varchar", "nvarchar", "char", "nchar"):
                    max_l = "MAX" if max_len == -1 else str(max_len)
                    display_type = f"{type_name}({max_l})"
                elif type_name in ("decimal", "numeric"):
                    display_type = f"{type_name}({precision},{scale})"
                else:
                    display_type = type_name

                columns.append(
                    {
                        "name": name,
                        "type": display_type,
                        "nullable": bool(is_nullable),
                        "primary_key": name in primary_keys,
                        "default": default,
                        "identity": bool(is_identity),
                    }
                )

            # Foreign keys
            fk_rows = conn.execute(
                text(
                    f"SELECT col.name, ref_tab.name, ref_col.name "
                    f"FROM [{database}].sys.foreign_key_columns fkc "
                    f"JOIN [{database}].sys.tables tab ON fkc.parent_object_id = tab.object_id "
                    f"JOIN [{database}].sys.columns col ON fkc.parent_object_id = col.object_id "
                    f"  AND fkc.parent_column_id = col.column_id "
                    f"JOIN [{database}].sys.tables ref_tab ON fkc.referenced_object_id = ref_tab.object_id "
                    f"JOIN [{database}].sys.columns ref_col ON fkc.referenced_object_id = ref_col.object_id "
                    f"  AND fkc.referenced_column_id = ref_col.column_id "
                    f"WHERE tab.name = :tbl"
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
                    f"SELECT i.name, c.name, i.is_unique "
                    f"FROM [{database}].sys.indexes i "
                    f"JOIN [{database}].sys.index_columns ic ON i.object_id = ic.object_id "
                    f"  AND i.index_id = ic.index_id "
                    f"JOIN [{database}].sys.columns c ON ic.object_id = c.object_id "
                    f"  AND ic.column_id = c.column_id "
                    f"JOIN [{database}].sys.tables t ON i.object_id = t.object_id "
                    f"WHERE t.name = :tbl AND i.is_primary_key = 0 AND i.name IS NOT NULL"
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
        database = self.config.database or ""
        engine = self.connect()
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    f"SELECT SUM(p.rows) FROM [{database}].sys.tables t "
                    f"JOIN [{database}].sys.partitions p ON t.object_id = p.object_id "
                    f"WHERE t.name = :tbl AND p.index_id < 2"
                ),
                {"tbl": table},
            ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0
