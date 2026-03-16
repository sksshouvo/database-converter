"""DDL builder — generates CREATE TABLE statements for a target engine."""
from __future__ import annotations

import re

from core.schema_mapper.schema_extractor import ColumnInfo, TableSchema
from core.schema_mapper.type_mapper import TypeMapper

_QUOTE = {
    "mysql": ("`", "`"),
    "mssql": ("[", "]"),
    "postgresql": ('"', '"'),
    "postgres": ('"', '"'),
}

# Matches parameterised types like varchar(255), decimal(10,2), char(36)
_PARAM_RE = re.compile(
    r"^(varchar|nvarchar|char|nchar|varbinary|binary|decimal|numeric|"
    r"character varying|character)\s*\(([^)]+)\)",
    re.IGNORECASE,
)

# MySQL string types that REQUIRE a length — bare VARCHAR is a syntax error
_MYSQL_NEEDS_LEN = {"varchar", "char", "varbinary", "binary"}
_DEFAULT_LEN = 255


def _quote(engine: str, name: str) -> str:
    left, right = _QUOTE.get(engine.lower(), ('"', '"'))
    return f"{left}{name}{right}"


def _forward_params(source_type: str, dest_type: str, dest_engine: str) -> str:
    """Carry length/precision params from *source_type* into *dest_type*.

    Examples:
        ``varchar(255)`` → ``VARCHAR(255)``
        ``decimal(10,2)`` → ``DECIMAL(10,2)``
        ``bigint`` + MySQL dest → ``BIGINT``   (no params needed)

    If *dest_type* already has params (e.g. NVARCHAR(MAX)), it is returned unchanged.
    """
    # Dest already has explicit params — leave as-is
    if "(" in dest_type:
        return dest_type

    m = _PARAM_RE.match(source_type.strip())
    dest_base = dest_type.split("(")[0].strip()

    if m:
        return f"{dest_base}({m.group(2)})"

    # MySQL bare VARCHAR/CHAR is invalid — apply a safe default length
    if dest_engine == "mysql" and dest_base.lower() in _MYSQL_NEEDS_LEN:
        return f"{dest_base}({_DEFAULT_LEN})"

    return dest_type


def _tbl_ref(engine: str, table: str, database: str | None) -> str:
    """Return a fully-qualified (or bare) table reference for DDL."""
    if engine == "mysql" and database:
        return f"`{database}`.`{table}`"
    if engine == "mssql" and database:
        return f"[{database}].[dbo].[{table}]"
    return _quote(engine, table)


class SchemaBuilder:
    """Converts a :class:`TableSchema` to engine-specific DDL strings."""

    def __init__(self, source_engine: str, dest_engine: str) -> None:
        self.source_engine = source_engine.lower()
        self.dest_engine = dest_engine.lower()
        self._mapper = TypeMapper()

    # ── Public API ────────────────────────────────────────────────────

    def build_create_table(self, schema: TableSchema, dest_database: str | None = None) -> str:
        """Return a ``CREATE TABLE IF NOT EXISTS`` DDL string for *schema*."""
        lines: list[str] = [self._column_def(col) for col in schema.columns]

        if schema.primary_keys:
            pk_cols = ", ".join(_quote(self.dest_engine, pk) for pk in schema.primary_keys)
            lines.append(f"    PRIMARY KEY ({pk_cols})")

        body = ",\n".join(lines)
        tbl = _tbl_ref(self.dest_engine, schema.table_name, dest_database)
        return f"CREATE TABLE IF NOT EXISTS {tbl} (\n{body}\n);"

    def build_indexes(self, schema: TableSchema, dest_database: str | None = None) -> list[str]:
        """Return ``CREATE [UNIQUE] INDEX`` DDL strings for non-PK indexes."""
        ddls = []
        tbl = _tbl_ref(self.dest_engine, schema.table_name, dest_database)
        for idx in schema.indexes:
            unique = "UNIQUE " if idx.unique else ""
            cols = ", ".join(_quote(self.dest_engine, c) for c in idx.columns)
            idx_name = _quote(self.dest_engine, idx.name)
            # Note: no IF NOT EXISTS — MySQL <8.0.22 does not support it on CREATE INDEX
            ddls.append(f"CREATE {unique}INDEX {idx_name} ON {tbl} ({cols});")
        return ddls

    def build_foreign_keys(self, schema: TableSchema, dest_database: str | None = None) -> list[str]:
        """Return ``ALTER TABLE … ADD FOREIGN KEY`` DDL strings."""
        ddls = []
        tbl = _tbl_ref(self.dest_engine, schema.table_name, dest_database)
        for fk in schema.foreign_keys:
            col = _quote(self.dest_engine, fk.column)
            ref_tbl = _tbl_ref(self.dest_engine, fk.ref_table, dest_database)
            ref_col = _quote(self.dest_engine, fk.ref_column)
            ddls.append(
                f"ALTER TABLE {tbl} ADD FOREIGN KEY ({col}) REFERENCES {ref_tbl} ({ref_col});"
            )
        return ddls

    # ── Private ───────────────────────────────────────────────────────

    def _column_def(self, col: ColumnInfo) -> str:
        dest_type = self._mapper.map(self.source_engine, self.dest_engine, col.type)
        dest_type = _forward_params(col.type, dest_type, self.dest_engine)

        col_name = _quote(self.dest_engine, col.name)

        # Identity / auto-increment handling
        is_auto = col.extra.get("identity") or "auto_increment" in (col.extra.get("extra") or "").lower()
        if is_auto:
            if self.dest_engine == "mssql":
                base = f"    {col_name} {dest_type} IDENTITY(1,1)"
            elif self.dest_engine == "postgresql":
                base = f"    {col_name} {'BIGSERIAL' if 'bigint' in dest_type.lower() else 'SERIAL'}"
            else:  # mysql
                base = f"    {col_name} {dest_type} AUTO_INCREMENT"
        else:
            base = f"    {col_name} {dest_type}"

        parts = [base]

        if not col.nullable:
            parts.append("NOT NULL")

        if (
            col.default is not None
            and not col.extra.get("identity")
            and not col.extra.get("is_sequence")
        ):
            default_val = col.default.strip()
            if default_val and not default_val.lower().startswith("nextval("):
                parts.append(f"DEFAULT {default_val}")

        return " ".join(parts)
