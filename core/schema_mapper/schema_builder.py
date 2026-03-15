"""DDL builder — generates CREATE TABLE statements for a target engine."""
from __future__ import annotations

from core.schema_mapper.schema_extractor import ColumnInfo, TableSchema
from core.schema_mapper.type_mapper import TypeMapper

_QUOTE = {
    "mysql": ("`", "`"),
    "mssql": ("[", "]"),
    "postgresql": ('"', '"'),
    "postgres": ('"', '"'),
}


def _quote(engine: str, name: str) -> str:
    left, right = _QUOTE.get(engine.lower(), ('"', '"'))
    return f"{left}{name}{right}"


class SchemaBuilder:
    """Converts a :class:`TableSchema` to a ``CREATE TABLE`` DDL string."""

    def __init__(self, source_engine: str, dest_engine: str) -> None:
        self.source_engine = source_engine.lower()
        self.dest_engine = dest_engine.lower()
        self._mapper = TypeMapper()

    def build_create_table(self, schema: TableSchema) -> str:
        """Return a ``CREATE TABLE IF NOT EXISTS`` DDL string for *schema*.

        Args:
            schema: Table metadata from :class:`SchemaExtractor`.

        Returns:
            A complete DDL string for the destination engine.
        """
        lines: list[str] = []
        for col in schema.columns:
            lines.append(self._column_def(col))

        if schema.primary_keys:
            pk_cols = ", ".join(_quote(self.dest_engine, pk) for pk in schema.primary_keys)
            lines.append(f"    PRIMARY KEY ({pk_cols})")

        body = ",\n".join(lines)
        tbl = _quote(self.dest_engine, schema.table_name)
        return f"CREATE TABLE IF NOT EXISTS {tbl} (\n{body}\n);"

    def build_indexes(self, schema: TableSchema) -> list[str]:
        """Return DDL strings for all non-PK indexes in *schema*."""
        ddls = []
        for idx in schema.indexes:
            unique = "UNIQUE " if idx.unique else ""
            cols = ", ".join(_quote(self.dest_engine, c) for c in idx.columns)
            tbl = _quote(self.dest_engine, schema.table_name)
            idx_name = _quote(self.dest_engine, idx.name)
            ddls.append(
                f"CREATE {unique}INDEX IF NOT EXISTS {idx_name} ON {tbl} ({cols});"
            )
        return ddls

    def build_foreign_keys(self, schema: TableSchema) -> list[str]:
        """Return ALTER TABLE … ADD FOREIGN KEY DDL strings."""
        ddls = []
        tbl = _quote(self.dest_engine, schema.table_name)
        for fk in schema.foreign_keys:
            col = _quote(self.dest_engine, fk.column)
            ref_tbl = _quote(self.dest_engine, fk.ref_table)
            ref_col = _quote(self.dest_engine, fk.ref_column)
            ddls.append(
                f"ALTER TABLE {tbl} ADD FOREIGN KEY ({col}) REFERENCES {ref_tbl} ({ref_col});"
            )
        return ddls

    # ── Private ───────────────────────────────────────────────────────

    def _column_def(self, col: ColumnInfo) -> str:
        dest_type = self._mapper.map(self.source_engine, self.dest_engine, col.type)
        col_name = _quote(self.dest_engine, col.name)
        parts = [f"    {col_name} {dest_type}"]

        # Identity / auto-increment
        if col.extra.get("identity") or "auto_increment" in (col.extra.get("extra") or "").lower():
            if self.dest_engine == "mssql":
                parts[0] = f"    {col_name} {dest_type} IDENTITY(1,1)"
            elif self.dest_engine == "postgresql":
                # Replace the type with SERIAL/BIGSERIAL if applicable
                if "bigint" in dest_type.lower():
                    parts[0] = f"    {col_name} BIGSERIAL"
                else:
                    parts[0] = f"    {col_name} SERIAL"
            else:  # mysql
                parts.append("AUTO_INCREMENT")

        if not col.nullable:
            parts.append("NOT NULL")

        if col.default is not None and not col.extra.get("identity") and not col.extra.get("is_sequence"):
            # Pass through simple defaults; skip function defaults that may not translate
            default_val = col.default.strip()
            if default_val and not default_val.lower().startswith("nextval("):
                parts.append(f"DEFAULT {default_val}")

        return " ".join(parts)
