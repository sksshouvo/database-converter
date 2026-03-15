"""Cross-engine data type mapping tables."""
from __future__ import annotations

from typing import ClassVar


class TypeMapper:
    """Maps source engine column types to equivalent destination engine types.

    Usage::

        mapper = TypeMapper()
        dest_type = mapper.map("mysql", "postgresql", "TINYINT(1)")
    """

    # Structure:  SOURCE_ENGINE → { SOURCE_TYPE_PATTERN → { DEST_ENGINE → DEST_TYPE } }
    # Keys are lower-cased; patterns are matched as prefix/substring for flexibility.
    _MAP: ClassVar[dict[str, dict[str, dict[str, str]]]] = {
        # ── MySQL source ──────────────────────────────────────────────────
        "mysql": {
            "tinyint(1)":     {"mysql": "TINYINT(1)",      "mssql": "BIT",              "postgresql": "BOOLEAN"},
            "tinyint":        {"mysql": "TINYINT",         "mssql": "TINYINT",          "postgresql": "SMALLINT"},
            "smallint":       {"mysql": "SMALLINT",        "mssql": "SMALLINT",         "postgresql": "SMALLINT"},
            "mediumint":      {"mysql": "MEDIUMINT",       "mssql": "INT",              "postgresql": "INTEGER"},
            "int":            {"mysql": "INT",             "mssql": "INT",              "postgresql": "INTEGER"},
            "bigint":         {"mysql": "BIGINT",          "mssql": "BIGINT",           "postgresql": "BIGINT"},
            "float":          {"mysql": "FLOAT",           "mssql": "FLOAT",            "postgresql": "DOUBLE PRECISION"},
            "double":         {"mysql": "DOUBLE",          "mssql": "FLOAT",            "postgresql": "DOUBLE PRECISION"},
            "decimal":        {"mysql": "DECIMAL",         "mssql": "DECIMAL",          "postgresql": "NUMERIC"},
            "numeric":        {"mysql": "NUMERIC",         "mssql": "NUMERIC",          "postgresql": "NUMERIC"},
            "char":           {"mysql": "CHAR",            "mssql": "NCHAR",            "postgresql": "CHAR"},
            "varchar":        {"mysql": "VARCHAR",         "mssql": "NVARCHAR",         "postgresql": "VARCHAR"},
            "tinytext":       {"mysql": "TINYTEXT",        "mssql": "NVARCHAR(255)",    "postgresql": "TEXT"},
            "text":           {"mysql": "LONGTEXT",        "mssql": "NVARCHAR(MAX)",    "postgresql": "TEXT"},
            "mediumtext":     {"mysql": "MEDIUMTEXT",      "mssql": "NVARCHAR(MAX)",    "postgresql": "TEXT"},
            "longtext":       {"mysql": "LONGTEXT",        "mssql": "NVARCHAR(MAX)",    "postgresql": "TEXT"},
            "date":           {"mysql": "DATE",            "mssql": "DATE",             "postgresql": "DATE"},
            "time":           {"mysql": "TIME",            "mssql": "TIME",             "postgresql": "TIME"},
            "datetime":       {"mysql": "DATETIME",        "mssql": "DATETIME2",        "postgresql": "TIMESTAMP"},
            "timestamp":      {"mysql": "TIMESTAMP",       "mssql": "DATETIME2",        "postgresql": "TIMESTAMPTZ"},
            "year":           {"mysql": "YEAR",            "mssql": "SMALLINT",         "postgresql": "SMALLINT"},
            "binary":         {"mysql": "BINARY",          "mssql": "BINARY",           "postgresql": "BYTEA"},
            "varbinary":      {"mysql": "VARBINARY",       "mssql": "VARBINARY",        "postgresql": "BYTEA"},
            "tinyblob":       {"mysql": "TINYBLOB",        "mssql": "VARBINARY(MAX)",   "postgresql": "BYTEA"},
            "blob":           {"mysql": "BLOB",            "mssql": "VARBINARY(MAX)",   "postgresql": "BYTEA"},
            "mediumblob":     {"mysql": "MEDIUMBLOB",      "mssql": "VARBINARY(MAX)",   "postgresql": "BYTEA"},
            "longblob":       {"mysql": "LONGBLOB",        "mssql": "VARBINARY(MAX)",   "postgresql": "BYTEA"},
            "enum":           {"mysql": "ENUM",            "mssql": "NVARCHAR(255)",    "postgresql": "VARCHAR(255)"},
            "set":            {"mysql": "SET",             "mssql": "NVARCHAR(255)",    "postgresql": "VARCHAR(255)"},
            "json":           {"mysql": "JSON",            "mssql": "NVARCHAR(MAX)",    "postgresql": "JSONB"},
            "boolean":        {"mysql": "TINYINT(1)",      "mssql": "BIT",              "postgresql": "BOOLEAN"},
        },

        # ── MSSQL source ──────────────────────────────────────────────────
        "mssql": {
            "bit":            {"mysql": "TINYINT(1)",      "mssql": "BIT",              "postgresql": "BOOLEAN"},
            "tinyint":        {"mysql": "TINYINT",         "mssql": "TINYINT",          "postgresql": "SMALLINT"},
            "smallint":       {"mysql": "SMALLINT",        "mssql": "SMALLINT",         "postgresql": "SMALLINT"},
            "int":            {"mysql": "INT",             "mssql": "INT",              "postgresql": "INTEGER"},
            "bigint":         {"mysql": "BIGINT",          "mssql": "BIGINT",           "postgresql": "BIGINT"},
            "float":          {"mysql": "FLOAT",           "mssql": "FLOAT",            "postgresql": "DOUBLE PRECISION"},
            "real":           {"mysql": "FLOAT",           "mssql": "REAL",             "postgresql": "REAL"},
            "decimal":        {"mysql": "DECIMAL",         "mssql": "DECIMAL",          "postgresql": "NUMERIC"},
            "numeric":        {"mysql": "NUMERIC",         "mssql": "NUMERIC",          "postgresql": "NUMERIC"},
            "money":          {"mysql": "DECIMAL(19,4)",   "mssql": "MONEY",            "postgresql": "NUMERIC(19,4)"},
            "smallmoney":     {"mysql": "DECIMAL(10,4)",   "mssql": "SMALLMONEY",       "postgresql": "NUMERIC(10,4)"},
            "char":           {"mysql": "CHAR",            "mssql": "CHAR",             "postgresql": "CHAR"},
            "nchar":          {"mysql": "CHAR",            "mssql": "NCHAR",            "postgresql": "CHAR"},
            "varchar":        {"mysql": "VARCHAR",         "mssql": "VARCHAR",          "postgresql": "VARCHAR"},
            "nvarchar":       {"mysql": "VARCHAR",         "mssql": "NVARCHAR",         "postgresql": "VARCHAR"},
            "text":           {"mysql": "LONGTEXT",        "mssql": "TEXT",             "postgresql": "TEXT"},
            "ntext":          {"mysql": "LONGTEXT",        "mssql": "NTEXT",            "postgresql": "TEXT"},
            "date":           {"mysql": "DATE",            "mssql": "DATE",             "postgresql": "DATE"},
            "time":           {"mysql": "TIME",            "mssql": "TIME",             "postgresql": "TIME"},
            "datetime":       {"mysql": "DATETIME",        "mssql": "DATETIME",         "postgresql": "TIMESTAMP"},
            "datetime2":      {"mysql": "DATETIME",        "mssql": "DATETIME2",        "postgresql": "TIMESTAMP"},
            "smalldatetime":  {"mysql": "DATETIME",        "mssql": "SMALLDATETIME",    "postgresql": "TIMESTAMP"},
            "datetimeoffset": {"mysql": "DATETIME",        "mssql": "DATETIMEOFFSET",   "postgresql": "TIMESTAMPTZ"},
            "binary":         {"mysql": "BINARY",          "mssql": "BINARY",           "postgresql": "BYTEA"},
            "varbinary":      {"mysql": "VARBINARY",       "mssql": "VARBINARY",        "postgresql": "BYTEA"},
            "image":          {"mysql": "LONGBLOB",        "mssql": "IMAGE",            "postgresql": "BYTEA"},
            "uniqueidentifier": {"mysql": "CHAR(36)",      "mssql": "UNIQUEIDENTIFIER", "postgresql": "UUID"},
            "xml":            {"mysql": "LONGTEXT",        "mssql": "XML",              "postgresql": "XML"},
        },

        # ── PostgreSQL source ─────────────────────────────────────────────
        "postgresql": {
            "boolean":            {"mysql": "TINYINT(1)",   "mssql": "BIT",             "postgresql": "BOOLEAN"},
            "smallint":           {"mysql": "SMALLINT",     "mssql": "SMALLINT",        "postgresql": "SMALLINT"},
            "integer":            {"mysql": "INT",          "mssql": "INT",             "postgresql": "INTEGER"},
            "bigint":             {"mysql": "BIGINT",       "mssql": "BIGINT",          "postgresql": "BIGINT"},
            "serial":             {"mysql": "INT",          "mssql": "INT IDENTITY",    "postgresql": "SERIAL"},
            "bigserial":          {"mysql": "BIGINT",       "mssql": "BIGINT IDENTITY", "postgresql": "BIGSERIAL"},
            "real":               {"mysql": "FLOAT",        "mssql": "REAL",            "postgresql": "REAL"},
            "double precision":   {"mysql": "DOUBLE",       "mssql": "FLOAT",           "postgresql": "DOUBLE PRECISION"},
            "numeric":            {"mysql": "DECIMAL",      "mssql": "NUMERIC",         "postgresql": "NUMERIC"},
            "decimal":            {"mysql": "DECIMAL",      "mssql": "DECIMAL",         "postgresql": "NUMERIC"},
            "character varying":  {"mysql": "VARCHAR",      "mssql": "NVARCHAR",        "postgresql": "VARCHAR"},
            "varchar":            {"mysql": "VARCHAR",      "mssql": "NVARCHAR",        "postgresql": "VARCHAR"},
            "character":          {"mysql": "CHAR",         "mssql": "NCHAR",           "postgresql": "CHAR"},
            "char":               {"mysql": "CHAR",         "mssql": "NCHAR",           "postgresql": "CHAR"},
            "text":               {"mysql": "LONGTEXT",     "mssql": "NVARCHAR(MAX)",   "postgresql": "TEXT"},
            "date":               {"mysql": "DATE",         "mssql": "DATE",            "postgresql": "DATE"},
            "time":               {"mysql": "TIME",         "mssql": "TIME",            "postgresql": "TIME"},
            "timestamp":          {"mysql": "DATETIME",     "mssql": "DATETIME2",       "postgresql": "TIMESTAMP"},
            "timestamptz":        {"mysql": "DATETIME",     "mssql": "DATETIMEOFFSET",  "postgresql": "TIMESTAMPTZ"},
            "timestamp with time zone": {"mysql": "DATETIME", "mssql": "DATETIMEOFFSET", "postgresql": "TIMESTAMPTZ"},
            "interval":           {"mysql": "VARCHAR(50)",  "mssql": "NVARCHAR(50)",    "postgresql": "INTERVAL"},
            "bytea":              {"mysql": "LONGBLOB",     "mssql": "VARBINARY(MAX)",  "postgresql": "BYTEA"},
            "uuid":               {"mysql": "CHAR(36)",     "mssql": "UNIQUEIDENTIFIER","postgresql": "UUID"},
            "json":               {"mysql": "JSON",         "mssql": "NVARCHAR(MAX)",   "postgresql": "JSON"},
            "jsonb":              {"mysql": "JSON",         "mssql": "NVARCHAR(MAX)",   "postgresql": "JSONB"},
            "xml":                {"mysql": "LONGTEXT",     "mssql": "XML",             "postgresql": "XML"},
            "array":              {"mysql": "JSON",         "mssql": "NVARCHAR(MAX)",   "postgresql": "TEXT[]"},
        },

        # Alias
        "postgres": {},
    }

    def __init__(self) -> None:
        # Populate alias
        self._MAP["postgres"] = self._MAP["postgresql"]

    def map(self, source_engine: str, dest_engine: str, source_type: str) -> str:
        """Return the destination type string for *source_type*.

        Matching is attempted in the following order:
        1. Exact lower-cased match
        2. Prefix match (e.g. ``varchar(255)`` → ``varchar``)
        3. Falls back to ``TEXT`` / ``NVARCHAR(MAX)`` / ``TEXT`` as a safe catch-all.

        Args:
            source_engine: Engine name of the source (mysql|mssql|postgresql).
            dest_engine: Engine name of the destination.
            source_type: Column type string from the source schema.

        Returns:
            Destination type string.
        """
        src = source_engine.lower()
        dst = dest_engine.lower()
        st = source_type.lower().strip()

        engine_map = self._MAP.get(src, {})

        # 1. Exact match
        if st in engine_map:
            return engine_map[st].get(dst, "TEXT")

        # 2. Prefix match – handles parameterised types like varchar(255)
        for key, dest_map in engine_map.items():
            if st.startswith(key):
                return dest_map.get(dst, "TEXT")

        # 3. Fallback
        fallback = {"mysql": "LONGTEXT", "mssql": "NVARCHAR(MAX)", "postgresql": "TEXT"}
        return fallback.get(dst, "TEXT")

    def is_mappable(self, source_engine: str, dest_engine: str, source_type: str) -> bool:
        """Return True if a mapping exists (non-fallback)."""
        src = source_engine.lower()
        st = source_type.lower().strip()
        engine_map = self._MAP.get(src, {})
        if st in engine_map:
            return True
        return any(st.startswith(key) for key in engine_map)
