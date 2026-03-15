"""Row value transformer for cross-engine data type conversions."""
from __future__ import annotations

import json
from datetime import datetime, date, time
from decimal import Decimal
from typing import Any


class RowTransformer:
    """Converts Python values between source and destination engine representations.

    Each engine has quirks:
    - MySQL: booleans as 0/1 (TINYINT); datetime strings; bytes for BLOBs
    - MSSQL: BIT → int 0/1; pyodbc may return special types
    - PostgreSQL: native bool; Decimal; bytes for bytea; dict/str for JSON
    """

    def __init__(self, source_engine: str, dest_engine: str) -> None:
        self.source_engine = source_engine.lower()
        self.dest_engine = dest_engine.lower()

    def transform_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """Return a new row dict with values converted for the destination engine."""
        return {col: self._convert(val) for col, val in row.items()}

    def _convert(self, value: Any) -> Any:
        """Apply engine-specific conversions to a single value."""
        if value is None:
            return None

        # ── Decimal ───────────────────────────────────────────────────
        if isinstance(value, Decimal):
            if self.dest_engine in ("mysql",):
                return float(value)
            return value  # psycopg2 / pyodbc handle Decimal natively

        # ── Boolean ───────────────────────────────────────────────────
        if isinstance(value, bool):
            if self.dest_engine == "mysql":
                return int(value)   # TINYINT(1)
            if self.dest_engine == "mssql":
                return int(value)   # BIT
            return value            # PostgreSQL: native bool

        # ── Integer that represents a bool in MySQL source ────────────
        if isinstance(value, int) and self.source_engine == "mysql" and self.dest_engine == "postgresql":
            # We can't know here if it was a TINYINT(1) bool column —
            # the column type mapping handles that; pass through unchanged.
            return value

        # ── datetime / date / time ────────────────────────────────────
        if isinstance(value, datetime):
            if self.dest_engine == "mysql":
                # MySQL DATETIME has no timezone awareness; strip tzinfo
                return value.replace(tzinfo=None)
            return value

        if isinstance(value, date):
            return value  # All engines handle datetime.date natively

        if isinstance(value, time):
            return value

        # ── bytes / bytearray (BLOBs) ─────────────────────────────────
        if isinstance(value, (bytes, bytearray)):
            if self.dest_engine == "postgresql":
                return bytes(value)   # psycopg2 accepts bytes for BYTEA
            if self.dest_engine == "mssql":
                return bytes(value)   # pyodbc accepts bytes for VARBINARY
            return bytes(value)

        # ── dict / list (JSON) ────────────────────────────────────────
        if isinstance(value, (dict, list)):
            if self.dest_engine == "mysql":
                return json.dumps(value)      # MySQL JSON column
            if self.dest_engine == "mssql":
                return json.dumps(value)      # stored as NVARCHAR(MAX)
            return value                      # psycopg2 handles dict → JSONB

        # ── str JSON from MySQL ───────────────────────────────────────
        if isinstance(value, str) and self.source_engine == "mysql" and self.dest_engine == "postgresql":
            # Try to parse JSON strings into dicts for JSONB columns
            stripped = value.strip()
            if stripped and stripped[0] in ("{", "["):
                try:
                    return json.loads(value)
                except (json.JSONDecodeError, ValueError):
                    pass

        return value
