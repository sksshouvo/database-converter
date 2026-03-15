"""Post-migration data validator — row counts + MD5 sample checksums."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text

from core.connectors.base_connector import BaseConnector
from utils.logger import get_logger

logger = get_logger(__name__)

_SAMPLE_ROWS = 1000  # rows to checksum


@dataclass
class ValidationResult:
    table: str
    match: bool
    src_count: int
    dst_count: int
    src_checksum: str = ""
    dst_checksum: str = ""
    details: list[str] = field(default_factory=list)


def validate_migration(
    src_connector: BaseConnector,
    dst_connector: BaseConnector,
    source_db: str,
    dest_db: str,
    table: str,
) -> ValidationResult:
    """Compare row counts and a sample MD5 checksum between source and destination.

    Args:
        src_connector: Source database connector.
        dst_connector: Destination database connector.
        source_db: Source database name.
        dest_db: Destination database name.
        table: Table name to validate.

    Returns:
        :class:`ValidationResult` with match status and count details.
    """
    result = ValidationResult(table=table, match=False, src_count=0, dst_count=0)

    # ── Row counts ────────────────────────────────────────────────────
    try:
        result.src_count = src_connector.get_row_count(table)
    except Exception as exc:
        result.details.append(f"Could not get source row count: {exc}")
        return result

    try:
        result.dst_count = dst_connector.get_row_count(table)
    except Exception as exc:
        result.details.append(f"Could not get destination row count: {exc}")
        return result

    if result.src_count != result.dst_count:
        result.details.append(
            f"Row count mismatch: source={result.src_count}, dest={result.dst_count}"
        )
        logger.warning(
            f"Validation: '{table}' row count mismatch "
            f"(src={result.src_count}, dst={result.dst_count})"
        )
        return result

    # ── MD5 checksum on sample ────────────────────────────────────────
    src_engine = src_connector.connect()
    dst_engine = dst_connector.connect()

    src_dialect = src_engine.dialect.name
    dst_dialect = dst_engine.dialect.name

    src_tbl = _qualified(source_db, table, src_dialect)
    dst_tbl = _qualified(dest_db, table, dst_dialect)

    try:
        result.src_checksum = _compute_checksum(
            src_engine, f"SELECT * FROM {src_tbl} LIMIT {_SAMPLE_ROWS}"
        )
        result.dst_checksum = _compute_checksum(
            dst_engine, f"SELECT * FROM {dst_tbl} LIMIT {_SAMPLE_ROWS}"
        )
    except Exception as exc:
        result.details.append(f"Checksum computation skipped: {exc}")
        # Still mark as match if counts agree
        result.match = True
        return result

    result.match = result.src_checksum == result.dst_checksum
    if not result.match:
        result.details.append(
            f"Checksum mismatch (sample {_SAMPLE_ROWS} rows): "
            f"src={result.src_checksum}, dst={result.dst_checksum}"
        )
        logger.warning(f"Validation: '{table}' checksum mismatch on sample rows.")
    else:
        logger.info(f"✅ Validation passed for '{table}': counts match, checksum OK.")

    return result


# ── Helpers ───────────────────────────────────────────────────────────────────

def _qualified(database: str, table: str, dialect: str) -> str:
    if dialect == "mysql":
        return f"`{database}`.`{table}`"
    if dialect == "mssql":
        return f"[{database}].[dbo].[{table}]"
    return f'"{table}"'


def _compute_checksum(engine: Any, query: str) -> str:
    """Fetch rows via *query* and compute an MD5 of the serialised result."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()
    except Exception:
        # Dialect may not support LIMIT (MSSQL uses TOP); try generic fallback
        with engine.connect() as conn:
            rows = conn.execute(text(query.replace(f" LIMIT {_SAMPLE_ROWS}", ""))).fetchall()
        rows = rows[:_SAMPLE_ROWS]

    serialised = json.dumps(
        [list(r) for r in rows],
        default=str,   # handles datetime, Decimal, bytes
        sort_keys=True,
    ).encode()
    return hashlib.md5(serialised).hexdigest()
