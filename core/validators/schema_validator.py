"""Schema compatibility validator."""
from __future__ import annotations

from dataclasses import dataclass

from core.schema_mapper.schema_extractor import TableSchema
from core.schema_mapper.type_mapper import TypeMapper
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class SchemaValidationResult:
    compatible: bool
    unmappable: list[dict]  # [{table, column, type}]
    warnings: list[str]


def validate_schema_compatibility(
    schemas: list[TableSchema],
    source_engine: str,
    dest_engine: str,
) -> SchemaValidationResult:
    """Check whether all column types in *schemas* are mappable for the target engine.

    Args:
        schemas: List of :class:`TableSchema` from the source database.
        source_engine: Source engine name.
        dest_engine: Destination engine name.

    Returns:
        :class:`SchemaValidationResult` with compatibility status and details.
    """
    mapper = TypeMapper()
    unmappable: list[dict] = []
    warnings: list[str] = []

    for schema in schemas:
        for col in schema.columns:
            if not mapper.is_mappable(source_engine, dest_engine, col.type):
                unmappable.append(
                    {
                        "table": schema.table_name,
                        "column": col.name,
                        "type": col.type,
                    }
                )
                warnings.append(
                    f"Table '{schema.table_name}', column '{col.name}': "
                    f"type '{col.type}' has no direct mapping → will use TEXT fallback."
                )
                logger.warning(warnings[-1])

    compatible = True  # we always fall back to TEXT, so it's always runnable
    if unmappable:
        logger.warning(
            f"{len(unmappable)} column(s) with no direct type mapping; "
            "TEXT fallback will be used."
        )

    return SchemaValidationResult(
        compatible=compatible,
        unmappable=unmappable,
        warnings=warnings,
    )
