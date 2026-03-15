"""Schema extraction using connector-provided metadata."""
from __future__ import annotations

from dataclasses import dataclass, field

from core.connectors.base_connector import BaseConnector


@dataclass
class ColumnInfo:
    name: str
    type: str
    nullable: bool
    primary_key: bool
    default: str | None = None
    extra: dict = field(default_factory=dict)


@dataclass
class ForeignKeyInfo:
    column: str
    ref_table: str
    ref_column: str


@dataclass
class IndexInfo:
    name: str
    columns: list[str]
    unique: bool


@dataclass
class TableSchema:
    """Full structural metadata for a single table."""

    database: str
    table_name: str
    columns: list[ColumnInfo]
    primary_keys: list[str]
    foreign_keys: list[ForeignKeyInfo]
    indexes: list[IndexInfo]


class SchemaExtractor:
    """Extracts full schema metadata from a database using a connector."""

    def __init__(self, connector: BaseConnector) -> None:
        self.connector = connector

    def extract_table(self, database: str, table: str) -> TableSchema:
        """Extract and return the full schema for a single table."""
        raw = self.connector.get_table_schema(database, table)

        columns = [
            ColumnInfo(
                name=c["name"],
                type=c["type"],
                nullable=c.get("nullable", True),
                primary_key=c.get("primary_key", False),
                default=c.get("default"),
                extra={k: v for k, v in c.items() if k not in {"name", "type", "nullable", "primary_key", "default"}},
            )
            for c in raw.get("columns", [])
        ]

        foreign_keys = [
            ForeignKeyInfo(
                column=fk["column"],
                ref_table=fk["ref_table"],
                ref_column=fk["ref_column"],
            )
            for fk in raw.get("foreign_keys", [])
        ]

        indexes = [
            IndexInfo(
                name=idx["name"],
                columns=idx["columns"],
                unique=idx.get("unique", False),
            )
            for idx in raw.get("indexes", [])
        ]

        return TableSchema(
            database=database,
            table_name=table,
            columns=columns,
            primary_keys=raw.get("primary_keys", []),
            foreign_keys=foreign_keys,
            indexes=indexes,
        )

    def extract_all(self, database: str) -> list[TableSchema]:
        """Extract schema for every table in *database*."""
        tables = self.connector.get_table_names(database)
        return [self.extract_table(database, t) for t in tables]
