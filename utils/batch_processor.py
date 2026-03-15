"""Batch processing utilities for streaming large result sets."""
from __future__ import annotations

from collections.abc import Generator, Iterable
from typing import Any, TypeVar

from sqlalchemy import text
from sqlalchemy.engine import Connection

T = TypeVar("T")


def chunks(iterable: Iterable[T], size: int) -> Generator[list[T], None, None]:
    """Yield successive non-overlapping lists of *size* items from *iterable*."""
    batch: list[T] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


class BatchProcessor:
    """Splits an iterable into chunks of a configurable size."""

    def __init__(self, batch_size: int = 1000) -> None:
        self.batch_size = batch_size

    def process(self, iterable: Iterable[T]) -> Generator[list[T], None, None]:
        """Yield batches from *iterable*."""
        yield from chunks(iterable, self.batch_size)


def stream_query(
    conn: Connection,
    query: str,
    params: dict[str, Any] | None = None,
    batch_size: int = 1000,
) -> Generator[list[dict[str, Any]], None, None]:
    """Execute *query* and stream results in batches without loading all rows into RAM.

    Args:
        conn: An active SQLAlchemy :class:`Connection`.
        query: Raw SQL query string (use :param placeholders for *params*).
        params: Optional bind parameters dict.
        batch_size: Number of rows per yielded batch.

    Yields:
        Lists of row dictionaries.
    """
    result = conn.execute(text(query), params or {})
    columns = list(result.keys())
    batch: list[dict[str, Any]] = []
    for row in result:
        batch.append(dict(zip(columns, row)))
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
