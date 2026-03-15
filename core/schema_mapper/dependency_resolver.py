"""Topological table dependency resolution using Kahn's algorithm."""
from __future__ import annotations

from collections import defaultdict, deque

from core.schema_mapper.schema_extractor import TableSchema


class CyclicDependencyError(RuntimeError):
    """Raised when FK relationships form a cycle that cannot be resolved."""


class DependencyResolver:
    """Resolves the FK-based dependency order for a set of tables.

    Uses **Kahn's algorithm** (BFS topological sort) to produce an ordered
    list of tables where each table appears after all tables it references
    via foreign keys.
    """

    def resolve(self, schemas: list[TableSchema]) -> list[str]:
        """Return a topologically ordered list of table names.

        Args:
            schemas: :class:`TableSchema` objects for all tables in the database.

        Returns:
            Ordered list of table names — safe migration order.

        Raises:
            CyclicDependencyError: If a circular FK dependency is detected.
        """
        table_names = {s.table_name for s in schemas}
        fk_map: dict[str, set[str]] = {s.table_name: set() for s in schemas}

        for schema in schemas:
            for fk in schema.foreign_keys:
                ref = fk.ref_table
                if ref in table_names and ref != schema.table_name:
                    fk_map[schema.table_name].add(ref)

        # Build in-degree map and adjacency list
        in_degree: dict[str, int] = {t: 0 for t in table_names}
        dependents: dict[str, list[str]] = defaultdict(list)

        for table, deps in fk_map.items():
            for dep in deps:
                dependents[dep].append(table)
                in_degree[table] += 1

        # Kahn's BFS
        queue: deque[str] = deque(t for t, deg in in_degree.items() if deg == 0)
        ordered: list[str] = []

        while queue:
            node = queue.popleft()
            ordered.append(node)
            for dependent in dependents[node]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if len(ordered) != len(table_names):
            cyclic = [t for t in table_names if t not in ordered]
            raise CyclicDependencyError(
                f"Circular FK dependencies detected among tables: {cyclic}. "
                "Consider disabling FK checks during migration."
            )

        return ordered
