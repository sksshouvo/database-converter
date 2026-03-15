"""Parallel table migration using ThreadPoolExecutor."""
from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.connectors.base_connector import BaseConnector
from core.data_migrator.migrator import TableMigrator
from models.migration_job import MigrationJob
from utils.config import config
from utils.logger import get_logger

logger = get_logger(__name__)


class ParallelMigrator:
    """Migrates multiple tables concurrently across a shared thread pool.

    Each table is assigned its own :class:`TableMigrator` submitted as a
    ``Future`` to a ``ThreadPoolExecutor``.  Progress and errors are logged
    per-table as futures complete.
    """

    def __init__(
        self,
        src_connector: BaseConnector,
        dst_connector: BaseConnector,
        source_db: str,
        dest_db: str,
        tables: list[str],
        max_workers: int | None = None,
        batch_size: int | None = None,
        job_id_prefix: str | None = None,
    ) -> None:
        self.src_connector = src_connector
        self.dst_connector = dst_connector
        self.source_db = source_db
        self.dest_db = dest_db
        self.tables = tables
        self.max_workers = max_workers or config.max_workers
        self.batch_size = batch_size or config.batch_size
        self.job_id_prefix = job_id_prefix or str(uuid.uuid4())

    def migrate_all(self) -> dict[str, MigrationJob]:
        """Submit all tables and wait for completion.

        Returns:
            Mapping of ``table_name → MigrationJob`` for every table.
        """
        results: dict[str, MigrationJob] = {}
        future_to_table: dict = {}

        logger.info(
            f"Starting parallel migration of {len(self.tables)} tables "
            f"with {self.max_workers} workers."
        )

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for table in self.tables:
                job_id = f"{self.job_id_prefix}::{table}"
                migrator = TableMigrator(
                    src_connector=self.src_connector,
                    dst_connector=self.dst_connector,
                    source_db=self.source_db,
                    dest_db=self.dest_db,
                    table=table,
                    job_id=job_id,
                    batch_size=self.batch_size,
                )
                future = executor.submit(migrator.migrate)
                future_to_table[future] = table

            for future in as_completed(future_to_table):
                table = future_to_table[future]
                try:
                    job = future.result()
                    results[table] = job
                    logger.info(
                        f"✅ {table}: {job.converted_rows}/{job.total_rows} rows "
                        f"({job.progress_pct}%)"
                    )
                except Exception as exc:
                    logger.error(f"❌ {table} failed: {exc}", exc_info=True)

        completed = sum(1 for j in results.values() if j.status == "completed")
        failed = len(self.tables) - completed
        logger.info(
            f"Migration complete — {completed} succeeded, {failed} failed "
            f"out of {len(self.tables)} tables."
        )
        return results
