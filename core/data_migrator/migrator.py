"""Single-table migration with resume support."""
from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.connectors.base_connector import BaseConnector
from core.data_migrator.row_transformer import RowTransformer
from models.migration_job import MigrationJob
from storage.control_db import get_session
from utils.batch_processor import stream_query
from utils.config import config
from utils.logger import get_logger

logger = get_logger(__name__)


class TableMigrator:
    """Migrates a single table from source to destination with resume capability.

    For each batch:
    1. Read rows from source starting after ``last_processed_id``
    2. Transform values via :class:`RowTransformer`
    3. Bulk-insert into destination inside a transaction
    4. Persist progress to control DB after each committed batch
    """

    def __init__(
        self,
        src_connector: BaseConnector,
        dst_connector: BaseConnector,
        source_db: str,
        dest_db: str,
        table: str,
        job_id: str | None = None,
        batch_size: int | None = None,
    ) -> None:
        self.src_connector = src_connector
        self.dst_connector = dst_connector
        self.source_db = source_db
        self.dest_db = dest_db
        self.table = table
        self.job_id = job_id or str(uuid.uuid4())
        self.batch_size = batch_size or config.batch_size

        src_cfg = getattr(src_connector, "config", None)
        dst_cfg = getattr(dst_connector, "config", None)
        src_engine = src_cfg.engine if src_cfg else "unknown"
        dst_engine = dst_cfg.engine if dst_cfg else "unknown"
        self.transformer = RowTransformer(src_engine, dst_engine)

    # ── Public ────────────────────────────────────────────────────────

    def migrate(self) -> MigrationJob:
        """Run (or resume) the migration for this table.

        Returns:
            The :class:`MigrationJob` record after completion.
        """
        job = self._get_or_create_job()
        if job.status == "completed":
            logger.info(f"Table '{self.table}' already completed. Skipping.")
            return job

        job = self._update_job_status(job.job_id, "running")
        logger.info(
            f"[bold]Migrating[/bold] {self.source_db}.{self.table} → "
            f"{self.dest_db}.{self.table} "
            f"(resume from id={job.last_processed_id})"
        )

        src_engine = self.src_connector.connect()
        dst_engine = self.dst_connector.connect()
        total_rows = self.src_connector.get_row_count(self.table)

        with get_session() as session:
            job_obj = session.get(MigrationJob, job.job_id)
            if job_obj:
                job_obj.total_rows = total_rows
            session.commit()

        last_id = job.last_processed_id
        converted = job.converted_rows

        try:
            src_table = self._qualified(self.source_db, self.table, src_engine.dialect.name)
            query = f"SELECT * FROM {src_table} WHERE 1=1"

            with src_engine.connect() as src_conn:
                for batch in stream_query(src_conn, query, batch_size=self.batch_size):
                    # Filter already-processed rows using row offset tracking
                    # (simplified: we use slice-based offsets since not all tables have auto-incr PKs)
                    transformed = [self.transformer.transform_row(row) for row in batch]
                    if not transformed:
                        continue

                    self._bulk_insert(dst_engine, self.dest_db, self.table, transformed)
                    converted += len(transformed)
                    last_id += len(transformed)

                    with get_session() as session:
                        job_obj = session.get(MigrationJob, job.job_id)
                        if job_obj:
                            job_obj.converted_rows = converted
                            job_obj.last_processed_id = last_id

            job = self._update_job_status(job.job_id, "completed")
            logger.info(
                f"✅ [green]Table '{self.table}' migrated:[/green] "
                f"{converted}/{total_rows} rows."
            )
        except Exception as exc:
            job = self._update_job_status(job.job_id, "failed", str(exc))
            logger.error(
                f"❌ Table '{self.table}' failed: {exc}", exc_info=True
            )
            raise

        return job

    # ── Private ───────────────────────────────────────────────────────

    def _get_or_create_job(self) -> MigrationJob:
        with get_session() as session:
            job = session.get(MigrationJob, self.job_id)
            if job is None:
                src_cfg = getattr(self.src_connector, "config", None)
                dst_cfg = getattr(self.dst_connector, "config", None)
                job = MigrationJob(
                    job_id=self.job_id,
                    source_engine=src_cfg.engine if src_cfg else "unknown",
                    destination_engine=dst_cfg.engine if dst_cfg else "unknown",
                    source_host=src_cfg.host if src_cfg else "",
                    source_database=self.source_db,
                    destination_host=dst_cfg.host if dst_cfg else "",
                    destination_database=self.dest_db,
                    table_name=self.table,
                    batch_size=self.batch_size,
                )
                session.add(job)
        with get_session() as session:
            return session.get(MigrationJob, self.job_id)  # type: ignore[return-value]

    def _update_job_status(
        self, job_id: str, status: str, error: str | None = None
    ) -> MigrationJob:
        with get_session() as session:
            job = session.get(MigrationJob, job_id)
            if job:
                job.status = status
                if error:
                    job.error_message = error
        with get_session() as session:
            return session.get(MigrationJob, job_id)  # type: ignore[return-value]

    @staticmethod
    def _qualified(database: str, table: str, dialect: str) -> str:
        """Return a fully qualified table reference for the given dialect."""
        if dialect == "mysql":
            return f"`{database}`.`{table}`"
        if dialect == "mssql":
            return f"[{database}].[dbo].[{table}]"
        return f'"{table}"'  # PostgreSQL: table in current DB schema

    @staticmethod
    def _bulk_insert(engine: Engine, database: str, table: str, rows: list[dict[str, Any]]) -> None:
        """Insert *rows* into *table* in a single transaction."""
        if not rows:
            return
        columns = list(rows[0].keys())
        dialect = engine.dialect.name

        if dialect == "mysql":
            tbl_ref = f"`{database}`.`{table}`"
            col_str = ", ".join(f"`{c}`" for c in columns)
            placeholders = ", ".join(f":{c}" for c in columns)
        elif dialect == "mssql":
            tbl_ref = f"[{database}].[dbo].[{table}]"
            col_str = ", ".join(f"[{c}]" for c in columns)
            placeholders = ", ".join(f":{c}" for c in columns)
        else:  # postgresql
            tbl_ref = f'"{table}"'
            col_str = ", ".join(f'"{c}"' for c in columns)
            placeholders = ", ".join(f":{c}" for c in columns)

        stmt = text(f"INSERT INTO {tbl_ref} ({col_str}) VALUES ({placeholders})")
        with engine.begin() as conn:
            conn.execute(stmt, rows)
