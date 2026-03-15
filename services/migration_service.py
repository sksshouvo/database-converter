"""Top-level migration orchestration service."""
from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text

from core.connectors.connector_factory import ConnectorFactory
from core.data_migrator.parallel_migrator import ParallelMigrator
from core.schema_mapper.dependency_resolver import DependencyResolver
from core.schema_mapper.schema_builder import SchemaBuilder
from core.schema_mapper.schema_extractor import SchemaExtractor
from core.validators.connection_validator import validate_connection
from core.validators.data_validator import validate_migration
from core.validators.schema_validator import validate_schema_compatibility
from models.database_config import DatabaseConfig
from models.migration_job import MigrationJob
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class MigrationConfig:
    source: DatabaseConfig
    destination: DatabaseConfig
    tables: list[str] | None = None  # None = all tables
    max_workers: int = 4
    batch_size: int = 1000
    run_validation: bool = True
    job_id_prefix: str | None = None


class MigrationService:
    """Top-level orchestrator for a complete database migration."""

    def run(self, cfg: MigrationConfig) -> dict[str, MigrationJob]:
        """Execute the full migration pipeline.

        Steps:
        1. Validate connections
        2. Extract source schema
        3. Validate schema compatibility
        4. Resolve table dependency order
        5. Build & apply DDL on destination
        6. Run parallel data migration
        7. Run post-migration validation

        Args:
            cfg: A :class:`MigrationConfig` describing source, destination, and options.

        Returns:
            Mapping of table name → :class:`MigrationJob` for every migrated table.
        """
        logger.info("=== Migration pipeline started ===")

        # ── 1. Validate connections ───────────────────────────────────
        for label, db_cfg in [("Source", cfg.source), ("Destination", cfg.destination)]:
            ok, err = validate_connection(db_cfg)
            if not ok:
                raise ConnectionError(f"{label} connection failed: {err}")
        logger.info("✅ Both connections validated.")

        # ── 2. Connectors ─────────────────────────────────────────────
        src_conn = ConnectorFactory.create(cfg.source)
        dst_conn = ConnectorFactory.create(cfg.destination)

        # ── 3. Schema extraction ──────────────────────────────────────
        src_db = cfg.source.database or ""
        dst_db = cfg.destination.database or ""
        extractor = SchemaExtractor(src_conn)

        if cfg.tables:
            schemas = [extractor.extract_table(src_db, t) for t in cfg.tables]
        else:
            schemas = extractor.extract_all(src_db)

        logger.info(f"Extracted schema for {len(schemas)} table(s).")

        # ── 4. Schema compatibility check ─────────────────────────────
        validation_result = validate_schema_compatibility(
            schemas, cfg.source.engine, cfg.destination.engine
        )
        if validation_result.warnings:
            for w in validation_result.warnings:
                logger.warning(w)

        # ── 5. Dependency resolution ──────────────────────────────────
        resolver = DependencyResolver()
        ordered_tables = resolver.resolve(schemas)
        logger.info(f"Table migration order: {ordered_tables}")

        # ── 6. Apply schema on destination ────────────────────────────
        builder = SchemaBuilder(cfg.source.engine, cfg.destination.engine)
        schema_map = {s.table_name: s for s in schemas}
        dst_engine = dst_conn.connect()

        with dst_engine.begin() as conn:
            for table_name in ordered_tables:
                schema = schema_map[table_name]
                ddl = builder.build_create_table(schema)
                logger.info(f"Creating table: {table_name}")
                try:
                    conn.execute(text(ddl))
                except Exception as exc:
                    logger.warning(f"Table '{table_name}' DDL error (may already exist): {exc}")

        # Apply indexes after all tables created
        with dst_engine.begin() as conn:
            for table_name in ordered_tables:
                schema = schema_map[table_name]
                for idx_ddl in builder.build_indexes(schema):
                    try:
                        conn.execute(text(idx_ddl))
                    except Exception as exc:
                        logger.warning(f"Index DDL warning for '{table_name}': {exc}")

        # Apply FK constraints last
        with dst_engine.begin() as conn:
            for table_name in ordered_tables:
                schema = schema_map[table_name]
                for fk_ddl in builder.build_foreign_keys(schema):
                    try:
                        conn.execute(text(fk_ddl))
                    except Exception as exc:
                        logger.warning(f"FK DDL warning for '{table_name}': {exc}")

        logger.info("✅ Destination schema applied.")

        # ── 7. Parallel data migration ────────────────────────────────
        migrator = ParallelMigrator(
            src_connector=src_conn,
            dst_connector=dst_conn,
            source_db=src_db,
            dest_db=dst_db,
            tables=ordered_tables,
            max_workers=cfg.max_workers,
            batch_size=cfg.batch_size,
            job_id_prefix=cfg.job_id_prefix,
        )
        jobs = migrator.migrate_all()

        # ── 8. Post-migration validation ──────────────────────────────
        if cfg.run_validation:
            logger.info("Running post-migration validation …")
            for table_name in ordered_tables:
                vr = validate_migration(src_conn, dst_conn, src_db, dst_db, table_name)
                if vr.match:
                    logger.info(f"✅ {table_name}: validation OK")
                else:
                    logger.warning(f"⚠️  {table_name}: {'; '.join(vr.details)}")

        logger.info("=== Migration pipeline complete ===")
        return jobs
