"""Entry point for the Universal Database Converter CLI."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# ── Ensure project root is on sys.path when run directly ─────────────────────
_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv()

from storage.control_db import init_db
from utils.logger import get_logger

logger = get_logger("db-converter")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="db-converter",
        description="Universal Database Converter CLI — migrate between MySQL, MSSQL, and PostgreSQL",
    )
    parser.add_argument(
        "--run-job",
        metavar="JOB_ID",
        help="Re-run a specific migration job by ID (used by cron scheduler).",
    )
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 1.0.0",
    )
    return parser.parse_args()


def _run_scheduled_job(job_id: str) -> None:
    """Re-execute a specific job — called by cron/Task Scheduler."""
    from storage.control_db import get_session
    from models.migration_job import MigrationJob
    from models.database_config import DatabaseConfig
    from services.migration_service import MigrationConfig, MigrationService

    with get_session() as session:
        job = session.get(MigrationJob, job_id)

    if not job:
        logger.error(f"Job {job_id!r} not found in control DB.")
        sys.exit(1)

    src = DatabaseConfig(
        engine=job.source_engine,
        host=job.source_host,
        port=_default_port(job.source_engine),
        username="",   # credentials not stored — user must supply via env vars
        password="",
        database=job.source_database,
    )
    dst = DatabaseConfig(
        engine=job.destination_engine,
        host=job.destination_host,
        port=_default_port(job.destination_engine),
        username="",
        password="",
        database=job.destination_database,
    )
    cfg = MigrationConfig(
        source=src,
        destination=dst,
        tables=[job.table_name],
        job_id_prefix=job_id,
    )
    svc = MigrationService()
    svc.run(cfg)


def _default_port(engine: str) -> int:
    return {"mysql": 3306, "mssql": 1433, "postgresql": 5432, "postgres": 5432}.get(
        engine.lower(), 5432
    )


def main() -> None:
    """Application entry point."""
    args = _parse_args()

    logger.info("Universal Database Converter CLI starting…")
    init_db()

    if args.run_job:
        _run_scheduled_job(args.run_job)
        return

    # Launch interactive menu
    from cli.menu import Menu
    menu = Menu()
    menu.run()


if __name__ == "__main__":
    main()
