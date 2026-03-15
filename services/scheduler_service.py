"""Scheduler service — persists and manages recurring migration schedules."""
from __future__ import annotations

import sys

from core.cron_manager.cron_manager import PlatformCronFactory
from models.migration_job import ScheduledJob
from storage.control_db import get_session
from utils.logger import get_logger

logger = get_logger(__name__)


def _make_command(job_id: str) -> str:
    """Return the shell command that re-runs a migration job by ID."""
    py = sys.executable
    return f'{py} -m cli.main --run-job "{job_id}"'


class SchedulerService:
    """Creates, lists, and removes recurring cron/Task Scheduler entries.

    All operations are persisted to the ``scheduled_jobs`` table and delegated
    to the OS-appropriate :class:`CronManager`.
    """

    def __init__(self) -> None:
        self._cron = PlatformCronFactory.create()

    # ── Public API ────────────────────────────────────────────────────

    def create_scheduled_job(self, job_id: str, interval_expr: str) -> ScheduledJob:
        """Register a new recurring schedule for *job_id*.

        Args:
            job_id: The :class:`MigrationJob` job_id to schedule.
            interval_expr: Standard 5-field cron expression, e.g. ``"0 2 * * *"``.

        Returns:
            Persisted :class:`ScheduledJob` instance.
        """
        command = _make_command(job_id)
        import uuid as _uuid
        schedule_id = str(_uuid.uuid4())
        os_job_id = self._cron.add_job(schedule_id, interval_expr, command)

        with get_session() as session:
            sj = ScheduledJob(
                schedule_id=schedule_id,
                job_id=job_id,
                interval_expr=interval_expr,
                os_job_id=os_job_id,
                status="active",
            )
            session.add(sj)

        logger.info(
            f"Scheduled job created: schedule_id={schedule_id}, "
            f"job_id={job_id}, expr='{interval_expr}'"
        )
        with get_session() as session:
            return session.get(ScheduledJob, schedule_id)  # type: ignore[return-value]

    def list_scheduled_jobs(self) -> list[ScheduledJob]:
        """Return all non-deleted :class:`ScheduledJob` records."""
        with get_session() as session:
            from sqlalchemy import select
            rows = session.execute(
                select(ScheduledJob).where(ScheduledJob.status != "deleted")
            ).scalars().all()
            return list(rows)

    def stop_scheduled_job(self, schedule_id: str) -> bool:
        """Disable (pause) a scheduled job without deleting it."""
        with get_session() as session:
            sj = session.get(ScheduledJob, schedule_id)
            if not sj:
                return False
            self._cron.disable_job(sj.os_job_id)
            sj.status = "stopped"
        logger.info(f"Scheduled job stopped: {schedule_id}")
        return True

    def delete_scheduled_job(self, schedule_id: str) -> bool:
        """Permanently remove a scheduled job from cron and control DB."""
        with get_session() as session:
            sj = session.get(ScheduledJob, schedule_id)
            if not sj:
                return False
            self._cron.remove_job(sj.os_job_id)
            sj.status = "deleted"
        logger.info(f"Scheduled job deleted: {schedule_id}")
        return True
