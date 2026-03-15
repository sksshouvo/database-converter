"""OS-aware cron/Task Scheduler manager for recurring migration jobs."""
from __future__ import annotations

import hashlib
import subprocess
import sys
from abc import ABC, abstractmethod

from utils.logger import get_logger

logger = get_logger(__name__)


# ── Abstract Base ──────────────────────────────────────────────────────────────

class CronManager(ABC):
    """Abstract interface for platform-specific job schedulers."""

    @abstractmethod
    def add_job(self, schedule_id: str, cron_expr: str, command: str) -> str:
        """Register a new scheduled job.

        Args:
            schedule_id: Unique identifier for this schedule.
            cron_expr: Cron expression (5-field standard).
            command: Shell command to execute.

        Returns:
            OS-level job identifier (cron hash or task name).
        """

    @abstractmethod
    def list_jobs(self) -> list[dict]:
        """Return a list of all managed scheduled jobs."""

    @abstractmethod
    def remove_job(self, os_job_id: str) -> bool:
        """Remove the scheduled job identified by *os_job_id*.

        Returns True on success.
        """

    @abstractmethod
    def enable_job(self, os_job_id: str) -> bool:
        """Enable (un-comment) a previously disabled job."""

    @abstractmethod
    def disable_job(self, os_job_id: str) -> bool:
        """Disable (comment-out) a job without deleting it."""


# ── Linux / macOS cron ────────────────────────────────────────────────────────

class LinuxCronManager(CronManager):
    """Manages cron jobs via *python-crontab* on Linux and macOS."""

    _COMMENT_PREFIX = "db-converter"

    def __init__(self) -> None:
        try:
            from crontab import CronTab  # type: ignore[import]
            self._CronTab = CronTab
        except ImportError:
            raise RuntimeError(
                "python-crontab is required for Linux/Mac scheduling. "
                "Install it with: pip install python-crontab"
            )

    def _cron(self):  # type: ignore[return]
        return self._CronTab(user=True)

    def _job_comment(self, schedule_id: str) -> str:
        return f"{self._COMMENT_PREFIX}:{schedule_id}"

    def add_job(self, schedule_id: str, cron_expr: str, command: str) -> str:
        cron = self._cron()
        job = cron.new(command=command, comment=self._job_comment(schedule_id))
        job.setall(cron_expr)
        cron.write()
        os_job_id = hashlib.md5(f"{cron_expr}:{command}".encode()).hexdigest()[:12]
        logger.info(f"Cron job added: '{cron_expr}' → {command}")
        return os_job_id

    def list_jobs(self) -> list[dict]:
        cron = self._cron()
        jobs = []
        for job in cron:
            if job.comment.startswith(self._COMMENT_PREFIX):
                jobs.append(
                    {
                        "comment": job.comment,
                        "schedule": str(job.slices),
                        "command": job.command,
                        "enabled": job.is_enabled(),
                    }
                )
        return jobs

    def remove_job(self, os_job_id: str) -> bool:
        cron = self._cron()
        removed = False
        for job in cron:
            if job.comment.startswith(self._COMMENT_PREFIX) and os_job_id in job.comment:
                cron.remove(job)
                removed = True
        cron.write()
        return removed

    def enable_job(self, os_job_id: str) -> bool:
        cron = self._cron()
        for job in cron:
            if os_job_id in job.comment:
                job.enable()
                cron.write()
                return True
        return False

    def disable_job(self, os_job_id: str) -> bool:
        cron = self._cron()
        for job in cron:
            if os_job_id in job.comment:
                job.enable(False)
                cron.write()
                return True
        return False


# ── Windows Task Scheduler ────────────────────────────────────────────────────

class WindowsTaskManager(CronManager):
    """Manages Windows Task Scheduler tasks via ``schtasks.exe``."""

    _TASK_PREFIX = "DbConverter"

    def _task_name(self, schedule_id: str) -> str:
        safe = schedule_id.replace(":", "_").replace("-", "_")
        return f"{self._TASK_PREFIX}_{safe}"

    def add_job(self, schedule_id: str, cron_expr: str, command: str) -> str:
        # Translate cron expression to schtasks /SC and /ST parameters (simplified)
        task_name = self._task_name(schedule_id)
        parts = cron_expr.strip().split()
        minute = parts[0] if len(parts) > 0 else "0"
        hour = parts[1] if len(parts) > 1 else "*"

        if hour == "*":
            sc = "MINUTE"
            mo = minute if minute != "*" else "1"
            cmd = [
                "schtasks", "/Create", "/F",
                "/TN", task_name,
                "/TR", command,
                "/SC", sc,
                "/MO", mo,
            ]
        else:
            sc = "DAILY"
            st = f"{hour.zfill(2)}:{minute.zfill(2)}"
            cmd = [
                "schtasks", "/Create", "/F",
                "/TN", task_name,
                "/TR", command,
                "/SC", sc,
                "/ST", st,
            ]

        subprocess.run(cmd, check=True, capture_output=True)
        logger.info(f"Windows Task created: {task_name}")
        return task_name

    def list_jobs(self) -> list[dict]:
        result = subprocess.run(
            ["schtasks", "/Query", "/FO", "CSV", "/V"],
            capture_output=True, text=True
        )
        jobs = []
        for line in result.stdout.splitlines():
            if self._TASK_PREFIX in line:
                jobs.append({"raw": line})
        return jobs

    def remove_job(self, os_job_id: str) -> bool:
        result = subprocess.run(
            ["schtasks", "/Delete", "/F", "/TN", os_job_id],
            capture_output=True, text=True
        )
        return result.returncode == 0

    def enable_job(self, os_job_id: str) -> bool:
        result = subprocess.run(
            ["schtasks", "/Change", "/TN", os_job_id, "/ENABLE"],
            capture_output=True, text=True
        )
        return result.returncode == 0

    def disable_job(self, os_job_id: str) -> bool:
        result = subprocess.run(
            ["schtasks", "/Change", "/TN", os_job_id, "/DISABLE"],
            capture_output=True, text=True
        )
        return result.returncode == 0


# ── Factory ───────────────────────────────────────────────────────────────────

class PlatformCronFactory:
    """Creates the appropriate :class:`CronManager` for the current OS."""

    @staticmethod
    def create() -> CronManager:
        """Return a :class:`LinuxCronManager` on Linux/Mac or :class:`WindowsTaskManager` on Windows."""
        if sys.platform.startswith("win"):
            return WindowsTaskManager()
        return LinuxCronManager()
