"""SQLAlchemy ORM models for the control database tables."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    """Shared declarative base for all ORM models."""


class MigrationJob(Base):
    """Represents a single table-level migration job."""

    __tablename__ = "migration_jobs"

    job_id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    source_engine = Column(String, nullable=False)
    destination_engine = Column(String, nullable=False)
    source_host = Column(String, nullable=False)
    source_database = Column(String, nullable=False)
    destination_host = Column(String, nullable=False)
    destination_database = Column(String, nullable=False)
    table_name = Column(String, nullable=False)
    total_rows = Column(Integer, default=0)
    converted_rows = Column(Integer, default=0)
    last_processed_id = Column(Integer, default=0)
    status = Column(
        String, default="pending"
    )  # pending | running | paused | completed | failed
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    batch_size = Column(Integer, default=1000)

    # Relationship
    scheduled_jobs = relationship(
        "ScheduledJob", back_populates="migration_job", cascade="all, delete-orphan"
    )

    @property
    def progress_pct(self) -> float:
        """Return completion percentage (0–100)."""
        if not self.total_rows:
            return 0.0
        return round(self.converted_rows / self.total_rows * 100, 2)

    def __repr__(self) -> str:
        return (
            f"<MigrationJob job_id={self.job_id!r} table={self.table_name!r} "
            f"status={self.status!r} progress={self.progress_pct}%>"
        )


class ScheduledJob(Base):
    """Represents a recurring schedule for a migration job."""

    __tablename__ = "scheduled_jobs"

    schedule_id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    job_id = Column(String, ForeignKey("migration_jobs.job_id"), nullable=False)
    interval_expr = Column(String, nullable=False)  # cron expression or human interval
    os_job_id = Column(String, nullable=True)       # cron line hash or Task Scheduler task name
    status = Column(String, default="active")        # active | stopped | deleted
    created_at = Column(DateTime, default=func.now())

    # Relationship
    migration_job = relationship("MigrationJob", back_populates="scheduled_jobs")

    def __repr__(self) -> str:
        return (
            f"<ScheduledJob schedule_id={self.schedule_id!r} "
            f"job_id={self.job_id!r} interval={self.interval_expr!r} "
            f"status={self.status!r}>"
        )
