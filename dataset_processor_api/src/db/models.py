"""SQLAlchemy ORM models for datasets, jobs, and reports."""

import uuid
from datetime import datetime
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


class Dataset(Base):
    """Dataset metadata persisted after uploads."""

    __tablename__ = "datasets"

    id: Mapped[uuid.UUID] = mapped_column(
        postgresql.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(sa.Text, nullable=False)
    original_filename: Mapped[str] = mapped_column(sa.Text, nullable=False)
    content_type: Mapped[str] = mapped_column(sa.Text, nullable=False)
    status: Mapped[str] = mapped_column(sa.Text, nullable=False)
    checksum_sha256: Mapped[str] = mapped_column(sa.Text, nullable=False)
    size_bytes: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    uploaded_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )
    processed_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True), nullable=True)
    row_count: Mapped[int | None] = mapped_column(sa.Integer, nullable=True)
    error: Mapped[str | None] = mapped_column(sa.Text, nullable=True)

    upload_bucket: Mapped[str] = mapped_column(sa.Text, nullable=False)
    upload_key: Mapped[str] = mapped_column(sa.Text, nullable=False)
    upload_etag: Mapped[str | None] = mapped_column(sa.Text, nullable=True)

    jobs: Mapped[list["Job"]] = relationship(back_populates="dataset", cascade="all, delete-orphan")
    report: Mapped[Optional["Report"]] = relationship(
        back_populates="dataset", uselist=False, cascade="all, delete-orphan"
    )

    __table_args__ = (
        sa.CheckConstraint(
            "status IN ('uploaded', 'processing', 'done', 'failed')",
            name="ck_datasets_status",
        ),
        sa.UniqueConstraint("checksum_sha256", name="uq_datasets_checksum_sha256"),
        sa.Index("ix_datasets_status", "status"),
        sa.Index("ix_datasets_uploaded_at", "uploaded_at"),
    )


class Job(Base):
    """Background processing job metadata."""

    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        postgresql.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    dataset_id: Mapped[uuid.UUID] = mapped_column(
        postgresql.UUID(as_uuid=True),
        sa.ForeignKey("datasets.id", ondelete="CASCADE"),
        nullable=False,
    )
    celery_task_id: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    state: Mapped[str] = mapped_column(sa.Text, nullable=False)
    progress: Mapped[int] = mapped_column(
        sa.Integer, nullable=False, default=0, server_default=sa.text("0")
    )
    queued_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )
    started_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True), nullable=True)
    error: Mapped[str | None] = mapped_column(sa.Text, nullable=True)

    dataset: Mapped[Dataset] = relationship(back_populates="jobs")

    __table_args__ = (
        sa.CheckConstraint(
            "state IN ('queued', 'started', 'retrying', 'success', 'failure')",
            name="ck_jobs_state",
        ),
        sa.CheckConstraint(
            "progress >= 0 AND progress <= 100",
            name="ck_jobs_progress",
        ),
        sa.Index("ix_jobs_dataset_id", "dataset_id"),
        sa.Index("ix_jobs_state", "state"),
        sa.Index("ix_jobs_queued_at", "queued_at"),
        sa.Index(
            "uq_jobs_active_dataset",
            "dataset_id",
            unique=True,
            postgresql_where=sa.text("state IN ('queued','started','retrying')"),
        ),
    )


class Report(Base):
    """Metadata for generated report objects in storage."""

    __tablename__ = "reports"

    id: Mapped[uuid.UUID] = mapped_column(
        postgresql.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    dataset_id: Mapped[uuid.UUID] = mapped_column(
        postgresql.UUID(as_uuid=True),
        sa.ForeignKey("datasets.id", ondelete="CASCADE"),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )
    report_bucket: Mapped[str] = mapped_column(sa.Text, nullable=False)
    report_key: Mapped[str] = mapped_column(sa.Text, nullable=False)
    report_etag: Mapped[str | None] = mapped_column(sa.Text, nullable=True)

    dataset: Mapped[Dataset] = relationship(back_populates="report")

    __table_args__ = (
        sa.UniqueConstraint("dataset_id", name="uq_reports_dataset_id"),
        sa.Index("ix_reports_created_at", "created_at"),
    )
