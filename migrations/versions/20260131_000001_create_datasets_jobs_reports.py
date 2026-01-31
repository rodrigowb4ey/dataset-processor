"""create datasets jobs reports

Revision ID: 20260131_000001
Revises:
Create Date: 2026-01-31 00:00:01

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260131_000001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "datasets",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("original_filename", sa.Text(), nullable=False),
        sa.Column("content_type", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("checksum_sha256", sa.Text(), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column(
            "uploaded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("row_count", sa.Integer(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("upload_bucket", sa.Text(), nullable=False),
        sa.Column("upload_key", sa.Text(), nullable=False),
        sa.Column("upload_etag", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "status IN ('uploaded', 'processing', 'done', 'failed')",
            name="ck_datasets_status",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_datasets"),
        sa.UniqueConstraint("checksum_sha256", name="uq_datasets_checksum_sha256"),
    )
    op.create_index("ix_datasets_status", "datasets", ["status"], unique=False)
    op.create_index(
        "ix_datasets_uploaded_at", "datasets", ["uploaded_at"], unique=False
    )

    op.create_table(
        "jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("dataset_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("celery_task_id", sa.Text(), nullable=True),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column(
            "progress",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "queued_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "state IN ('queued', 'started', 'retrying', 'success', 'failure')",
            name="ck_jobs_state",
        ),
        sa.CheckConstraint(
            "progress >= 0 AND progress <= 100",
            name="ck_jobs_progress",
        ),
        sa.ForeignKeyConstraint(
            ["dataset_id"],
            ["datasets.id"],
            name="fk_jobs_dataset_id_datasets",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_jobs"),
    )
    op.create_index("ix_jobs_dataset_id", "jobs", ["dataset_id"], unique=False)
    op.create_index("ix_jobs_state", "jobs", ["state"], unique=False)
    op.create_index("ix_jobs_queued_at", "jobs", ["queued_at"], unique=False)

    op.create_table(
        "reports",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("dataset_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "report_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("report_bucket", sa.Text(), nullable=False),
        sa.Column("report_key", sa.Text(), nullable=False),
        sa.Column("report_etag", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["dataset_id"],
            ["datasets.id"],
            name="fk_reports_dataset_id_datasets",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_reports"),
        sa.UniqueConstraint("dataset_id", name="uq_reports_dataset_id"),
    )
    op.create_index("ix_reports_created_at", "reports", ["created_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_reports_created_at", table_name="reports")
    op.drop_table("reports")

    op.drop_index("ix_jobs_queued_at", table_name="jobs")
    op.drop_index("ix_jobs_state", table_name="jobs")
    op.drop_index("ix_jobs_dataset_id", table_name="jobs")
    op.drop_table("jobs")

    op.drop_index("ix_datasets_uploaded_at", table_name="datasets")
    op.drop_index("ix_datasets_status", table_name="datasets")
    op.drop_table("datasets")
