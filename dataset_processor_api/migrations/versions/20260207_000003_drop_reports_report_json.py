"""drop reports report_json column

Revision ID: 20260207_000003
Revises: 20260206_000002
Create Date: 2026-02-07 00:00:03

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260207_000003"
down_revision = "20260206_000002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("reports", "report_json")


def downgrade() -> None:
    op.add_column(
        "reports",
        sa.Column("report_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
