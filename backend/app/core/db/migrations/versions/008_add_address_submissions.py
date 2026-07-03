"""Add address_submissions moderation queue for user-submitted addresses.

Revision ID: 008
Revises: 007
Create Date: 2026-06-30 12:00:00.000000
"""

from typing import Sequence, Union

from alembic import op

revision: str = "008"
down_revision: Union[str, None] = "007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS address_submissions (
            id            BIGSERIAL PRIMARY KEY,
            postcode_raw  VARCHAR(20),
            postcode_norm VARCHAR(10),
            house_number  VARCHAR(100),
            house_name    VARCHAR(200),
            flat          VARCHAR(50),
            street        VARCHAR(200),
            city          VARCHAR(100),
            county        VARCHAR(100),
            status        VARCHAR(20) NOT NULL DEFAULT 'pending',
            review_note   TEXT,
            submitter_ip  VARCHAR(45),
            address_id    INTEGER REFERENCES addresses(id),
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            reviewed_at   TIMESTAMPTZ
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_address_submissions_status "
        "ON address_submissions (status)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_address_submissions_postcode_norm "
        "ON address_submissions (postcode_norm)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS address_submissions")
