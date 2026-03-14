"""Add duplicate_of column for soft-mark deduplication.

Revision ID: 006
Revises: 005
Create Date: 2026-03-14 12:00:00.000000
"""

from typing import Sequence, Union

from alembic import op

revision: str = "006"
down_revision: Union[str, None] = "005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Adding a nullable column with no default is instant in PostgreSQL 11+
    op.execute(
        "ALTER TABLE addresses ADD COLUMN IF NOT EXISTS "
        "duplicate_of INTEGER REFERENCES addresses(id)"
    )
    # Partial index for fast filtering of non-duplicate rows
    op.execute(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_addresses_not_duplicate "
        "ON addresses (id) WHERE duplicate_of IS NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_addresses_not_duplicate")
    op.execute("ALTER TABLE addresses DROP COLUMN IF EXISTS duplicate_of")
