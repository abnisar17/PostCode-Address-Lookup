"""Add addresses.added_by_key_id to attribute API-added addresses.

Revision ID: 009
Revises: 008
Create Date: 2026-07-04 12:00:00.000000
"""

from typing import Sequence, Union

from alembic import op

revision: str = "009"
down_revision: Union[str, None] = "008"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Nullable column with no default — instant metadata change in PostgreSQL 11+.
    op.execute(
        "ALTER TABLE addresses ADD COLUMN IF NOT EXISTS "
        "added_by_key_id INTEGER REFERENCES api_keys(id)"
    )
    # Partial index — tiny (only api-added rows), so no CONCURRENTLY needed.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_addresses_added_by_key "
        "ON addresses (added_by_key_id) WHERE added_by_key_id IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_addresses_added_by_key")
    op.execute("ALTER TABLE addresses DROP COLUMN IF EXISTS added_by_key_id")
