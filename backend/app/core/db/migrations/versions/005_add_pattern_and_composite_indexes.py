"""Add varchar_pattern_ops index for postcode autocomplete and composite
index for postcode address listing sort.

Revision ID: 005
Revises: 004
Create Date: 2026-03-01 12:00:00.000000
"""

from typing import Sequence, Union

from alembic import op

revision: str = "005"
down_revision: Union[str, None] = "004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # varchar_pattern_ops allows B-tree prefix matching (LIKE 'SW1A%')
    op.execute(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_postcodes_postcode_no_space_pattern "
        "ON postcodes (postcode_no_space varchar_pattern_ops)"
    )
    # Composite index covers WHERE postcode_id = ? ORDER BY street, house_number
    op.execute(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_addresses_postcode_street_house "
        "ON addresses (postcode_id, street, house_number)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_addresses_postcode_street_house")
    op.execute("DROP INDEX IF EXISTS ix_postcodes_postcode_no_space_pattern")
