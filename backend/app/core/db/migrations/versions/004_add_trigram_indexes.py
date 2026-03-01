"""Add pg_trgm extension and trigram GIN indexes for address search.

Revision ID: 004
Revises: 003
Create Date: 2026-03-01 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op

revision: str = "004"
down_revision: Union[str, None] = "003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.execute(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_addresses_street_trgm "
        "ON addresses USING gin (street gin_trgm_ops)"
    )
    op.execute(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_addresses_city_trgm "
        "ON addresses USING gin (city gin_trgm_ops)"
    )
    op.execute(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_addresses_house_name_trgm "
        "ON addresses USING gin (house_name gin_trgm_ops)"
    )
    op.execute(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_addresses_suburb_trgm "
        "ON addresses USING gin (suburb gin_trgm_ops)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_addresses_suburb_trgm")
    op.execute("DROP INDEX IF EXISTS ix_addresses_house_name_trgm")
    op.execute("DROP INDEX IF EXISTS ix_addresses_city_trgm")
    op.execute("DROP INDEX IF EXISTS ix_addresses_street_trgm")
