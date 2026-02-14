"""change_osm_id_to_bigint

Revision ID: f844d3523221
Revises: 001
Create Date: 2026-02-14 04:30:29.182958
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'f844d3523221'
down_revision: Union[str, None] = '001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        'addresses', 'osm_id',
        existing_type=sa.INTEGER(),
        type_=sa.BigInteger(),
    )


def downgrade() -> None:
    op.alter_column(
        'addresses', 'osm_id',
        existing_type=sa.BigInteger(),
        type_=sa.INTEGER(),
    )
