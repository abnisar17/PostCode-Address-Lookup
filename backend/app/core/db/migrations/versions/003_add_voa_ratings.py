"""Add VOA non-domestic rating list table.

Revision ID: 003
Revises: 002
Create Date: 2026-02-24 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "003"
down_revision: Union[str, None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "voa_ratings",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("uarn", sa.BigInteger, unique=True, nullable=False),
        sa.Column("billing_authority_code", sa.String(10)),
        sa.Column("description_code", sa.String(10)),
        sa.Column("description_text", sa.String(60)),
        sa.Column("firm_name", sa.String(200)),
        sa.Column(
            "address_id",
            sa.Integer,
            sa.ForeignKey("addresses.id"),
            index=True,
        ),
        sa.Column("postcode_norm", sa.String(10)),
        sa.Column("number_or_name", sa.String(100)),
        sa.Column("street", sa.String(200)),
        sa.Column("town", sa.String(100)),
        sa.Column("postal_district", sa.String(100)),
        sa.Column("county", sa.String(100)),
        sa.Column("sub_street_1", sa.String(100)),
        sa.Column("sub_street_2", sa.String(100)),
        sa.Column("sub_street_3", sa.String(100)),
        sa.Column("rateable_value", sa.Integer),
        sa.Column("effective_date", sa.String(11)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
    )
    op.create_index("ix_voa_ratings_postcode_norm", "voa_ratings", ["postcode_norm"])


def downgrade() -> None:
    op.drop_index("ix_voa_ratings_postcode_norm")
    op.drop_table("voa_ratings")
