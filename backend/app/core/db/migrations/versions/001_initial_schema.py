"""Initial schema — data_sources, postcodes, addresses.

Revision ID: 001
Revises:
Create Date: 2025-01-01 00:00:00.000000
"""
from typing import Sequence, Union

import geoalchemy2
import sqlalchemy as sa
from alembic import op

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Enable PostGIS
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")

    # data_sources
    op.create_table(
        "data_sources",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("source_name", sa.String(50), unique=True, nullable=False),
        sa.Column("file_hash", sa.String(64)),
        sa.Column("record_count", sa.Integer),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("error_message", sa.Text),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
    )

    # postcodes
    op.create_table(
        "postcodes",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("postcode", sa.String(10), unique=True, nullable=False),
        sa.Column("postcode_no_space", sa.String(10), nullable=False),
        sa.Column(
            "location",
            geoalchemy2.Geometry("POINT", srid=4326, from_text="ST_GeomFromEWKT"),
        ),
        sa.Column("latitude", sa.Float),
        sa.Column("longitude", sa.Float),
        sa.Column("easting", sa.Integer),
        sa.Column("northing", sa.Integer),
        sa.Column("country_code", sa.String(10)),
        sa.Column("region_code", sa.String(10)),
        sa.Column("local_authority", sa.String(10)),
        sa.Column("parliamentary_const", sa.String(10)),
        sa.Column("ward_code", sa.String(10)),
        sa.Column("parish_code", sa.String(10)),
        sa.Column("positional_quality", sa.Integer),
        sa.Column("is_terminated", sa.Boolean, server_default=sa.text("false")),
        sa.Column("date_introduced", sa.String(10)),
        sa.Column("date_terminated", sa.String(10)),
        sa.Column("source", sa.String(20)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
    )
    op.create_index(
        "ix_postcodes_postcode_no_space", "postcodes", ["postcode_no_space"]
    )
    op.create_index(
        "ix_postcodes_location",
        "postcodes",
        ["location"],
        postgresql_using="gist",
    )

    # addresses
    op.create_table(
        "addresses",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("postcode_id", sa.Integer, sa.ForeignKey("postcodes.id"), index=True),
        sa.Column("postcode_raw", sa.String(20)),
        sa.Column("postcode_norm", sa.String(10)),
        sa.Column("house_number", sa.String(50)),
        sa.Column("house_name", sa.String(200)),
        sa.Column("flat", sa.String(50)),
        sa.Column("street", sa.String(200)),
        sa.Column("suburb", sa.String(100)),
        sa.Column("city", sa.String(100)),
        sa.Column("county", sa.String(100)),
        sa.Column(
            "location",
            geoalchemy2.Geometry("POINT", srid=4326, from_text="ST_GeomFromEWKT"),
        ),
        sa.Column("latitude", sa.Float),
        sa.Column("longitude", sa.Float),
        sa.Column("osm_id", sa.BigInteger),
        sa.Column("osm_type", sa.String(10)),
        sa.Column("confidence", sa.Float, server_default=sa.text("0.0")),
        sa.Column("is_complete", sa.Boolean, server_default=sa.text("false")),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
        sa.UniqueConstraint("osm_id", "osm_type", name="uq_addresses_osm"),
    )
    op.create_index("ix_addresses_postcode_norm", "addresses", ["postcode_norm"])
    op.create_index(
        "ix_addresses_location",
        "addresses",
        ["location"],
        postgresql_using="gist",
    )


def downgrade() -> None:
    op.drop_table("addresses")
    op.drop_table("postcodes")
    op.drop_table("data_sources")
