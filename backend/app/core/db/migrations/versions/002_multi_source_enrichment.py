"""Add multi-source support and enrichment tables.

Adds source/source_id/uprn to addresses table.
Creates price_paid, companies, food_ratings, uprn_coordinates tables.
Backfills existing OSM data with source identifiers.

Revision ID: 002
Revises: f844d3523221
Create Date: 2026-02-23 00:00:00.000000
"""

from typing import Sequence, Union

import geoalchemy2
import sqlalchemy as sa
from alembic import op

revision: str = "002"
down_revision: Union[str, None] = "f844d3523221"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── Add multi-source columns to addresses ──────────────────────
    op.add_column("addresses", sa.Column("source", sa.String(20)))
    op.add_column("addresses", sa.Column("source_id", sa.String(100)))
    op.add_column("addresses", sa.Column("uprn", sa.BigInteger))

    op.create_index("ix_addresses_uprn", "addresses", ["uprn"])
    op.create_index("ix_addresses_source", "addresses", ["source"])
    op.create_unique_constraint(
        "uq_addresses_source", "addresses", ["source", "source_id"]
    )

    # Backfill existing OSM data
    op.execute(
        "UPDATE addresses "
        "SET source = 'osm', "
        "    source_id = osm_type || ':' || osm_id "
        "WHERE osm_id IS NOT NULL AND source IS NULL"
    )

    # ── price_paid table ───────────────────────────────────────────
    op.create_table(
        "price_paid",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "transaction_id", sa.String(50), unique=True, nullable=False
        ),
        sa.Column(
            "address_id", sa.Integer, sa.ForeignKey("addresses.id"), index=True
        ),
        sa.Column("postcode_norm", sa.String(10)),
        sa.Column("price", sa.Integer, nullable=False),
        sa.Column("date_of_transfer", sa.Date),
        sa.Column("property_type", sa.String(1)),
        sa.Column("old_new", sa.String(1)),
        sa.Column("duration", sa.String(1)),
        sa.Column("paon", sa.String(100)),
        sa.Column("saon", sa.String(100)),
        sa.Column("street", sa.String(200)),
        sa.Column("locality", sa.String(100)),
        sa.Column("town", sa.String(100)),
        sa.Column("district", sa.String(100)),
        sa.Column("county", sa.String(100)),
        sa.Column("ppd_category", sa.String(1)),
        sa.Column("record_status", sa.String(1)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_price_paid_postcode_norm", "price_paid", ["postcode_norm"]
    )
    op.create_index(
        "ix_price_paid_date", "price_paid", ["date_of_transfer"]
    )

    # ── companies table ────────────────────────────────────────────
    op.create_table(
        "companies",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "company_number", sa.String(10), unique=True, nullable=False
        ),
        sa.Column("company_name", sa.String(300)),
        sa.Column("company_status", sa.String(30)),
        sa.Column("company_type", sa.String(50)),
        sa.Column("sic_code_1", sa.String(10)),
        sa.Column("sic_code_2", sa.String(10)),
        sa.Column("sic_code_3", sa.String(10)),
        sa.Column("sic_code_4", sa.String(10)),
        sa.Column("incorporation_date", sa.String(10)),
        sa.Column(
            "address_id", sa.Integer, sa.ForeignKey("addresses.id"), index=True
        ),
        sa.Column("postcode_norm", sa.String(10)),
        sa.Column("address_line_1", sa.String(200)),
        sa.Column("address_line_2", sa.String(200)),
        sa.Column("post_town", sa.String(100)),
        sa.Column("county", sa.String(100)),
        sa.Column("country", sa.String(100)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_companies_postcode_norm", "companies", ["postcode_norm"]
    )

    # ── food_ratings table ─────────────────────────────────────────
    op.create_table(
        "food_ratings",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("fhrs_id", sa.Integer, unique=True, nullable=False),
        sa.Column("business_name", sa.String(300)),
        sa.Column("business_type", sa.String(100)),
        sa.Column("business_type_id", sa.Integer),
        sa.Column("rating_value", sa.String(30)),
        sa.Column("rating_date", sa.Date),
        sa.Column(
            "address_id", sa.Integer, sa.ForeignKey("addresses.id"), index=True
        ),
        sa.Column("postcode_norm", sa.String(10)),
        sa.Column("address_line_1", sa.String(200)),
        sa.Column("address_line_2", sa.String(200)),
        sa.Column("address_line_3", sa.String(200)),
        sa.Column("address_line_4", sa.String(200)),
        sa.Column("latitude", sa.Float),
        sa.Column("longitude", sa.Float),
        sa.Column("local_authority_code", sa.String(10)),
        sa.Column("local_authority_name", sa.String(100)),
        sa.Column("scores_hygiene", sa.Integer),
        sa.Column("scores_structural", sa.Integer),
        sa.Column("scores_management", sa.Integer),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_food_ratings_postcode_norm", "food_ratings", ["postcode_norm"]
    )

    # ── uprn_coordinates table ─────────────────────────────────────
    op.create_table(
        "uprn_coordinates",
        sa.Column("uprn", sa.BigInteger, primary_key=True),
        sa.Column("latitude", sa.Float, nullable=False),
        sa.Column("longitude", sa.Float, nullable=False),
    )


def downgrade() -> None:
    op.drop_table("uprn_coordinates")
    op.drop_table("food_ratings")
    op.drop_table("companies")
    op.drop_table("price_paid")

    op.drop_constraint("uq_addresses_source", "addresses", type_="unique")
    op.drop_index("ix_addresses_source", table_name="addresses")
    op.drop_index("ix_addresses_uprn", table_name="addresses")
    op.drop_column("addresses", "uprn")
    op.drop_column("addresses", "source_id")
    op.drop_column("addresses", "source")
