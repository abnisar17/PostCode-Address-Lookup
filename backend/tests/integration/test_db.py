"""Integration tests for database models and operations.

Requires Docker (testcontainers) — skipped if unavailable.
"""

import pytest
from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.core.db.models import Address, DataSource, Postcode


@pytest.mark.integration
class TestDataSourceModel:
    def test_create_data_source(self, db_session: Session):
        ds = DataSource(source_name="codepoint", status="pending")
        db_session.add(ds)
        db_session.flush()

        assert ds.id is not None
        assert ds.source_name == "codepoint"
        assert ds.status == "pending"

    def test_unique_source_name(self, db_session: Session):
        ds1 = DataSource(source_name="osm", status="pending")
        db_session.add(ds1)
        db_session.flush()

        ds2 = DataSource(source_name="osm", status="pending")
        db_session.add(ds2)
        with pytest.raises(Exception):
            db_session.flush()


@pytest.mark.integration
class TestPostcodeModel:
    def test_create_postcode(self, db_session: Session):
        pc = Postcode(
            postcode="SW1A 1AA",
            postcode_no_space="SW1A1AA",
            latitude=51.5014,
            longitude=-0.1419,
            source="codepoint",
        )
        db_session.add(pc)
        db_session.flush()

        assert pc.id is not None

    def test_postcode_unique_constraint(self, db_session: Session):
        pc1 = Postcode(postcode="SW1A 2AA", postcode_no_space="SW1A2AA", source="test")
        db_session.add(pc1)
        db_session.flush()

        pc2 = Postcode(postcode="SW1A 2AA", postcode_no_space="SW1A2AA", source="test")
        db_session.add(pc2)
        with pytest.raises(Exception):
            db_session.flush()

    def test_upsert_on_conflict(self, db_session: Session):
        # Insert
        stmt = pg_insert(Postcode).values(
            postcode="EC1A 1BB",
            postcode_no_space="EC1A1BB",
            latitude=51.52,
            longitude=-0.10,
            source="codepoint",
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["postcode"],
            set_={"source": "updated"},
        )
        db_session.execute(stmt)
        db_session.flush()

        # Upsert
        stmt2 = pg_insert(Postcode).values(
            postcode="EC1A 1BB",
            postcode_no_space="EC1A1BB",
            latitude=51.52,
            longitude=-0.10,
            source="codepoint",
        )
        stmt2 = stmt2.on_conflict_do_update(
            index_elements=["postcode"],
            set_={"source": "merged"},
        )
        db_session.execute(stmt2)
        db_session.flush()

        result = db_session.query(Postcode).filter_by(postcode="EC1A 1BB").one()
        assert result.source == "merged"


@pytest.mark.integration
class TestAddressModel:
    def test_create_address(self, db_session: Session):
        addr = Address(
            osm_id=12345,
            osm_type="node",
            house_number="10",
            street="Downing Street",
            city="London",
            postcode_raw="SW1A 2AA",
            postcode_norm="SW1A 2AA",
            latitude=51.5033,
            longitude=-0.1276,
        )
        db_session.add(addr)
        db_session.flush()

        assert addr.id is not None

    def test_osm_unique_constraint(self, db_session: Session):
        a1 = Address(osm_id=99999, osm_type="node", latitude=51.5, longitude=-0.1)
        db_session.add(a1)
        db_session.flush()

        a2 = Address(osm_id=99999, osm_type="node", latitude=51.5, longitude=-0.1)
        db_session.add(a2)
        with pytest.raises(Exception):
            db_session.flush()

    def test_on_conflict_do_nothing(self, db_session: Session):
        stmt = pg_insert(Address).values(
            osm_id=88888,
            osm_type="way",
            street="Test Street",
            latitude=51.5,
            longitude=-0.1,
        )
        db_session.execute(stmt)
        db_session.flush()

        stmt2 = pg_insert(Address).values(
            osm_id=88888,
            osm_type="way",
            street="Different Street",
            latitude=52.0,
            longitude=-1.0,
        )
        stmt2 = stmt2.on_conflict_do_nothing(constraint="uq_addresses_osm")
        result = db_session.execute(stmt2)
        db_session.flush()

        # psycopg3 returns -1 for ON CONFLICT DO NOTHING rowcount
        assert result.rowcount in (0, -1)
        addr = db_session.query(Address).filter_by(osm_id=88888).one()
        assert addr.street == "Test Street"

    def test_fk_link_to_postcode(self, db_session: Session):
        pc = Postcode(postcode="W1A 0AX", postcode_no_space="W1A0AX", source="test")
        db_session.add(pc)
        db_session.flush()

        addr = Address(
            osm_id=77777,
            osm_type="node",
            postcode_norm="W1A 0AX",
            postcode_id=pc.id,
            latitude=51.52,
            longitude=-0.14,
        )
        db_session.add(addr)
        db_session.flush()

        assert addr.postcode_ref is not None
        assert addr.postcode_ref.postcode == "W1A 0AX"
