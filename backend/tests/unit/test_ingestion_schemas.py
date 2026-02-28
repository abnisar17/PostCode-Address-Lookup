"""Unit tests for the new ingestion Pydantic schemas."""

from datetime import date

import pytest
from pydantic import ValidationError

from app.ingestion.schemas import (
    CompaniesHouseRecord,
    EPCRecord,
    FSARatingRecord,
    LandRegistryRecord,
    UPRNRecord,
)


class TestLandRegistryRecord:
    def test_valid_record(self):
        rec = LandRegistryRecord(
            transaction_id="ABC-123",
            price=250000,
            date_of_transfer=date(2024, 6, 15),
            postcode_raw="SW1A 1AA",
            postcode_norm="SW1A 1AA",
            property_type="D",
            paon="10",
            street="High Street",
            town="London",
        )
        assert rec.price == 250000
        assert rec.postcode_norm == "SW1A 1AA"

    def test_truncates_long_paon(self):
        rec = LandRegistryRecord(
            transaction_id="T1",
            price=100000,
            date_of_transfer=date(2024, 1, 1),
            paon="x" * 200,
        )
        assert len(rec.paon) == 100

    def test_requires_transaction_id(self):
        with pytest.raises(ValidationError):
            LandRegistryRecord(price=100000, date_of_transfer=date(2024, 1, 1))


class TestCompaniesHouseRecord:
    def test_valid_record(self):
        rec = CompaniesHouseRecord(
            company_number="12345678",
            company_name="Test Ltd",
            company_status="Active",
            postcode_norm="EC1A 1BB",
        )
        assert rec.company_number == "12345678"

    def test_truncates_long_company_name(self):
        rec = CompaniesHouseRecord(
            company_number="00000001",
            company_name="A" * 500,
        )
        assert len(rec.company_name) == 300


class TestFSARatingRecord:
    def test_valid_record(self):
        rec = FSARatingRecord(
            fhrs_id=123456,
            business_name="The Pizza Place",
            rating_value="5",
            postcode_norm="W1A 0AX",
        )
        assert rec.fhrs_id == 123456
        assert rec.rating_value == "5"

    def test_optional_scores(self):
        rec = FSARatingRecord(fhrs_id=1)
        assert rec.scores_hygiene is None
        assert rec.scores_structural is None
        assert rec.scores_management is None


class TestUPRNRecord:
    def test_valid_record(self):
        rec = UPRNRecord(uprn=10023456789, latitude=51.5, longitude=-0.14)
        assert rec.uprn == 10023456789

    def test_requires_all_fields(self):
        with pytest.raises(ValidationError):
            UPRNRecord(uprn=10023456789)


class TestEPCRecord:
    def test_valid_record(self):
        rec = EPCRecord(
            lmk_key="abc-123",
            uprn=10023456789,
            postcode_norm="SW1A 1AA",
            current_energy_rating="C",
            current_energy_efficiency=72,
        )
        assert rec.lmk_key == "abc-123"
        assert rec.current_energy_rating == "C"

    def test_optional_uprn(self):
        rec = EPCRecord(lmk_key="abc-123")
        assert rec.uprn is None
