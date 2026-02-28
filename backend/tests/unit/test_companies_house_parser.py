"""Unit tests for the Companies House parser."""

import csv
import io
import zipfile
from pathlib import Path

import pytest

from app.core.exceptions import ParseError
from app.ingestion.companies_house import parse_companies_house
from app.ingestion.schemas import CompaniesHouseRecord

# Standard Companies House CSV header (simplified to the fields we use)
_CH_HEADERS = [
    "CompanyNumber", "CompanyName", "CompanyCategory", "CompanyStatus",
    "SICCode.SicText_1", "SICCode.SicText_2", "SICCode.SicText_3", "SICCode.SicText_4",
    "IncorporationDate",
    "RegAddress.AddressLine1", "RegAddress.AddressLine2",
    "RegAddress.PostTown", "RegAddress.County", "RegAddress.Country",
    "RegAddress.PostCode",
]


def _make_ch_zip(tmp_path: Path, rows: list[dict]) -> Path:
    """Create a ZIP containing a Companies House CSV."""
    zip_path = tmp_path / "companies.zip"
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=_CH_HEADERS)
    writer.writeheader()
    writer.writerows(rows)

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("BasicCompanyDataAsOneFile.csv", csv_buffer.getvalue())

    return zip_path


def _make_valid_row(**overrides) -> dict:
    base = {
        "CompanyNumber": "12345678",
        "CompanyName": "Test Ltd",
        "CompanyCategory": "Private Limited Company",
        "CompanyStatus": "Active",
        "SICCode.SicText_1": "62020 - Information technology consultancy",
        "SICCode.SicText_2": "",
        "SICCode.SicText_3": "",
        "SICCode.SicText_4": "",
        "IncorporationDate": "15/03/2020",
        "RegAddress.AddressLine1": "10 High Street",
        "RegAddress.AddressLine2": "Suite 5",
        "RegAddress.PostTown": "London",
        "RegAddress.County": "Greater London",
        "RegAddress.Country": "United Kingdom",
        "RegAddress.PostCode": "SW1A 1AA",
    }
    base.update(overrides)
    return base


class TestParseCompaniesHouse:
    def test_parses_valid_row(self, tmp_path):
        zip_path = _make_ch_zip(tmp_path, [_make_valid_row()])

        batches = list(parse_companies_house(zip_path, batch_size=100))
        assert len(batches) == 1
        assert len(batches[0]) == 1

        rec = batches[0][0]
        assert isinstance(rec, CompaniesHouseRecord)
        assert rec.company_number == "12345678"
        assert rec.company_name == "Test Ltd"
        assert rec.postcode_norm == "SW1A 1AA"
        assert rec.sic_code_1 == "62020"
        assert rec.incorporation_date == "2020-03-15"

    def test_skips_invalid_postcode(self, tmp_path):
        zip_path = _make_ch_zip(tmp_path, [
            _make_valid_row(**{"RegAddress.PostCode": "INVALID"}),
        ])

        batches = list(parse_companies_house(zip_path, batch_size=100))
        total = sum(len(b) for b in batches)
        assert total == 0

    def test_skips_old_dissolved(self, tmp_path):
        zip_path = _make_ch_zip(tmp_path, [
            _make_valid_row(
                CompanyStatus="Dissolved",
                IncorporationDate="01/01/2000",
            ),
        ])

        batches = list(parse_companies_house(
            zip_path, batch_size=100, skip_old_dissolved=True
        ))
        total = sum(len(b) for b in batches)
        assert total == 0

    def test_keeps_recent_dissolved(self, tmp_path):
        zip_path = _make_ch_zip(tmp_path, [
            _make_valid_row(
                CompanyStatus="Dissolved",
                IncorporationDate="01/01/2020",
            ),
        ])

        batches = list(parse_companies_house(
            zip_path, batch_size=100, skip_old_dissolved=True
        ))
        total = sum(len(b) for b in batches)
        assert total == 1

    def test_batching(self, tmp_path):
        rows = [
            _make_valid_row(CompanyNumber=f"{10000000 + i}")
            for i in range(5)
        ]
        zip_path = _make_ch_zip(tmp_path, rows)

        batches = list(parse_companies_house(zip_path, batch_size=2))
        total = sum(len(b) for b in batches)
        assert total == 5

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(ParseError):
            list(parse_companies_house(tmp_path / "missing.zip"))

    def test_corrupt_zip_raises(self, tmp_path):
        bad_zip = tmp_path / "bad.zip"
        bad_zip.write_text("not a zip")

        with pytest.raises(ParseError):
            list(parse_companies_house(bad_zip))

    def test_extracts_sic_codes(self, tmp_path):
        zip_path = _make_ch_zip(tmp_path, [
            _make_valid_row(**{
                "SICCode.SicText_1": "62020 - IT consultancy",
                "SICCode.SicText_2": "70100 - Activities of head offices",
            }),
        ])

        batches = list(parse_companies_house(zip_path, batch_size=100))
        rec = batches[0][0]
        assert rec.sic_code_1 == "62020"
        assert rec.sic_code_2 == "70100"
