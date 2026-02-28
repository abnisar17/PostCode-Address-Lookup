"""Unit tests for the Land Registry Price Paid parser."""

import csv
import tempfile
from pathlib import Path

import pytest

from app.core.exceptions import ParseError
from app.ingestion.land_registry import parse_land_registry
from app.ingestion.schemas import LandRegistryRecord


def _write_csv(rows: list[list[str]], path: Path) -> None:
    """Write rows to a headerless CSV file (Land Registry format)."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(rows)


def _make_valid_row(
    *,
    transaction_id: str = "{ABC-123-DEF}",
    price: str = "250000",
    date: str = "2024-06-15",
    postcode: str = "SW1A 1AA",
    property_type: str = "D",
    old_new: str = "N",
    duration: str = "F",
    paon: str = "10",
    saon: str = "",
    street: str = "HIGH STREET",
    locality: str = "",
    town: str = "LONDON",
    district: str = "WESTMINSTER",
    county: str = "GREATER LONDON",
    ppd_category: str = "A",
    record_status: str = "A",
) -> list[str]:
    return [
        transaction_id, price, date, postcode, property_type,
        old_new, duration, paon, saon, street, locality, town,
        district, county, ppd_category, record_status,
    ]


class TestParseLandRegistry:
    def test_parses_valid_row(self, tmp_path):
        csv_path = tmp_path / "pp.csv"
        _write_csv([_make_valid_row()], csv_path)

        batches = list(parse_land_registry(csv_path, batch_size=100))
        assert len(batches) == 1
        assert len(batches[0]) == 1

        rec = batches[0][0]
        assert isinstance(rec, LandRegistryRecord)
        assert rec.transaction_id == "ABC-123-DEF"
        assert rec.price == 250000
        assert rec.postcode_norm == "SW1A 1AA"
        assert rec.property_type == "D"

    def test_skips_zero_price(self, tmp_path):
        csv_path = tmp_path / "pp.csv"
        _write_csv([_make_valid_row(price="0")], csv_path)

        batches = list(parse_land_registry(csv_path, batch_size=100))
        assert batches == [] or all(len(b) == 0 for b in batches)

    def test_skips_invalid_postcode(self, tmp_path):
        csv_path = tmp_path / "pp.csv"
        _write_csv([_make_valid_row(postcode="INVALID")], csv_path)

        batches = list(parse_land_registry(csv_path, batch_size=100))
        assert batches == [] or all(len(b) == 0 for b in batches)

    def test_skips_invalid_date(self, tmp_path):
        csv_path = tmp_path / "pp.csv"
        _write_csv([_make_valid_row(date="not-a-date")], csv_path)

        batches = list(parse_land_registry(csv_path, batch_size=100))
        assert batches == [] or all(len(b) == 0 for b in batches)

    def test_skips_short_rows(self, tmp_path):
        csv_path = tmp_path / "pp.csv"
        _write_csv([["only", "three", "cols"]], csv_path)

        batches = list(parse_land_registry(csv_path, batch_size=100))
        assert batches == [] or all(len(b) == 0 for b in batches)

    def test_batching(self, tmp_path):
        csv_path = tmp_path / "pp.csv"
        rows = [
            _make_valid_row(
                transaction_id=f"{{TX-{i}}}",
                price=str(100000 + i * 10000),
            )
            for i in range(5)
        ]
        _write_csv(rows, csv_path)

        batches = list(parse_land_registry(csv_path, batch_size=2))
        total_records = sum(len(b) for b in batches)
        assert total_records == 5
        assert len(batches) == 3  # 2, 2, 1

    def test_file_not_found_raises(self, tmp_path):
        missing = tmp_path / "nope.csv"
        with pytest.raises(ParseError):
            list(parse_land_registry(missing))

    def test_normalises_postcode(self, tmp_path):
        csv_path = tmp_path / "pp.csv"
        _write_csv([_make_valid_row(postcode="sw1a1aa")], csv_path)

        batches = list(parse_land_registry(csv_path, batch_size=100))
        assert batches[0][0].postcode_norm == "SW1A 1AA"

    def test_multiple_rows(self, tmp_path):
        csv_path = tmp_path / "pp.csv"
        rows = [
            _make_valid_row(transaction_id="{TX-001}", price="200000"),
            _make_valid_row(transaction_id="{TX-002}", price="300000", property_type="F"),
            _make_valid_row(transaction_id="{TX-003}", postcode="INVALID"),  # skipped
        ]
        _write_csv(rows, csv_path)

        batches = list(parse_land_registry(csv_path, batch_size=100))
        total = sum(len(b) for b in batches)
        assert total == 2
