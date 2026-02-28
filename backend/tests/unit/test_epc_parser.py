"""Unit tests for the EPC parser."""

import csv
from pathlib import Path

import pytest

from app.core.exceptions import ParseError
from app.ingestion.epc import parse_epc
from app.ingestion.schemas import EPCRecord

# Standard EPC CSV header (simplified to fields we use)
_EPC_HEADERS = [
    "LMK_KEY", "UPRN", "ADDRESS1", "ADDRESS2", "ADDRESS3",
    "POSTTOWN", "POSTCODE", "COUNTY",
    "CURRENT_ENERGY_RATING", "CURRENT_ENERGY_EFFICIENCY",
    "PROPERTY_TYPE", "BUILT_FORM", "TOTAL_FLOOR_AREA", "LODGEMENT_DATE",
]


def _write_epc_csv(path: Path, rows: list[dict]) -> None:
    """Write an EPC CSV with header and data rows."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_EPC_HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def _make_valid_row(**overrides) -> dict:
    base = {
        "LMK_KEY": "abc123-def456",
        "UPRN": "10023456789",
        "ADDRESS1": "10 High Street",
        "ADDRESS2": "",
        "ADDRESS3": "",
        "POSTTOWN": "London",
        "POSTCODE": "SW1A 1AA",
        "COUNTY": "Greater London",
        "CURRENT_ENERGY_RATING": "C",
        "CURRENT_ENERGY_EFFICIENCY": "72",
        "PROPERTY_TYPE": "House",
        "BUILT_FORM": "Detached",
        "TOTAL_FLOOR_AREA": "95.5",
        "LODGEMENT_DATE": "2024-01-15",
    }
    base.update(overrides)
    return base


class TestParseEPC:
    def test_parses_valid_row(self, tmp_path):
        epc_dir = tmp_path / "epc"
        epc_dir.mkdir()
        _write_epc_csv(epc_dir / "local_authority_001.csv", [_make_valid_row()])

        batches = list(parse_epc(epc_dir, batch_size=100))
        assert len(batches) == 1
        assert len(batches[0]) == 1

        rec = batches[0][0]
        assert isinstance(rec, EPCRecord)
        assert rec.lmk_key == "abc123-def456"
        assert rec.uprn == 10023456789
        assert rec.postcode_norm == "SW1A 1AA"
        assert rec.current_energy_rating == "C"
        assert rec.current_energy_efficiency == 72
        assert rec.total_floor_area == pytest.approx(95.5)

    def test_skips_invalid_postcode(self, tmp_path):
        epc_dir = tmp_path / "epc"
        epc_dir.mkdir()
        _write_epc_csv(epc_dir / "test.csv", [
            _make_valid_row(POSTCODE="INVALID"),
        ])

        batches = list(parse_epc(epc_dir, batch_size=100))
        total = sum(len(b) for b in batches)
        assert total == 0

    def test_skips_missing_lmk_key(self, tmp_path):
        epc_dir = tmp_path / "epc"
        epc_dir.mkdir()
        _write_epc_csv(epc_dir / "test.csv", [
            _make_valid_row(LMK_KEY=""),
        ])

        batches = list(parse_epc(epc_dir, batch_size=100))
        total = sum(len(b) for b in batches)
        assert total == 0

    def test_handles_missing_uprn(self, tmp_path):
        epc_dir = tmp_path / "epc"
        epc_dir.mkdir()
        _write_epc_csv(epc_dir / "test.csv", [
            _make_valid_row(UPRN=""),
        ])

        batches = list(parse_epc(epc_dir, batch_size=100))
        rec = batches[0][0]
        assert rec.uprn is None

    def test_batching(self, tmp_path):
        epc_dir = tmp_path / "epc"
        epc_dir.mkdir()
        rows = [_make_valid_row(LMK_KEY=f"key-{i}") for i in range(5)]
        _write_epc_csv(epc_dir / "test.csv", rows)

        batches = list(parse_epc(epc_dir, batch_size=2))
        total = sum(len(b) for b in batches)
        assert total == 5

    def test_reads_multiple_csvs(self, tmp_path):
        epc_dir = tmp_path / "epc"
        epc_dir.mkdir()
        _write_epc_csv(epc_dir / "la_001.csv", [_make_valid_row(LMK_KEY="key-a")])
        _write_epc_csv(epc_dir / "la_002.csv", [_make_valid_row(LMK_KEY="key-b")])

        batches = list(parse_epc(epc_dir, batch_size=100))
        total = sum(len(b) for b in batches)
        assert total == 2

    def test_dir_not_found_raises(self, tmp_path):
        with pytest.raises(ParseError):
            list(parse_epc(tmp_path / "nope"))

    def test_not_a_dir_raises(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("hello")
        with pytest.raises(ParseError):
            list(parse_epc(f))

    def test_empty_dir_raises(self, tmp_path):
        epc_dir = tmp_path / "epc"
        epc_dir.mkdir()
        with pytest.raises(ParseError):
            list(parse_epc(epc_dir))

    def test_handles_optional_numeric_fields(self, tmp_path):
        epc_dir = tmp_path / "epc"
        epc_dir.mkdir()
        _write_epc_csv(epc_dir / "test.csv", [
            _make_valid_row(
                TOTAL_FLOOR_AREA="",
                CURRENT_ENERGY_EFFICIENCY="",
                LODGEMENT_DATE="",
            ),
        ])

        batches = list(parse_epc(epc_dir, batch_size=100))
        rec = batches[0][0]
        assert rec.total_floor_area is None
        assert rec.current_energy_efficiency is None
        assert rec.lodgement_date is None
