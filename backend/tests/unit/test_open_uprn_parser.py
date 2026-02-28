"""Unit tests for the OS Open UPRN parser."""

import csv
import io
import zipfile
from pathlib import Path

import pytest

from app.core.exceptions import ParseError
from app.ingestion.open_uprn import parse_open_uprn
from app.ingestion.schemas import UPRNRecord


def _make_uprn_zip(tmp_path: Path, rows: list[dict]) -> Path:
    """Create a ZIP containing a UPRN CSV with header and data rows."""
    zip_path = tmp_path / "open-uprn.zip"
    csv_buffer = io.StringIO()
    fieldnames = ["UPRN", "X_COORDINATE", "Y_COORDINATE", "LATITUDE", "LONGITUDE"]
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("osopenuprn_202402.csv", csv_buffer.getvalue())

    return zip_path


class TestParseOpenUPRN:
    def test_parses_valid_row(self, tmp_path):
        zip_path = _make_uprn_zip(tmp_path, [
            {"UPRN": "10023456789", "X_COORDINATE": "529000", "Y_COORDINATE": "180000",
             "LATITUDE": "51.501009", "LONGITUDE": "-0.141588"},
        ])

        batches = list(parse_open_uprn(zip_path, batch_size=100))
        assert len(batches) == 1
        assert len(batches[0]) == 1

        rec = batches[0][0]
        assert isinstance(rec, UPRNRecord)
        assert rec.uprn == 10023456789
        assert rec.latitude == pytest.approx(51.501009)
        assert rec.longitude == pytest.approx(-0.141588)

    def test_skips_missing_fields(self, tmp_path):
        zip_path = _make_uprn_zip(tmp_path, [
            {"UPRN": "10023456789", "X_COORDINATE": "", "Y_COORDINATE": "",
             "LATITUDE": "", "LONGITUDE": ""},
        ])

        batches = list(parse_open_uprn(zip_path, batch_size=100))
        total = sum(len(b) for b in batches)
        assert total == 0

    def test_batching(self, tmp_path):
        rows = [
            {"UPRN": str(10000000000 + i), "X_COORDINATE": "529000",
             "Y_COORDINATE": "180000", "LATITUDE": "51.5", "LONGITUDE": "-0.14"}
            for i in range(5)
        ]
        zip_path = _make_uprn_zip(tmp_path, rows)

        batches = list(parse_open_uprn(zip_path, batch_size=2))
        total = sum(len(b) for b in batches)
        assert total == 5
        assert len(batches) == 3

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(ParseError):
            list(parse_open_uprn(tmp_path / "missing.zip"))

    def test_corrupt_zip_raises(self, tmp_path):
        bad_zip = tmp_path / "bad.zip"
        bad_zip.write_text("not a zip file")

        with pytest.raises(ParseError):
            list(parse_open_uprn(bad_zip))
