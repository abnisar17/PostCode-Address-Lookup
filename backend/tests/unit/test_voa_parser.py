"""Unit tests for the VOA Non-Domestic Rating List parser."""

import csv
import io
import tempfile
import zipfile
from pathlib import Path

import pytest

from app.ingestion.voa import parse_voa


def _make_voa_row(
    *,
    entry_num="1",
    ba_code="1234",
    community_code="E00001",
    ba_ref="REF001",
    desc_code="CS",
    desc_text="Shop",
    uarn="12345678",
    full_prop_id="1 HIGH STREET, LONDON",
    firm_name="TEST LTD",
    number_or_name="1",
    street="HIGH STREET",
    town="LONDON",
    postal_district="CITY OF LONDON",
    county="GREATER LONDON",
    postcode="EC1A 1AA",
    effective_date="01-APR-2023",
    composite="N",
    rateable_value="50000",
    appeal_code="",
    assessment_ref="99999999",
    alteration_date="01-APR-2023",
    scat_code="249",
    sub_street_3="",
    sub_street_2="",
    sub_street_1="",
    case_number="",
    current_from="01-APR-2023",
    current_to="",
):
    return [
        entry_num, ba_code, community_code, ba_ref,
        desc_code, desc_text, uarn, full_prop_id,
        firm_name, number_or_name, street, town,
        postal_district, county, postcode, effective_date,
        composite, rateable_value, appeal_code, assessment_ref,
        alteration_date, scat_code, sub_street_3, sub_street_2,
        sub_street_1, case_number, current_from, current_to,
    ]


def _write_csv(path: Path, rows: list[list[str]], delimiter="*"):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=delimiter)
        for row in rows:
            writer.writerow(row)


def _write_zip(zip_path: Path, csv_name: str, rows: list[list[str]], delimiter="*"):
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=delimiter)
    for row in rows:
        writer.writerow(row)
    csv_data = buf.getvalue().encode("utf-8")

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(csv_name, csv_data)


class TestParseVOA:
    def test_parses_valid_row(self, tmp_path):
        csv_file = tmp_path / "voa.csv"
        _write_csv(csv_file, [_make_voa_row()])

        batches = list(parse_voa(csv_file, batch_size=100))
        assert len(batches) == 1
        assert len(batches[0]) == 1

        rec = batches[0][0]
        assert rec.uarn == 12345678
        assert rec.postcode_norm == "EC1A 1AA"
        assert rec.street == "HIGH STREET"
        assert rec.town == "LONDON"
        assert rec.firm_name == "TEST LTD"
        assert rec.rateable_value == 50000

    def test_skips_invalid_postcode(self, tmp_path):
        csv_file = tmp_path / "voa.csv"
        _write_csv(csv_file, [_make_voa_row(postcode="INVALID")])

        batches = list(parse_voa(csv_file, batch_size=100))
        assert len(batches) == 0

    def test_skips_invalid_uarn(self, tmp_path):
        csv_file = tmp_path / "voa.csv"
        _write_csv(csv_file, [_make_voa_row(uarn="NOT_A_NUMBER")])

        batches = list(parse_voa(csv_file, batch_size=100))
        assert len(batches) == 0

    def test_skips_short_rows(self, tmp_path):
        csv_file = tmp_path / "voa.csv"
        _write_csv(csv_file, [["only", "three", "fields"]])

        batches = list(parse_voa(csv_file, batch_size=100))
        assert len(batches) == 0

    def test_handles_missing_rateable_value(self, tmp_path):
        csv_file = tmp_path / "voa.csv"
        _write_csv(csv_file, [_make_voa_row(rateable_value="")])

        batches = list(parse_voa(csv_file, batch_size=100))
        assert len(batches) == 1
        assert batches[0][0].rateable_value is None

    def test_batch_yielding(self, tmp_path):
        csv_file = tmp_path / "voa.csv"
        rows = [
            _make_voa_row(uarn=str(i), postcode="SW1A 1AA")
            for i in range(25)
        ]
        _write_csv(csv_file, rows)

        batches = list(parse_voa(csv_file, batch_size=10))
        assert len(batches) == 3  # 10, 10, 5
        assert len(batches[0]) == 10
        assert len(batches[1]) == 10
        assert len(batches[2]) == 5

    def test_parses_zip_file(self, tmp_path):
        zip_file = tmp_path / "voa.zip"
        _write_zip(zip_file, "data.csv", [_make_voa_row()])

        batches = list(parse_voa(zip_file, batch_size=100))
        assert len(batches) == 1
        assert batches[0][0].uarn == 12345678

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(Exception, match="not found"):
            list(parse_voa(tmp_path / "nonexistent.csv"))
