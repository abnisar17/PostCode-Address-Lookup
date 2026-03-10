"""Charity Commission parser — CSV/ZIP path → Iterator of validated record batches."""

import csv
import io
import sys
import zipfile

# Charity Commission data can have very large fields (charity_activities)
csv.field_size_limit(sys.maxsize)
from collections.abc import Iterator
from pathlib import Path

from pydantic import ValidationError

from app.core.exceptions import ParseError
from app.core.logging import get_logger
from app.core.utils.postcode import normalise_postcode
from app.ingestion.schemas import CharityRecord

log = get_logger(__name__)


def _find_data_file(zf: zipfile.ZipFile) -> str:
    """Find the main data file inside a ZIP (CSV or TXT)."""
    candidates = [
        n for n in zf.namelist()
        if (n.lower().endswith(".csv") or n.lower().endswith(".txt"))
        and not n.startswith("__")
        and "readme" not in n.lower()
    ]
    if not candidates:
        raise ParseError("No CSV/TXT found in Charity ZIP", source="charity")
    # Prefer files with 'charity' in the name
    charity_files = [n for n in candidates if "charity" in n.lower()]
    pool = charity_files or candidates
    return max(pool, key=lambda n: zf.getinfo(n).file_size)


def _detect_delimiter(first_line: str) -> str:
    """Detect whether file uses tab or comma as delimiter."""
    if "\t" in first_line:
        return "\t"
    return ","


def _open_csv(file_path: Path):
    """Open a CSV/TSV from either a ZIP or plain file. Returns (reader, context_manager)."""
    if file_path.suffix.lower() == ".zip":
        zf = zipfile.ZipFile(file_path, "r")
        data_name = _find_data_file(zf)
        log.info("Using Charity data file", source="charity", file=data_name)
        f = io.TextIOWrapper(zf.open(data_name), encoding="utf-8-sig")
        # Peek at first line to detect delimiter
        first_line = f.readline()
        f.seek(0)
        delimiter = _detect_delimiter(first_line)
        return csv.DictReader(f, delimiter=delimiter), zf
    else:
        f = open(file_path, encoding="utf-8-sig", newline="")
        first_line = f.readline()
        f.seek(0)
        delimiter = _detect_delimiter(first_line)
        return csv.DictReader(f, delimiter=delimiter), f


def parse_charity(
    file_path: Path,
    *,
    batch_size: int = 10_000,
    skip_removed: bool = True,
) -> Iterator[list[CharityRecord]]:
    """Parse Charity Commission register CSV/ZIP, yielding batches of validated records.

    Download from: https://register-of-charities.charitycommission.gov.uk/register/full-register-download
    """
    if not file_path.exists():
        raise ParseError(f"Charity file not found: {file_path}", source="charity")

    skipped = 0
    total = 0
    batch: list[CharityRecord] = []

    reader, ctx = _open_csv(file_path)
    try:
        if reader.fieldnames:
            reader.fieldnames = [h.strip() for h in reader.fieldnames]
            log.info("Charity CSV columns", source="charity", columns=reader.fieldnames[:10])

        for row in reader:
            total += 1

            # Try multiple possible column names for charity number
            charity_num = (
                row.get("charity_number")
                or row.get("reg_charity_number")
                or row.get("registered_charity_number")
                or row.get("organisation_number")
                or ""
            ).strip()
            if not charity_num:
                skipped += 1
                continue

            # Skip removed charities
            if skip_removed:
                status = (row.get("charity_registration_status") or row.get("charity_status") or "").strip().lower()
                if status in ("removed", "deregistered"):
                    skipped += 1
                    continue

            # Try multiple postcode column names
            raw_postcode = (
                row.get("charity_contact_postcode")
                or row.get("postcode")
                or row.get("Postcode")
                or ""
            ).strip()
            norm = normalise_postcode(raw_postcode)
            if not norm:
                skipped += 1
                continue

            try:
                record = CharityRecord(
                    charity_number=charity_num,
                    charity_name=(
                        row.get("charity_name") or row.get("name") or ""
                    ).strip() or None,
                    charity_status=(
                        row.get("charity_registration_status") or row.get("charity_status") or ""
                    ).strip() or None,
                    postcode_raw=raw_postcode,
                    postcode_norm=norm,
                    address_line_1=(
                        row.get("charity_contact_address1") or row.get("address_line_1") or ""
                    ).strip() or None,
                    address_line_2=(
                        row.get("charity_contact_address2") or row.get("address_line_2") or ""
                    ).strip() or None,
                    address_line_3=(
                        row.get("charity_contact_address3") or row.get("address_line_3") or ""
                    ).strip() or None,
                    city=(
                        row.get("charity_contact_address4") or row.get("address_line_4") or ""
                    ).strip() or None,
                    county=(
                        row.get("charity_contact_address5") or row.get("address_line_5") or ""
                    ).strip() or None,
                )
            except (ValidationError, ValueError):
                skipped += 1
                continue

            batch.append(record)
            if len(batch) >= batch_size:
                yield batch
                batch = []
    finally:
        ctx.close()

    if batch:
        yield batch

    log.info("Charity parsing complete", source="charity", total=total, skipped=skipped)
