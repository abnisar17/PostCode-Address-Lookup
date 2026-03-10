"""NHS Organisation Data Service parser — CSV path → Iterator of validated record batches.

The NHS epraccur.csv has NO header row. Fields are positional:
  0: Organisation Code
  1: Name
  2: National Grouping
  3: High Level Health Geography
  4: Address Line 1
  5: Address Line 2
  6: Address Line 3
  7: Address Line 4 (town/city)
  8: Address Line 5 (county)
  9: Postcode
  10: Open Date
  11: Close Date
  12: Status Code (ACTIVE/INACTIVE)
  ...
"""

import csv
from collections.abc import Iterator
from pathlib import Path

from pydantic import ValidationError

from app.core.exceptions import ParseError
from app.core.logging import get_logger
from app.core.utils.postcode import normalise_postcode
from app.ingestion.schemas import NHSRecord

log = get_logger(__name__)

# Column indices for NHS epraccur.csv (no header)
_COL_ORG_CODE = 0
_COL_NAME = 1
_COL_ADDR1 = 4
_COL_ADDR2 = 5
_COL_ADDR3 = 6
_COL_ADDR4 = 7  # town/city
_COL_ADDR5 = 8  # county
_COL_POSTCODE = 9
_COL_STATUS = 12


def _col(row: list[str], idx: int) -> str:
    """Safely get a column by index, stripping quotes and whitespace."""
    if idx < len(row):
        return row[idx].strip().strip('"')
    return ""


def parse_nhs(
    csv_path: Path,
    *,
    batch_size: int = 5_000,
    skip_closed: bool = True,
) -> Iterator[list[NHSRecord]]:
    """Parse NHS ODS CSV (no header row), yielding batches of validated records.

    Download from: https://digital.nhs.uk/services/organisation-data-service/data-search-and-export/csv-downloads
    """
    if not csv_path.exists():
        raise ParseError(f"NHS file not found: {csv_path}", source="nhs")

    skipped = 0
    total = 0
    batch: list[NHSRecord] = []

    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        # Detect if there's a header row (some NHS files have headers, some don't)
        first_line = f.readline()
        f.seek(0)

        has_header = "Organisation Code" in first_line or "org_code" in first_line.lower()

        if has_header:
            reader = csv.DictReader(f)
            if reader.fieldnames:
                reader.fieldnames = [h.strip() for h in reader.fieldnames]
            for row in reader:
                total += 1
                org_code = (row.get("Organisation Code") or row.get("Code") or "").strip()
                if not org_code:
                    skipped += 1
                    continue
                if skip_closed:
                    status = (row.get("Status Code") or row.get("Status") or "").strip().upper()
                    if status in ("C", "CLOSED", "INACTIVE"):
                        skipped += 1
                        continue
                raw_postcode = (row.get("Postcode") or row.get("Post Code") or "").strip()
                norm = normalise_postcode(raw_postcode)
                if not norm:
                    skipped += 1
                    continue
                try:
                    record = NHSRecord(
                        org_code=org_code,
                        name=(row.get("Name") or "").strip() or None,
                        org_type=(row.get("Organisation Sub Type Code") or "").strip() or None,
                        status=(row.get("Status Code") or row.get("Status") or "").strip() or None,
                        postcode_raw=raw_postcode,
                        postcode_norm=norm,
                        address_line_1=(row.get("Address Line 1") or "").strip() or None,
                        address_line_2=(row.get("Address Line 2") or "").strip() or None,
                        address_line_3=(row.get("Address Line 3") or "").strip() or None,
                        address_line_4=(row.get("Address Line 4") or "").strip() or None,
                        city=(row.get("Address Line 5") or row.get("City") or "").strip() or None,
                    )
                except (ValidationError, ValueError):
                    skipped += 1
                    continue
                batch.append(record)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
        else:
            # No header — positional CSV (epraccur.csv format)
            reader = csv.reader(f)
            for cols in reader:
                total += 1

                org_code = _col(cols, _COL_ORG_CODE)
                if not org_code:
                    skipped += 1
                    continue

                if skip_closed:
                    status = _col(cols, _COL_STATUS).upper()
                    if status in ("C", "CLOSED", "INACTIVE"):
                        skipped += 1
                        continue

                raw_postcode = _col(cols, _COL_POSTCODE)
                norm = normalise_postcode(raw_postcode)
                if not norm:
                    skipped += 1
                    continue

                try:
                    record = NHSRecord(
                        org_code=org_code,
                        name=_col(cols, _COL_NAME) or None,
                        org_type=None,
                        status=_col(cols, _COL_STATUS) or None,
                        postcode_raw=raw_postcode,
                        postcode_norm=norm,
                        address_line_1=_col(cols, _COL_ADDR1) or None,
                        address_line_2=_col(cols, _COL_ADDR2) or None,
                        address_line_3=_col(cols, _COL_ADDR3) or None,
                        address_line_4=_col(cols, _COL_ADDR4) or None,
                        city=_col(cols, _COL_ADDR5) or None,
                    )
                except (ValidationError, ValueError):
                    skipped += 1
                    continue

                batch.append(record)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []

    if batch:
        yield batch

    log.info("NHS parsing complete", source="nhs", total=total, skipped=skipped)
