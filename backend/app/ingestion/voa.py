"""VOA Non-Domestic Rating List parser — pure function: ZIP path → Iterator of validated record batches.

The VOA Compiled Rating List is a free download from:
https://voaratinglists.blob.core.windows.net/html/rlidata.htm

The CSV files use asterisk (*) as the delimiter and contain 28 fields per row.
"""

import csv
import io
import zipfile
from collections.abc import Iterator
from pathlib import Path

from pydantic import ValidationError

from app.core.exceptions import ParseError
from app.core.logging import get_logger
from app.core.utils.postcode import normalise_postcode
from app.ingestion.schemas import VOARecord

log = get_logger(__name__)

# VOA CSV is asterisk-delimited with 28 fields (no header row):
#  0: Incrementing Entry Number
#  1: Billing Authority Code
#  2: NDR Community Code
#  3: BA Reference Number
#  4: Primary Description Code
#  5: Primary Description Text
#  6: UARN (Unique Address Reference Number)
#  7: Full Property Identifier
#  8: Firms Name
#  9: Number Or Name
# 10: Street
# 11: Town
# 12: Postal District
# 13: County
# 14: Postcode
# 15: Effective Date
# 16: Composite Indicator
# 17: Rateable Value
# 18: Appeal Settlement Code
# 19: Assessment Reference Number
# 20: List Alteration Date
# 21: SCAT Code And Suffix
# 22: Sub Street level 3
# 23: Sub Street level 2
# 24: Sub Street level 1
# 25: Case Number
# 26: Current From Date
# 27: Current To Date


def _find_voa_csv(zf: zipfile.ZipFile) -> str:
    """Find the main rating list CSV inside the ZIP."""
    candidates = [
        n
        for n in zf.namelist()
        if n.lower().endswith(".csv")
        and not n.startswith("__")
        and "readme" not in n.lower()
    ]
    if not candidates:
        raise ParseError("No CSV found in VOA ZIP", source="voa")

    return max(candidates, key=lambda n: zf.getinfo(n).file_size)


def parse_voa(
    file_path: Path,
    *,
    batch_size: int = 10_000,
) -> Iterator[list[VOARecord]]:
    """Parse VOA Non-Domestic Rating List, yielding batches of validated records.

    Handles both ZIP files and plain CSV files. The CSV uses asterisk (*)
    as the field delimiter and contains 28 columns with no header row.
    Rows with invalid/missing UARN or postcode are skipped.
    """
    if not file_path.exists():
        raise ParseError(
            f"VOA file not found: {file_path}",
            source="voa",
        )

    skipped = 0
    total = 0
    batch: list[VOARecord] = []

    def _process_reader(reader: Iterator) -> Iterator[list[VOARecord]]:
        nonlocal skipped, total, batch

        for line_num, row in enumerate(reader, start=1):
            total += 1

            if len(row) < 28:
                skipped += 1
                log.debug(
                    "Row too short",
                    source="voa",
                    line=line_num,
                    columns=len(row),
                )
                continue

            # ── UARN validation ──────────────────────────────────
            raw_uarn = row[6].strip()
            try:
                uarn = int(raw_uarn)
            except (ValueError, TypeError):
                skipped += 1
                continue

            # ── Postcode validation ──────────────────────────────
            raw_postcode = row[14].strip()
            norm_postcode = normalise_postcode(raw_postcode)
            if not norm_postcode:
                skipped += 1
                continue

            # ── Rateable value ───────────────────────────────────
            raw_rv = row[17].strip()
            try:
                rateable_value = int(raw_rv) if raw_rv else None
            except (ValueError, TypeError):
                rateable_value = None

            # ── Build the record ─────────────────────────────────
            try:
                record = VOARecord(
                    uarn=uarn,
                    billing_authority_code=row[1].strip() or None,
                    description_code=row[4].strip() or None,
                    description_text=row[5].strip() or None,
                    firm_name=row[8].strip() or None,
                    number_or_name=row[9].strip() or None,
                    street=row[10].strip() or None,
                    town=row[11].strip() or None,
                    postal_district=row[12].strip() or None,
                    county=row[13].strip() or None,
                    postcode_raw=raw_postcode or None,
                    postcode_norm=norm_postcode,
                    rateable_value=rateable_value,
                    effective_date=row[15].strip() or None,
                    sub_street_3=row[22].strip() or None,
                    sub_street_2=row[23].strip() or None,
                    sub_street_1=row[24].strip() or None,
                )
            except (ValidationError, ValueError):
                skipped += 1
                continue

            batch.append(record)
            if len(batch) >= batch_size:
                yield batch
                batch = []

    if file_path.suffix.lower() == ".zip":
        try:
            with zipfile.ZipFile(file_path, "r") as zf:
                csv_name = _find_voa_csv(zf)
                log.info("Using VOA CSV", source="voa", file=csv_name)

                with zf.open(csv_name) as f:
                    reader = csv.reader(
                        io.TextIOWrapper(f, encoding="utf-8-sig"),
                        delimiter="*",
                    )
                    yield from _process_reader(reader)

        except zipfile.BadZipFile as exc:
            raise ParseError(
                f"Corrupt ZIP file: {file_path}",
                source="voa",
                detail=str(exc),
            ) from exc
    else:
        # Plain CSV file
        with open(file_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f, delimiter="*")
            yield from _process_reader(reader)

    if batch:
        yield batch

    log.info(
        "VOA parsing complete",
        source="voa",
        total=total,
        skipped=skipped,
    )
