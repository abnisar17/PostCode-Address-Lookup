"""HM Land Registry Price Paid Data parser — pure function: CSV path → Iterator of validated record batches."""

import csv
from collections.abc import Iterator
from datetime import date
from pathlib import Path

from pydantic import ValidationError

from app.core.exceptions import ParseError
from app.core.logging import get_logger
from app.core.utils.address import normalise_city, normalise_street
from app.core.utils.postcode import normalise_postcode
from app.ingestion.schemas import LandRegistryRecord

log = get_logger(__name__)

# Land Registry Price Paid CSV has NO header row — 16 fixed columns:
#  0: Transaction unique ID  (GUID in {curly braces})
#  1: Price
#  2: Date of Transfer       (YYYY-MM-DD)
#  3: Postcode
#  4: Property Type           (D/S/T/F/O)
#  5: Old/New                 (Y/N)
#  6: Duration                (F/L)
#  7: PAON                    (Primary Addressable Object Name)
#  8: SAON                    (Secondary Addressable Object Name)
#  9: Street
# 10: Locality
# 11: Town/City
# 12: District
# 13: County
# 14: PPD Category            (A/B)
# 15: Record Status           (A/C/D)


def parse_land_registry(
    csv_path: Path,
    *,
    batch_size: int = 10_000,
) -> Iterator[list[LandRegistryRecord]]:
    """Parse HM Land Registry Price Paid Data CSV, yielding batches of validated records.

    The CSV is headerless with 16 fixed columns per the Land Registry specification.
    Rows with a zero/invalid price or missing/invalid postcode are skipped.
    """
    if not csv_path.exists():
        raise ParseError(
            f"Land Registry file not found: {csv_path}",
            source="land_registry",
        )

    skipped = 0
    total = 0
    batch: list[LandRegistryRecord] = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for line_num, row in enumerate(reader, start=1):
            total += 1

            if len(row) < 16:
                skipped += 1
                log.debug(
                    "Row too short",
                    source="land_registry",
                    line=line_num,
                    columns=len(row),
                )
                continue

            # ── Price validation ──────────────────────────────────
            raw_price = row[1].strip()
            try:
                price = int(raw_price)
            except (ValueError, TypeError):
                skipped += 1
                log.debug(
                    "Invalid price",
                    source="land_registry",
                    line=line_num,
                    raw=raw_price,
                )
                continue

            if price == 0:
                skipped += 1
                continue

            # ── Postcode validation ───────────────────────────────
            raw_postcode = row[3].strip()
            norm_postcode = normalise_postcode(raw_postcode)
            if not norm_postcode:
                skipped += 1
                log.debug(
                    "Invalid postcode",
                    source="land_registry",
                    line=line_num,
                    raw=raw_postcode,
                )
                continue

            # ── Transaction ID — strip curly braces ───────────────
            transaction_id = row[0].strip().strip("{}")

            # ── Date of transfer ──────────────────────────────────
            raw_date = row[2].strip()
            try:
                # Land Registry dates may include time: "1995-03-24 00:00"
                date_of_transfer = date.fromisoformat(raw_date.split()[0])
            except (ValueError, TypeError, IndexError):
                skipped += 1
                log.debug(
                    "Invalid date",
                    source="land_registry",
                    line=line_num,
                    raw=raw_date,
                )
                continue

            # ── Build the record ──────────────────────────────────
            try:
                record = LandRegistryRecord(
                    transaction_id=transaction_id,
                    price=price,
                    date_of_transfer=date_of_transfer,
                    postcode_raw=raw_postcode or None,
                    postcode_norm=norm_postcode,
                    property_type=row[4].strip() or None,
                    old_new=row[5].strip() or None,
                    duration=row[6].strip() or None,
                    paon=row[7].strip() or None,
                    saon=row[8].strip() or None,
                    street=normalise_street(row[9].strip()),
                    locality=row[10].strip() or None,
                    town=normalise_city(row[11].strip()),
                    district=row[12].strip() or None,
                    county=row[13].strip() or None,
                    ppd_category=row[14].strip() or None,
                    record_status=row[15].strip() or None,
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

    log.info(
        "Land Registry parsing complete",
        source="land_registry",
        total=total,
        skipped=skipped,
    )
