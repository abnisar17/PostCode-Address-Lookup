"""DVSA Active MOT Test Stations parser — CSV path → Iterator of validated record batches."""

import csv
from collections.abc import Iterator
from pathlib import Path

from pydantic import ValidationError

from app.core.exceptions import ParseError
from app.core.logging import get_logger
from app.core.utils.postcode import normalise_postcode
from app.ingestion.schemas import DVSARecord

log = get_logger(__name__)


def parse_dvsa(
    csv_path: Path,
    *,
    batch_size: int = 5_000,
) -> Iterator[list[DVSARecord]]:
    """Parse DVSA Active MOT Test Stations CSV, yielding batches of validated records.

    Download from: https://www.gov.uk/government/publications/active-mot-test-stations
    """
    if not csv_path.exists():
        raise ParseError(f"DVSA file not found: {csv_path}", source="dvsa")

    skipped = 0
    total = 0
    batch: list[DVSARecord] = []

    with open(csv_path, encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames:
            reader.fieldnames = [h.strip() for h in reader.fieldnames]

        for row in reader:
            total += 1

            station_num = (
                row.get("Site_Number")
                or row.get("MOT Test Station Number")
                or row.get("Site Number")
                or ""
            ).strip()
            if not station_num:
                skipped += 1
                continue

            raw_postcode = (row.get("Postcode") or row.get("Post Code") or "").strip()
            norm = normalise_postcode(raw_postcode)
            if not norm:
                skipped += 1
                continue

            try:
                record = DVSARecord(
                    station_number=station_num,
                    site_name=(
                        row.get("Trading_Name")
                        or row.get("Site Name")
                        or row.get("Name")
                        or ""
                    ).strip() or None,
                    postcode_raw=raw_postcode,
                    postcode_norm=norm,
                    address_line_1=(row.get("Address1") or row.get("Address Line 1") or "").strip() or None,
                    address_line_2=(row.get("Address2") or row.get("Address Line 2") or "").strip() or None,
                    address_line_3=(row.get("Address3") or row.get("Address Line 3") or "").strip() or None,
                    town=(row.get("Town") or "").strip() or None,
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

    log.info("DVSA parsing complete", source="dvsa", total=total, skipped=skipped)
