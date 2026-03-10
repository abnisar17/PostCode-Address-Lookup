"""CQC Care Quality Commission parser — CSV path → Iterator of validated record batches."""

import csv
from collections.abc import Iterator
from pathlib import Path

from pydantic import ValidationError

from app.core.exceptions import ParseError
from app.core.logging import get_logger
from app.core.utils.postcode import normalise_postcode
from app.ingestion.schemas import CQCRecord

log = get_logger(__name__)


def parse_cqc(
    csv_path: Path,
    *,
    batch_size: int = 2_000,
) -> Iterator[list[CQCRecord]]:
    """Parse CQC Care Directory CSV, yielding batches of validated records.

    The CQC CSV has 4 metadata rows before the actual header row.
    Columns: Name, Also known as, Address, Postcode, Phone number, ...
    The Address field is a single comma-separated string.

    Download from: https://www.cqc.org.uk/about-us/transparency/using-cqc-data
    """
    if not csv_path.exists():
        raise ParseError(f"CQC file not found: {csv_path}", source="cqc")

    skipped = 0
    total = 0
    batch: list[CQCRecord] = []

    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        # Skip metadata rows until we find the header
        for line in f:
            stripped = line.strip()
            if stripped.startswith("Name,") or stripped.startswith('"Name",'):
                break
        else:
            raise ParseError("Could not find header row in CQC CSV", source="cqc")

        # Re-create the reader with the header we just found
        # We need to put the header back — use a chain
        import itertools
        reader = csv.DictReader(itertools.chain([line], f))
        if reader.fieldnames:
            reader.fieldnames = [h.strip() for h in reader.fieldnames]

        for row in reader:
            total += 1

            raw_postcode = (row.get("Postcode") or "").strip()
            norm = normalise_postcode(raw_postcode)
            if not norm:
                skipped += 1
                continue

            location_id = (
                row.get("CQC Location ID (for office use only)") or ""
            ).strip()
            if not location_id:
                skipped += 1
                continue

            # Address is a single comma-separated field like "7-9 White Kennet Street,London"
            raw_address = (row.get("Address") or "").strip()
            addr_parts = [p.strip() for p in raw_address.split(",") if p.strip()]
            address_line_1 = addr_parts[0] if len(addr_parts) > 0 else None
            city = addr_parts[-1] if len(addr_parts) > 1 else None
            address_line_2 = addr_parts[1] if len(addr_parts) > 2 else None

            try:
                record = CQCRecord(
                    location_id=location_id,
                    location_name=(row.get("Name") or "").strip() or None,
                    care_home=None,
                    location_type=(row.get("Service types") or "").strip() or None,
                    postcode_raw=raw_postcode,
                    postcode_norm=norm,
                    address_line_1=address_line_1,
                    address_line_2=address_line_2,
                    city=city,
                    county=(row.get("Region") or "").strip() or None,
                    latitude=None,
                    longitude=None,
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

    log.info("CQC parsing complete", source="cqc", total=total, skipped=skipped)
