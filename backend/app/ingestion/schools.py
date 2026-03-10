"""GIAS (Get Information About Schools) parser — CSV path → Iterator of validated record batches."""

import csv
from collections.abc import Iterator
from pathlib import Path

from pydantic import ValidationError

from app.core.exceptions import ParseError
from app.core.logging import get_logger
from app.core.utils.postcode import normalise_postcode
from app.ingestion.schemas import SchoolRecord

log = get_logger(__name__)


def parse_schools(
    csv_path: Path,
    *,
    batch_size: int = 5_000,
    skip_closed: bool = True,
) -> Iterator[list[SchoolRecord]]:
    """Parse GIAS schools CSV, yielding batches of validated records.

    Download from: https://get-information-schools.service.gov.uk/
    (Search → Download all → CSV)
    """
    if not csv_path.exists():
        raise ParseError(f"Schools file not found: {csv_path}", source="schools")

    skipped = 0
    total = 0
    batch: list[SchoolRecord] = []

    with open(csv_path, encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames:
            reader.fieldnames = [h.strip() for h in reader.fieldnames]

        for row in reader:
            total += 1

            # Skip closed schools
            if skip_closed:
                status = (row.get("EstablishmentStatus (name)") or row.get("EstablishmentStatus") or "").strip().lower()
                if "closed" in status:
                    skipped += 1
                    continue

            urn_raw = (row.get("URN") or "").strip()
            if not urn_raw or not urn_raw.isdigit():
                skipped += 1
                continue

            raw_postcode = (row.get("Postcode") or "").strip()
            norm = normalise_postcode(raw_postcode)
            if not norm:
                skipped += 1
                continue

            try:
                record = SchoolRecord(
                    urn=int(urn_raw),
                    name=(row.get("EstablishmentName") or "").strip() or None,
                    establishment_type=(row.get("TypeOfEstablishment (name)") or row.get("TypeOfEstablishment") or "").strip() or None,
                    status=(row.get("EstablishmentStatus (name)") or row.get("EstablishmentStatus") or "").strip() or None,
                    postcode_raw=raw_postcode,
                    postcode_norm=norm,
                    street=(row.get("Street") or "").strip() or None,
                    locality=(row.get("Locality") or "").strip() or None,
                    town=(row.get("Town") or row.get("Address3") or "").strip() or None,
                    county=(row.get("County (name)") or row.get("County") or "").strip() or None,
                    latitude=_parse_float(row.get("Latitude")),
                    longitude=_parse_float(row.get("Longitude")),
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

    log.info("Schools parsing complete", source="schools", total=total, skipped=skipped)


def _parse_float(raw) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return None
        try:
            val = float(raw)
            return val if val != 0.0 else None
        except ValueError:
            return None
    return None
