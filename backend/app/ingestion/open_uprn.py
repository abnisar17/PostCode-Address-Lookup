"""OS Open UPRN parser — pure function: ZIP path → Iterator of validated record batches."""

import csv
import io
import zipfile
from collections.abc import Iterator
from pathlib import Path

from pydantic import ValidationError

from app.core.exceptions import ParseError
from app.core.logging import get_logger
from app.ingestion.schemas import UPRNRecord

log = get_logger(__name__)


def _find_uprn_csv(zf: zipfile.ZipFile) -> str:
    """Find the UPRN data CSV inside the ZIP (ignoring metadata/docs)."""
    candidates = [
        n
        for n in zf.namelist()
        if n.lower().endswith(".csv")
        and not n.startswith("__")
        and "/Doc/" not in n
    ]
    if not candidates:
        raise ParseError("No CSV found in Open UPRN ZIP", source="open_uprn")

    # Pick the largest CSV (the main data file)
    return max(candidates, key=lambda n: zf.getinfo(n).file_size)


def parse_open_uprn(
    zip_path: Path,
    *,
    batch_size: int = 10_000,
) -> Iterator[list[UPRNRecord]]:
    """Parse OS Open UPRN ZIP file, yielding batches of validated records.

    The ZIP contains a CSV with headers: UPRN, X_COORDINATE, Y_COORDINATE,
    LATITUDE, LONGITUDE. We only extract UPRN, LATITUDE, and LONGITUDE.
    """
    if not zip_path.exists():
        raise ParseError(
            f"Open UPRN file not found: {zip_path}",
            source="open_uprn",
        )

    skipped = 0
    total = 0
    batch: list[UPRNRecord] = []

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_name = _find_uprn_csv(zf)
            log.info("Using Open UPRN CSV", source="open_uprn", file=csv_name)

            with zf.open(csv_name) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))

                for line_num, row in enumerate(reader, start=2):
                    total += 1

                    raw_uprn = row.get("UPRN", "").strip()
                    raw_lat = row.get("LATITUDE", "").strip()
                    raw_lon = row.get("LONGITUDE", "").strip()

                    if not raw_uprn or not raw_lat or not raw_lon:
                        skipped += 1
                        log.debug(
                            "Missing required field",
                            source="open_uprn",
                            file=csv_name,
                            line=line_num,
                        )
                        continue

                    try:
                        record = UPRNRecord(
                            uprn=int(raw_uprn),
                            latitude=float(raw_lat),
                            longitude=float(raw_lon),
                        )
                    except (ValidationError, ValueError):
                        skipped += 1
                        continue

                    batch.append(record)
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []

    except zipfile.BadZipFile as exc:
        raise ParseError(
            f"Corrupt ZIP file: {zip_path}",
            source="open_uprn",
            detail=str(exc),
        ) from exc

    if batch:
        yield batch

    log.info(
        "Open UPRN parsing complete",
        source="open_uprn",
        total=total,
        skipped=skipped,
    )
