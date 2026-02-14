"""Code-Point Open parser — pure function: ZIP path → Iterator of validated record batches."""

import csv
import io
import zipfile
from collections.abc import Iterator
from pathlib import Path

from pydantic import ValidationError

from app.core.exceptions import ParseError
from app.core.logging import get_logger
from app.core.utils.coordinates import osgb36_to_wgs84
from app.core.utils.postcode import normalise_postcode
from app.ingestion.schemas import CodePointRecord

log = get_logger(__name__)

# Code-Point Open CSV columns (no header row in data files)
_COLUMNS = [
    "postcode",
    "positional_quality",
    "eastings",
    "northings",
    "country_code",
    "nhs_region",
    "nhs_ha",
    "admin_county",
    "admin_district",
    "admin_ward",
]


def parse_codepoint(
    zip_path: Path,
    *,
    batch_size: int = 10_000,
) -> Iterator[list[CodePointRecord]]:
    """Parse Code-Point Open ZIP file, yielding batches of validated records.

    Each CSV file inside the ZIP represents one postcode area (e.g. AB.csv, B.csv).
    Rows are headerless with fixed column positions.
    """
    if not zip_path.exists():
        raise ParseError(
            f"Code-Point file not found: {zip_path}",
            source="codepoint",
        )

    skipped = 0
    total = 0
    batch: list[CodePointRecord] = []

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_names = sorted(
                n for n in zf.namelist()
                if n.endswith(".csv") and not n.startswith("__") and "/Doc/" not in n
            )

            for csv_name in csv_names:
                with zf.open(csv_name) as f:
                    reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                    for line_num, row in enumerate(reader, start=1):
                        total += 1

                        if len(row) < 5:
                            skipped += 1
                            continue

                        raw_postcode = row[0].strip()
                        norm = normalise_postcode(raw_postcode)
                        if not norm:
                            skipped += 1
                            log.debug(
                                "Invalid postcode",
                                source="codepoint",
                                file=csv_name,
                                line=line_num,
                                raw=raw_postcode,
                            )
                            continue

                        try:
                            easting = int(row[2].strip())
                            northing = int(row[3].strip())
                        except (ValueError, IndexError):
                            skipped += 1
                            continue

                        lat, lon = osgb36_to_wgs84(easting, northing)

                        try:
                            record = CodePointRecord(
                                postcode=raw_postcode,
                                postcode_norm=norm,
                                easting=easting,
                                northing=northing,
                                latitude=lat,
                                longitude=lon,
                                positional_quality=int(row[1].strip()) if row[1].strip() else 0,
                                country_code=row[4].strip() if len(row) > 4 else "",
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
            source="codepoint",
            detail=str(exc),
        ) from exc

    if batch:
        yield batch

    log.info(
        "Code-Point parsing complete",
        source="codepoint",
        total=total,
        skipped=skipped,
    )
