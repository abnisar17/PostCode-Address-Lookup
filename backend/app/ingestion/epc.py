"""EPC Open Data parser — pure function: directory of CSVs → Iterator of validated record batches.

EPC (Energy Performance Certificate) data is published as bulk CSVs, one per
local authority. This module iterates through every CSV in the given directory
(sorted by name), parsing and validating each row into an EPCRecord.

Download the bulk data from https://epc.opendatacommunities.org or via the
EPC API (requires free registration).
"""

import csv
from collections.abc import Iterator
from datetime import date
from pathlib import Path

from pydantic import ValidationError

from app.core.exceptions import ParseError
from app.core.logging import get_logger
from app.core.utils.postcode import normalise_postcode
from app.ingestion.schemas import EPCRecord

log = get_logger(__name__)


def parse_epc(
    epc_dir: Path,
    *,
    batch_size: int = 2_000,
) -> Iterator[list[EPCRecord]]:
    """Parse EPC domestic certificate CSVs, yielding batches of validated records.

    The ``epc_dir`` directory should contain one or more CSV files (typically
    one per local authority). Each CSV has a header row with columns including
    LMK_KEY, UPRN, ADDRESS1-3, POSTTOWN, POSTCODE, COUNTY,
    CURRENT_ENERGY_RATING, CURRENT_ENERGY_EFFICIENCY, PROPERTY_TYPE,
    BUILT_FORM, TOTAL_FLOOR_AREA, and LODGEMENT_DATE.

    Rows without a valid postcode are skipped.

    Args:
        epc_dir: Path to a directory containing EPC CSV files.
        batch_size: Number of records per yielded batch.

    Yields:
        Lists of EPCRecord, each up to ``batch_size`` elements.
    """
    if not epc_dir.exists():
        raise ParseError(
            f"EPC directory not found: {epc_dir}",
            source="epc",
        )

    if not epc_dir.is_dir():
        raise ParseError(
            f"EPC path is not a directory: {epc_dir}",
            source="epc",
        )

    # Support both flat CSVs in epc_dir and the official bulk download layout
    # where each local authority has a subfolder containing certificates.csv
    csv_files = sorted(p for p in epc_dir.iterdir() if p.suffix.lower() == ".csv")
    if not csv_files:
        # Look for certificates.csv inside subdirectories
        csv_files = sorted(
            p
            for d in epc_dir.iterdir()
            if d.is_dir()
            for p in d.iterdir()
            if p.name.lower() == "certificates.csv"
        )
    if not csv_files:
        raise ParseError(
            f"No CSV files found in EPC directory: {epc_dir}",
            source="epc",
        )

    log.info(
        "Starting EPC parse",
        source="epc",
        directory=str(epc_dir),
        csv_count=len(csv_files),
        batch_size=batch_size,
    )

    skipped = 0
    total = 0
    batch: list[EPCRecord] = []

    for csv_path in csv_files:
        log.debug("Processing EPC CSV", source="epc", file=csv_path.name)

        try:
            with open(csv_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)

                for line_num, row in enumerate(reader, start=2):
                    total += 1

                    # ── Postcode validation ─────────────────────────
                    raw_postcode = row.get("POSTCODE", "").strip()
                    norm_postcode = normalise_postcode(raw_postcode)
                    if not norm_postcode:
                        skipped += 1
                        log.debug(
                            "Invalid postcode",
                            source="epc",
                            file=csv_path.name,
                            line=line_num,
                            raw=raw_postcode,
                        )
                        continue

                    # ── LMK_KEY (required) ──────────────────────────
                    lmk_key = row.get("LMK_KEY", "").strip()
                    if not lmk_key:
                        skipped += 1
                        log.debug(
                            "Missing LMK_KEY",
                            source="epc",
                            file=csv_path.name,
                            line=line_num,
                        )
                        continue

                    # ── UPRN (optional, parse as int) ───────────────
                    raw_uprn = row.get("UPRN", "").strip()
                    uprn: int | None = None
                    if raw_uprn:
                        try:
                            uprn = int(raw_uprn)
                        except (ValueError, TypeError):
                            uprn = None

                    # ── Total floor area (optional, parse as float) ─
                    raw_floor_area = row.get("TOTAL_FLOOR_AREA", "").strip()
                    total_floor_area: float | None = None
                    if raw_floor_area:
                        try:
                            total_floor_area = float(raw_floor_area)
                        except (ValueError, TypeError):
                            total_floor_area = None

                    # ── Lodgement date (optional, parse YYYY-MM-DD) ─
                    raw_date = row.get("LODGEMENT_DATE", "").strip()
                    lodgement_date: date | None = None
                    if raw_date:
                        try:
                            lodgement_date = date.fromisoformat(raw_date)
                        except (ValueError, TypeError):
                            lodgement_date = None

                    # ── Energy efficiency (optional, parse as int) ──
                    raw_efficiency = row.get("CURRENT_ENERGY_EFFICIENCY", "").strip()
                    current_energy_efficiency: int | None = None
                    if raw_efficiency:
                        try:
                            current_energy_efficiency = int(raw_efficiency)
                        except (ValueError, TypeError):
                            current_energy_efficiency = None

                    # ── Build the record ────────────────────────────
                    try:
                        record = EPCRecord(
                            lmk_key=lmk_key,
                            uprn=uprn,
                            postcode_raw=raw_postcode or None,
                            postcode_norm=norm_postcode,
                            address_line_1=row.get("ADDRESS1", "").strip() or None,
                            address_line_2=row.get("ADDRESS2", "").strip() or None,
                            address_line_3=row.get("ADDRESS3", "").strip() or None,
                            post_town=row.get("POSTTOWN", "").strip() or None,
                            county=row.get("COUNTY", "").strip() or None,
                            current_energy_rating=row.get("CURRENT_ENERGY_RATING", "").strip() or None,
                            current_energy_efficiency=current_energy_efficiency,
                            property_type=row.get("PROPERTY_TYPE", "").strip() or None,
                            built_form=row.get("BUILT_FORM", "").strip() or None,
                            total_floor_area=total_floor_area,
                            lodgement_date=lodgement_date,
                        )
                    except (ValidationError, ValueError):
                        skipped += 1
                        continue

                    batch.append(record)
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []

        except OSError as exc:
            log.warning(
                "Failed to read EPC CSV",
                source="epc",
                file=csv_path.name,
                error=str(exc),
            )
            skipped += 1
            continue

    if batch:
        yield batch

    log.info(
        "EPC parsing complete",
        source="epc",
        total=total,
        skipped=skipped,
    )


def download_epc_bulk(api_base_url: str, api_key: str, dest_dir: Path) -> None:
    """Download EPC domestic certificates as bulk CSV.

    Uses the EPC API to download all domestic certificates.
    API requires Authorization header: ``Basic {base64(email:api_key)}``
    Endpoint: ``GET /domestic/search?size=5000&from=0``

    Note:
        This is a placeholder. The actual EPC bulk download process requires
        registration at https://epc.opendatacommunities.org. Users can either:

        1. Register and download bulk data manually from the website.
        2. Register for API access and provide credentials here.

    Args:
        api_base_url: Base URL for the EPC API (e.g. ``https://epc.opendatacommunities.org/api/v1``).
        api_key: API key obtained after registration.
        dest_dir: Directory to save downloaded CSV files.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)

    log.info(
        "EPC bulk download is a manual step",
        source="epc",
        api_base_url=api_base_url,
        dest_dir=str(dest_dir),
    )
    log.info(
        "To download EPC data:\n"
        "  1. Register at https://epc.opendatacommunities.org\n"
        "  2. Download the 'All domestic certificates' bulk file\n"
        "  3. Extract the CSV files into: %s\n"
        "  4. Run the EPC parser with parse_epc(Path('%s'))",
        str(dest_dir),
        str(dest_dir),
        source="epc",
    )
