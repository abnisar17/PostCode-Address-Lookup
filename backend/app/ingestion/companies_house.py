"""Companies House Basic Company Data parser — pure function: ZIP path → Iterator of validated record batches."""

import csv
import io
import re
import zipfile
from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path

from pydantic import ValidationError

from app.core.exceptions import ParseError
from app.core.logging import get_logger
from app.core.utils.postcode import normalise_postcode
from app.ingestion.schemas import CompaniesHouseRecord

log = get_logger(__name__)

# SIC code text format: "70100 - Activities of head offices" → extract "70100"
_SIC_CODE_RE = re.compile(r"^(\d+)")

# Companies dissolved longer than this many years ago are skipped
_DISSOLVED_CUTOFF_YEARS = 10


def _extract_sic_code(raw: str | None) -> str | None:
    """Extract the numeric SIC code from a SIC text string.

    E.g. "70100 - Activities of head offices" → "70100"
    Returns None if the input is empty or contains no leading digits.
    """
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    match = _SIC_CODE_RE.match(raw)
    return match.group(1) if match else None


def _parse_incorporation_date(raw: str | None) -> str | None:
    """Parse DD/MM/YYYY date string and return as ISO format string (YYYY-MM-DD).

    Returns None if the input is empty or unparseable.
    """
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    try:
        dt = datetime.strptime(raw, "%d/%m/%Y")
        return dt.date().isoformat()
    except ValueError:
        return None


def _is_old_dissolved(status: str | None, inc_date_raw: str | None) -> bool:
    """Check if a company is dissolved and was incorporated more than
    _DISSOLVED_CUTOFF_YEARS ago (making it unlikely to be relevant)."""
    if not status:
        return False
    if status.strip().lower() != "dissolved":
        return False
    if not inc_date_raw or not inc_date_raw.strip():
        # Dissolved but no date — skip to be safe
        return True
    try:
        inc_date = datetime.strptime(inc_date_raw.strip(), "%d/%m/%Y").date()
    except ValueError:
        return True
    cutoff = date.today().replace(year=date.today().year - _DISSOLVED_CUTOFF_YEARS)
    return inc_date < cutoff


def _find_companies_csv(zf: zipfile.ZipFile) -> str:
    """Find the main Companies House data CSV inside the ZIP.

    The ZIP typically contains a single large CSV file. If multiple CSVs exist,
    pick the largest one (ignoring metadata / readme files).
    """
    candidates = [
        n
        for n in zf.namelist()
        if n.lower().endswith(".csv")
        and not n.startswith("__")
        and "readme" not in n.lower()
        and "metadata" not in n.lower()
    ]
    if not candidates:
        raise ParseError("No CSV found in Companies House ZIP", source="companies_house")

    # Pick the largest file — the main data CSV dwarfs any ancillary files
    return max(candidates, key=lambda n: zf.getinfo(n).file_size)


def parse_companies_house(
    zip_path: Path,
    *,
    batch_size: int = 10_000,
    skip_old_dissolved: bool = True,
) -> Iterator[list[CompaniesHouseRecord]]:
    """Parse Companies House Basic Company Data ZIP file, yielding batches of validated records.

    The ZIP contains one large CSV with a header row. Key columns include
    CompanyNumber, CompanyName, CompanyStatus, CompanyCategory, SICCode fields,
    IncorporationDate, and RegAddress.* fields.

    Parameters
    ----------
    zip_path:
        Path to the downloaded Companies House ZIP file.
    batch_size:
        Number of records per yielded batch.
    skip_old_dissolved:
        If True, skip dissolved companies incorporated more than 10 years ago.
    """
    if not zip_path.exists():
        raise ParseError(
            f"Companies House file not found: {zip_path}",
            source="companies_house",
        )

    skipped = 0
    total = 0
    batch: list[CompaniesHouseRecord] = []

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_name = _find_companies_csv(zf)
            log.info("Using Companies House CSV", source="companies_house", file=csv_name)

            with zf.open(csv_name) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                # Strip leading/trailing spaces from column headers
                if reader.fieldnames:
                    reader.fieldnames = [h.strip() for h in reader.fieldnames]
                log.info(
                    "Companies House CSV columns",
                    source="companies_house",
                    columns=reader.fieldnames,
                )

                for line_num, row in enumerate(reader, start=2):
                    total += 1

                    # --- Postcode validation ---
                    raw_postcode = row.get("RegAddress.PostCode", "").strip()
                    norm = normalise_postcode(raw_postcode)
                    if not norm:
                        skipped += 1
                        continue

                    # --- Optional: skip old dissolved companies ---
                    if skip_old_dissolved:
                        status = row.get("CompanyStatus", "").strip()
                        inc_date_raw = row.get("IncorporationDate", "").strip()
                        if _is_old_dissolved(status, inc_date_raw):
                            skipped += 1
                            continue

                    # --- Extract SIC codes ---
                    sic_1 = _extract_sic_code(row.get("SICCode.SicText_1", ""))
                    sic_2 = _extract_sic_code(row.get("SICCode.SicText_2", ""))
                    sic_3 = _extract_sic_code(row.get("SICCode.SicText_3", ""))
                    sic_4 = _extract_sic_code(row.get("SICCode.SicText_4", ""))

                    # --- Parse incorporation date ---
                    incorporation_date = _parse_incorporation_date(
                        row.get("IncorporationDate", "")
                    )

                    try:
                        record = CompaniesHouseRecord(
                            company_number=row.get("CompanyNumber", "").strip(),
                            company_name=row.get("CompanyName", "").strip() or None,
                            company_status=row.get("CompanyStatus", "").strip() or None,
                            company_type=row.get("CompanyCategory", "").strip() or None,
                            sic_code_1=sic_1,
                            sic_code_2=sic_2,
                            sic_code_3=sic_3,
                            sic_code_4=sic_4,
                            incorporation_date=incorporation_date,
                            postcode_raw=raw_postcode,
                            postcode_norm=norm,
                            address_line_1=row.get("RegAddress.AddressLine1", "").strip() or None,
                            address_line_2=row.get("RegAddress.AddressLine2", "").strip() or None,
                            post_town=row.get("RegAddress.PostTown", "").strip() or None,
                            county=row.get("RegAddress.County", "").strip() or None,
                            country=row.get("RegAddress.Country", "").strip() or None,
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
            source="companies_house",
            detail=str(exc),
        ) from exc

    if batch:
        yield batch

    log.info(
        "Companies House parsing complete",
        source="companies_house",
        total=total,
        skipped=skipped,
    )
