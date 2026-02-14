"""NSPL parser — pure function: ZIP path → Iterator of validated record batches."""

import csv
import io
import zipfile
from collections.abc import Iterator
from pathlib import Path

from pydantic import ValidationError

from app.core.exceptions import ParseError
from app.core.logging import get_logger
from app.core.utils.postcode import normalise_postcode
from app.ingestion.schemas import NSPLRecord

log = get_logger(__name__)

# Column name mappings: (preferred_patterns, fallback_name)
# NSPL releases suffix columns with year codes (e.g. ctry25cd, lad25cd).
_COLUMN_MAP = {
    "country_code": (["ctry"], "ctry"),
    "region_code": (["rgn"], "rgn"),
    "local_authority": (["lad", "laua"], "laua"),
    "parliamentary_const": (["pcon"], "pcon"),
    "ward_code": (["wd", "ward"], "ward"),
    "parish_code": (["parish"], "parish"),
}


def _resolve_columns(headers: list[str]) -> dict[str, str]:
    """Map logical field names to actual CSV column names.

    Handles versioned column names like 'ctry25cd', 'lad25cd', etc.
    """
    resolved: dict[str, str] = {}
    for field, (prefixes, fallback) in _COLUMN_MAP.items():
        # Try exact fallback first
        if fallback in headers:
            resolved[field] = fallback
            continue
        # Try prefix match (e.g. 'ctry' matches 'ctry25cd')
        for prefix in prefixes:
            matches = [h for h in headers if h.startswith(prefix)]
            if matches:
                resolved[field] = matches[0]
                break
        else:
            resolved[field] = fallback  # will produce empty string
    return resolved


def _find_nspl_csv(zf: zipfile.ZipFile) -> str:
    """Find the main NSPL data CSV inside the ZIP (ignoring metadata files)."""
    candidates = [
        n
        for n in zf.namelist()
        if n.lower().endswith(".csv")
        and "nspl" in n.lower()
        and "metadata" not in n.lower()
        and "multi_csv" not in n.lower()
        and not n.startswith("__")
    ]
    if not candidates:
        # Fallback: largest CSV in the archive
        csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_files:
            raise ParseError("No CSV found in NSPL ZIP", source="nspl")
        candidates = csv_files

    # Pick the largest file (main NSPL CSV is ~960MB vs split files ~5-20MB each)
    return max(candidates, key=lambda n: zf.getinfo(n).file_size)


def parse_nspl(
    zip_path: Path,
    *,
    batch_size: int = 10_000,
) -> Iterator[list[NSPLRecord]]:
    """Parse NSPL ZIP file, yielding batches of validated records.

    NSPL CSV has a header row. Column names vary by release — we try
    both the current (suffixed with year, e.g. ctry25cd) and legacy names.
    Key columns:
    - pcds: postcode
    - ctry*cd / ctry: country code
    - rgn*cd / rgn: region code
    - lad*cd / laua: local authority
    - pcon*cd / pcon: parliamentary constituency
    - wd*cd / ward: ward code
    - dointr: date introduced (YYYYMM)
    - doterm: date terminated (YYYYMM or empty)
    """
    if not zip_path.exists():
        raise ParseError(f"NSPL file not found: {zip_path}", source="nspl")

    skipped = 0
    total = 0
    batch: list[NSPLRecord] = []

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_name = _find_nspl_csv(zf)
            log.info("Using NSPL CSV", source="nspl", file=csv_name)

            with zf.open(csv_name) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
                col = _resolve_columns(reader.fieldnames or [])
                log.info("Resolved NSPL columns", source="nspl", columns=col)

                for line_num, row in enumerate(reader, start=2):
                    total += 1

                    raw_postcode = row.get("pcds", "").strip()
                    norm = normalise_postcode(raw_postcode)
                    if not norm:
                        skipped += 1
                        continue

                    doterm = row.get("doterm", "").strip()

                    try:
                        record = NSPLRecord(
                            postcode_norm=norm,
                            country_code=row.get(col["country_code"], "").strip(),
                            region_code=row.get(col["region_code"], "").strip() or None,
                            local_authority=row.get(col["local_authority"], "").strip() or None,
                            parliamentary_const=row.get(col["parliamentary_const"], "").strip() or None,
                            ward_code=row.get(col["ward_code"], "").strip() or None,
                            parish_code=row.get(col["parish_code"], "").strip() or None,
                            date_introduced=row.get("dointr", "").strip() or None,
                            date_terminated=doterm or None,
                            is_terminated=bool(doterm),
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
            source="nspl",
            detail=str(exc),
        ) from exc

    if batch:
        yield batch

    log.info(
        "NSPL parsing complete",
        source="nspl",
        total=total,
        skipped=skipped,
    )
