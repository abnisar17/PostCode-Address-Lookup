"""FSA Food Hygiene Ratings parser — fetches from the FSA REST API and yields validated record batches."""

from collections.abc import Iterator
from datetime import date, datetime

import httpx
from pydantic import ValidationError

from app.core.exceptions import ParseError
from app.core.logging import get_logger
from app.core.utils.postcode import normalise_postcode
from app.ingestion.schemas import FSARatingRecord

log = get_logger(__name__)

# Default API page size — the FSA API supports up to 5000 per page
_API_PAGE_SIZE = 5000

# Required headers for the FSA API
_API_HEADERS = {
    "x-api-version": "2",
    "accept": "application/json",
}

# HTTP timeout in seconds for each API request
_REQUEST_TIMEOUT = 60.0


def _parse_rating_date(raw: str | None) -> date | None:
    """Parse an ISO-format datetime string from the FSA API into a date.

    The API returns dates like "2024-01-15T00:00:00".
    Returns None if the input is empty or unparseable.
    """
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw).date()
    except ValueError:
        return None


def _parse_float(raw) -> float | None:
    """Safely parse a float from the API response.

    The API sometimes returns empty strings, None, or "0" for coordinates.
    Returns None if the value is empty or not a valid number.
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw) if raw != 0 else None
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


def _parse_int(raw) -> int | None:
    """Safely parse an int from the API response.

    Returns None if the value is empty, None, or not a valid integer.
    """
    if raw is None:
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        return int(raw)
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return None
        try:
            return int(raw)
        except ValueError:
            return None
    return None


def _fetch_authorities(client: httpx.Client, api_base_url: str) -> list[dict]:
    """Fetch the list of all local authorities from the FSA API."""
    endpoint = f"{api_base_url}/Authorities/basic"
    try:
        response = client.get(endpoint)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise ParseError(
            f"FSA Authorities API returned HTTP {exc.response.status_code}",
            source="fsa",
            detail=str(exc),
        ) from exc
    except httpx.RequestError as exc:
        raise ParseError(
            f"FSA Authorities API request failed: {exc}",
            source="fsa",
            detail=str(exc),
        ) from exc
    data = response.json()
    return data.get("authorities", [])


def _parse_establishment(item: dict, skipped_count: list[int]) -> FSARatingRecord | None:
    """Parse a single FSA establishment into a validated record, or None if invalid."""
    raw_postcode = (item.get("PostCode") or "").strip()
    norm = normalise_postcode(raw_postcode)
    if not norm:
        skipped_count[0] += 1
        return None

    latitude = _parse_float(item.get("Latitude"))
    longitude = _parse_float(item.get("Longitude"))
    rating_date = _parse_rating_date(item.get("RatingDate"))
    scores = item.get("Scores") or {}

    try:
        return FSARatingRecord(
            fhrs_id=item["FHRSID"],
            business_name=(item.get("BusinessName") or "").strip() or None,
            business_type=(item.get("BusinessType") or "").strip() or None,
            business_type_id=_parse_int(item.get("BusinessTypeID")),
            rating_value=(item.get("RatingValue") or "").strip() or None,
            rating_date=rating_date,
            postcode_raw=raw_postcode,
            postcode_norm=norm,
            address_line_1=(item.get("AddressLine1") or "").strip() or None,
            address_line_2=(item.get("AddressLine2") or "").strip() or None,
            address_line_3=(item.get("AddressLine3") or "").strip() or None,
            address_line_4=(item.get("AddressLine4") or "").strip() or None,
            latitude=latitude,
            longitude=longitude,
            local_authority_code=(item.get("LocalAuthorityCode") or "").strip() or None,
            local_authority_name=(item.get("LocalAuthorityName") or "").strip() or None,
            scores_hygiene=_parse_int(scores.get("Hygiene")),
            scores_structural=_parse_int(scores.get("Structural")),
            scores_management=_parse_int(scores.get("ConfidenceInManagement")),
        )
    except (ValidationError, ValueError, KeyError):
        skipped_count[0] += 1
        return None


def fetch_and_parse_fsa(
    api_base_url: str,
    *,
    batch_size: int = 2000,
) -> Iterator[list[FSARatingRecord]]:
    """Fetch FSA Food Hygiene Rating data from the REST API, yielding batches of validated records.

    The FSA API requires a filter parameter, so this function iterates over all
    local authorities and fetches establishments for each one.

    Parameters
    ----------
    api_base_url:
        Base URL for the FSA API (e.g. "https://api.ratings.food.gov.uk").
    batch_size:
        Number of records per yielded batch.
    """
    skipped_count = [0]  # mutable counter for helper
    total = 0
    batch: list[FSARatingRecord] = []

    api_base_url = api_base_url.rstrip("/")
    endpoint = f"{api_base_url}/Establishments"

    try:
        with httpx.Client(headers=_API_HEADERS, timeout=_REQUEST_TIMEOUT) as client:
            authorities = _fetch_authorities(client, api_base_url)
            log.info(
                "FSA: fetching by local authority",
                source="fsa",
                authority_count=len(authorities),
            )

            for auth_idx, authority in enumerate(authorities, 1):
                auth_id = authority.get("LocalAuthorityId")
                auth_name = authority.get("Name", "Unknown")
                if not auth_id:
                    continue

                page_number = 1
                total_pages: int | None = None

                while True:
                    log.info(
                        "Fetching FSA page",
                        source="fsa",
                        authority=f"{auth_name} ({auth_idx}/{len(authorities)})",
                        page=page_number,
                        total_pages=total_pages,
                    )

                    params = {
                        "localAuthorityId": auth_id,
                        "pageSize": _API_PAGE_SIZE,
                        "pageNumber": page_number,
                    }

                    try:
                        response = client.get(endpoint, params=params)
                        response.raise_for_status()
                    except httpx.HTTPStatusError as exc:
                        log.warning(
                            "FSA API error for authority, skipping",
                            source="fsa",
                            authority=auth_name,
                            status=exc.response.status_code,
                        )
                        break
                    except httpx.RequestError as exc:
                        log.warning(
                            "FSA API request failed for authority, skipping",
                            source="fsa",
                            authority=auth_name,
                            error=str(exc),
                        )
                        break

                    data = response.json()
                    establishments = data.get("establishments", [])
                    meta = data.get("meta", {})

                    if total_pages is None:
                        total_count = meta.get("totalCount", 0)
                        page_size = meta.get("pageSize", _API_PAGE_SIZE)
                        total_pages = (total_count + page_size - 1) // page_size if page_size > 0 else 1

                    if not establishments:
                        break

                    for item in establishments:
                        total += 1
                        record = _parse_establishment(item, skipped_count)
                        if record is None:
                            continue
                        batch.append(record)
                        if len(batch) >= batch_size:
                            yield batch
                            batch = []

                    page_number += 1
                    if total_pages is not None and page_number > total_pages:
                        break

    except ParseError:
        raise
    except Exception as exc:
        raise ParseError(
            f"Unexpected error fetching FSA data: {exc}",
            source="fsa",
            detail=str(exc),
        ) from exc

    if batch:
        yield batch

    log.info(
        "FSA parsing complete",
        source="fsa",
        total=total,
        skipped=skipped_count[0],
    )
