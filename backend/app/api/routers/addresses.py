"""Address endpoints — search and individual lookup.

Provides filtered search across all addresses (by street, city, postcode, etc.)
with offset pagination, plus retrieval of a single address by its database ID
including linked enrichment data (house prices, companies, food ratings).
"""

import re

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from pydantic import BaseModel, Field
from sqlalchemy import func, literal_column, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.api.schemas import (
    AddressDetailResponse,
    AddressListResponse,
    AddressResponse,
    CompanyResponse,
    ErrorResponse,
    FoodRatingResponse,
    PricePaidResponse,
    VOARatingResponse,
)
from app.core.db.models import Address, AddressSubmission, Postcode
from app.core.utils.postcode import normalise_postcode, postcode_no_space

router = APIRouter(prefix="/addresses", tags=["Addresses"])


@router.get(
    "/search",
    response_model=AddressListResponse,
    summary="Search addresses with filters",
    description=(
        "Search across all addresses using optional filter parameters. "
        "All text filters use case-insensitive partial matching (SQL `ILIKE`), "
        "so `q=downing` will match 'Downing Street'.\n\n"
        "**Filters can be combined** — for example, searching for "
        "`street=High&city=London` returns addresses on streets containing "
        "'High' in the city of London.\n\n"
        "Results are paginated. Use `page` and `page_size` to navigate."
    ),
    responses={
        200: {
            "description": "Paginated address results (may be empty if no matches)",
            "content": {
                "application/json": {
                    "example": {
                        "count": 2,
                        "total": 42,
                        "page": 1,
                        "page_size": 20,
                        "results": [
                            {
                                "id": 1,
                                "postcode_raw": "SW1A 1AA",
                                "house_number": "10",
                                "house_name": None,
                                "flat": None,
                                "street": "Downing Street",
                                "suburb": None,
                                "city": "London",
                                "county": None,
                                "latitude": 51.5034,
                                "longitude": -0.1276,
                                "confidence": 0.95,
                                "is_complete": True,
                                "source": "osm",
                                "uprn": None,
                            }
                        ],
                    }
                }
            },
        },
        422: {"description": "Invalid pagination or filter parameters"},
    },
)
async def search_addresses(
    q: str | None = Query(
        default=None,
        min_length=2,
        description=(
            "General text search — matches against street, city, "
            "house name, and suburb simultaneously (case-insensitive)"
        ),
        examples=["Downing"],
    ),
    postcode: str | None = Query(
        default=None,
        description=(
            "Filter by postcode. Normalised internally, so 'sw1a1aa' "
            "and 'SW1A 1AA' are equivalent"
        ),
        examples=["SW1A 1AA"],
    ),
    street: str | None = Query(
        default=None,
        description="Filter by street name (case-insensitive partial match)",
        examples=["Downing"],
    ),
    city: str | None = Query(
        default=None,
        description="Filter by city or town (case-insensitive partial match)",
        examples=["London"],
    ),
    source: str | None = Query(
        default=None,
        description="Filter by data source (osm, land_registry, epc, companies_house, fsa, voa)",
        examples=["osm"],
    ),
    page: int = Query(
        default=1,
        ge=1,
        description="Page number (1-indexed)",
    ),
    page_size: int = Query(
        default=20,
        ge=1,
        le=100,
        description="Number of results per page (max 100)",
    ),
    db: AsyncSession = Depends(get_db),
) -> AddressListResponse:
    # Require at least one filter to avoid full-table scans
    if not any([q, postcode, street, city, source]):
        return AddressListResponse(
            count=0, total=0, page=page, page_size=page_size, results=[]
        )

    # Build WHERE conditions once, reuse for both count and data queries
    # Always exclude soft-marked duplicates
    conditions = [Address.duplicate_of.is_(None)]

    # General text search across multiple fields
    if q:
        pattern = f"%{q}%"
        conditions.append(
            Address.street.ilike(pattern)
            | Address.city.ilike(pattern)
            | Address.house_name.ilike(pattern)
            | Address.suburb.ilike(pattern)
        )

    # Postcode filter
    if postcode:
        normalised = normalise_postcode(postcode)
        if normalised:
            conditions.append(Address.postcode_norm == normalised)
        else:
            # Invalid postcode format — return empty results
            return AddressListResponse(
                count=0, total=0, page=page, page_size=page_size, results=[]
            )

    # Individual field filters
    if street:
        conditions.append(Address.street.ilike(f"%{street}%"))
    if city:
        conditions.append(Address.city.ilike(f"%{city}%"))
    if source:
        conditions.append(Address.source == source)

    # Set a per-query statement timeout so only search queries are bounded
    await db.execute(text("SET LOCAL statement_timeout = '10s'"))

    # Capped count — stop scanning once we reach 10 000 to avoid full-table scans
    COUNT_CAP = 10_000
    capped_sub = (
        select(literal_column("1"))
        .select_from(Address)
        .where(*conditions)
        .limit(COUNT_CAP)
        .subquery()
    )
    count_stmt = select(func.count()).select_from(capped_sub)

    offset = (page - 1) * page_size

    # ORDER BY id uses the primary-key index, avoiding a full sort of all matches
    data_stmt = (
        select(Address)
        .where(*conditions)
        .order_by(Address.id)
        .offset(offset)
        .limit(page_size)
    )

    result = await db.execute(data_stmt)
    rows = result.scalars().all()

    total = await db.scalar(count_stmt) or 0

    return AddressListResponse(
        count=len(rows),
        total=total,
        page=page,
        page_size=page_size,
        results=[AddressResponse.model_validate(r) for r in rows],
    )


@router.get(
    "/{address_id}",
    response_model=AddressDetailResponse,
    summary="Get a single address by ID with enrichment data",
    description=(
        "Retrieve the full details of a single address using its "
        "internal database identifier, including linked house prices, "
        "companies, and food ratings. Useful for deep-linking to a "
        "specific address from search results."
    ),
    responses={
        200: {"description": "Address found with enrichment data"},
        404: {
            "model": ErrorResponse,
            "description": "No address exists with the given ID",
        },
    },
)
async def get_address(
    address_id: int = Path(
        description="Unique database identifier for the address",
        examples=[1, 42, 10500],
    ),
    db: AsyncSession = Depends(get_db),
) -> AddressDetailResponse:
    stmt = (
        select(Address)
        .where(Address.id == address_id)
        .options(
            selectinload(Address.price_paid_records),
            selectinload(Address.company_records),
            selectinload(Address.food_rating_records),
            selectinload(Address.voa_rating_records),
        )
    )
    result = await db.execute(stmt)
    address = result.scalars().first()

    if address is None:
        raise HTTPException(
            status_code=404,
            detail=f"Address with id {address_id} not found",
        )

    detail = AddressDetailResponse.model_validate(address)
    detail.price_paid = [
        PricePaidResponse.model_validate(pp) for pp in address.price_paid_records
    ]
    detail.companies = [
        CompanyResponse.model_validate(c) for c in address.company_records
    ]
    detail.food_ratings = [
        FoodRatingResponse.model_validate(fr) for fr in address.food_rating_records
    ]
    detail.voa_ratings = [
        VOARatingResponse.model_validate(vr) for vr in address.voa_rating_records
    ]
    return detail


# ── Submit a missing address (moderation queue) ──────────────────


class AddressSubmitRequest(BaseModel):
    postcode: str = Field(min_length=5, max_length=10, examples=["HG1 2BP"])
    house_number: str | None = Field(default=None, max_length=100, examples=["15"])
    house_name: str | None = Field(default=None, max_length=200)
    flat: str | None = Field(default=None, max_length=50)
    street: str | None = Field(default=None, max_length=200, examples=["Fewston Crescent"])
    city: str | None = Field(default=None, max_length=100, examples=["Harrogate"])
    county: str | None = Field(default=None, max_length=100)


class AddressSubmitResponse(BaseModel):
    detail: str
    id: int


@router.post(
    "/submit",
    response_model=AddressSubmitResponse,
    status_code=201,
    summary="Submit a missing address for review",
    description=(
        "Submit an address that is missing from the database. Submissions are "
        "**not** added immediately — they enter a moderation queue and appear "
        "in search only after an administrator approves them."
    ),
    responses={
        201: {"description": "Submission received and queued for review"},
        404: {"model": ErrorResponse, "description": "Postcode not found in the database"},
        422: {"model": ErrorResponse, "description": "Invalid postcode or no address detail provided"},
    },
)
async def submit_address(
    body: AddressSubmitRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> AddressSubmitResponse:
    normalised = normalise_postcode(body.postcode)
    if normalised is None:
        raise HTTPException(
            status_code=422, detail=f"'{body.postcode}' is not a valid UK postcode format"
        )

    # The postcode must exist in our dataset (submissions attach to a real postcode).
    no_space = postcode_no_space(normalised)
    postcode_row = await db.scalar(
        select(Postcode).where(Postcode.postcode_no_space == no_space)
    )
    if postcode_row is None:
        raise HTTPException(status_code=404, detail=f"Postcode '{normalised}' not found")

    # Require at least a street or a house identifier — reject empty submissions.
    if not (body.house_number or body.house_name or body.street):
        raise HTTPException(
            status_code=422,
            detail="Provide at least a street name or a house number/name.",
        )

    def _clean(v: str | None) -> str | None:
        v = v.strip() if v else None
        return v or None

    submission = AddressSubmission(
        postcode_raw=body.postcode.strip().upper(),
        postcode_norm=normalised,
        house_number=_clean(body.house_number),
        house_name=_clean(body.house_name),
        flat=_clean(body.flat),
        street=_clean(body.street),
        city=_clean(body.city),
        county=_clean(body.county),
        status="pending",
        submitter_ip=request.client.host if request.client else None,
    )
    db.add(submission)
    await db.commit()
    await db.refresh(submission)

    return AddressSubmitResponse(
        detail="Thanks — your address has been submitted and will appear once reviewed.",
        id=submission.id,
    )


# ── Direct add via API (authenticated, no queue) ─────────────────

# Street-suffix abbreviations, expanded only on the LAST token for dedup
# comparison (so leading "St" = Saint is never mangled).
_STREET_SUFFIX = {
    "ST": "STREET", "RD": "ROAD", "AVE": "AVENUE", "AV": "AVENUE",
    "CRES": "CRESCENT", "CR": "CRESCENT", "LN": "LANE", "DR": "DRIVE",
    "CT": "COURT", "PL": "PLACE", "SQ": "SQUARE", "GDNS": "GARDENS",
    "TER": "TERRACE", "CL": "CLOSE", "PK": "PARK", "GRV": "GROVE",
}


def _norm_token(value: str | None) -> str:
    """Uppercase, strip punctuation, collapse whitespace — for dedup matching."""
    if not value:
        return ""
    v = re.sub(r"[.,]", " ", value.upper())
    return re.sub(r"\s+", " ", v).strip()


def _norm_street(street: str | None) -> str:
    v = _norm_token(street)
    if not v:
        return ""
    parts = v.split(" ")
    if parts[-1] in _STREET_SUFFIX:
        parts[-1] = _STREET_SUFFIX[parts[-1]]
    return " ".join(parts)


class AddressCreateRequest(BaseModel):
    postcode: str = Field(min_length=5, max_length=10, examples=["HG1 2BP"])
    house_number: str | None = Field(default=None, max_length=100, examples=["15"])
    house_name: str | None = Field(default=None, max_length=200)
    flat: str | None = Field(default=None, max_length=50)
    street: str | None = Field(default=None, max_length=200, examples=["Fewston Crescent"])
    city: str | None = Field(default=None, max_length=100, examples=["Harrogate"])
    county: str | None = Field(default=None, max_length=100)


class AddressCreateResponse(BaseModel):
    created: bool = Field(description="True if a new address was inserted; False if it already existed")
    id: int = Field(description="Database id of the created or matched address")
    detail: str


@router.post(
    "",
    response_model=AddressCreateResponse,
    status_code=201,
    summary="Add an address directly (API, authenticated)",
    description=(
        "Insert an address straight into the master database — intended for "
        "authenticated integrations (EPOS/partners) that add an address when a "
        "lookup returns nothing. Requires a valid API key.\n\n"
        "Guardrails: the postcode must be a valid UK format **and** exist in the "
        "dataset; at least a street or house number/name is required; and the "
        "request is **de-duplicated** — if a matching address already exists at "
        "that postcode the existing one is returned (HTTP 200) instead of "
        "creating a duplicate. New rows are tagged `source=api` and attributed "
        "to the calling API key."
    ),
    responses={
        201: {"description": "New address created"},
        200: {"description": "A matching address already existed; returned as-is"},
        401: {"description": "API key missing"},
        403: {"description": "API key invalid or deactivated"},
        404: {"model": ErrorResponse, "description": "Postcode not found in the dataset"},
        422: {"model": ErrorResponse, "description": "Invalid postcode or no address detail provided"},
    },
)
async def create_address(
    body: AddressCreateRequest,
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db),
) -> AddressCreateResponse:
    normalised = normalise_postcode(body.postcode)
    if normalised is None:
        raise HTTPException(
            status_code=422, detail=f"'{body.postcode}' is not a valid UK postcode format"
        )

    if not (body.house_number or body.house_name or body.street):
        raise HTTPException(
            status_code=422,
            detail="Provide at least a street name or a house number/name.",
        )

    no_space = postcode_no_space(normalised)
    postcode_row = await db.scalar(
        select(Postcode).where(Postcode.postcode_no_space == no_space)
    )
    if postcode_row is None:
        raise HTTPException(status_code=404, detail=f"Postcode '{normalised}' not found")

    # ── De-dup: compare against existing non-duplicate addresses at this postcode ──
    want = (
        _norm_token(body.house_number),
        _norm_token(body.house_name),
        _norm_token(body.flat),
        _norm_street(body.street),
    )
    existing_rows = (
        await db.execute(
            select(Address)
            .where(Address.postcode_norm == normalised)
            .where(Address.duplicate_of.is_(None))
        )
    ).scalars().all()
    for row in existing_rows:
        have = (
            _norm_token(row.house_number),
            _norm_token(row.house_name),
            _norm_token(row.flat),
            _norm_street(row.street),
        )
        if have == want:
            response.status_code = 200
            return AddressCreateResponse(
                created=False, id=row.id, detail="Address already exists"
            )

    location = None
    if postcode_row.latitude is not None and postcode_row.longitude is not None:
        location = func.ST_SetSRID(
            func.ST_MakePoint(postcode_row.longitude, postcode_row.latitude), 4326
        )

    def _clean(v: str | None) -> str | None:
        v = v.strip() if v else None
        return v or None

    address = Address(
        postcode_id=postcode_row.id,
        postcode_raw=body.postcode.strip().upper(),
        postcode_norm=normalised,
        house_number=_clean(body.house_number),
        house_name=_clean(body.house_name),
        flat=_clean(body.flat),
        street=_clean(body.street),
        city=_clean(body.city),
        county=_clean(body.county),
        latitude=postcode_row.latitude,
        longitude=postcode_row.longitude,
        location=location,
        source="api",
        confidence=0.6,
        is_complete=bool((body.house_number or body.house_name) and body.street),
        added_by_key_id=getattr(request.state, "api_key_id", None),
    )
    address.source_id = None  # api rows are identified by id + added_by_key_id
    db.add(address)
    await db.commit()
    await db.refresh(address)

    return AddressCreateResponse(
        created=True, id=address.id, detail="Address added to the database"
    )
