"""Address endpoints — search and individual lookup.

Provides filtered search across all addresses (by street, city, postcode, etc.)
with offset pagination, plus retrieval of a single address by its database ID
including linked enrichment data (house prices, companies, food ratings).
"""

from fastapi import APIRouter, Depends, HTTPException, Path, Query
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
from app.core.db.models import Address
from app.core.utils.postcode import normalise_postcode

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
