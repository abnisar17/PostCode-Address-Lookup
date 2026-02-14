"""Address endpoints — search and individual lookup.

Provides filtered search across all addresses (by street, city, postcode, etc.)
with offset pagination, plus retrieval of a single address by its database ID.
"""

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.api.schemas import AddressListResponse, AddressResponse, ErrorResponse
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
                            }
                        ],
                    }
                }
            },
        },
        422: {"description": "Invalid pagination or filter parameters"},
    },
)
def search_addresses(
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
    db: Session = Depends(get_db),
) -> AddressListResponse:
    query = db.query(Address)

    # General text search across multiple fields
    if q:
        pattern = f"%{q}%"
        query = query.filter(
            Address.street.ilike(pattern)
            | Address.city.ilike(pattern)
            | Address.house_name.ilike(pattern)
            | Address.suburb.ilike(pattern)
        )

    # Postcode filter
    if postcode:
        normalised = normalise_postcode(postcode)
        if normalised:
            query = query.filter(Address.postcode_norm == normalised)
        else:
            # Invalid postcode format — return empty results
            return AddressListResponse(
                count=0, total=0, page=page, page_size=page_size, results=[]
            )

    # Individual field filters
    if street:
        query = query.filter(Address.street.ilike(f"%{street}%"))
    if city:
        query = query.filter(Address.city.ilike(f"%{city}%"))

    total = query.count()
    offset = (page - 1) * page_size

    rows = (
        query
        .order_by(Address.city, Address.street, Address.house_number)
        .offset(offset)
        .limit(page_size)
        .all()
    )

    return AddressListResponse(
        count=len(rows),
        total=total,
        page=page,
        page_size=page_size,
        results=[AddressResponse.model_validate(r) for r in rows],
    )


@router.get(
    "/{address_id}",
    response_model=AddressResponse,
    summary="Get a single address by ID",
    description=(
        "Retrieve the full details of a single address using its "
        "internal database identifier. Useful for deep-linking to a "
        "specific address from search results."
    ),
    responses={
        200: {"description": "Address found"},
        404: {
            "model": ErrorResponse,
            "description": "No address exists with the given ID",
        },
    },
)
def get_address(
    address_id: int = Path(
        description="Unique database identifier for the address",
        examples=[1, 42, 10500],
    ),
    db: Session = Depends(get_db),
) -> AddressResponse:
    address = db.get(Address, address_id)
    if address is None:
        raise HTTPException(
            status_code=404,
            detail=f"Address with id {address_id} not found",
        )
    return AddressResponse.model_validate(address)
