"""Postcode endpoints — the core of the API.

Provides postcode lookup (the primary use case) and prefix-based
autocomplete for building type-ahead search UIs.
"""

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.deps import get_db
from app.api.schemas import (
    AddressDetailResponse,
    CompanyResponse,
    ErrorResponse,
    FoodRatingResponse,
    PostcodeAutocompleteItem,
    PostcodeAutocompleteResponse,
    PostcodeLookupResponse,
    PostcodeResponse,
    PricePaidResponse,
    VOARatingResponse,
)
from app.core.db.models import Address, Postcode
from app.core.utils.postcode import normalise_postcode, postcode_no_space

router = APIRouter(prefix="/postcodes", tags=["Postcodes"])


# ── Autocomplete (must be defined before /{postcode} to avoid path collision) ──


@router.get(
    "/autocomplete",
    response_model=PostcodeAutocompleteResponse,
    summary="Autocomplete postcodes by prefix",
    description=(
        "Returns postcodes matching the given prefix, ordered alphabetically. "
        "Intended for type-ahead search fields in the frontend.\n\n"
        "The search is performed against the space-free form of the postcode "
        "(e.g. `SW1A`) so callers do not need to worry about formatting.\n\n"
        "**Example:** `GET /postcodes/autocomplete?q=SW1A` returns postcodes "
        "like SW1A 0AA, SW1A 0PW, SW1A 1AA, etc."
    ),
    responses={
        200: {
            "description": "Matching postcodes (may be empty if no matches)",
            "content": {
                "application/json": {
                    "example": {
                        "query": "SW1A",
                        "count": 3,
                        "results": [
                            {"postcode": "SW1A 0AA", "postcode_no_space": "SW1A0AA"},
                            {"postcode": "SW1A 0PW", "postcode_no_space": "SW1A0PW"},
                            {"postcode": "SW1A 1AA", "postcode_no_space": "SW1A1AA"},
                        ],
                    }
                }
            },
        },
        422: {"description": "Query parameter missing or too short"},
    },
)
async def autocomplete_postcodes(
    q: str = Query(
        ...,
        min_length=2,
        max_length=8,
        description=(
            "Postcode prefix to search for (minimum 2 characters). "
            "Spaces are stripped automatically. Case-insensitive."
        ),
        examples=["SW1A", "EC1"],
    ),
    limit: int = Query(
        default=10,
        ge=1,
        le=50,
        description="Maximum number of suggestions to return",
    ),
    db: AsyncSession = Depends(get_db),
) -> PostcodeAutocompleteResponse:
    prefix = q.strip().upper().replace(" ", "")

    stmt = (
        select(Postcode.postcode, Postcode.postcode_no_space)
        .where(Postcode.postcode_no_space.startswith(prefix))
        .order_by(Postcode.postcode_no_space)
        .limit(limit)
    )
    result = await db.execute(stmt)
    rows = result.all()

    return PostcodeAutocompleteResponse(
        query=q.strip(),
        count=len(rows),
        results=[
            PostcodeAutocompleteItem(postcode=r.postcode, postcode_no_space=r.postcode_no_space)
            for r in rows
        ],
    )


# ── Postcode Lookup ──────────────────────────────────────────────


def _build_address_detail(address: Address) -> AddressDetailResponse:
    """Build an AddressDetailResponse from an Address ORM model with eager-loaded relations."""
    base = AddressDetailResponse.model_validate(address)
    base.price_paid = [
        PricePaidResponse.model_validate(pp) for pp in address.price_paid_records
    ]
    base.companies = [
        CompanyResponse.model_validate(c) for c in address.company_records
    ]
    base.food_ratings = [
        FoodRatingResponse.model_validate(fr) for fr in address.food_rating_records
    ]
    base.voa_ratings = [
        VOARatingResponse.model_validate(vr) for vr in address.voa_rating_records
    ]
    return base


@router.get(
    "/{postcode}",
    response_model=PostcodeLookupResponse,
    summary="Look up a postcode and its addresses",
    description=(
        "**Primary endpoint.** Given a UK postcode, returns the postcode's "
        "geographic metadata and every address linked to it, including "
        "enrichment data (house prices, companies, food ratings).\n\n"
        "The postcode is normalised automatically — `sw1a1aa`, `SW1A 1AA`, and "
        "`sw1a 1aa` all resolve to the same record.\n\n"
        "Addresses are sorted by street name, then house number."
    ),
    responses={
        200: {"description": "Postcode found with its addresses"},
        404: {
            "model": ErrorResponse,
            "description": "No postcode matching the input was found in the database",
        },
        422: {
            "model": ErrorResponse,
            "description": (
                "The input could not be recognised as a valid UK postcode "
                "(e.g. too short, wrong format)"
            ),
        },
    },
)
async def lookup_postcode(
    postcode: str = Path(
        description=(
            "UK postcode to look up. Accepts any common format: "
            "'SW1A1AA', 'SW1A 1AA', 'sw1a 1aa'. "
            "Normalised internally before querying."
        ),
        examples=["SW1A1AA", "SW1A 1AA", "EC1A1BB"],
    ),
    db: AsyncSession = Depends(get_db),
) -> PostcodeLookupResponse:
    normalised = normalise_postcode(postcode)
    if normalised is None:
        raise HTTPException(
            status_code=422,
            detail=f"'{postcode}' is not a valid UK postcode format",
        )

    no_space = postcode_no_space(normalised)

    stmt = select(Postcode).where(Postcode.postcode_no_space == no_space)
    result = await db.execute(stmt)
    postcode_row = result.scalars().first()

    if postcode_row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Postcode '{normalised}' not found",
        )

    # Eager-load enrichment relationships to avoid N+1 queries
    addr_stmt = (
        select(Address)
        .where(Address.postcode_id == postcode_row.id)
        .options(
            selectinload(Address.price_paid_records),
            selectinload(Address.company_records),
            selectinload(Address.food_rating_records),
            selectinload(Address.voa_rating_records),
        )
        .order_by(Address.street, Address.house_number)
    )
    addr_result = await db.execute(addr_stmt)
    addresses = addr_result.scalars().all()

    return PostcodeLookupResponse(
        postcode=PostcodeResponse.model_validate(postcode_row),
        address_count=len(addresses),
        addresses=[_build_address_detail(a) for a in addresses],
    )
