"""Postcode endpoints — the core of the API.

Provides postcode lookup (the primary use case) and prefix-based
autocomplete for building type-ahead search UIs.
"""

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.api.schemas import (
    AddressResponse,
    ErrorResponse,
    PostcodeAutocompleteItem,
    PostcodeAutocompleteResponse,
    PostcodeLookupResponse,
    PostcodeResponse,
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
def autocomplete_postcodes(
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
    db: Session = Depends(get_db),
) -> PostcodeAutocompleteResponse:
    prefix = q.strip().upper().replace(" ", "")

    rows = (
        db.query(Postcode.postcode, Postcode.postcode_no_space)
        .filter(Postcode.postcode_no_space.startswith(prefix))
        .order_by(Postcode.postcode_no_space)
        .limit(limit)
        .all()
    )

    return PostcodeAutocompleteResponse(
        query=q.strip(),
        count=len(rows),
        results=[
            PostcodeAutocompleteItem(postcode=r.postcode, postcode_no_space=r.postcode_no_space)
            for r in rows
        ],
    )


# ── Postcode Lookup ──────────────────────────────────────────────


@router.get(
    "/{postcode}",
    response_model=PostcodeLookupResponse,
    summary="Look up a postcode and its addresses",
    description=(
        "**Primary endpoint.** Given a UK postcode, returns the postcode's "
        "geographic metadata and every address linked to it.\n\n"
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
def lookup_postcode(
    postcode: str = Path(
        description=(
            "UK postcode to look up. Accepts any common format: "
            "'SW1A1AA', 'SW1A 1AA', 'sw1a 1aa'. "
            "Normalised internally before querying."
        ),
        examples=["SW1A1AA", "SW1A 1AA", "EC1A1BB"],
    ),
    db: Session = Depends(get_db),
) -> PostcodeLookupResponse:
    normalised = normalise_postcode(postcode)
    if normalised is None:
        raise HTTPException(
            status_code=422,
            detail=f"'{postcode}' is not a valid UK postcode format",
        )

    no_space = postcode_no_space(normalised)

    postcode_row = (
        db.query(Postcode)
        .filter(Postcode.postcode_no_space == no_space)
        .first()
    )
    if postcode_row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Postcode '{normalised}' not found",
        )

    addresses = (
        db.query(Address)
        .filter(Address.postcode_id == postcode_row.id)
        .order_by(Address.street, Address.house_number)
        .all()
    )

    return PostcodeLookupResponse(
        postcode=PostcodeResponse.model_validate(postcode_row),
        address_count=len(addresses),
        addresses=[AddressResponse.model_validate(a) for a in addresses],
    )
