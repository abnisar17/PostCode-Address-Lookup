"""Pydantic v2 response models for the Postcode Lookup API.

These schemas define the JSON shape returned by every endpoint.
They are intentionally separate from the ingestion schemas — the API
exposes only the fields relevant to consumers, and flattens the
PostGIS geometry into simple lat/lon floats.
"""

from pydantic import BaseModel, ConfigDict, Field


# ── Postcode ─────────────────────────────────────────────────────

class PostcodeResponse(BaseModel):
    """A single UK postcode with its geographic coordinates and admin metadata."""

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Internal database identifier")
    postcode: str = Field(
        description="Formatted postcode with space, e.g. 'SW1A 1AA'",
        examples=["SW1A 1AA"],
    )
    postcode_no_space: str = Field(
        description="Postcode without space, e.g. 'SW1A1AA'",
        examples=["SW1A1AA"],
    )
    latitude: float | None = Field(
        description="WGS84 latitude (decimal degrees)",
        examples=[51.501009],
    )
    longitude: float | None = Field(
        description="WGS84 longitude (decimal degrees)",
        examples=[-0.141588],
    )
    country_code: str | None = Field(
        default=None,
        description="ONS country code (e.g. 'E92000001' for England)",
    )
    region_code: str | None = Field(
        default=None,
        description="ONS region code",
    )
    local_authority: str | None = Field(
        default=None,
        description="ONS local authority district code",
    )
    is_terminated: bool = Field(
        description="Whether this postcode has been terminated by Royal Mail",
    )


class PostcodeAutocompleteItem(BaseModel):
    """A single autocomplete suggestion returned during prefix search."""

    model_config = ConfigDict(from_attributes=True)

    postcode: str = Field(
        description="Formatted postcode with space",
        examples=["SW1A 1AA"],
    )
    postcode_no_space: str = Field(
        description="Postcode without space, for use in URL paths",
        examples=["SW1A1AA"],
    )


class PostcodeAutocompleteResponse(BaseModel):
    """Autocomplete results for a postcode prefix search."""

    query: str = Field(
        description="The original search prefix that was submitted",
        examples=["SW1A"],
    )
    count: int = Field(description="Number of matching postcodes returned")
    results: list[PostcodeAutocompleteItem] = Field(
        description="Matching postcodes ordered alphabetically",
    )


# ── Address ──────────────────────────────────────────────────────

class AddressResponse(BaseModel):
    """A single address record with its location and quality metadata."""

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Internal database identifier")
    postcode_raw: str | None = Field(
        default=None,
        description="Postcode as it appeared in the source data",
        examples=["SW1A 1AA"],
    )
    house_number: str | None = Field(
        default=None,
        description="Street number, e.g. '10'",
        examples=["10"],
    )
    house_name: str | None = Field(
        default=None,
        description="Named building, e.g. 'Buckingham Palace'",
    )
    flat: str | None = Field(
        default=None,
        description="Flat or apartment identifier",
        examples=["Flat 3"],
    )
    street: str | None = Field(
        default=None,
        description="Street name, e.g. 'Downing Street'",
        examples=["Downing Street"],
    )
    suburb: str | None = Field(
        default=None,
        description="Suburb or district within a city",
    )
    city: str | None = Field(
        default=None,
        description="City or town name",
        examples=["London"],
    )
    county: str | None = Field(
        default=None,
        description="County name",
    )
    latitude: float | None = Field(
        default=None,
        description="WGS84 latitude (decimal degrees)",
        examples=[51.501009],
    )
    longitude: float | None = Field(
        default=None,
        description="WGS84 longitude (decimal degrees)",
        examples=[-0.141588],
    )
    confidence: float | None = Field(
        default=None,
        description="Data quality confidence score (0.0–1.0)",
    )
    is_complete: bool = Field(
        description="Whether the address has all key fields populated",
    )

    @property
    def formatted(self) -> str:
        """Human-readable one-line address (not serialised by default)."""
        parts = [self.flat, self.house_number, self.house_name, self.street,
                 self.suburb, self.city, self.county, self.postcode_raw]
        return ", ".join(p for p in parts if p)


class AddressListResponse(BaseModel):
    """Paginated list of addresses returned by search or postcode lookup."""

    count: int = Field(description="Number of addresses in this page")
    total: int = Field(description="Total number of addresses matching the query")
    page: int = Field(description="Current page number (1-indexed)")
    page_size: int = Field(description="Maximum number of results per page")
    results: list[AddressResponse] = Field(description="Address records")


# ── Postcode Lookup (primary endpoint) ───────────────────────────

class PostcodeLookupResponse(BaseModel):
    """Full postcode lookup result: postcode metadata plus all addresses at that postcode.

    This is the primary response for the `GET /postcodes/{postcode}` endpoint —
    the main use case of the API.
    """

    postcode: PostcodeResponse = Field(
        description="Postcode geographic and administrative details",
    )
    address_count: int = Field(
        description="Total number of addresses linked to this postcode",
    )
    addresses: list[AddressResponse] = Field(
        description="All addresses at this postcode, ordered by street then house number",
    )


# ── Health ───────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    """API health check result including database connectivity and record counts."""

    status: str = Field(
        description="Overall health status: 'healthy' or 'unhealthy'",
        examples=["healthy"],
    )
    database: str = Field(
        description="Database connectivity status: 'connected' or 'unreachable'",
        examples=["connected"],
    )
    postcode_count: int = Field(
        description="Total number of postcodes in the database",
    )
    address_count: int = Field(
        description="Total number of addresses in the database",
    )


# ── Error ────────────────────────────────────────────────────────

class ErrorResponse(BaseModel):
    """Structured error returned for 4xx and 5xx responses."""

    detail: str = Field(
        description="Human-readable error message describing what went wrong",
        examples=["Postcode 'XY1 2AB' not found"],
    )
