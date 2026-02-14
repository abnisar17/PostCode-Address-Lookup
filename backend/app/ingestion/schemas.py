from pydantic import BaseModel, field_validator


def _truncate(max_len: int):
    """Create a Pydantic field_validator that truncates strings to max_len."""

    def _validate(cls, v):
        if isinstance(v, str) and len(v) > max_len:
            return v[:max_len]
        return v

    return _validate


class CodePointRecord(BaseModel):
    """Validated record from Code-Point Open CSV."""

    postcode: str
    postcode_norm: str
    easting: int
    northing: int
    latitude: float
    longitude: float
    positional_quality: int
    country_code: str


class NSPLRecord(BaseModel):
    """Validated record from ONS NSPL CSV."""

    postcode_norm: str
    country_code: str
    region_code: str | None = None
    local_authority: str | None = None
    parliamentary_const: str | None = None
    ward_code: str | None = None
    parish_code: str | None = None
    date_introduced: str | None = None
    date_terminated: str | None = None
    is_terminated: bool = False


class OSMAddressRecord(BaseModel):
    """Validated address record from OSM .pbf."""

    osm_id: int
    osm_type: str
    house_number: str | None = None
    house_name: str | None = None
    flat: str | None = None
    street: str | None = None
    suburb: str | None = None
    city: str | None = None
    county: str | None = None
    postcode_raw: str | None = None
    postcode_norm: str | None = None
    latitude: float
    longitude: float

    # Truncate to match DB column limits
    _trunc_house_number = field_validator("house_number", mode="before")(_truncate(50))
    _trunc_house_name = field_validator("house_name", mode="before")(_truncate(200))
    _trunc_flat = field_validator("flat", mode="before")(_truncate(50))
    _trunc_street = field_validator("street", mode="before")(_truncate(200))
    _trunc_suburb = field_validator("suburb", mode="before")(_truncate(100))
    _trunc_city = field_validator("city", mode="before")(_truncate(100))
    _trunc_county = field_validator("county", mode="before")(_truncate(100))
    _trunc_postcode_raw = field_validator("postcode_raw", mode="before")(_truncate(20))
