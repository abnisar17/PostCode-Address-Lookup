from datetime import date

from pydantic import BaseModel, field_validator


def _truncate(max_len: int):
    """Create a Pydantic field_validator that truncates strings to max_len."""

    def _validate(cls, v):
        if isinstance(v, str) and len(v) > max_len:
            return v[:max_len]
        return v

    return _validate


# ── Existing sources ───────────────────────────────────────────────


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


# ── Land Registry Price Paid Data ──────────────────────────────────


class LandRegistryRecord(BaseModel):
    """Validated record from HM Land Registry Price Paid Data CSV.

    A single row represents one property transaction. The address fields
    are used both to create/update an address entry and to store the
    price paid transaction.
    """

    transaction_id: str
    price: int
    date_of_transfer: date
    postcode_raw: str | None = None
    postcode_norm: str | None = None
    property_type: str | None = None  # D=Detached, S=Semi, T=Terraced, F=Flat, O=Other
    old_new: str | None = None  # Y=new build, N=existing
    duration: str | None = None  # F=freehold, L=leasehold
    paon: str | None = None  # Primary Addressable Object Name (house number/name)
    saon: str | None = None  # Secondary Addressable Object Name (flat)
    street: str | None = None
    locality: str | None = None
    town: str | None = None
    district: str | None = None
    county: str | None = None
    ppd_category: str | None = None  # A=standard, B=additional
    record_status: str | None = None  # A/C/D

    _trunc_paon = field_validator("paon", mode="before")(_truncate(100))
    _trunc_saon = field_validator("saon", mode="before")(_truncate(100))
    _trunc_street = field_validator("street", mode="before")(_truncate(200))
    _trunc_locality = field_validator("locality", mode="before")(_truncate(100))
    _trunc_town = field_validator("town", mode="before")(_truncate(100))
    _trunc_district = field_validator("district", mode="before")(_truncate(100))
    _trunc_county = field_validator("county", mode="before")(_truncate(100))


# ── Companies House ────────────────────────────────────────────────


class CompaniesHouseRecord(BaseModel):
    """Validated record from Companies House basic company data CSV."""

    company_number: str
    company_name: str | None = None
    company_status: str | None = None
    company_type: str | None = None
    sic_code_1: str | None = None
    sic_code_2: str | None = None
    sic_code_3: str | None = None
    sic_code_4: str | None = None
    incorporation_date: str | None = None
    postcode_raw: str | None = None
    postcode_norm: str | None = None
    address_line_1: str | None = None
    address_line_2: str | None = None
    post_town: str | None = None
    county: str | None = None
    country: str | None = None

    _trunc_company_name = field_validator("company_name", mode="before")(_truncate(300))
    _trunc_addr1 = field_validator("address_line_1", mode="before")(_truncate(200))
    _trunc_addr2 = field_validator("address_line_2", mode="before")(_truncate(200))
    _trunc_post_town = field_validator("post_town", mode="before")(_truncate(100))
    _trunc_county = field_validator("county", mode="before")(_truncate(100))
    _trunc_country = field_validator("country", mode="before")(_truncate(100))


# ── FSA Food Hygiene Ratings ───────────────────────────────────────


class FSARatingRecord(BaseModel):
    """Validated record from FSA Food Hygiene Rating Scheme API."""

    fhrs_id: int
    business_name: str | None = None
    business_type: str | None = None
    business_type_id: int | None = None
    rating_value: str | None = None
    rating_date: date | None = None
    postcode_raw: str | None = None
    postcode_norm: str | None = None
    address_line_1: str | None = None
    address_line_2: str | None = None
    address_line_3: str | None = None
    address_line_4: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    local_authority_code: str | None = None
    local_authority_name: str | None = None
    scores_hygiene: int | None = None
    scores_structural: int | None = None
    scores_management: int | None = None

    _trunc_business_name = field_validator("business_name", mode="before")(_truncate(300))
    _trunc_business_type = field_validator("business_type", mode="before")(_truncate(100))
    _trunc_addr1 = field_validator("address_line_1", mode="before")(_truncate(200))
    _trunc_addr2 = field_validator("address_line_2", mode="before")(_truncate(200))
    _trunc_addr3 = field_validator("address_line_3", mode="before")(_truncate(200))
    _trunc_addr4 = field_validator("address_line_4", mode="before")(_truncate(200))
    _trunc_la_name = field_validator("local_authority_name", mode="before")(_truncate(100))


# ── OS Open UPRN ───────────────────────────────────────────────────


class UPRNRecord(BaseModel):
    """Validated record from OS Open UPRN CSV."""

    uprn: int
    latitude: float
    longitude: float


# ── EPC (Energy Performance Certificates) ──────────────────────────


class EPCRecord(BaseModel):
    """Validated record from EPC domestic certificate data CSV."""

    lmk_key: str  # unique certificate ID
    uprn: int | None = None
    postcode_raw: str | None = None
    postcode_norm: str | None = None
    address_line_1: str | None = None
    address_line_2: str | None = None
    address_line_3: str | None = None
    post_town: str | None = None
    county: str | None = None
    current_energy_rating: str | None = None  # A-G
    current_energy_efficiency: int | None = None
    property_type: str | None = None
    built_form: str | None = None
    total_floor_area: float | None = None
    lodgement_date: date | None = None

    _trunc_addr1 = field_validator("address_line_1", mode="before")(_truncate(200))
    _trunc_addr2 = field_validator("address_line_2", mode="before")(_truncate(200))
    _trunc_addr3 = field_validator("address_line_3", mode="before")(_truncate(200))
    _trunc_post_town = field_validator("post_town", mode="before")(_truncate(100))
    _trunc_county = field_validator("county", mode="before")(_truncate(100))


# ── VOA Non-Domestic Rating List ─────────────────────────────────


class VOARecord(BaseModel):
    """Validated record from VOA Non-Domestic Rating List CSV.

    Represents a non-domestic hereditament (commercial property) with
    its address and rateable value.
    """

    uarn: int  # Unique Address Reference Number
    billing_authority_code: str | None = None
    description_code: str | None = None
    description_text: str | None = None
    firm_name: str | None = None
    number_or_name: str | None = None
    street: str | None = None
    town: str | None = None
    postal_district: str | None = None
    county: str | None = None
    postcode_raw: str | None = None
    postcode_norm: str | None = None
    rateable_value: int | None = None
    effective_date: str | None = None
    sub_street_3: str | None = None  # unit/suite
    sub_street_2: str | None = None  # floor
    sub_street_1: str | None = None  # building name

    _trunc_desc_text = field_validator("description_text", mode="before")(_truncate(60))
    _trunc_firm_name = field_validator("firm_name", mode="before")(_truncate(200))
    _trunc_number = field_validator("number_or_name", mode="before")(_truncate(100))
    _trunc_street = field_validator("street", mode="before")(_truncate(200))
    _trunc_town = field_validator("town", mode="before")(_truncate(100))
    _trunc_postal_district = field_validator("postal_district", mode="before")(_truncate(100))
    _trunc_county = field_validator("county", mode="before")(_truncate(100))
    _trunc_sub3 = field_validator("sub_street_3", mode="before")(_truncate(100))
    _trunc_sub2 = field_validator("sub_street_2", mode="before")(_truncate(100))
    _trunc_sub1 = field_validator("sub_street_1", mode="before")(_truncate(100))


# ── CQC Care Quality Commission ──────────────────────────────────


class CQCRecord(BaseModel):
    """Validated record from CQC Care Directory CSV."""

    location_id: str
    location_name: str | None = None
    care_home: str | None = None
    location_type: str | None = None
    postcode_raw: str | None = None
    postcode_norm: str | None = None
    address_line_1: str | None = None
    address_line_2: str | None = None
    city: str | None = None
    county: str | None = None
    latitude: float | None = None
    longitude: float | None = None

    _trunc_name = field_validator("location_name", mode="before")(_truncate(300))
    _trunc_addr1 = field_validator("address_line_1", mode="before")(_truncate(200))
    _trunc_addr2 = field_validator("address_line_2", mode="before")(_truncate(200))
    _trunc_city = field_validator("city", mode="before")(_truncate(100))
    _trunc_county = field_validator("county", mode="before")(_truncate(100))


# ── Charity Commission ───────────────────────────────────────────


class CharityRecord(BaseModel):
    """Validated record from Charity Commission register CSV."""

    charity_number: str
    charity_name: str | None = None
    charity_status: str | None = None
    postcode_raw: str | None = None
    postcode_norm: str | None = None
    address_line_1: str | None = None
    address_line_2: str | None = None
    address_line_3: str | None = None
    city: str | None = None
    county: str | None = None

    _trunc_name = field_validator("charity_name", mode="before")(_truncate(300))
    _trunc_addr1 = field_validator("address_line_1", mode="before")(_truncate(200))
    _trunc_addr2 = field_validator("address_line_2", mode="before")(_truncate(200))
    _trunc_addr3 = field_validator("address_line_3", mode="before")(_truncate(200))
    _trunc_city = field_validator("city", mode="before")(_truncate(100))
    _trunc_county = field_validator("county", mode="before")(_truncate(100))


# ── GIAS Schools ─────────────────────────────────────────────────


class SchoolRecord(BaseModel):
    """Validated record from GIAS schools CSV."""

    urn: int
    name: str | None = None
    establishment_type: str | None = None
    status: str | None = None
    postcode_raw: str | None = None
    postcode_norm: str | None = None
    street: str | None = None
    locality: str | None = None
    town: str | None = None
    county: str | None = None
    latitude: float | None = None
    longitude: float | None = None

    _trunc_name = field_validator("name", mode="before")(_truncate(300))
    _trunc_street = field_validator("street", mode="before")(_truncate(200))
    _trunc_locality = field_validator("locality", mode="before")(_truncate(100))
    _trunc_town = field_validator("town", mode="before")(_truncate(100))
    _trunc_county = field_validator("county", mode="before")(_truncate(100))


# ── NHS Organisation Data ────────────────────────────────────────


class NHSRecord(BaseModel):
    """Validated record from NHS ODS CSV."""

    org_code: str
    name: str | None = None
    org_type: str | None = None
    status: str | None = None
    postcode_raw: str | None = None
    postcode_norm: str | None = None
    address_line_1: str | None = None
    address_line_2: str | None = None
    address_line_3: str | None = None
    address_line_4: str | None = None
    city: str | None = None

    _trunc_name = field_validator("name", mode="before")(_truncate(300))
    _trunc_addr1 = field_validator("address_line_1", mode="before")(_truncate(200))
    _trunc_addr2 = field_validator("address_line_2", mode="before")(_truncate(200))
    _trunc_addr3 = field_validator("address_line_3", mode="before")(_truncate(200))
    _trunc_addr4 = field_validator("address_line_4", mode="before")(_truncate(200))
    _trunc_city = field_validator("city", mode="before")(_truncate(100))


# ── DVSA MOT Test Stations ───────────────────────────────────────


class DVSARecord(BaseModel):
    """Validated record from DVSA Active MOT Test Stations CSV."""

    station_number: str
    site_name: str | None = None
    postcode_raw: str | None = None
    postcode_norm: str | None = None
    address_line_1: str | None = None
    address_line_2: str | None = None
    address_line_3: str | None = None
    town: str | None = None

    _trunc_name = field_validator("site_name", mode="before")(_truncate(300))
    _trunc_addr1 = field_validator("address_line_1", mode="before")(_truncate(200))
    _trunc_addr2 = field_validator("address_line_2", mode="before")(_truncate(200))
    _trunc_addr3 = field_validator("address_line_3", mode="before")(_truncate(200))
    _trunc_town = field_validator("town", mode="before")(_truncate(100))
