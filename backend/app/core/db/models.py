from datetime import datetime

from geoalchemy2 import Geometry
from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class DataSource(Base):
    __tablename__ = "data_sources"

    id: Mapped[int] = mapped_column(primary_key=True)
    source_name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    file_hash: Mapped[str | None] = mapped_column(String(64))
    record_count: Mapped[int | None] = mapped_column(Integer)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(20), default="pending", nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class Postcode(Base):
    __tablename__ = "postcodes"
    __table_args__ = (
        Index("ix_postcodes_postcode_no_space", "postcode_no_space"),
        Index("ix_postcodes_postcode_no_space_pattern", "postcode_no_space",
              postgresql_ops={"postcode_no_space": "varchar_pattern_ops"}),
        Index("ix_postcodes_location", "location", postgresql_using="gist"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    postcode: Mapped[str] = mapped_column(String(10), unique=True, nullable=False)
    postcode_no_space: Mapped[str] = mapped_column(String(10), nullable=False)

    # PostGIS point (WGS84)
    location: Mapped[str | None] = mapped_column(Geometry("POINT", srid=4326))
    latitude: Mapped[float | None] = mapped_column(Float)
    longitude: Mapped[float | None] = mapped_column(Float)

    # Original OSGB36 coordinates from Code-Point
    easting: Mapped[int | None] = mapped_column(Integer)
    northing: Mapped[int | None] = mapped_column(Integer)

    # Admin codes from NSPL
    country_code: Mapped[str | None] = mapped_column(String(10))
    region_code: Mapped[str | None] = mapped_column(String(10))
    local_authority: Mapped[str | None] = mapped_column(String(10))
    parliamentary_const: Mapped[str | None] = mapped_column(String(10))
    ward_code: Mapped[str | None] = mapped_column(String(10))
    parish_code: Mapped[str | None] = mapped_column(String(10))

    # Metadata
    positional_quality: Mapped[int | None] = mapped_column(Integer)
    is_terminated: Mapped[bool] = mapped_column(Boolean, default=False)
    date_introduced: Mapped[str | None] = mapped_column(String(10))
    date_terminated: Mapped[str | None] = mapped_column(String(10))
    source: Mapped[str | None] = mapped_column(String(20))

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Relationships
    addresses: Mapped[list["Address"]] = relationship(back_populates="postcode_ref")


class Address(Base):
    __tablename__ = "addresses"
    __table_args__ = (
        UniqueConstraint("osm_id", "osm_type", name="uq_addresses_osm"),
        UniqueConstraint("source", "source_id", name="uq_addresses_source"),
        Index("ix_addresses_postcode_norm", "postcode_norm"),
        Index("ix_addresses_location", "location", postgresql_using="gist"),
        Index("ix_addresses_uprn", "uprn"),
        Index("ix_addresses_source", "source"),
        Index("ix_addresses_street_trgm", "street", postgresql_using="gin",
              postgresql_ops={"street": "gin_trgm_ops"}),
        Index("ix_addresses_city_trgm", "city", postgresql_using="gin",
              postgresql_ops={"city": "gin_trgm_ops"}),
        Index("ix_addresses_house_name_trgm", "house_name", postgresql_using="gin",
              postgresql_ops={"house_name": "gin_trgm_ops"}),
        Index("ix_addresses_suburb_trgm", "suburb", postgresql_using="gin",
              postgresql_ops={"suburb": "gin_trgm_ops"}),
        Index("ix_addresses_postcode_street_house", "postcode_id", "street", "house_number"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    postcode_id: Mapped[int | None] = mapped_column(
        ForeignKey("postcodes.id"), index=True
    )
    postcode_raw: Mapped[str | None] = mapped_column(String(20))
    postcode_norm: Mapped[str | None] = mapped_column(String(10))

    # Address fields
    house_number: Mapped[str | None] = mapped_column(String(100))
    house_name: Mapped[str | None] = mapped_column(String(200))
    flat: Mapped[str | None] = mapped_column(String(50))
    street: Mapped[str | None] = mapped_column(String(200))
    suburb: Mapped[str | None] = mapped_column(String(100))
    city: Mapped[str | None] = mapped_column(String(100))
    county: Mapped[str | None] = mapped_column(String(100))

    # PostGIS point (WGS84)
    location: Mapped[str | None] = mapped_column(Geometry("POINT", srid=4326))
    latitude: Mapped[float | None] = mapped_column(Float)
    longitude: Mapped[float | None] = mapped_column(Float)

    # OSM identifiers (kept for backward compatibility)
    osm_id: Mapped[int | None] = mapped_column(BigInteger)
    osm_type: Mapped[str | None] = mapped_column(String(10))

    # Multi-source support
    source: Mapped[str | None] = mapped_column(String(20))  # osm, land_registry, epc, companies_house, fsa
    source_id: Mapped[str | None] = mapped_column(String(100))  # unique ID from original dataset
    uprn: Mapped[int | None] = mapped_column(BigInteger)  # Unique Property Reference Number

    # Quality
    confidence: Mapped[float | None] = mapped_column(Float, default=0.0)
    is_complete: Mapped[bool] = mapped_column(Boolean, default=False)

    # Dedup: soft-mark duplicates (NULL = not a duplicate, else points to keeper address)
    duplicate_of: Mapped[int | None] = mapped_column(
        ForeignKey("addresses.id"), index=False
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Relationships
    postcode_ref: Mapped[Postcode | None] = relationship(back_populates="addresses")
    price_paid_records: Mapped[list["PricePaid"]] = relationship(back_populates="address")
    company_records: Mapped[list["Company"]] = relationship(back_populates="address")
    food_rating_records: Mapped[list["FoodRating"]] = relationship(back_populates="address")
    voa_rating_records: Mapped[list["VOARating"]] = relationship(back_populates="address")


# ---------------------------------------------------------------------------
# Enrichment tables
# ---------------------------------------------------------------------------


class PricePaid(Base):
    """HM Land Registry Price Paid Data — house sale transactions."""

    __tablename__ = "price_paid"
    __table_args__ = (
        Index("ix_price_paid_postcode_norm", "postcode_norm"),
        Index("ix_price_paid_date", "date_of_transfer"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    transaction_id: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    address_id: Mapped[int | None] = mapped_column(
        ForeignKey("addresses.id"), index=True
    )
    postcode_norm: Mapped[str | None] = mapped_column(String(10))

    # Transaction details
    price: Mapped[int] = mapped_column(Integer, nullable=False)
    date_of_transfer: Mapped[datetime | None] = mapped_column(Date)
    property_type: Mapped[str | None] = mapped_column(String(1))  # D/S/T/F/O
    old_new: Mapped[str | None] = mapped_column(String(1))  # Y=new, N=existing
    duration: Mapped[str | None] = mapped_column(String(1))  # F=freehold, L=leasehold

    # Address as recorded in Land Registry
    paon: Mapped[str | None] = mapped_column(String(100))  # house number/name
    saon: Mapped[str | None] = mapped_column(String(100))  # flat/sub-building
    street: Mapped[str | None] = mapped_column(String(200))
    locality: Mapped[str | None] = mapped_column(String(100))
    town: Mapped[str | None] = mapped_column(String(100))
    district: Mapped[str | None] = mapped_column(String(100))
    county: Mapped[str | None] = mapped_column(String(100))

    # Metadata
    ppd_category: Mapped[str | None] = mapped_column(String(1))  # A=standard, B=additional
    record_status: Mapped[str | None] = mapped_column(String(1))  # A/C/D

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Relationships
    address: Mapped[Address | None] = relationship(back_populates="price_paid_records")


class Company(Base):
    """Companies House — registered company data."""

    __tablename__ = "companies"
    __table_args__ = (
        Index("ix_companies_postcode_norm", "postcode_norm"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    company_number: Mapped[str] = mapped_column(String(10), unique=True, nullable=False)
    company_name: Mapped[str | None] = mapped_column(String(300))
    company_status: Mapped[str | None] = mapped_column(String(50))
    company_type: Mapped[str | None] = mapped_column(String(100))
    sic_code_1: Mapped[str | None] = mapped_column(String(10))
    sic_code_2: Mapped[str | None] = mapped_column(String(10))
    sic_code_3: Mapped[str | None] = mapped_column(String(10))
    sic_code_4: Mapped[str | None] = mapped_column(String(10))
    incorporation_date: Mapped[str | None] = mapped_column(String(10))

    address_id: Mapped[int | None] = mapped_column(
        ForeignKey("addresses.id"), index=True
    )
    postcode_norm: Mapped[str | None] = mapped_column(String(10))
    address_line_1: Mapped[str | None] = mapped_column(String(200))
    address_line_2: Mapped[str | None] = mapped_column(String(200))
    post_town: Mapped[str | None] = mapped_column(String(100))
    county: Mapped[str | None] = mapped_column(String(100))
    country: Mapped[str | None] = mapped_column(String(100))

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Relationships
    address: Mapped[Address | None] = relationship(back_populates="company_records")


class FoodRating(Base):
    """FSA Food Hygiene Ratings."""

    __tablename__ = "food_ratings"
    __table_args__ = (
        Index("ix_food_ratings_postcode_norm", "postcode_norm"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    fhrs_id: Mapped[int] = mapped_column(Integer, unique=True, nullable=False)
    business_name: Mapped[str | None] = mapped_column(String(300))
    business_type: Mapped[str | None] = mapped_column(String(100))
    business_type_id: Mapped[int | None] = mapped_column(Integer)
    rating_value: Mapped[str | None] = mapped_column(String(30))
    rating_date: Mapped[datetime | None] = mapped_column(Date)

    address_id: Mapped[int | None] = mapped_column(
        ForeignKey("addresses.id"), index=True
    )
    postcode_norm: Mapped[str | None] = mapped_column(String(10))
    address_line_1: Mapped[str | None] = mapped_column(String(200))
    address_line_2: Mapped[str | None] = mapped_column(String(200))
    address_line_3: Mapped[str | None] = mapped_column(String(200))
    address_line_4: Mapped[str | None] = mapped_column(String(200))

    latitude: Mapped[float | None] = mapped_column(Float)
    longitude: Mapped[float | None] = mapped_column(Float)

    local_authority_code: Mapped[str | None] = mapped_column(String(10))
    local_authority_name: Mapped[str | None] = mapped_column(String(100))
    scores_hygiene: Mapped[int | None] = mapped_column(Integer)
    scores_structural: Mapped[int | None] = mapped_column(Integer)
    scores_management: Mapped[int | None] = mapped_column(Integer)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Relationships
    address: Mapped[Address | None] = relationship(back_populates="food_rating_records")


class VOARating(Base):
    """VOA Non-Domestic Rating List — commercial property valuations."""

    __tablename__ = "voa_ratings"
    __table_args__ = (
        Index("ix_voa_ratings_postcode_norm", "postcode_norm"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    uarn: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False)
    billing_authority_code: Mapped[str | None] = mapped_column(String(10))
    description_code: Mapped[str | None] = mapped_column(String(10))
    description_text: Mapped[str | None] = mapped_column(String(60))
    firm_name: Mapped[str | None] = mapped_column(String(200))

    address_id: Mapped[int | None] = mapped_column(
        ForeignKey("addresses.id"), index=True
    )
    postcode_norm: Mapped[str | None] = mapped_column(String(10))
    number_or_name: Mapped[str | None] = mapped_column(String(100))
    street: Mapped[str | None] = mapped_column(String(200))
    town: Mapped[str | None] = mapped_column(String(100))
    postal_district: Mapped[str | None] = mapped_column(String(100))
    county: Mapped[str | None] = mapped_column(String(100))
    sub_street_1: Mapped[str | None] = mapped_column(String(100))
    sub_street_2: Mapped[str | None] = mapped_column(String(100))
    sub_street_3: Mapped[str | None] = mapped_column(String(100))

    rateable_value: Mapped[int | None] = mapped_column(Integer)
    effective_date: Mapped[str | None] = mapped_column(String(11))

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Relationships
    address: Mapped[Address | None] = relationship(back_populates="voa_rating_records")


class UPRNCoordinate(Base):
    """OS Open UPRN — coordinate lookup table for geocoding via UPRN."""

    __tablename__ = "uprn_coordinates"

    uprn: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)


# ── API Key Management ─────────────────────────────────────────


class ApiKey(Base):
    """API key for authenticated access."""

    __tablename__ = "api_keys"

    id: Mapped[int] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    user_name: Mapped[str] = mapped_column(String(100), nullable=False)
    email: Mapped[str | None] = mapped_column(String(200))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    rate_limit_per_day: Mapped[int] = mapped_column(Integer, default=10000, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    usage_logs: Mapped[list["ApiUsage"]] = relationship(back_populates="api_key")


class ApiUsage(Base):
    """API usage log — tracks every request per API key."""

    __tablename__ = "api_usage"
    __table_args__ = (
        Index("ix_api_usage_key_timestamp", "api_key_id", "timestamp"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    api_key_id: Mapped[int] = mapped_column(
        ForeignKey("api_keys.id"), index=True, nullable=False
    )
    endpoint: Mapped[str] = mapped_column(String(200), nullable=False)
    method: Mapped[str] = mapped_column(String(10), nullable=False)
    query_params: Mapped[str | None] = mapped_column(Text)
    status_code: Mapped[int] = mapped_column(Integer, nullable=False)
    response_time_ms: Mapped[int | None] = mapped_column(Integer)
    ip_address: Mapped[str | None] = mapped_column(String(45))
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    api_key: Mapped[ApiKey | None] = relationship(back_populates="usage_logs")
