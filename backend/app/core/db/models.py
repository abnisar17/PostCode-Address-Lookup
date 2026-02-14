from datetime import datetime

from geoalchemy2 import Geometry
from sqlalchemy import (
    BigInteger,
    Boolean,
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
        Index("ix_addresses_postcode_norm", "postcode_norm"),
        Index("ix_addresses_location", "location", postgresql_using="gist"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    postcode_id: Mapped[int | None] = mapped_column(
        ForeignKey("postcodes.id"), index=True
    )
    postcode_raw: Mapped[str | None] = mapped_column(String(20))
    postcode_norm: Mapped[str | None] = mapped_column(String(10))

    # Address fields
    house_number: Mapped[str | None] = mapped_column(String(50))
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

    # OSM identifiers (unique constraint for idempotency)
    osm_id: Mapped[int | None] = mapped_column(BigInteger)
    osm_type: Mapped[str | None] = mapped_column(String(10))

    # Quality
    confidence: Mapped[float | None] = mapped_column(Float, default=0.0)
    is_complete: Mapped[bool] = mapped_column(Boolean, default=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Relationships
    postcode_ref: Mapped[Postcode | None] = relationship(back_populates="addresses")
