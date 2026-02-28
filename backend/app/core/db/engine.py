from sqlalchemy import create_engine as sa_create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker

from app.core.exceptions import DatabaseError


# ── Sync helpers (used by the ingestion pipeline) ──────────────────


def create_engine(database_url: str, **kwargs) -> Engine:
    """Create a SQLAlchemy engine with sensible defaults."""
    return sa_create_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        **kwargs,
    )


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Create a session factory bound to the given engine."""
    return sessionmaker(bind=engine, expire_on_commit=False)


# ── Async helpers (used by the API layer) ──────────────────────────


def _async_url(database_url: str) -> str:
    """Convert a psycopg sync URL to its async variant."""
    return database_url.replace("postgresql+psycopg://", "postgresql+psycopg_async://")


def create_async_engine_instance(database_url: str, **kwargs) -> AsyncEngine:
    """Create an async SQLAlchemy engine with the same pool settings as sync."""
    return create_async_engine(
        _async_url(database_url),
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        **kwargs,
    )


def create_async_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create an async session factory bound to the given engine."""
    return async_sessionmaker(bind=engine, expire_on_commit=False)


def ensure_postgis(engine: Engine) -> None:
    """Enable the PostGIS extension if not already active."""
    try:
        with engine.connect() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
            conn.commit()
    except Exception as exc:
        raise DatabaseError(f"Failed to enable PostGIS: {exc}") from exc
