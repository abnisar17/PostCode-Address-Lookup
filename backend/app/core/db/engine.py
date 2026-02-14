from sqlalchemy import create_engine as sa_create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.exceptions import DatabaseError


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


def ensure_postgis(engine: Engine) -> None:
    """Enable the PostGIS extension if not already active."""
    try:
        with engine.connect() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
            conn.commit()
    except Exception as exc:
        raise DatabaseError(f"Failed to enable PostGIS: {exc}") from exc
