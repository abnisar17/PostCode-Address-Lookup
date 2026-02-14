"""FastAPI dependency injection providers.

Mirrors the Typer callback pattern used in the ingestion CLI:
a cached Settings singleton and a per-request database session
that is automatically closed after use.
"""

from collections.abc import Generator
from functools import lru_cache

from sqlalchemy.orm import Session

from app.core.config import Settings
from app.core.db.engine import create_engine, create_session_factory


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached application Settings singleton.

    Uses ``@lru_cache`` so the .env file is read exactly once,
    and the same instance is reused for every request.
    """
    return Settings()


@lru_cache(maxsize=1)
def _session_factory():
    """Internal: build and cache a sessionmaker bound to the engine."""
    settings = get_settings()
    engine = create_engine(settings.database_url)
    return create_session_factory(engine)


def get_db() -> Generator[Session, None, None]:
    """Yield a SQLAlchemy session for a single request, then close it.

    Usage in a router::

        @router.get("/example")
        def example(db: Session = Depends(get_db)):
            ...
    """
    session = _session_factory()()
    try:
        yield session
    finally:
        session.close()
