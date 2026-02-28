"""FastAPI dependency injection providers.

Provides an async database session for the API layer and a cached
Settings singleton.  The ingestion CLI continues to use the sync
engine defined in ``app.core.db.engine``.
"""

from collections.abc import AsyncGenerator
from functools import lru_cache

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.db.engine import create_async_engine_instance, create_async_session_factory


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached application Settings singleton.

    Uses ``@lru_cache`` so the .env file is read exactly once,
    and the same instance is reused for every request.
    """
    return Settings()


@lru_cache(maxsize=1)
def _session_factory():
    """Internal: build and cache an async sessionmaker bound to the engine."""
    settings = get_settings()
    engine = create_async_engine_instance(settings.database_url)
    return create_async_session_factory(engine)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Yield an async SQLAlchemy session for a single request, then close it.

    Usage in a router::

        @router.get("/example")
        async def example(db: AsyncSession = Depends(get_db)):
            ...
    """
    async with _session_factory()() as session:
        yield session


async def dispose_engine() -> None:
    """Dispose of the async engine's connection pool (call on shutdown)."""
    factory = _session_factory()
    await factory.kw["bind"].dispose()
