import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.core.db.models import Base


@pytest.fixture(scope="session")
def db_engine():
    """Spin up a PostgreSQL+PostGIS container for integration tests.

    Uses testcontainers to manage the container lifecycle.
    Falls back to skip if Docker is not available.
    """
    try:
        from testcontainers.postgres import PostgresContainer

        with PostgresContainer(
            image="postgis/postgis:16-3.4-alpine",
            username="test",
            password="test",
            dbname="test_postcode",
        ) as pg:
            url = pg.get_connection_url().replace("psycopg2", "psycopg")
            engine = create_engine(url)

            with engine.connect() as conn:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
                conn.commit()

            Base.metadata.create_all(engine)
            yield engine
            engine.dispose()

    except Exception as exc:
        pytest.skip(f"Docker not available for integration tests: {exc}")


@pytest.fixture
def db_session(db_engine) -> Session:
    """Provide a transactional session that rolls back after each test."""
    connection = db_engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)

    yield session

    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture
def db_session_factory(db_engine) -> sessionmaker[Session]:
    """Provide a session factory for tests that need it."""
    return sessionmaker(bind=db_engine, expire_on_commit=False)
