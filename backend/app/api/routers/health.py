"""Health check endpoint.

Verifies database connectivity and reports record counts so operators
can confirm that the data ingestion pipeline has completed successfully.
"""

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db
from app.api.schemas import HealthResponse

router = APIRouter(tags=["Health"])


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Check API and database health",
    description=(
        "Performs a lightweight connectivity check against the database and "
        "returns the total number of postcodes, addresses, and enrichment records "
        "currently loaded. Use this endpoint for uptime monitoring and to verify "
        "that data ingestion has run successfully."
    ),
    responses={
        200: {
            "description": "Service is healthy and database is reachable",
            "content": {
                "application/json": {
                    "example": {
                        "status": "healthy",
                        "database": "connected",
                        "postcode_count": 2_700_000,
                        "address_count": 800_000,
                        "price_paid_count": 28_000_000,
                        "company_count": 5_000_000,
                        "food_rating_count": 600_000,
                        "voa_rating_count": 2_000_000,
                    }
                }
            },
        },
        503: {"description": "Database is unreachable"},
    },
)
async def check_health(db: AsyncSession = Depends(get_db)) -> HealthResponse:
    try:
        await db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception:
        return HealthResponse(
            status="unhealthy",
            database="unreachable",
            postcode_count=0,
            address_count=0,
        )

    # Use pg_class.reltuples for fast approximate counts (instant, no table scan)
    table_names = [
        "postcodes", "addresses", "price_paid",
        "companies", "food_ratings", "voa_ratings",
    ]
    rows = await db.execute(
        text(
            "SELECT relname, GREATEST(reltuples, 0)::bigint "
            "FROM pg_class WHERE relname = ANY(:tables)"
        ),
        {"tables": table_names},
    )
    counts = dict(rows.all())
    postcode_count = counts.get("postcodes", 0)
    address_count = counts.get("addresses", 0)
    price_paid_count = counts.get("price_paid", 0)
    company_count = counts.get("companies", 0)
    food_rating_count = counts.get("food_ratings", 0)
    voa_rating_count = counts.get("voa_ratings", 0)

    return HealthResponse(
        status="healthy",
        database=db_status,
        postcode_count=postcode_count,
        address_count=address_count,
        price_paid_count=price_paid_count,
        company_count=company_count,
        food_rating_count=food_rating_count,
        voa_rating_count=voa_rating_count,
    )
