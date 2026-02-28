"""Health check endpoint.

Verifies database connectivity and reports record counts so operators
can confirm that the data ingestion pipeline has completed successfully.
"""

from fastapi import APIRouter, Depends
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.api.schemas import HealthResponse
from app.core.db.models import Address, Company, FoodRating, Postcode, PricePaid, VOARating

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
def check_health(db: Session = Depends(get_db)) -> HealthResponse:
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception:
        return HealthResponse(
            status="unhealthy",
            database="unreachable",
            postcode_count=0,
            address_count=0,
        )

    postcode_count = db.scalar(func.count(Postcode.id)) or 0
    address_count = db.scalar(func.count(Address.id)) or 0
    price_paid_count = db.scalar(func.count(PricePaid.id)) or 0
    company_count = db.scalar(func.count(Company.id)) or 0
    food_rating_count = db.scalar(func.count(FoodRating.id)) or 0
    voa_rating_count = db.scalar(func.count(VOARating.id)) or 0

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
