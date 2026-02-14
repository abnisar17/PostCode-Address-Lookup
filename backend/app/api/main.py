"""FastAPI application factory.

Creates and configures the application with routers, CORS, exception
handlers, and structured logging. The module-level ``app`` instance is
the ASGI entry point used by uvicorn.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.errors import register_exception_handlers
from app.api.routers import addresses, health, postcodes
from app.core.logging import setup_logging


def create_app() -> FastAPI:
    """Build the FastAPI application with all middleware and routes wired up."""
    setup_logging()

    application = FastAPI(
        title="UK Postcode & Address Lookup API",
        summary="Look up UK postcodes and their associated addresses",
        description=(
            "A read-only REST API for querying UK postcodes and addresses.\n\n"
            "**Primary use case:** enter a postcode, get back every address at that postcode.\n\n"
            "Data is sourced from Ordnance Survey Code-Point Open, the ONS National Statistics "
            "Postcode Lookup (NSPL), and OpenStreetMap address extracts. The database contains "
            "~2.7 million postcodes and ~800K addresses.\n\n"
            "## Quick Start\n\n"
            "1. **Look up a postcode:** `GET /postcodes/SW1A1AA`\n"
            "2. **Autocomplete:** `GET /postcodes/autocomplete?q=SW1A`\n"
            "3. **Search addresses:** `GET /addresses/search?city=London&street=Downing`\n"
            "4. **Health check:** `GET /health`\n"
        ),
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_tags=[
            {
                "name": "Health",
                "description": (
                    "Operational health checks. Use `/health` for uptime monitoring "
                    "and to verify database connectivity and data ingestion status."
                ),
            },
            {
                "name": "Postcodes",
                "description": (
                    "Look up postcode metadata and linked addresses. "
                    "The `GET /postcodes/{postcode}` endpoint is the primary entry point "
                    "for the frontend's address lookup flow."
                ),
            },
            {
                "name": "Addresses",
                "description": (
                    "Search and retrieve individual address records. "
                    "Supports filtered search by street, city, and postcode "
                    "with paginated results."
                ),
            },
        ],
    )

    # CORS — allow the frontend (next phase) to call the API
    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    # Exception handlers
    register_exception_handlers(application)

    # Routers
    application.include_router(health.router)
    application.include_router(postcodes.router)
    application.include_router(addresses.router)

    return application


app = create_app()
