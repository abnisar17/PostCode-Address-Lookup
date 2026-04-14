"""FastAPI application factory.

Creates and configures the application with routers, CORS, exception
handlers, and structured logging. The module-level ``app`` instance is
the ASGI entry point used by uvicorn.
"""

from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from app.api.deps import dispose_engine
from app.api.errors import register_exception_handlers
from app.api.middleware import ApiKeyMiddleware
from app.api.routers import addresses, admin, apidocs, health, postcodes
from app.core.logging import setup_logging


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Async lifespan: dispose the connection pool on shutdown."""
    yield
    await dispose_engine()


def create_app() -> FastAPI:
    """Build the FastAPI application with all middleware and routes wired up."""
    setup_logging()

    application = FastAPI(
        lifespan=lifespan,
        title="UK Postcode & Address Lookup API",
        summary="Look up UK postcodes and their associated addresses",
        description=(
            "A read-only REST API for querying UK postcodes and addresses with enrichment data.\n\n"
            "**Primary use case:** enter a postcode, get back every address at that postcode "
            "with linked house prices, company registrations, and food hygiene ratings.\n\n"
            "Data is sourced from Ordnance Survey Code-Point Open, ONS NSPL, OpenStreetMap, "
            "HM Land Registry, EPC Open Data, Companies House, and FSA Food Hygiene Ratings.\n\n"
            "## Quick Start\n\n"
            "1. **Look up a postcode:** `GET /api/postcodes/SW1A1AA`\n"
            "2. **Autocomplete:** `GET /api/postcodes/autocomplete?q=SW1A`\n"
            "3. **Search addresses:** `GET /api/addresses/search?city=London&street=Downing`\n"
            "4. **Address detail:** `GET /api/addresses/{id}` (includes enrichment data)\n"
            "5. **Health check:** `GET /api/health`\n"
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
        allow_methods=["GET", "POST", "PATCH", "DELETE"],
        allow_headers=["*"],
    )

    # API key authentication (only enforced when REQUIRE_API_KEY=true)
    application.add_middleware(ApiKeyMiddleware)

    # Disable browser caching for API responses
    @application.middleware("http")
    async def no_cache_headers(request: Request, call_next):
        response: Response = await call_next(request)
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        return response

    # Exception handlers
    register_exception_handlers(application)

    # Routers — all under /api prefix
    api = APIRouter(prefix="/api")
    api.include_router(health.router)
    api.include_router(postcodes.router)
    api.include_router(addresses.router)
    api.include_router(admin.router)
    api.include_router(apidocs.router)
    application.include_router(api)

    return application


app = create_app()
