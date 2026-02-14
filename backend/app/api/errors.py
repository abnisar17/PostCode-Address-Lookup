"""Global exception handlers that map domain exceptions to HTTP responses.

Each handler logs the error with structured context and returns
a consistent ``ErrorResponse`` JSON body.
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.core.exceptions import DatabaseError, PostcodeLookupError
from app.core.logging import get_logger

log = get_logger(__name__)


def register_exception_handlers(app: FastAPI) -> None:
    """Attach all custom exception handlers to the FastAPI application."""

    @app.exception_handler(DatabaseError)
    def handle_database_error(_request: Request, exc: DatabaseError) -> JSONResponse:
        log.error("database_error", detail=str(exc))
        return JSONResponse(
            status_code=503,
            content={"detail": "Database is temporarily unavailable"},
        )

    @app.exception_handler(PostcodeLookupError)
    def handle_domain_error(_request: Request, exc: PostcodeLookupError) -> JSONResponse:
        log.error("domain_error", type=type(exc).__name__, detail=str(exc))
        return JSONResponse(
            status_code=500,
            content={"detail": "An internal error occurred"},
        )
