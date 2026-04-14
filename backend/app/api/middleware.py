"""API key authentication middleware.

Validates the `apiKey` query parameter on every /api/ request
(except health, docs, and admin endpoints). Logs usage to the
api_usage table for monitoring.
"""

import time

from fastapi import Request, Response
from sqlalchemy import select
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from app.api.deps import get_settings, _session_factory
from app.core.db.models import ApiKey, ApiUsage


# Paths that don't require API key auth
EXEMPT_PATHS = {
    "/api/health",
    "/api/admin",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/docs/api",
}


def _is_exempt(path: str) -> bool:
    """Check if the request path is exempt from API key auth."""
    for exempt in EXEMPT_PATHS:
        if path == exempt or path.startswith(exempt + "/") or path.startswith(exempt + "?"):
            return True
    return False


class ApiKeyMiddleware(BaseHTTPMiddleware):
    """Middleware that validates API key and logs usage."""

    async def dispatch(self, request: Request, call_next) -> Response:
        settings = get_settings()

        # Skip auth if not enabled or path is exempt
        if not settings.require_api_key or not request.url.path.startswith("/api/") or _is_exempt(request.url.path):
            return await call_next(request)

        # Extract API key from query param or header
        api_key_value = (
            request.query_params.get("apiKey")
            or request.headers.get("X-API-Key")
        )

        if not api_key_value:
            return JSONResponse(
                status_code=401,
                content={"detail": "API key required. Pass ?apiKey=YOUR_KEY or X-API-Key header."},
            )

        # Validate key against database
        async with _session_factory()() as session:
            result = await session.execute(
                select(ApiKey).where(ApiKey.key == api_key_value)
            )
            key_record = result.scalars().first()

            if not key_record:
                return JSONResponse(
                    status_code=403,
                    content={"detail": "Invalid API key."},
                )

            if not key_record.is_active:
                return JSONResponse(
                    status_code=403,
                    content={"detail": "API key has been deactivated."},
                )

            # Process request and measure response time
            start = time.monotonic()
            response = await call_next(request)
            elapsed_ms = int((time.monotonic() - start) * 1000)

            # Log usage (fire-and-forget, don't block response)
            try:
                usage = ApiUsage(
                    api_key_id=key_record.id,
                    endpoint=request.url.path,
                    method=request.method,
                    query_params=str(request.query_params) if request.query_params else None,
                    status_code=response.status_code,
                    response_time_ms=elapsed_ms,
                    ip_address=request.client.host if request.client else None,
                )
                session.add(usage)
                await session.commit()
            except Exception:
                pass  # Don't let logging failures break the API

            return response
