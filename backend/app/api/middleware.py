"""API key authentication middleware.

Validates the API key (``X-API-Key`` header or ``apiKey`` query param) on
every /api/ request except the exempt paths, enforces the per-key daily
rate limit, and logs usage to the api_usage table for monitoring.

Keys are matched by SHA-256 hash — the plaintext key is never stored.
"""

import time
from datetime import datetime, timezone

from fastapi import Request, Response
from sqlalchemy import func, select
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from app.api.deps import get_settings, _session_factory
from app.api.security import hash_api_key
from app.core.db.models import ApiKey, ApiUsage


# Paths that don't require API key auth
EXEMPT_PATHS = {
    "/api/health",
    "/api/admin",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/api/documentation",
}

# Query params that must never be persisted to the usage log in cleartext.
_REDACTED_PARAMS = {"apikey", "password"}


def _is_exempt(path: str) -> bool:
    """Check if the request path is exempt from API key auth."""
    for exempt in EXEMPT_PATHS:
        if path == exempt or path.startswith(exempt + "/"):
            return True
    return False


def _redact_query(request: Request) -> str | None:
    """Serialise query params for logging, masking secrets like apiKey."""
    items = request.query_params.multi_items()
    if not items:
        return None
    return "&".join(
        f"{k}=***" if k.lower() in _REDACTED_PARAMS else f"{k}={v}"
        for k, v in items
    )


class ApiKeyMiddleware(BaseHTTPMiddleware):
    """Middleware that validates the API key, enforces rate limits, logs usage."""

    async def dispatch(self, request: Request, call_next) -> Response:
        settings = get_settings()

        # Let CORS preflight through untouched — it carries no API key.
        if request.method == "OPTIONS":
            return await call_next(request)

        # Skip auth if not enabled or path is exempt
        if (
            not settings.require_api_key
            or not request.url.path.startswith("/api/")
            or _is_exempt(request.url.path)
        ):
            return await call_next(request)

        # Extract API key from header or query param
        api_key_value = (
            request.headers.get("X-API-Key")
            or request.query_params.get("apiKey")
        )

        if not api_key_value:
            return JSONResponse(
                status_code=401,
                content={"detail": "API key required. Pass X-API-Key header or ?apiKey=YOUR_KEY."},
            )

        async with _session_factory()() as session:
            # Validate key by hash — plaintext is never stored.
            key_record = (
                await session.execute(
                    select(ApiKey).where(ApiKey.key_hash == hash_api_key(api_key_value))
                )
            ).scalars().first()

            if not key_record:
                return JSONResponse(status_code=403, content={"detail": "Invalid API key."})

            if not key_record.is_active:
                return JSONResponse(
                    status_code=403, content={"detail": "API key has been deactivated."}
                )

            # Enforce per-key daily rate limit
            today_start = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            used_today = await session.scalar(
                select(func.count(ApiUsage.id))
                .where(ApiUsage.api_key_id == key_record.id)
                .where(ApiUsage.timestamp >= today_start)
            ) or 0

            if used_today >= key_record.rate_limit_per_day:
                return JSONResponse(
                    status_code=429,
                    content={
                        "detail": (
                            f"Daily rate limit of {key_record.rate_limit_per_day} "
                            "requests exceeded. Try again tomorrow."
                        )
                    },
                )

            # Expose the authenticated key to downstream endpoints (attribution).
            request.state.api_key_id = key_record.id

            # Process request and measure response time
            start = time.monotonic()
            response = await call_next(request)
            elapsed_ms = int((time.monotonic() - start) * 1000)

            # Log usage (never block the API on a logging failure)
            try:
                usage = ApiUsage(
                    api_key_id=key_record.id,
                    endpoint=request.url.path,
                    method=request.method,
                    query_params=_redact_query(request),
                    status_code=response.status_code,
                    response_time_ms=elapsed_ms,
                    ip_address=request.client.host if request.client else None,
                )
                session.add(usage)
                await session.commit()
            except Exception:
                pass

            return response
