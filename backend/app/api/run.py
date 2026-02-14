"""Uvicorn entry point for the ``uv run serve`` CLI command.

Reads host/port from Settings so they can be overridden via
environment variables or ``.env``.
"""

import uvicorn

from app.api.deps import get_settings


def main() -> None:
    """Start the uvicorn server."""
    settings = get_settings()
    uvicorn.run(
        "app.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=True,
    )


if __name__ == "__main__":
    main()
