# Stage 1: Builder
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /backend

# Install dependencies first (cached layer)
COPY pyproject.toml uv.lock* ./
RUN uv sync --no-install-project --frozen 2>/dev/null || uv sync --no-install-project

# Copy source and install project
COPY app/ app/
COPY alembic.ini ./
RUN uv sync --frozen 2>/dev/null || uv sync

# Stage 2: Runtime
FROM python:3.12-slim-bookworm

# Runtime C libraries needed by pyproj, pyosmium, psycopg
RUN apt-get update && apt-get install -y --no-install-recommends \
    libexpat1 \
    libproj25 \
    libgeos-c1v5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /backend

# Copy virtual environment from builder
COPY --from=builder /backend/.venv /backend/.venv
COPY --from=builder /backend/pyproject.toml /backend/alembic.ini ./
COPY --from=builder /backend/app app/

# Ensure venv is on PATH
ENV PATH="/backend/.venv/bin:$PATH"
ENV VIRTUAL_ENV="/backend/.venv"

CMD ["serve"]
