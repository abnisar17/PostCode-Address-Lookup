# UK Postcode Address Lookup

## Context

Build a data ingestion pipeline that downloads, parses, normalizes, and loads UK address data from 3 public sources into PostgreSQL+PostGIS. This is the foundation for a postcode lookup API (similar to getaddress.io) using only free/open data.

## Architecture

### Monorepo Layout

The project is a monorepo with independent packages for backend and frontend. Root-level files handle orchestration (Docker, Makefile, shared config). Each package is self-contained with its own deps, Dockerfile, and tests.

```
PostcodeAddressLookup/
├── docker-compose.yml               # Orchestrates all services (db, backend, frontend)
├── Makefile                          # Top-level commands, delegates to sub-packages
├── .env.example
├── .gitignore
├── data/                             # Shared download dir, mounted into backend container
│   └── .gitkeep
├── docs/
│   └── plan.md
│
├── backend/                          # Python package (self-contained)
│   ├── pyproject.toml
│   ├── uv.lock
│   ├── alembic.ini
│   ├── Dockerfile
│   ├── app/                          # Package root — imports: from app.core.xxx
│   │   ├── __init__.py
│   │   │
│   │   ├── core/                     # SHARED LAYER — single source of truth
│   │   │   ├── __init__.py
│   │   │   ├── config.py             # pydantic-settings (DATABASE_URL, DATA_DIR, source URLs)
│   │   │   ├── logging.py            # structlog configuration + get_logger()
│   │   │   ├── exceptions.py         # Exception hierarchy (see below)
│   │   │   ├── db/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── engine.py         # SQLAlchemy engine factory + session factory
│   │   │   │   ├── models.py         # ORM: DataSource, Postcode, Address
│   │   │   │   ├── loader.py         # Generic batch_load() — DRY: one place for batch logic
│   │   │   │   └── migrations/       # Alembic migrations
│   │   │   │       ├── env.py
│   │   │   │       ├── script.py.mako
│   │   │   │       └── versions/
│   │   │   │           └── 001_initial_schema.py
│   │   │   └── utils/
│   │   │       ├── __init__.py
│   │   │       ├── postcode.py       # normalise_postcode() with UK regex validation
│   │   │       ├── address.py        # normalise_street(), normalise_city()
│   │   │       └── coordinates.py    # osgb36_to_wgs84() via pyproj Transformer
│   │   │
│   │   ├── ingestion/                # PIPELINE LAYER — depends on core only
│   │   │   ├── __init__.py
│   │   │   ├── cli.py                # Typer CLI with DI via callback
│   │   │   ├── schemas.py            # Pydantic models: CodePointRecord, NSPLRecord, OSMAddressRecord
│   │   │   ├── downloader.py         # Async httpx streaming download with caching + SHA-256
│   │   │   ├── codepoint.py          # Pure parser: ZIP path → Iterator[list[CodePointRecord]]
│   │   │   ├── nspl.py               # Pure parser: ZIP path → Iterator[list[NSPLRecord]]
│   │   │   ├── osm.py                # Pure parser: .pbf path → Iterator[list[OSMAddressRecord]]
│   │   │   └── merge.py              # link_postcodes(), score_confidence(), deduplicate()
│   │   │
│   │   └── api/                      # API LAYER — depends on core only (built later)
│   │       └── __init__.py
│   │
│   └── tests/
│       ├── __init__.py
│       ├── conftest.py               # Shared fixtures, testcontainers PostgreSQL
│       ├── unit/
│       │   ├── __init__.py
│       │   ├── test_postcode_utils.py
│       │   ├── test_address_utils.py
│       │   └── test_coordinates.py
│       └── integration/
│           ├── __init__.py
│           └── test_db.py            # Tests against real PostgreSQL+PostGIS
│
└── frontend/                         # JS/TS app (added later)
    ├── package.json
    ├── Dockerfile
    └── src/
```

### Application Layers

Strict dependency graph — layers never cross-import:

```
ingestion → core ← api
```

- **`core/`** — Shared foundation and single source of truth. Config, DB engine, ORM models, utilities, exceptions. Both ingestion and API import from here.
- **`ingestion/`** — Standalone pipeline. CLI + parsers + loaders. Depends only on `core/`. Never imported by `api/`.
- **`api/`** — Standalone FastAPI app (built later). Depends only on `core/`. Never imported by `ingestion/`.

The database schema (defined in `core/db/models.py`) is the contract between the two. Ingestion writes, API reads.

### Design Principles

- **SRP**: Each module has one reason to change. Parsers parse (pure functions: path → records). The batch loader loads. The CLI wires them together.
- **DRY**: Batch insert logic lives in one place (`core/db/loader.py`). All 3 sources use it. Parsers only provide records and an upsert function.
- **DIP**: CLI commands receive dependencies (config, session factory) via Typer callback — not module-level globals. Enables testing without a real DB.
- **Testability**: Parsers are pure functions (file in → records out, no DB). Utils are pure functions. Only `loader.py` and CLI touch the DB.
- **No over-engineering**: No repository pattern, no service layer, no plugin system, no ABC hierarchies. Direct SQLAlchemy, simple imports, convention over abstraction.

## Tech Stack

- **Python 3.12** with **uv** (no pip/bare python)
- **FastAPI** (installed now, API built later)
- **PostgreSQL 16 + PostGIS 3.4** via Docker Compose
- **osmium** (PyPI package name) for OSM .pbf parsing
- **SQLAlchemy 2.x + GeoAlchemy2 + psycopg3** for database
- **Alembic** for schema migrations (tracked, reversible)
- **Typer** for CLI, **httpx** for async downloads, **pyproj** for coordinate conversion
- **Pydantic v2** for parsing validation (schemas between parsers and DB)
- **structlog** for structured logging with context
- **Rich** for progress bars and CLI output
- **testcontainers** for integration tests against real PostgreSQL+PostGIS

## Data Sources (MVP)

| Source | Format | Size | Records | Provides |
|--------|--------|------|---------|----------|
| **OSM** (Geofabrik GB) | .pbf | ~1.9GB | ~2-3M with addr tags | Full addresses (house number, street, city, postcode, lat/lon) |
| **Code-Point Open** (OS) | ZIP of CSVs | ~150MB | ~1.7M postcodes | Postcode centroids (easting/northing in OSGB36) |
| **ONS NSPL** | ZIP with CSV | ~178MB | ~2.6M postcodes | Admin hierarchies (local authority, region, ward, etc.) |

## Exception Hierarchy

Defined in `core/exceptions.py`. Flat — no source-specific subclasses. Each carries context attributes for structlog.

```python
class PostcodeLookupError(Exception):
    """Base exception for the entire project."""

class ConfigError(PostcodeLookupError):
    """Missing or invalid configuration / environment variables."""

class DownloadError(PostcodeLookupError):
    """Network failure, bad HTTP status, or hash mismatch."""
    def __init__(self, message: str, *, source: str, url: str | None = None, status_code: int | None = None):

class ParseError(PostcodeLookupError):
    """Malformed source data that failed validation."""
    def __init__(self, message: str, *, source: str, line: int | None = None, detail: str | None = None):

class DatabaseError(PostcodeLookupError):
    """Connection refused, migration failed, or query error."""

class PipelineError(PostcodeLookupError):
    """Orchestration error — e.g., 'run load-postcodes before merge'."""
```

## Error Handling Strategy

Consistent across all sources — no ad-hoc per-parser handling:

| Scenario | Behaviour |
|----------|-----------|
| Single row fails Pydantic validation | **Skip and count**. Log at DEBUG with `ParseError` context. Summary at end: "Skipped N/M records". |
| Batch insert fails (DB error) | **Rollback that batch**, log at ERROR with batch range. Continue to next batch. |
| pyosmium encounters corrupt element | **Skip**, log at WARNING. Continue. |
| Entire file unreadable / missing | **Raise** `ParseError` or `DownloadError`. Command fails, `data_sources.status='failed'`. |
| User hits Ctrl+C during long operation | **Graceful shutdown**. Finish and commit current batch, then exit. Prior batches preserved. |
| Missing prerequisite (e.g. merge before load) | **Raise** `PipelineError` with clear message. |

## Database Schema

### `data_sources` — Track ingestion state per source
- `id` (PK, auto), `source_name` (UNIQUE: 'codepoint'/'nspl'/'osm'), `file_hash` (SHA-256), `record_count`, `started_at`, `completed_at`, `status` ('pending'/'downloading'/'ingesting'/'completed'/'failed'), `error_message`

### `postcodes` — Canonical postcode table (~2.6M rows)
- `id` (PK, auto), `postcode` (UNIQUE, normalized "SW1A 1AA"), `postcode_no_space` (indexed, "SW1A1AA")
- `location` (PostGIS POINT SRID 4326, GiST indexed), `latitude`, `longitude`
- `easting`, `northing` (original OSGB36 from Code-Point)
- Admin codes from NSPL: `country_code`, `region_code`, `local_authority`, `parliamentary_const`, `ward_code`, `parish_code`
- `positional_quality`, `is_terminated`, `date_introduced`, `date_terminated`, `source`

### `addresses` — Individual addresses from OSM (~2-3M rows)
- `id` (PK, auto), `postcode_id` (FK → postcodes, nullable), `postcode_raw`, `postcode_norm` (indexed)
- `house_number`, `house_name`, `flat`, `street`, `suburb`, `city`, `county`
- `location` (PostGIS POINT, GiST indexed), `latitude`, `longitude`
- `osm_id` + `osm_type` (UNIQUE constraint for idempotency)
- `confidence` (0.0-1.0), `is_complete` (has postcode + street + number/name)

Schema is managed by Alembic. `init-db` runs `alembic upgrade head` (not `create_all()`).

## Generic Batch Loader (DRY)

All 3 sources use the same `core/db/loader.py`. Parsers only provide the records iterator and the upsert function.

```python
@dataclass
class LoadResult:
    source: str
    total: int
    loaded: int
    skipped: int
    failed_batches: int
    duration: float

def batch_load(
    session_factory: sessionmaker,
    records: Iterator[list[BaseModel]],
    upsert_fn: Callable[[Session, list[BaseModel]], int],
    *,
    source: str,
    batch_size: int = 10_000,
    label: str = "Loading",
) -> LoadResult:
    """Generic batch loader with Rich progress, error handling, and graceful shutdown."""
```

Each parser provides:
- `parse_codepoint(path) → Iterator[list[CodePointRecord]]` (pure function)
- `upsert_postcodes(session, batch) → int` (DB function)

The loader handles: batching, progress bars, commit/rollback, skip counting, SIGINT, structlog context.

## Pydantic Validation Schemas

Each parser yields validated Pydantic models — not raw dicts. This catches bad data before it reaches the DB and defines a clear interface between parsing and loading.

```python
class CodePointRecord(BaseModel):
    postcode: str                    # raw from CSV
    postcode_norm: str               # after normalise_postcode()
    easting: int
    northing: int
    latitude: float                  # after osgb36_to_wgs84()
    longitude: float
    positional_quality: int
    country_code: str

class NSPLRecord(BaseModel):
    postcode_norm: str
    country_code: str
    region_code: str | None
    local_authority: str | None
    parliamentary_const: str | None
    ward_code: str | None
    parish_code: str | None
    date_introduced: str | None
    date_terminated: str | None
    is_terminated: bool

class OSMAddressRecord(BaseModel):
    osm_id: int
    osm_type: str                    # 'node' / 'way' / 'relation'
    house_number: str | None
    house_name: str | None
    flat: str | None
    street: str | None
    suburb: str | None
    city: str | None
    county: str | None
    postcode_raw: str | None
    postcode_norm: str | None        # after normalise_postcode()
    latitude: float
    longitude: float
```

## CLI Dependency Injection

The Typer app uses a `@app.callback()` to build shared state (config, session factory) once. Commands receive it — they don't create their own connections. This makes commands testable.

```python
@dataclass
class AppState:
    config: Settings
    session_factory: sessionmaker

@app.callback()
def main(ctx: typer.Context):
    settings = Settings()
    engine = create_engine(settings.database_url)
    ctx.obj = AppState(
        config=settings,
        session_factory=create_session_factory(engine),
    )

@app.command()
def load_postcodes(ctx: typer.Context, truncate: bool = False):
    state: AppState = ctx.obj
    # uses state.session_factory, state.config
```

## CLI Commands

All invoked via `cd backend && uv run ingest <command>` (or `make <target>` from repo root):

| Command | Make target | What it does |
|---------|-------------|-------------|
| `init-db` | `make init` | Runs `alembic upgrade head` (creates tables + PostGIS extension) |
| `download [source] [--force]` | `make download` | Async concurrent download with caching + SHA-256 |
| `load-postcodes [--truncate]` | — | Code-Point first, then NSPL merge via `ON CONFLICT DO UPDATE` |
| `load-osm [--truncate] [--batch-size]` | — | Stream-parse .pbf, batched `ON CONFLICT DO NOTHING` |
| `merge` | — | `link_postcodes()` → `score_confidence()` → `deduplicate()` |
| `status` | `make status` | Rich table: record counts, link rates, avg confidence |
| `all [--force-download]` | `make ingest` | Full pipeline in sequence |

All commands show Rich progress bars for long operations.

## merge.py — Three Distinct Functions (SRP)

```python
def link_postcodes(session_factory) -> int:
    """SET addresses.postcode_id = postcodes.id WHERE postcode_norm matches.
    Only touches rows WHERE postcode_id IS NULL (idempotent)."""

def score_confidence(session_factory) -> int:
    """Compute weighted confidence score per address.
    Weights: postcode_fk=0.3, street=0.2, house=0.2, city=0.15, coords=0.1, suburb=0.05"""

def deduplicate(session_factory) -> int:
    """Mark/remove duplicate addresses (same postcode + street + house number/name).
    Keep highest confidence, mark others as duplicates."""
```

## Docker Setup

**docker-compose.yml** (at repo root): Three services:
- `db`: `postgis/postgis:16-3.4-alpine` with healthcheck (`pg_isready`)
- `backend`: Build from `./backend`, mounts `./data:/data`, `entrypoint: ["uv", "run"]`
- `frontend`: (commented out, added later) Build from `./frontend`, port 3000

Usage: `docker compose run backend ingest all`

**backend/Dockerfile**: Multi-stage build:
1. Builder stage: `ghcr.io/astral-sh/uv:python3.12-bookworm-slim`, sync deps (cached layer), then copy `app/` and sync project
2. Runtime stage: `python:3.12-slim-bookworm` + runtime C libs (libexpat, libproj), copy .venv from builder

## Makefile

Root-level Makefile delegates to `backend/`:

```makefile
# Infrastructure
db:               docker compose up db -d
db-stop:          docker compose down
shell:            docker compose exec db psql -U postgres -d postcode_lookup

# Backend (ingestion)
init:             cd backend && uv run ingest init-db
download:         cd backend && uv run ingest download
ingest:           cd backend && uv run ingest all
status:           cd backend && uv run ingest status

# Testing
test:             cd backend && uv run pytest
test-unit:        cd backend && uv run pytest tests/unit
test-integration: cd backend && uv run pytest tests/integration

# Code quality
lint:             cd backend && uv run ruff check app/ tests/
format:           cd backend && uv run ruff format app/ tests/

# Deps
install:          cd backend && uv sync
```

## Configuration

All in `app/core/config.py` via pydantic-settings. Data source URLs are configurable — not hardcoded in parsers/downloader.

```python
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    # Database
    database_url: str = "postgresql+psycopg://postgres:postgres@localhost:5432/postcode_lookup"

    # Paths
    data_dir: Path = Path("data")

    # Data source URLs (configurable — swap to mirrors or smaller extracts for testing)
    osm_download_url: str = "https://download.geofabrik.de/europe/great-britain-latest.osm.pbf"
    codepoint_download_url: str = "https://api.os.uk/downloads/v1/products/CodePointOpen/downloads?area=GB&format=CSV&redirect"
    nspl_download_url: str = "https://www.arcgis.com/sharing/rest/content/items/.../data"

    # Ingestion tuning
    batch_size: int = 10_000
    osm_index_type: str = "flex_mem"     # 'flex_mem' (fast, ~4-6GB RAM) or 'sparse_file_array' (slow, low RAM)
    log_level: str = "INFO"
    log_format: str = "console"          # 'console' (dev) or 'json' (production)
```

## Key Design Decisions

1. **Monorepo**: Root owns orchestration (Docker, Makefile, env). `backend/` and `frontend/` are self-contained packages with their own deps, Dockerfiles, and tests.
2. **Flat import root**: `backend/app/` — imports are `from app.core.xxx`, no redundant `src/` nesting.
3. **Layered architecture**: `core/` is the shared single source of truth. `ingestion/` and `api/` depend on `core/` but never on each other. The database schema is the contract.
4. **Alembic migrations**: Schema changes are tracked, versioned, and reversible. `init-db` runs `alembic upgrade head`, not `create_all()`.
5. **Pydantic validation**: Each parser yields typed, validated Pydantic models. Bad data is caught before DB insertion.
6. **Exception hierarchy**: Flat, context-rich exceptions in `core/exceptions.py`. Consistent error handling strategy across all sources.
7. **Generic batch loader (DRY)**: One `batch_load()` function handles progress, commits, rollbacks, SIGINT. All 3 sources use it.
8. **Dependency injection**: Typer callback builds config + session factory. Commands receive deps, don't create them.
9. **Pure parsers**: Each parser is a pure function (file path → record iterator). No DB, no side effects.
10. **Structured logging (structlog)**: Every log carries context. JSON in production, console in dev.
11. **Rich progress bars**: Download, parse, and insert operations show real-time progress.
12. **Async downloads**: All 3 sources download concurrently via async httpx.
13. **Testcontainers**: Integration tests spin up real PostgreSQL+PostGIS automatically.
14. **Idempotency**: Downloads skip existing files, postcodes `ON CONFLICT DO UPDATE`, addresses `ON CONFLICT DO NOTHING`, merge `WHERE postcode_id IS NULL`.
15. **Graceful shutdown**: SIGINT handler finishes current batch before exiting.
16. **Configurable source URLs**: In `config.py`, not hardcoded in parsers.
17. **Batch inserts**: Default 10,000 rows/batch (configurable). Each batch committed independently.
18. **Coordinate conversion**: OSGB36 → WGS84 via single reused pyproj Transformer instance.
19. **Postcode merge strategy**: Code-Point coordinates preferred, NSPL admin codes always taken.
20. **Confidence scoring**: Weighted sum — postcode_fk: 0.3, street: 0.2, house: 0.2, city: 0.15, coords: 0.1, suburb: 0.05.

## What We Deliberately Don't Add

- **No repository pattern** — direct SQLAlchemy is appropriate for a data pipeline
- **No service layer** between CLI and DB — unnecessary indirection for batch operations
- **No plugin/registry system** for data sources — 3 sources, just import them
- **No async DB for ingestion** — sync is simpler and sufficient for batch inserts (async only for downloads)
- **No ABC for parsers** — convention over abstraction: `parse_X(path) → Iterator[list[Record]]`
- **No event bus** — sequential pipeline, no need for pub/sub

## Implementation Order

### Phase 1: Project Skeleton + Docker
1. Monorepo structure: `backend/`, `frontend/`, root orchestration files
2. `backend/pyproject.toml` with all deps and `[project.scripts] ingest = "app.ingestion.cli:app"`
3. `.env.example`, `.gitignore`, `Makefile`
4. `docker-compose.yml` (db + backend services) + `backend/Dockerfile` (multi-stage)
5. `app/core/config.py` (pydantic-settings, including source URLs)
6. `app/core/logging.py` (structlog setup)
7. `app/core/exceptions.py` (exception hierarchy)

### Phase 2: Core Database
8. `app/core/db/engine.py` — SQLAlchemy engine factory + session factory
9. `app/core/db/models.py` — all 3 ORM models
10. `app/core/db/loader.py` — generic `batch_load()` with progress + error handling + SIGINT
11. `backend/alembic.ini` + `app/core/db/migrations/env.py`
12. Initial Alembic migration (`001_initial_schema.py`)
13. `init-db` CLI command (runs `alembic upgrade head`)

### Phase 3: Utilities
14. `app/core/utils/postcode.py` — normalize + validate UK postcodes
15. `app/core/utils/coordinates.py` — OSGB36 → WGS84
16. `app/core/utils/address.py` — street/city normalization
17. Unit tests for all utilities

### Phase 4: Download Pipeline
18. `app/ingestion/schemas.py` — Pydantic validation models
19. `app/ingestion/downloader.py` — async httpx streaming, .tmp atomic rename, SHA-256, concurrent
20. `download` CLI command with per-source and `--force` support

### Phase 5: Load Postcodes
21. `app/ingestion/codepoint.py` — pure parser: ZIP path → Iterator[list[CodePointRecord]]
22. `app/ingestion/nspl.py` — pure parser: ZIP path → Iterator[list[NSPLRecord]]
23. `load-postcodes` command — Code-Point first, NSPL merge, wired through `batch_load()`

### Phase 6: Load OSM
24. `app/ingestion/osm.py` — pure parser: .pbf path → Iterator[list[OSMAddressRecord]]
25. `load-osm` command — wired through `batch_load()` with ON CONFLICT DO NOTHING

### Phase 7: Merge + Status + All
26. `app/ingestion/merge.py` — `link_postcodes()`, `score_confidence()`, `deduplicate()`
27. `merge`, `status`, `all` commands

### Phase 8: Integration Tests
28. `backend/tests/conftest.py` — testcontainers PostgreSQL+PostGIS fixture
29. `backend/tests/integration/test_db.py` — model creation, upserts, FK linking

## Verification

1. `make db` → PostgreSQL running
2. `make init` → tables created (check with `make shell` then `\dt`)
3. `cd backend && uv run ingest download` → 3 files in `data/` directory
4. `cd backend && uv run ingest load-postcodes` → ~2.6M rows in postcodes table
5. `cd backend && uv run ingest load-osm` → ~2-3M rows in addresses table
6. `cd backend && uv run ingest merge` → addresses linked, confidence scores computed
7. `make status` → shows counts, link rate, avg confidence
8. `make ingest` → full pipeline end-to-end
9. Spot check: `SELECT * FROM addresses WHERE postcode_norm = 'SW1A 1AA'`
10. `make test` → all unit + integration tests pass
