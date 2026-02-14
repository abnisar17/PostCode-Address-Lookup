# Implementation Tracker

## Phase 1: Project Skeleton + Docker — COMPLETE

- [x] Monorepo structure: `backend/`, `frontend/`, root orchestration files
- [x] `backend/pyproject.toml` with all deps and `[project.scripts] ingest = "app.ingestion.cli:app"`
- [x] `.env.example`, `.gitignore`, `Makefile`
- [x] `docker-compose.yml` (db + backend services) + `backend/Dockerfile` (multi-stage)
- [x] `app/core/config.py` (pydantic-settings, including source URLs)
- [x] `app/core/logging.py` (structlog setup)
- [x] `app/core/exceptions.py` (exception hierarchy)

## Phase 2: Core Database — COMPLETE

- [x] `app/core/db/engine.py` — SQLAlchemy engine factory + session factory
- [x] `app/core/db/models.py` — all 3 ORM models (DataSource, Postcode, Address)
- [x] `app/core/db/loader.py` — generic `batch_load()` with progress + error handling + SIGINT
- [x] `backend/alembic.ini` + `app/core/db/migrations/env.py`
- [x] Initial Alembic migration (`001_initial_schema.py`)
- [x] `init-db` CLI command (runs `alembic upgrade head`)

## Phase 3: Utilities — COMPLETE

- [x] `app/core/utils/postcode.py` — normalize + validate UK postcodes
- [x] `app/core/utils/coordinates.py` — OSGB36 → WGS84
- [x] `app/core/utils/address.py` — street/city normalization
- [x] Unit tests for all utilities (42 tests passing)

## Phase 4: Download Pipeline — COMPLETE

- [x] `app/ingestion/schemas.py` — Pydantic validation models
- [x] `app/ingestion/downloader.py` — async httpx streaming, .tmp atomic rename, SHA-256, concurrent
- [x] `download` CLI command with per-source and `--force` support

## Phase 5: Load Postcodes — COMPLETE

- [x] `app/ingestion/codepoint.py` — pure parser: ZIP path → Iterator[list[CodePointRecord]]
- [x] `app/ingestion/nspl.py` — pure parser: ZIP path → Iterator[list[NSPLRecord]]
- [x] `load-postcodes` command — Code-Point first, NSPL merge, wired through `batch_load()`

## Phase 6: Load OSM — COMPLETE

- [x] `app/ingestion/osm.py` — pure parser: .pbf path → Iterator[list[OSMAddressRecord]]
- [x] `load-osm` command — wired through `batch_load()` with ON CONFLICT DO NOTHING

## Phase 7: Merge + Status + All — COMPLETE

- [x] `app/ingestion/merge.py` — `link_postcodes()`, `score_confidence()`, `deduplicate()`
- [x] `merge`, `status`, `all` commands

## Phase 8: Integration Tests — COMPLETE

- [x] `backend/tests/conftest.py` — testcontainers PostgreSQL+PostGIS fixture
- [x] `backend/tests/integration/test_db.py` — model creation, upserts, FK linking

## Verification Checklist

- [x] `make db` → PostgreSQL running
- [x] `make init` → tables created
- [x] `cd backend && uv run ingest download` → 3 files in `data/`
- [x] `cd backend && uv run ingest load-postcodes` → ~2.6M rows
- [x] `cd backend && uv run ingest load-osm` → ~2-3M rows
- [x] `cd backend && uv run ingest merge` → addresses linked + scored
- [x] `make status` → shows counts, link rate, avg confidence
- [x] `make ingest` → full pipeline end-to-end
- [x] Spot check: `SELECT * FROM addresses WHERE postcode_norm = 'SW1A 1AA'`
- [x] `make test` → unit tests pass (42/42)
- [x] `make test-integration` → integration tests pass (requires Docker)

## Phase 9: FastAPI API Layer — COMPLETE

- [x] `app/api/schemas.py` — Pydantic v2 response models with field-level descriptions and examples
- [x] `app/api/deps.py` — DI providers: `get_settings()` (lru_cached), `get_db()` (session-per-request)
- [x] `app/api/errors.py` — Exception handlers: `DatabaseError` → 503, `PostcodeLookupError` → 500
- [x] `app/api/routers/health.py` — `GET /health` (DB connectivity + record counts)
- [x] `app/api/routers/postcodes.py` — `GET /postcodes/autocomplete?q=` + `GET /postcodes/{postcode}`
- [x] `app/api/routers/addresses.py` — `GET /addresses/search?q=&city=&street=&postcode=` + `GET /addresses/{id}`
- [x] `app/api/main.py` — App factory with CORS, error handlers, OpenAPI tags, Swagger docs
- [x] `app/api/run.py` — Uvicorn entry point for `uv run serve`
- [x] `core/config.py` updated with `api_host`, `api_port` settings
- [x] `pyproject.toml` updated with `serve` script entry
- [x] `Makefile` updated with `make serve` target
- [x] `docker-compose.yml` updated with port 8000 mapping + serve command
- [x] All 51 existing tests still passing, ruff clean

### API Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | DB check + record counts |
| GET | `/postcodes/autocomplete?q=SW1A&limit=10` | Prefix search for type-ahead UI |
| GET | `/postcodes/{postcode}` | **Primary**: postcode info + all addresses |
| GET | `/addresses/search?q=&postcode=&street=&city=&page=&page_size=` | Filtered address search with pagination |
| GET | `/addresses/{id}` | Single address by ID |

## Future Work

- [ ] Frontend (`frontend/`)
