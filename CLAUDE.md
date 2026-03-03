# CLAUDE.md — Project guide for Claude Code

## Project overview

UK Postcode & Address Lookup — a full-stack app with 69M+ addresses from 7 government data sources, enriched with property prices, company registrations, food hygiene ratings, and commercial valuations.

## Tech stack

- **Backend**: Python 3.12, FastAPI, SQLAlchemy 2 (async), Typer CLI, uv
- **Frontend**: SvelteKit 5, Svelte 5 runes, Tailwind CSS v4, TypeScript
- **Database**: PostgreSQL 16, PostGIS 3.4, pg_trgm
- **Infra**: Docker Compose (3 services: db, backend, frontend)

## Key commands

```bash
make up              # Start all services (dev mode, hot-reload)
make up-prod         # Start all services (production)
make down            # Stop everything
make shell           # Open psql shell
make test            # Run all backend tests
make lint            # Lint backend + frontend
```

## Project structure

```
backend/app/
  api/              # FastAPI routes, schemas, deps
    routers/        # health.py, postcodes.py, addresses.py
  core/db/          # SQLAlchemy engine, models, migrations
  ingestion/        # CLI, parsers, merge pipeline, downloader
frontend/src/lib/
  api/              # client.ts (fetch wrapper), types.ts (interfaces)
  components/       # Svelte components (AddressCard, PostcodeSearch, etc.)
```

## Database schema

- **postcodes** — 2.7M UK postcodes with coordinates and admin codes
- **addresses** — 69M addresses from OSM, Land Registry, Companies House, FSA, VOA, EPC
- **price_paid** — 31M house sale transactions (FK → addresses)
- **companies** — 5.5M company registrations (FK → addresses)
- **food_ratings** — 505K food hygiene ratings (FK → addresses)
- **voa_ratings** — 2.3M non-domestic property valuations (FK → addresses)
- **uprn_coordinates** — 35M UPRN→coordinate lookup table

## Important indexes

- `ix_postcodes_postcode_no_space_pattern` — varchar_pattern_ops for autocomplete prefix search
- `ix_addresses_street_trgm`, `_city_trgm`, `_house_name_trgm`, `_suburb_trgm` — GIN trigram indexes for ILIKE search
- `ix_addresses_postcode_street_house` — composite index for postcode lookup sorting
- All enrichment tables have `address_id` FK indexes and `postcode_norm` indexes

## API endpoints

- `GET /api/health` — instant counts via pg_class.reltuples
- `GET /api/postcodes/autocomplete?q=...` — prefix search, varchar_pattern_ops index
- `GET /api/postcodes/{postcode}` — lookup with paginated addresses + eager-loaded enrichment
- `GET /api/addresses/search?q=...&city=...&street=...` — trigram ILIKE search, capped count at 10K, ORDER BY id, SET LOCAL statement_timeout = 10s
- `GET /api/addresses/{id}` — single address with all enrichment data

## Performance conventions

- Search endpoint uses `SET LOCAL statement_timeout = '10s'` (scoped to that transaction only)
- Count queries are capped at 10,000 via subquery LIMIT to prevent full-table scans
- Health endpoint uses `pg_class.reltuples` for instant approximate counts
- Postcode autocomplete uses `varchar_pattern_ops` index for B-tree prefix scans
- Frontend lazy-loads enrichment data on search results via "View details" button

## Code conventions

- Backend uses async SQLAlchemy sessions (psycopg async driver)
- Frontend uses Svelte 5 runes ($state, $derived, $effect)
- Migrations in backend/app/core/db/migrations/versions/ (numbered 001-005)
- Alembic migrations use CREATE INDEX CONCURRENTLY (safe for live DB)
- The alembic.ini hardcodes localhost; inside Docker the DB host is `db`
