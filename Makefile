.PHONY: db db-stop init download ingest status test test-unit test-integration lint format shell serve

# --- Infrastructure ---
db:
	docker compose up db -d

db-stop:
	docker compose down

shell:
	docker compose exec db psql -U postgres -d postcode_lookup

# --- Backend (ingestion) ---
init:
	cd backend && uv run ingest init-db

download:
	cd backend && uv run ingest download

ingest:
	cd backend && uv run ingest all

status:
	cd backend && uv run ingest status

# --- API ---
serve:
	cd backend && uv run serve

# --- Testing ---
test:
	cd backend && uv run pytest

test-unit:
	cd backend && uv run pytest tests/unit

test-integration:
	cd backend && uv run pytest tests/integration

# --- Code quality ---
lint:
	cd backend && uv run ruff check app/ tests/

format:
	cd backend && uv run ruff format app/ tests/

# --- Backend deps ---
install:
	cd backend && uv sync
