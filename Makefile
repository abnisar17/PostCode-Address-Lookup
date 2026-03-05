.PHONY: help up up-prod down build logs shell init download ingest ingest-lr ingest-uprn ingest-companies ingest-fsa ingest-epc ingest-voa status test test-unit test-integration lint format install fe-build fe-check

.DEFAULT_GOAL := help

help: ## Show this help
	@echo "Usage: make <target>"
	@echo ""
	@echo "Docker"
	@echo "  up              Start all services (dev mode, hot-reload)"
	@echo "  up-prod         Start all services (production mode)"
	@echo "  down            Stop all services"
	@echo "  build           Rebuild Docker images"
	@echo "  logs            Tail container logs"
	@echo "  shell           Open psql shell"
	@echo ""
	@echo "Ingestion"
	@echo "  init            Create database tables"
	@echo "  download        Download source data"
	@echo "  ingest          Run full ingestion pipeline (all sources)"
	@echo "  ingest-lr       Load Land Registry price paid data"
	@echo "  ingest-uprn     Load OS Open UPRN coordinates"
	@echo "  ingest-companies Load Companies House data"
	@echo "  ingest-fsa      Load FSA food ratings (from API)"
	@echo "  ingest-epc      Load EPC certificates (requires manual download)"
	@echo "  ingest-voa      Load VOA non-domestic rating list"
	@echo "  status          Show ingestion status"
	@echo ""
	@echo "Testing & Quality"
	@echo "  test            Run all tests"
	@echo "  test-unit       Run unit tests only"
	@echo "  test-integration Run integration tests only"
	@echo "  lint            Lint backend + frontend"
	@echo "  format          Format backend + frontend"
	@echo ""
	@echo "Setup"
	@echo "  install         Install all dependencies (backend + frontend)"
	@echo "  fe-build        Production build frontend (static)"
	@echo "  fe-check        Type-check frontend (svelte-check)"

# --- Docker ---
# docker-compose.override.yml is auto-merged, giving dev hot-reload
up:
	docker compose up --build

up-prod:
	docker compose -f docker-compose.yml up --build

down:
	docker compose down

build:
	docker compose build

logs:
	docker compose logs -f

shell:
	docker compose exec db psql -U postgres -d postcode_lookup

# --- Ingestion ---
init:
	cd backend && uv run ingest init-db

download:
	cd backend && uv run ingest download

ingest:
	cd backend && uv run ingest all

ingest-lr:
	cd backend && uv run ingest load-land-registry

ingest-uprn:
	cd backend && uv run ingest load-uprn

ingest-companies:
	cd backend && uv run ingest load-companies

ingest-fsa:
	cd backend && uv run ingest load-fsa

ingest-epc:
	cd backend && uv run ingest load-epc

ingest-voa:
	cd backend && uv run ingest load-voa

status:
	cd backend && uv run ingest status

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
	cd frontend && npm run lint

format:
	cd backend && uv run ruff format app/ tests/
	cd frontend && npm run format

# --- Setup ---
install:
	cd backend && uv sync
	cd frontend && npm install

fe-build:
	cd frontend && npm run build

fe-check:
	cd frontend && npm run check
