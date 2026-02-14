.PHONY: help db db-stop init download ingest status test test-unit test-integration lint format shell serve install fe-install fe-dev fe-build fe-preview fe-lint fe-format fe-check

.DEFAULT_GOAL := help

help: ## Show this help
	@echo "Usage: make <target>"
	@echo ""
	@echo "Infrastructure"
	@echo "  db              Start PostgreSQL (detached)"
	@echo "  db-stop         Stop all containers"
	@echo "  shell           Open psql shell"
	@echo ""
	@echo "Backend — Ingestion"
	@echo "  init            Create database tables"
	@echo "  download        Download source data"
	@echo "  ingest          Run full ingestion pipeline"
	@echo "  status          Show ingestion status"
	@echo ""
	@echo "Backend — API"
	@echo "  serve           Start backend API server"
	@echo ""
	@echo "Backend — Testing"
	@echo "  test            Run all tests"
	@echo "  test-unit       Run unit tests only"
	@echo "  test-integration Run integration tests only"
	@echo ""
	@echo "Backend — Code Quality"
	@echo "  lint            Lint backend (ruff)"
	@echo "  format          Format backend (ruff)"
	@echo "  install         Install backend deps (uv sync)"
	@echo ""
	@echo "Frontend"
	@echo "  fe-install      Install frontend deps"
	@echo "  fe-dev          Start frontend dev server"
	@echo "  fe-build        Production build (static)"
	@echo "  fe-preview      Preview production build"
	@echo "  fe-lint         Lint frontend (eslint)"
	@echo "  fe-format       Format frontend (prettier)"
	@echo "  fe-check        Type-check frontend (svelte-check)"

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

# --- Frontend ---
fe-install:
	cd frontend && npm install

fe-dev:
	cd frontend && npm run dev

fe-build:
	cd frontend && npm run build

fe-preview:
	cd frontend && npm run preview

fe-lint:
	cd frontend && npm run lint

fe-format:
	cd frontend && npm run format

fe-check:
	cd frontend && npm run check
