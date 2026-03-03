# UK Postcode & Address Lookup

A full-stack web application for looking up UK postcodes and their associated addresses with enrichment data. It ingests data from seven free, open government data sources, stores it in a PostgreSQL/PostGIS database, serves it through a FastAPI REST API, and presents it via a SvelteKit frontend.

## Architecture

```mermaid
graph LR
    subgraph Data Sources
        CP[OS CodePoint Open<br/>~1.7M postcodes]
        NSPL[ONS NSPL<br/>~2.7M postcodes]
        OSM[OpenStreetMap<br/>~800K addresses]
        LR[Land Registry<br/>~31M transactions]
        CH[Companies House<br/>~5.5M companies]
        FSA[Food Standards Agency<br/>~505K ratings]
        VOA[Valuation Office Agency<br/>~2.3M ratings]
    end

    subgraph Docker Compose
        FE["Frontend<br/>SvelteKit :3000"]
        BE["Backend<br/>FastAPI :8000"]
        DB[("PostgreSQL<br/>+ PostGIS<br/>:5432")]
    end

    CP & NSPL & OSM & LR & CH & FSA & VOA -->|ingestion CLI| BE
    FE -->|REST API| BE
    BE --> DB
```

The application has three main layers:

- **Frontend** — SvelteKit 5 single-page application with Tailwind CSS. Provides postcode autocomplete, address search, and enrichment data display (price history, companies, food ratings, VOA ratings).
- **Backend** — Python/FastAPI application with two roles: a data ingestion CLI that downloads, parses, and loads data from 7 sources, and a REST API with 5 endpoints.
- **Database** — PostgreSQL 16 with PostGIS 3.4 extension for geospatial queries. Stores ~2.7M postcodes, ~69M addresses, and ~40M enrichment records.

## Tech Stack

| Layer | Technology |
|-------|------------|
| Frontend | SvelteKit 5, Svelte 5 (runes), Tailwind CSS v4, TypeScript |
| Backend | Python 3.12, FastAPI, SQLAlchemy 2, Typer CLI |
| Database | PostgreSQL 16, PostGIS 3.4, pg_trgm |
| Ingestion | httpx (async downloads), osmium (PBF parsing), pyproj (coordinate conversion) |
| Tooling | uv (Python), npm (Node.js), Docker Compose, Ruff, Prettier, ESLint |
| Testing | pytest, testcontainers (real PostGIS in tests) |

## Prerequisites

You need to install these three tools before running the project.

### 1. Docker Desktop

Docker runs the PostgreSQL database in a container.

- **Windows**: Download and install from https://www.docker.com/products/docker-desktop/
  - During installation, make sure **WSL 2** backend is enabled
  - After installation, open Docker Desktop and wait until it shows "Docker Desktop is running"
- **Linux (Ubuntu/Debian)**:
  ```bash
  sudo apt-get update
  sudo apt-get install -y docker.io docker-compose-plugin
  sudo systemctl start docker
  sudo systemctl enable docker
  sudo usermod -aG docker $USER
  ```
  Log out and log back in after running the last command.
- **Linux (Fedora/RHEL)**:
  ```bash
  sudo dnf install docker docker-compose-plugin
  sudo systemctl start docker
  sudo systemctl enable docker
  sudo usermod -aG docker $USER
  ```

Verify Docker is working:
```bash
docker --version
docker compose version
```

### 2. uv (Python Package Manager)

uv is a fast Python package manager that replaces pip/virtualenv.

- **Windows** (PowerShell):
  ```powershell
  powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
  ```
- **Linux / macOS**:
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```

Close and reopen your terminal after installation. Verify:
```bash
uv --version
```

### 3. Node.js 22+

Node.js is needed for the frontend.

- **Windows**: Download the LTS installer from https://nodejs.org/ and run it
- **Linux (Ubuntu/Debian)**:
  ```bash
  curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
  sudo apt-get install -y nodejs
  ```
- **Linux (Fedora/RHEL)**:
  ```bash
  curl -fsSL https://rpm.nodesource.com/setup_22.x | sudo bash -
  sudo dnf install -y nodejs
  ```

Verify:
```bash
node --version
npm --version
```

### 4. Make (Optional but Recommended)

The project includes a Makefile with shortcut commands. `make` is pre-installed on Linux/macOS.

- **Windows**: If using Git Bash, `make` may not be available. You can either:
  - Install via [Chocolatey](https://chocolatey.org/): `choco install make`
  - Or skip `make` and run the underlying commands directly (shown in all steps below)

## Getting Started

### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd PostcodeAddressLookup
```

### Step 2: Create the Environment File

This file contains database connection settings and data source URLs.

**Linux / macOS / Git Bash:**
```bash
cp .env.example .env
```

**Windows (CMD):**
```cmd
copy .env.example .env
```

**Windows (PowerShell):**
```powershell
Copy-Item .env.example .env
```

### Step 3: Start the Database

```bash
docker compose up db -d
```

This starts a PostgreSQL 16 + PostGIS 3.4 container in the background on port **5432**.

Wait 10-15 seconds, then verify it is healthy:
```bash
docker compose ps
```
The `db` service should show status as **healthy**.

### Step 4: Install Backend Dependencies

```bash
cd backend
uv sync
```

This creates a `.venv` virtual environment inside `backend/` and installs all Python dependencies.

### Step 5: Initialize the Database Schema

```bash
uv run ingest init-db
```

This runs Alembic migrations to create all database tables: `postcodes`, `addresses`, `data_sources`, `price_paid`, `companies`, `food_ratings`, `voa_ratings`, and `uprn_coordinates`.

### Step 6: Download Source Data (~10+ GB)

```bash
uv run ingest download
```

This downloads source files into the `backend/data/` folder:

| File | Size | Source |
|------|------|--------|
| `codepoint-open.zip` | ~150 MB | Ordnance Survey — postcode coordinates |
| `nspl.zip` | ~178 MB | ONS — administrative area codes |
| `great-britain-latest.osm.pbf` | ~1.9 GB | Geofabrik/OpenStreetMap — building addresses |
| `pp-complete.csv` | ~4.5 GB | Land Registry — price paid data |
| `BasicCompanyDataAsOneFile-*.csv` | ~1.5 GB | Companies House — company registrations |
| `osopenuprn_*.csv` | ~2.1 GB | OS Open UPRN — coordinate lookup |
| `UK-en-*.xml` (API) | ~50 MB | FSA food hygiene ratings (fetched via API) |
| `uk-valuation-office-agency-*.csv` | ~500 MB | VOA non-domestic rating list |

Downloads are cached — if files already exist, they are skipped.

### Step 7: Run the Ingestion Pipeline

```bash
uv run ingest all
```

This runs the full pipeline in sequence:
1. **Load CodePoint postcodes** — ~1.7M postcodes with coordinates
2. **Merge NSPL data** — adds admin codes (region, local authority, ward) to postcodes
3. **Load OSM addresses** — ~800K street addresses from OpenStreetMap
4. **Load Land Registry** — ~31M house sale transactions
5. **Load UPRN coordinates** — ~35M UPRN geocoding records
6. **Load Companies House** — ~5.5M company registrations
7. **Load FSA food ratings** — ~505K food hygiene ratings
8. **Load VOA ratings** — ~2.3M non-domestic property valuations
9. **Merge** — link postcodes, geocode addresses, link enrichment data, score confidence, deduplicate

Individual sources can also be loaded separately: `make ingest-lr`, `make ingest-companies`, `make ingest-fsa`, `make ingest-voa`.

**This step takes several hours** depending on your hardware and internet speed. You will see progress bars for each stage.

### Step 8: Verify Data Loaded

```bash
uv run ingest status
```

You should see record counts similar to:
- Postcodes: ~2.7M
- Addresses: ~69M
- Price Paid: ~31M
- Companies: ~5.5M
- Food Ratings: ~505K
- VOA Ratings: ~2.3M

### Step 9: Start the Backend API

```bash
uv run serve
```

The FastAPI backend is now running at **http://localhost:8000**.

You can verify by visiting:
- Swagger API docs: http://localhost:8000/docs
- Health check: http://localhost:8000/health

**Keep this terminal open.** The API must stay running while you use the frontend.

### Step 10: Start the Frontend (New Terminal)

Open a **second terminal** window and run:

```bash
cd PostcodeAddressLookup/frontend
npm install
npm run dev
```

The SvelteKit frontend is now running at **http://localhost:5173**.

### Step 11: Open the Application

Open your browser and go to **http://localhost:5173**.

You will see two tabs:
- **Postcode Lookup** — Type a postcode (e.g., `SW1A 1AA`, `M1 1AE`, `B1 1BB`) to see autocomplete suggestions, then select one to view all addresses at that postcode.
- **Address Search** — Search for any address text (e.g., `Downing Street`, `Oxford Road`) with optional filters for city, street, and postcode.

The status bar at the bottom of the page shows whether the API is connected and how many records are in the database.

## Running with Docker Compose (Alternative)

After completing Steps 1-8 (data must be ingested first), you can run all three services together:

```bash
docker compose up
```

This starts:
| Service | URL |
|---------|-----|
| PostgreSQL + PostGIS | localhost:5432 |
| Backend API (FastAPI) | http://localhost:8000 |
| Frontend (Nginx) | http://localhost:3000 |

Note: In Docker Compose mode, the frontend runs on port **3000** (served by Nginx) instead of 5173.

## Starting Again After First Setup

Once data is ingested, you only need to start the services. You do **not** need to re-download or re-ingest.

**Terminal 1 — Database + Backend:**
```bash
cd PostcodeAddressLookup
docker compose up db -d
cd backend
uv run serve
```

**Terminal 2 — Frontend:**
```bash
cd PostcodeAddressLookup/frontend
npm run dev
```

## Stopping the Application

- **Frontend**: Press `Ctrl+C` in the frontend terminal
- **Backend**: Press `Ctrl+C` in the backend terminal
- **Database**: Run from the project root:
  ```bash
  docker compose down
  ```
  Note: Your data is preserved in a Docker volume. It will still be there next time you start the database.

To completely remove all data (start fresh):
```bash
docker compose down -v
```

## Project Structure

```
PostcodeAddressLookup/
├── backend/                    # Python backend (FastAPI + ingestion CLI)
│   ├── app/
│   │   ├── core/               # Shared foundation layer
│   │   │   ├── config.py       # Settings via pydantic-settings (.env)
│   │   │   ├── logging.py      # Structured logging (structlog)
│   │   │   ├── exceptions.py   # Custom exception hierarchy
│   │   │   ├── db/
│   │   │   │   ├── engine.py   # SQLAlchemy engine + session factory
│   │   │   │   ├── models.py   # ORM models (Postcode, Address, PricePaid, Company, FoodRating, VOARating)
│   │   │   │   ├── loader.py   # Generic batch loader with progress bars
│   │   │   │   └── migrations/ # Alembic database migrations
│   │   │   └── utils/
│   │   │       ├── postcode.py # UK postcode validation + normalization
│   │   │       ├── address.py  # Street/city name normalization
│   │   │       └── coordinates.py  # OSGB36 to WGS84 conversion
│   │   ├── ingestion/          # Data ingestion pipeline
│   │   │   ├── cli.py          # Typer CLI commands (download, load, merge)
│   │   │   ├── downloader.py   # Async concurrent file downloads
│   │   │   ├── codepoint.py    # CodePoint Open ZIP parser
│   │   │   ├── nspl.py         # NSPL ZIP parser
│   │   │   ├── osm.py          # OpenStreetMap PBF parser
│   │   │   ├── merge.py        # Link postcodes, score confidence, deduplicate
│   │   │   └── schemas.py      # Pydantic validation models for ingestion
│   │   └── api/                # REST API layer
│   │       ├── main.py         # FastAPI app factory (CORS, error handlers)
│   │       ├── run.py          # Uvicorn entry point
│   │       ├── deps.py         # Dependency injection (settings, DB session)
│   │       ├── errors.py       # Exception to HTTP status mapping
│   │       ├── schemas.py      # API response models
│   │       └── routers/
│   │           ├── health.py   # GET /health
│   │           ├── postcodes.py # GET /postcodes/autocomplete, /postcodes/{postcode}
│   │           └── addresses.py # GET /addresses/search, /addresses/{id}
│   ├── tests/
│   │   ├── unit/               # Pure function tests (no DB needed)
│   │   └── integration/        # Tests against real PostgreSQL+PostGIS
│   ├── data/                   # Downloaded source files (gitignored)
│   ├── pyproject.toml          # Python project config + dependencies
│   ├── Dockerfile              # Multi-stage Docker build
│   └── alembic.ini             # Database migration config
├── frontend/                   # SvelteKit frontend
│   ├── src/
│   │   ├── lib/
│   │   │   ├── api/
│   │   │   │   ├── client.ts   # Typed API client (fetch wrapper)
│   │   │   │   └── types.ts    # TypeScript interfaces for API responses
│   │   │   └── components/
│   │   │       ├── PostcodeSearch.svelte  # Autocomplete postcode input
│   │   │       ├── AddressSearch.svelte   # Address search with filters
│   │   │       ├── AddressCard.svelte     # Single address display card
│   │   │       ├── AddressList.svelte     # List of addresses for a postcode
│   │   │       ├── SearchResults.svelte   # Paginated search results
│   │   │       ├── Pagination.svelte      # Page navigation
│   │   │       ├── TabSwitcher.svelte     # Tab navigation
│   │   │       ├── Spinner.svelte         # Loading indicator
│   │   │       └── StatusBar.svelte       # API health status footer
│   │   └── routes/
│   │       ├── +layout.svelte  # App shell (header, footer)
│   │       ├── +layout.js      # Disables SSR (client-only SPA)
│   │       └── +page.svelte    # Main page (tab switching, state management)
│   ├── package.json            # Node.js dependencies
│   ├── Dockerfile              # Multi-stage build (Node + Nginx)
│   ├── nginx.conf              # Production static file server config
│   └── .env                    # Frontend environment (API URL)
├── docker-compose.yml          # Orchestrates all 3 services
├── Makefile                    # Shortcut commands (see below)
├── .env.example                # Template for backend environment variables
├── .gitignore                  # Excludes data files, node_modules, .venv, etc.
└── docs/
    ├── plan.md                 # Architecture and design decisions
    └── tracker.md              # Implementation progress tracker
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Health check — returns DB status and record counts |
| GET | `/api/postcodes/autocomplete?q=SW1A&limit=10` | Postcode prefix search for type-ahead UI |
| GET | `/api/postcodes/{postcode}?page=1&page_size=20` | Postcode lookup — returns postcode info + paginated addresses with enrichment data |
| GET | `/api/addresses/search?q=downing&city=London&page=1&page_size=20` | Full-text address search with optional filters and pagination |
| GET | `/api/addresses/{id}` | Single address by database ID with full enrichment data (prices, companies, food ratings, VOA) |

All endpoints return JSON. Full interactive documentation is available at http://localhost:8000/docs when the backend is running.

### Address Sources

Each address record has a `source` field indicating its origin:

| Source value | Dataset | Typical content |
|---|---|---|
| `osm` | OpenStreetMap | Residential and commercial street addresses |
| `land_registry` | HM Land Registry | Addresses from property sale transactions |
| `companies_house` | Companies House | Business registration addresses |
| `fsa` | Food Standards Agency | Food establishment addresses |
| `voa` | Valuation Office Agency | Non-domestic property addresses |
| `epc` | Energy Performance Certificates | Domestic building addresses |

### Confidence Score

Each address has a `confidence` score (0–100%) indicating data quality:

- **Phase 1 (up to 80%)**: Points for having key fields — postcode (+20%), street (+15%), house number/name (+15%), city (+10%), coordinates (+10%), suburb (+5%), UPRN (+5%)
- **Phase 2 (+15%)**: Bonus if the same address appears in 2+ different data sources
- **Phase 3 (+5%)**: Bonus if the address has any linked enrichment data

## Data Sources

### Core Address Data

| Source | Records | Description |
|--------|---------|-------------|
| [OS CodePoint Open](https://www.ordnancesurvey.co.uk/products/code-point-open) | ~1.7M | Postcode centroids with OSGB36 coordinates (converted to WGS84) |
| [ONS NSPL](https://geoportal.statistics.gov.uk/) | ~2.7M | National Statistics Postcode Lookup — all UK postcodes including terminated, with administrative area codes |
| [OpenStreetMap](https://download.geofabrik.de/) | ~800K | Building addresses extracted from Great Britain PBF export |
| [OS Open UPRN](https://www.ordnancesurvey.co.uk/products/os-open-uprn) | ~35M | UPRN to coordinate mappings for geocoding |

### Enrichment Data

Enrichment records are linked to addresses and provide additional context:

| Source | Records | Description | What it shows |
|--------|---------|-------------|---------------|
| [HM Land Registry](https://www.gov.uk/government/collections/price-paid-data) | ~31M | House sale transactions | Sale price, date, property type (Detached/Semi/Terraced/Flat), Freehold/Leasehold, new build flag |
| [Companies House](https://www.gov.uk/government/organisations/companies-house) | ~5.5M | Registered company addresses | Company name, status (Active/Dissolved), type (Ltd/PLC), SIC code, incorporation date |
| [Food Standards Agency](https://ratings.food.gov.uk/) | ~505K | Food hygiene inspection ratings | Business name, rating (0–5), business type, hygiene/structural/management scores |
| [Valuation Office Agency](https://www.gov.uk/government/organisations/valuation-office-agency) | ~2.3M | Non-domestic property valuations | Property description (Shop/Office/etc.), rateable value, firm name |

### How enrichment linking works

Each enrichment record is linked to an address via a two-phase matching process:
1. **Exact match** — direct join on source-specific IDs (transaction ID, company number, FHRS ID, UARN)
2. **Text fallback** — case-insensitive matching on postcode + street/address components for unmatched records

## Make Targets (Shortcut Commands)

If you have `make` installed, these shortcuts are available from the project root:

| Command | What It Does | Equivalent Without Make |
|---------|-------------|------------------------|
| `make db` | Start PostgreSQL container | `docker compose up db -d` |
| `make db-stop` | Stop all containers | `docker compose down` |
| `make shell` | Open psql database shell | `docker compose exec db psql -U postgres -d postcode_lookup` |
| `make install` | Install backend dependencies | `cd backend && uv sync` |
| `make init` | Create database tables | `cd backend && uv run ingest init-db` |
| `make download` | Download source data | `cd backend && uv run ingest download` |
| `make ingest` | Run full ingestion pipeline | `cd backend && uv run ingest all` |
| `make status` | Show ingestion statistics | `cd backend && uv run ingest status` |
| `make serve` | Start backend API server | `cd backend && uv run serve` |
| `make test` | Run all backend tests | `cd backend && uv run pytest` |
| `make test-unit` | Run unit tests only | `cd backend && uv run pytest tests/unit` |
| `make test-integration` | Run integration tests | `cd backend && uv run pytest tests/integration` |
| `make lint` | Lint backend code | `cd backend && uv run ruff check app/ tests/` |
| `make format` | Format backend code | `cd backend && uv run ruff format app/ tests/` |
| `make fe-install` | Install frontend dependencies | `cd frontend && npm install` |
| `make fe-dev` | Start frontend dev server | `cd frontend && npm run dev` |
| `make fe-build` | Production frontend build | `cd frontend && npm run build` |
| `make fe-lint` | Lint frontend code | `cd frontend && npm run lint` |
| `make fe-format` | Format frontend code | `cd frontend && npm run format` |
| `make fe-check` | Type-check frontend | `cd frontend && npm run check` |

## Configuration

All backend settings are configured via environment variables in the `.env` file (created from `.env.example`):

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql+psycopg://postgres:postgres@localhost:5432/postcode_lookup` | PostgreSQL connection string |
| `DATA_DIR` | `data` | Directory for downloaded source files |
| `BATCH_SIZE` | `2000` | Records per database insert batch |
| `OSM_INDEX_TYPE` | `sparse_file_array` | OSM parser memory mode (`sparse_file_array` for low RAM, `flex_mem` for faster but uses ~4-6 GB RAM) |
| `LOG_LEVEL` | `INFO` | Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `LOG_FORMAT` | `console` | Log output format (`console` for development, `json` for production) |

Frontend configuration is in `frontend/.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `VITE_API_BASE_URL` | `http://localhost:8000` | Backend API URL that the frontend calls |

## Running Tests

Backend tests require Docker to be running (integration tests spin up a temporary PostGIS container).

```bash
cd backend

# Run all tests
uv run pytest

# Run only unit tests (no Docker required)
uv run pytest tests/unit

# Run only integration tests (requires Docker)
uv run pytest tests/integration

# Run with verbose output
uv run pytest -v
```

## Troubleshooting

### Database container won't start
- Make sure Docker Desktop is running (Windows) or the Docker daemon is active (Linux: `sudo systemctl start docker`)
- Check if port 5432 is already in use: `docker compose logs db`

### "uv: command not found"
- Close and reopen your terminal after installing uv
- On Windows, you may need to restart your PC for the PATH to update

### "npm: command not found" or wrong Node version
- Verify installation: `node --version` (should be 22+)
- On Linux, make sure you installed from NodeSource, not the system package (which may be outdated)

### Ingestion is very slow
- The OSM step is the heaviest (~800K addresses from a 1.9 GB file)
- To speed it up, change `OSM_INDEX_TYPE=flex_mem` in `.env` (uses ~4-6 GB RAM but much faster)
- Total ingestion time: 15-60+ minutes depending on hardware

### Frontend shows "API unavailable"
- Make sure the backend is running (`uv run serve` in the backend directory)
- Check that `frontend/.env` has `VITE_API_BASE_URL=http://localhost:8000`
- Check the backend health: visit http://localhost:8000/health in your browser

### "make: command not found" (Windows)
- Install via Chocolatey: `choco install make`
- Or use the equivalent commands shown in the Make Targets table above

### Port already in use
- Database (5432): Stop other PostgreSQL instances or change the port in `docker-compose.yml`
- Backend (8000): Kill the existing process or set `API_PORT` in `.env`
- Frontend (5173): The dev server will automatically try the next available port
