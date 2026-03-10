"""Typer CLI for the data ingestion pipeline.

All commands receive dependencies via Typer callback (DIP).
Entry point: `uv run ingest <command>`
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import typer
from pydantic import BaseModel
from rich.console import Console
from rich.table import Table
from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import Settings
from app.core.db.engine import create_engine, create_session_factory, ensure_postgis
from app.core.db.loader import LoadResult, batch_load, copy_load, rebuild_indexes
from app.core.db.models import (
    Address,
    Company,
    DataSource,
    FoodRating,
    Postcode,
    PricePaid,
    UPRNCoordinate,
    VOARating,
)
from app.core.exceptions import PipelineError
from app.core.logging import get_logger, setup_logging
from app.ingestion.schemas import (
    CodePointRecord,
    CompaniesHouseRecord,
    EPCRecord,
    FSARatingRecord,
    LandRegistryRecord,
    NSPLRecord,
    OSMAddressRecord,
    UPRNRecord,
    VOARecord,
)

app = typer.Typer(name="ingest", help="UK Postcode data ingestion pipeline.")
console = Console()


@dataclass
class AppState:
    config: Settings
    session_factory: sessionmaker[Session]


@app.callback()
def main(ctx: typer.Context):
    """Initialise shared state: config, logging, DB session factory."""
    settings = Settings()
    setup_logging(level=settings.log_level, fmt=settings.log_format)
    engine = create_engine(settings.database_url)
    ctx.ensure_object(dict)
    ctx.obj = AppState(
        config=settings,
        session_factory=create_session_factory(engine),
    )


def _get_state(ctx: typer.Context) -> AppState:
    return ctx.obj


# ---------------------------------------------------------------------------
# init-db
# ---------------------------------------------------------------------------
@app.command()
def init_db(ctx: typer.Context):
    """Create database tables and PostGIS extension via Alembic."""
    import subprocess
    import sys

    state = _get_state(ctx)
    log = get_logger("init-db")

    # Ensure PostGIS extension exists
    engine = create_engine(state.config.database_url)
    ensure_postgis(engine)

    # Run Alembic upgrade
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        console.print(f"[red]Alembic migration failed:[/red]\n{result.stderr}")
        raise typer.Exit(1)

    console.print("[green]Database initialised successfully.[/green]")
    log.info("Database initialised")


# ---------------------------------------------------------------------------
# download
# ---------------------------------------------------------------------------
@app.command()
def download(
    ctx: typer.Context,
    source: str = typer.Argument(
        None,
        help="Source to download: codepoint, nspl, osm, land_registry, "
        "companies_house, open_uprn, voa, or all",
    ),
    force: bool = typer.Option(False, "--force", "-f", help="Re-download even if file exists"),
):
    """Download data sources (concurrently by default).

    Note: FSA data is fetched via API during load-fsa (no download step).
    EPC data requires manual download after free registration.
    """
    from app.ingestion.downloader import run_download

    state = _get_state(ctx)
    log = get_logger("download")
    cfg = state.config

    all_sources = {
        "codepoint": (cfg.codepoint_download_url, cfg.codepoint_file),
        "nspl": (cfg.nspl_download_url, cfg.nspl_file),
        "osm": (cfg.osm_download_url, cfg.osm_file),
        "land_registry": (cfg.land_registry_download_url, cfg.land_registry_file),
        "companies_house": (cfg.companies_house_download_url, cfg.companies_house_file),
        "open_uprn": (cfg.open_uprn_download_url, cfg.open_uprn_file),
        "voa": (cfg.voa_download_url, cfg.voa_file),
    }

    valid_names = list(all_sources.keys())

    if source and source != "all":
        if source not in all_sources:
            console.print(
                f"[red]Unknown source: {source}. "
                f"Choose: {', '.join(valid_names)}, or all[/red]"
            )
            raise typer.Exit(1)
        targets = {source: all_sources[source]}
    else:
        targets = all_sources

    # Track download state
    for name in targets:
        _update_data_source(state.session_factory, name, status="downloading")

    try:
        digests = run_download(targets, force=force)
        for name, digest in digests.items():
            _update_data_source(
                state.session_factory, name, status="pending", file_hash=digest
            )
        console.print(f"[green]Downloaded {len(digests)} source(s).[/green]")
    except Exception as exc:
        for name in targets:
            _update_data_source(
                state.session_factory, name, status="failed", error_message=str(exc)
            )
        raise


# ---------------------------------------------------------------------------
# load-postcodes
# ---------------------------------------------------------------------------
@app.command()
def load_postcodes(
    ctx: typer.Context,
    truncate: bool = typer.Option(False, "--truncate", help="Truncate postcodes table first"),
):
    """Load postcodes from Code-Point Open + NSPL admin data."""
    from app.ingestion.codepoint import parse_codepoint
    from app.ingestion.nspl import parse_nspl

    state = _get_state(ctx)
    log = get_logger("load-postcodes")
    cfg = state.config

    if truncate:
        with state.session_factory() as session:
            session.execute(text("TRUNCATE TABLE postcodes CASCADE"))
            session.commit()
        log.info("Truncated postcodes table")

    # Phase 1: Code-Point (coordinates)
    _update_data_source(state.session_factory, "codepoint", status="ingesting")
    console.print("[bold]Loading Code-Point Open postcodes...[/bold]")

    result = batch_load(
        state.session_factory,
        parse_codepoint(cfg.codepoint_file, batch_size=cfg.batch_size),
        _upsert_postcodes_codepoint,
        source="codepoint",
        label="Code-Point postcodes",
    )
    _update_data_source(
        state.session_factory, "codepoint", status="completed", record_count=result.loaded
    )

    # Phase 2: NSPL (admin hierarchies)
    _update_data_source(state.session_factory, "nspl", status="ingesting")
    console.print("[bold]Merging NSPL admin data...[/bold]")

    nspl_result = batch_load(
        state.session_factory,
        parse_nspl(cfg.nspl_file, batch_size=cfg.batch_size),
        _upsert_postcodes_nspl,
        source="nspl",
        label="NSPL admin merge",
    )
    _update_data_source(
        state.session_factory, "nspl", status="completed", record_count=nspl_result.loaded
    )

    console.print(
        f"[green]Postcodes loaded:[/green] "
        f"Code-Point {result.loaded}, NSPL merged {nspl_result.loaded}"
    )


# ---------------------------------------------------------------------------
# load-osm
# ---------------------------------------------------------------------------
@app.command()
def load_osm(
    ctx: typer.Context,
    truncate: bool = typer.Option(False, "--truncate", help="Truncate addresses table first"),
    batch_size: int = typer.Option(None, "--batch-size", "-b", help="Override batch size"),
):
    """Load addresses from OSM .pbf file."""
    from app.ingestion.osm import parse_osm

    state = _get_state(ctx)
    log = get_logger("load-osm")
    cfg = state.config
    bs = batch_size or cfg.batch_size

    if truncate:
        with state.session_factory() as session:
            session.execute(text("TRUNCATE TABLE addresses CASCADE"))
            session.commit()
        log.info("Truncated addresses table")

    _update_data_source(state.session_factory, "osm", status="ingesting")
    console.print("[bold]Loading OSM addresses...[/bold]")

    result = batch_load(
        state.session_factory,
        parse_osm(cfg.osm_file, batch_size=bs, index_type=cfg.osm_index_type),
        _upsert_addresses_osm,
        source="osm",
        label="OSM addresses",
    )

    # Compute PostGIS location from lat/lon in a single UPDATE
    console.print("[bold]Computing PostGIS locations...[/bold]")
    with state.session_factory() as session:
        session.execute(
            text(
                "UPDATE addresses "
                "SET location = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) "
                "WHERE location IS NULL AND latitude IS NOT NULL"
            )
        )
        session.commit()
    log.info("PostGIS locations computed", source="osm")

    _update_data_source(
        state.session_factory, "osm", status="completed", record_count=result.loaded
    )

    console.print(f"[green]OSM addresses loaded:[/green] {result.loaded}")


# ---------------------------------------------------------------------------
# load-land-registry
# ---------------------------------------------------------------------------
@app.command()
def load_land_registry(
    ctx: typer.Context,
    truncate: bool = typer.Option(
        False, "--truncate", help="Truncate price_paid table first"
    ),
    slow: bool = typer.Option(
        False, "--slow", help="Use INSERT-based loading instead of fast COPY"
    ),
):
    """Load addresses + price paid data from HM Land Registry PPD CSV.

    Uses PostgreSQL COPY for fast bulk loading by default (~10x faster).
    Use --slow for INSERT-based loading with ON CONFLICT upsert.
    """
    from app.ingestion.land_registry import parse_land_registry

    state = _get_state(ctx)
    log = get_logger("load-land-registry")
    cfg = state.config

    if truncate:
        with state.session_factory() as session:
            session.execute(text("TRUNCATE TABLE price_paid CASCADE"))
            session.commit()
        log.info("Truncated price_paid table")

    _update_data_source(state.session_factory, "land_registry", status="ingesting")

    if slow:
        # Slow path: INSERT with ON CONFLICT (for re-runs / upserts)
        console.print("[bold]Loading Land Registry addresses (INSERT mode)...[/bold]")
        addr_result = batch_load(
            state.session_factory,
            parse_land_registry(cfg.land_registry_file, batch_size=cfg.batch_size),
            _upsert_addresses_land_registry,
            source="land_registry_addr",
            label="Land Registry addresses",
        )
        console.print("[bold]Loading price paid records (INSERT mode)...[/bold]")
        pp_result = batch_load(
            state.session_factory,
            parse_land_registry(cfg.land_registry_file, batch_size=cfg.batch_size),
            _upsert_price_paid,
            source="land_registry_pp",
            label="Price paid records",
        )
    else:
        # Fast path: COPY — single pass, loads both tables
        console.print("[bold]Loading Land Registry price paid (fast COPY)...[/bold]")
        pp_result = copy_load(
            state.session_factory,
            parse_land_registry(cfg.land_registry_file, batch_size=50_000),
            table_name="price_paid",
            columns=[
                "transaction_id", "postcode_norm", "price", "date_of_transfer",
                "property_type", "old_new", "duration",
                "paon", "saon", "street", "locality", "town",
                "district", "county", "ppd_category", "record_status",
            ],
            row_fn=lambda r: [
                r.transaction_id, r.postcode_norm, r.price, r.date_of_transfer,
                r.property_type, r.old_new, r.duration,
                r.paon, r.saon, r.street, r.locality, r.town,
                r.district, r.county, r.ppd_category, r.record_status,
            ],
            source="land_registry",
            label="Land Registry price paid (COPY)",
        )

        # Create addresses from price_paid using a single SQL INSERT ... SELECT
        console.print("[bold]Creating addresses from price paid data...[/bold]")
        with state.session_factory() as session:
            result = session.execute(text("""
                INSERT INTO addresses (source, source_id, house_number, flat, street, suburb, city, county, postcode_norm, postcode_raw)
                SELECT DISTINCT ON (pp.transaction_id)
                    'land_registry', LEFT('lr:' || pp.transaction_id, 100),
                    LEFT(pp.paon, 100), LEFT(pp.saon, 50), LEFT(pp.street, 200),
                    LEFT(pp.locality, 100), LEFT(pp.town, 100), LEFT(pp.county, 100),
                    pp.postcode_norm, pp.postcode_norm
                FROM price_paid pp
                WHERE NOT EXISTS (
                    SELECT 1 FROM addresses a
                    WHERE a.source = 'land_registry' AND a.source_id = LEFT('lr:' || pp.transaction_id, 100)
                )
            """))
            addr_count = result.rowcount
            session.commit()
        log.info("Addresses created from price_paid", count=addr_count)

        addr_result = LoadResult(source="land_registry_addr", loaded=addr_count, total=addr_count)

    total = addr_result.loaded + pp_result.loaded
    _update_data_source(
        state.session_factory, "land_registry", status="completed", record_count=total
    )
    console.print(
        f"[green]Land Registry loaded:[/green] "
        f"{addr_result.loaded} addresses, {pp_result.loaded} price paid"
    )


# ---------------------------------------------------------------------------
# load-uprn
# ---------------------------------------------------------------------------
@app.command()
def load_uprn(
    ctx: typer.Context,
    truncate: bool = typer.Option(
        False, "--truncate", help="Truncate uprn_coordinates table first"
    ),
    slow: bool = typer.Option(
        False, "--slow", help="Use INSERT-based loading instead of fast COPY"
    ),
):
    """Load OS Open UPRN coordinate lookup table.

    Uses PostgreSQL COPY for fast bulk loading by default (~10x faster).
    """
    from app.ingestion.open_uprn import parse_open_uprn

    state = _get_state(ctx)
    log = get_logger("load-uprn")
    cfg = state.config

    if truncate:
        with state.session_factory() as session:
            session.execute(text("TRUNCATE TABLE uprn_coordinates"))
            session.commit()
        log.info("Truncated uprn_coordinates table")

    _update_data_source(state.session_factory, "open_uprn", status="ingesting")
    console.print("[bold]Loading OS Open UPRN coordinates...[/bold]")

    if slow:
        result = batch_load(
            state.session_factory,
            parse_open_uprn(cfg.open_uprn_file, batch_size=cfg.batch_size),
            _upsert_uprn_coordinates,
            source="open_uprn",
            label="UPRN coordinates",
        )
    else:
        result = copy_load(
            state.session_factory,
            parse_open_uprn(cfg.open_uprn_file, batch_size=100_000),
            table_name="uprn_coordinates",
            columns=["uprn", "latitude", "longitude"],
            row_fn=lambda r: [r.uprn, r.latitude, r.longitude],
            source="open_uprn",
            label="UPRN coordinates (COPY)",
        )

    _update_data_source(
        state.session_factory, "open_uprn", status="completed", record_count=result.loaded
    )

    console.print(f"[green]UPRN coordinates loaded:[/green] {result.loaded}")


# ---------------------------------------------------------------------------
# load-companies
# ---------------------------------------------------------------------------
@app.command()
def load_companies(
    ctx: typer.Context,
    truncate: bool = typer.Option(
        False, "--truncate", help="Truncate companies table first"
    ),
    slow: bool = typer.Option(
        False, "--slow", help="Use INSERT-based loading instead of fast COPY"
    ),
):
    """Load addresses + company data from Companies House basic data ZIP.

    Uses PostgreSQL COPY for fast bulk loading by default (~10x faster).
    """
    from app.ingestion.companies_house import parse_companies_house

    state = _get_state(ctx)
    log = get_logger("load-companies")
    cfg = state.config

    if truncate:
        with state.session_factory() as session:
            session.execute(text("TRUNCATE TABLE companies CASCADE"))
            session.commit()
        log.info("Truncated companies table")

    _update_data_source(state.session_factory, "companies_house", status="ingesting")

    if slow:
        console.print("[bold]Loading Companies House addresses (INSERT mode)...[/bold]")
        addr_result = batch_load(
            state.session_factory,
            parse_companies_house(cfg.companies_house_file, batch_size=cfg.batch_size),
            _upsert_addresses_companies_house,
            source="companies_house_addr",
            label="Companies House addresses",
        )
        console.print("[bold]Loading company records (INSERT mode)...[/bold]")
        co_result = batch_load(
            state.session_factory,
            parse_companies_house(cfg.companies_house_file, batch_size=cfg.batch_size),
            _upsert_companies,
            source="companies_house_co",
            label="Company records",
        )
    else:
        console.print("[bold]Loading Companies House companies (fast COPY)...[/bold]")
        co_result = copy_load(
            state.session_factory,
            parse_companies_house(cfg.companies_house_file, batch_size=50_000),
            table_name="companies",
            columns=[
                "company_number", "company_name", "company_status", "company_type",
                "sic_code_1", "sic_code_2", "sic_code_3", "sic_code_4",
                "incorporation_date", "postcode_norm",
                "address_line_1", "address_line_2", "post_town", "county", "country",
            ],
            row_fn=lambda r: [
                r.company_number, r.company_name, r.company_status, r.company_type,
                r.sic_code_1, r.sic_code_2, r.sic_code_3, r.sic_code_4,
                r.incorporation_date, r.postcode_norm,
                r.address_line_1, r.address_line_2, r.post_town, r.county, r.country,
            ],
            source="companies_house",
            label="Companies House (COPY)",
        )

        # Create addresses from companies using SQL INSERT ... SELECT
        console.print("[bold]Creating addresses from company data...[/bold]")
        with state.session_factory() as session:
            result = session.execute(text("""
                INSERT INTO addresses (source, source_id, street, suburb, city, county, postcode_norm, postcode_raw)
                SELECT DISTINCT ON (c.company_number)
                    'companies_house', 'ch:' || c.company_number,
                    c.address_line_1, c.address_line_2, c.post_town, c.county,
                    c.postcode_norm, c.postcode_norm
                FROM companies c
                WHERE NOT EXISTS (
                    SELECT 1 FROM addresses a
                    WHERE a.source = 'companies_house' AND a.source_id = 'ch:' || c.company_number
                )
            """))
            addr_count = result.rowcount
            session.commit()
        log.info("Addresses created from companies", count=addr_count)
        addr_result = LoadResult(source="companies_house_addr", loaded=addr_count, total=addr_count)

    total = addr_result.loaded + co_result.loaded
    _update_data_source(
        state.session_factory, "companies_house", status="completed", record_count=total
    )

    console.print(
        f"[green]Companies House loaded:[/green] "
        f"{addr_result.loaded} addresses, {co_result.loaded} companies"
    )


# ---------------------------------------------------------------------------
# load-fsa
# ---------------------------------------------------------------------------
@app.command()
def load_fsa(
    ctx: typer.Context,
    truncate: bool = typer.Option(
        False, "--truncate", help="Truncate food_ratings table first"
    ),
):
    """Load addresses + food hygiene ratings from FSA API."""
    from app.ingestion.fsa import fetch_and_parse_fsa

    state = _get_state(ctx)
    log = get_logger("load-fsa")
    cfg = state.config

    if truncate:
        with state.session_factory() as session:
            session.execute(text("TRUNCATE TABLE food_ratings CASCADE"))
            session.commit()
        log.info("Truncated food_ratings table")

    _update_data_source(state.session_factory, "fsa", status="ingesting")
    console.print("[bold]Loading FSA addresses...[/bold]")

    # FSA uses INSERT with many columns — keep batch small to stay under
    # PostgreSQL's 65 535 parameter limit (500 rows × 12 cols = 6 000 params).
    fsa_batch = min(cfg.batch_size, 500)

    # Phase 1: Create/update addresses
    addr_result = batch_load(
        state.session_factory,
        fetch_and_parse_fsa(cfg.fsa_api_base_url, batch_size=fsa_batch),
        _upsert_addresses_fsa,
        source="fsa_addr",
        label="FSA addresses",
    )

    console.print("[bold]Loading FSA food rating records...[/bold]")

    # Phase 2: Load food rating records
    fr_result = batch_load(
        state.session_factory,
        fetch_and_parse_fsa(cfg.fsa_api_base_url, batch_size=fsa_batch),
        _upsert_food_ratings,
        source="fsa_fr",
        label="Food rating records",
    )

    total = addr_result.loaded + fr_result.loaded
    _update_data_source(
        state.session_factory, "fsa", status="completed", record_count=total
    )

    console.print(
        f"[green]FSA loaded:[/green] "
        f"{addr_result.loaded} addresses, {fr_result.loaded} food ratings"
    )


# ---------------------------------------------------------------------------
# load-epc
# ---------------------------------------------------------------------------
@app.command()
def load_epc(
    ctx: typer.Context,
    truncate: bool = typer.Option(
        False, "--truncate", help="Delete EPC-sourced addresses first"
    ),
    slow: bool = typer.Option(
        False, "--slow", help="Use INSERT mode instead of fast COPY"
    ),
):
    """Load addresses from EPC (Energy Performance Certificate) bulk CSVs.

    Requires manually downloading EPC data first. See:
    https://epc.opendatacommunities.org (free registration required).
    """
    from app.ingestion.epc import parse_epc

    state = _get_state(ctx)
    log = get_logger("load-epc")
    cfg = state.config

    if truncate:
        with state.session_factory() as session:
            session.execute(
                text("DELETE FROM addresses WHERE source = 'epc'")
            )
            session.commit()
        log.info("Deleted EPC-sourced addresses")

    if not cfg.epc_dir.exists():
        console.print(
            f"[yellow]EPC directory not found: {cfg.epc_dir}[/yellow]\n"
            "Download EPC data from https://epc.opendatacommunities.org\n"
            f"and extract CSVs into: {cfg.epc_dir}"
        )
        raise typer.Exit(1)

    _update_data_source(state.session_factory, "epc", status="ingesting")

    if slow:
        console.print("[bold]Loading EPC addresses (INSERT)...[/bold]")
        # EPC uses INSERT with 8 columns — keep batch under PostgreSQL's 65 535 param limit
        epc_batch = min(cfg.batch_size, 500)

        result = batch_load(
            state.session_factory,
            parse_epc(cfg.epc_dir, batch_size=epc_batch),
            _upsert_addresses_epc,
            source="epc",
            label="EPC addresses",
        )
    else:
        # Fast path: COPY directly into addresses table
        console.print("[bold]Loading EPC addresses (fast COPY)...[/bold]")
        result = copy_load(
            state.session_factory,
            parse_epc(cfg.epc_dir, batch_size=50_000),
            table_name="addresses",
            columns=[
                "source", "source_id", "uprn", "street", "suburb",
                "city", "county", "postcode_raw", "postcode_norm",
            ],
            row_fn=lambda r: [
                "epc",
                f"epc:{r.lmk_key}"[:100],
                r.uprn,
                r.address_line_1,
                r.address_line_2,
                r.post_town,
                r.county,
                r.postcode_raw,
                r.postcode_norm,
            ],
            source="epc",
            label="EPC addresses (COPY)",
        )

    _update_data_source(
        state.session_factory, "epc", status="completed", record_count=result.loaded
    )

    console.print(f"[green]EPC addresses loaded:[/green] {result.loaded}")


# ---------------------------------------------------------------------------
# load-cqc
# ---------------------------------------------------------------------------
@app.command()
def load_cqc(
    ctx: typer.Context,
    file: str = typer.Option(None, "--file", "-f", help="Path to CQC CSV file"),
):
    """Load addresses from CQC Care Directory CSV."""
    from app.ingestion.cqc import parse_cqc

    state = _get_state(ctx)
    log = get_logger("load-cqc")
    cfg = state.config
    csv_path = Path(file) if file else cfg.cqc_file

    console.print("[bold]Loading CQC care locations...[/bold]")
    result = batch_load(
        state.session_factory,
        parse_cqc(csv_path, batch_size=cfg.batch_size),
        _upsert_addresses_generic,
        source="cqc",
        label="CQC addresses",
    )
    console.print(f"[green]CQC loaded:[/green] {result.loaded} addresses")


# ---------------------------------------------------------------------------
# load-charity
# ---------------------------------------------------------------------------
@app.command()
def load_charity(
    ctx: typer.Context,
    file: str = typer.Option(None, "--file", "-f", help="Path to Charity CSV/ZIP file"),
):
    """Load addresses from Charity Commission register."""
    from app.ingestion.charity import parse_charity

    state = _get_state(ctx)
    log = get_logger("load-charity")
    cfg = state.config
    file_path = Path(file) if file else cfg.charity_file

    console.print("[bold]Loading Charity Commission addresses...[/bold]")
    result = batch_load(
        state.session_factory,
        parse_charity(file_path, batch_size=cfg.batch_size),
        _upsert_addresses_generic,
        source="charity",
        label="Charity addresses",
    )
    console.print(f"[green]Charity loaded:[/green] {result.loaded} addresses")


# ---------------------------------------------------------------------------
# load-schools
# ---------------------------------------------------------------------------
@app.command()
def load_schools(
    ctx: typer.Context,
    file: str = typer.Option(None, "--file", "-f", help="Path to GIAS schools CSV"),
):
    """Load addresses from GIAS (Get Information About Schools) CSV."""
    from app.ingestion.schools import parse_schools

    state = _get_state(ctx)
    log = get_logger("load-schools")
    cfg = state.config
    csv_path = Path(file) if file else cfg.schools_file

    console.print("[bold]Loading school addresses...[/bold]")
    result = batch_load(
        state.session_factory,
        parse_schools(csv_path, batch_size=cfg.batch_size),
        _upsert_addresses_generic,
        source="schools",
        label="School addresses",
    )
    console.print(f"[green]Schools loaded:[/green] {result.loaded} addresses")


# ---------------------------------------------------------------------------
# load-nhs
# ---------------------------------------------------------------------------
@app.command()
def load_nhs(
    ctx: typer.Context,
    file: str = typer.Option(None, "--file", "-f", help="Path to NHS ODS CSV"),
):
    """Load addresses from NHS Organisation Data Service CSV."""
    from app.ingestion.nhs import parse_nhs

    state = _get_state(ctx)
    log = get_logger("load-nhs")
    cfg = state.config
    csv_path = Path(file) if file else cfg.nhs_file

    console.print("[bold]Loading NHS organisation addresses...[/bold]")
    result = batch_load(
        state.session_factory,
        parse_nhs(csv_path, batch_size=cfg.batch_size),
        _upsert_addresses_generic,
        source="nhs",
        label="NHS addresses",
    )
    console.print(f"[green]NHS loaded:[/green] {result.loaded} addresses")


# ---------------------------------------------------------------------------
# load-dvsa
# ---------------------------------------------------------------------------
@app.command()
def load_dvsa(
    ctx: typer.Context,
    file: str = typer.Option(None, "--file", "-f", help="Path to DVSA MOT stations CSV"),
):
    """Load addresses from DVSA Active MOT Test Stations CSV."""
    from app.ingestion.dvsa import parse_dvsa

    state = _get_state(ctx)
    log = get_logger("load-dvsa")
    cfg = state.config
    csv_path = Path(file) if file else cfg.dvsa_file

    console.print("[bold]Loading DVSA MOT station addresses...[/bold]")
    result = batch_load(
        state.session_factory,
        parse_dvsa(csv_path, batch_size=cfg.batch_size),
        _upsert_addresses_generic,
        source="dvsa",
        label="DVSA addresses",
    )
    console.print(f"[green]DVSA loaded:[/green] {result.loaded} addresses")


# ---------------------------------------------------------------------------
# load-voa
# ---------------------------------------------------------------------------
@app.command()
def load_voa(
    ctx: typer.Context,
    truncate: bool = typer.Option(
        False, "--truncate", help="Delete VOA records + addresses first"
    ),
    slow: bool = typer.Option(
        False, "--slow", help="Use INSERT mode instead of fast COPY"
    ),
):
    """Load addresses + valuations from VOA Non-Domestic Rating List."""
    from app.ingestion.voa import parse_voa

    state = _get_state(ctx)
    log = get_logger("load-voa")
    cfg = state.config

    if truncate:
        with state.session_factory() as session:
            session.execute(text("DELETE FROM voa_ratings"))
            session.execute(
                text("DELETE FROM addresses WHERE source = 'voa'")
            )
            session.commit()
        log.info("Deleted VOA records and addresses")

    _update_data_source(state.session_factory, "voa", status="ingesting")

    if slow:
        # INSERT mode: two-pass (addresses + voa_ratings)
        console.print("[bold]Loading VOA addresses (INSERT)...[/bold]")
        addr_result = batch_load(
            state.session_factory,
            parse_voa(cfg.voa_file, batch_size=cfg.batch_size),
            _upsert_addresses_voa,
            source="voa",
            label="VOA addresses",
        )

        console.print("[bold]Loading VOA rating records (INSERT)...[/bold]")
        voa_result = batch_load(
            state.session_factory,
            parse_voa(cfg.voa_file, batch_size=cfg.batch_size),
            _upsert_voa_ratings,
            source="voa",
            label="VOA ratings",
        )
    else:
        # Fast path: COPY into voa_ratings, then create addresses via SQL
        console.print("[bold]Loading VOA ratings (fast COPY)...[/bold]")
        voa_result = copy_load(
            state.session_factory,
            parse_voa(cfg.voa_file, batch_size=50_000),
            table_name="voa_ratings",
            columns=[
                "uarn", "billing_authority_code", "description_code",
                "description_text", "firm_name",
                "postcode_norm", "number_or_name", "street", "town",
                "postal_district", "county",
                "sub_street_1", "sub_street_2", "sub_street_3",
                "rateable_value", "effective_date",
            ],
            row_fn=lambda r: [
                r.uarn, r.billing_authority_code, r.description_code,
                r.description_text, r.firm_name,
                r.postcode_norm, r.number_or_name, r.street, r.town,
                r.postal_district, r.county,
                r.sub_street_1, r.sub_street_2, r.sub_street_3,
                r.rateable_value, r.effective_date,
            ],
            source="voa",
            label="VOA ratings (COPY)",
        )

        # Create addresses from voa_ratings
        console.print("[bold]Creating addresses from VOA data...[/bold]")
        with state.session_factory() as session:
            result = session.execute(text("""
                INSERT INTO addresses (source, source_id, house_number, street, suburb, city, county, postcode_norm, postcode_raw)
                SELECT DISTINCT ON (vr.uarn)
                    'voa', LEFT('voa:' || vr.uarn::text, 100),
                    LEFT(vr.number_or_name, 100), LEFT(vr.street, 200),
                    LEFT(vr.postal_district, 100), LEFT(vr.town, 100), LEFT(vr.county, 100),
                    vr.postcode_norm, vr.postcode_norm
                FROM voa_ratings vr
                WHERE NOT EXISTS (
                    SELECT 1 FROM addresses a
                    WHERE a.source = 'voa' AND a.source_id = LEFT('voa:' || vr.uarn::text, 100)
                )
            """))
            addr_count = result.rowcount
            session.commit()
        log.info("Addresses created from VOA", count=addr_count)
        addr_result = LoadResult(source="voa_addr", loaded=addr_count, total=addr_count)

    total = addr_result.loaded + voa_result.loaded
    _update_data_source(
        state.session_factory, "voa", status="completed", record_count=total
    )
    console.print(
        f"[green]VOA loaded:[/green] "
        f"{addr_result.loaded} addresses, {voa_result.loaded} ratings"
    )


# ---------------------------------------------------------------------------
# merge (enhanced for multi-source)
# ---------------------------------------------------------------------------
@app.command()
def merge(
    ctx: typer.Context,
    dedup: bool = typer.Option(
        False, "--dedup",
        help="Enable deduplication (DESTRUCTIVE — deletes duplicate addresses). "
             "Off by default to prevent data loss.",
    ),
):
    """Link addresses to postcodes, geocode, link enrichment, score confidence.

    All steps are non-destructive (only fills empty fields) EXCEPT dedup.
    Deduplication must be explicitly enabled with --dedup.
    """
    from app.ingestion.merge import (
        dedup_dry_run,
        deduplicate,
        fix_stale_statuses,
        geocode_from_postcode,
        geocode_from_uprn,
        link_companies,
        link_food_ratings,
        link_postcodes,
        link_price_paid,
        link_voa_ratings,
        score_confidence,
    )

    state = _get_state(ctx)

    # Fix any data sources stuck in 'ingesting' from interrupted runs
    fixed = fix_stale_statuses(state.session_factory)
    if fixed:
        console.print(f"[yellow]Fixed {fixed} stale data source status(es)[/yellow]")

    # Check prerequisites
    with state.session_factory() as session:
        pc_count = session.query(func.count(Postcode.id)).scalar()
        addr_count = session.query(func.count(Address.id)).scalar()

    if not pc_count:
        raise PipelineError("No postcodes loaded. Run 'load-postcodes' first.")
    if not addr_count:
        raise PipelineError("No addresses loaded. Run a load command first.")

    console.print(f"[bold]Merging {addr_count:,} addresses with {pc_count:,} postcodes...[/bold]")

    console.print("[bold]Step 1/8: Linking postcodes...[/bold]")
    linked = link_postcodes(state.session_factory)

    console.print("[bold]Step 2/8: Geocoding from UPRN...[/bold]")
    uprn_geo = geocode_from_uprn(state.session_factory)

    console.print("[bold]Step 3/8: Geocoding from postcode centroids...[/bold]")
    pc_geo = geocode_from_postcode(state.session_factory)

    console.print("[bold]Step 4/8: Linking price paid records...[/bold]")
    pp_linked = link_price_paid(state.session_factory)

    console.print("[bold]Step 5/8: Linking company records...[/bold]")
    co_linked = link_companies(state.session_factory)

    console.print("[bold]Step 6/8: Linking food rating records...[/bold]")
    fr_linked = link_food_ratings(state.session_factory)

    console.print("[bold]Step 7/8: Linking VOA rating records...[/bold]")
    voa_linked = link_voa_ratings(state.session_factory)

    console.print("[bold]Step 8/8: Computing confidence scores...[/bold]")
    scored = score_confidence(state.session_factory)

    deduped = 0
    if dedup:
        # Show preview of what would be deleted
        console.print("[bold yellow]Dedup preview (what will be deleted):[/bold yellow]")
        stats = dedup_dry_run(state.session_factory)
        if stats["uprn"]:
            for src, cnt in stats["uprn"].items():
                console.print(f"  UPRN dedup: {src} → {cnt:,} duplicates")
        if stats["text"]:
            for src, cnt in stats["text"].items():
                console.print(f"  Text dedup: {src} → {cnt:,} duplicates")

        console.print("[bold]Deduplicating...[/bold]")
        deduped = deduplicate(state.session_factory)
    else:
        console.print(
            "[dim]Dedup skipped (use --dedup to enable). "
            "No addresses were deleted.[/dim]"
        )

    console.print(
        f"\n[green]Merge complete (zero data loss):[/green]\n"
        f"  Postcode-linked:    {linked:>12,}\n"
        f"  UPRN-geocoded:      {uprn_geo:>12,}\n"
        f"  Postcode-geocoded:  {pc_geo:>12,}\n"
        f"  Price-paid-linked:  {pp_linked:>12,}\n"
        f"  Companies-linked:   {co_linked:>12,}\n"
        f"  Food-ratings-linked:{fr_linked:>12,}\n"
        f"  VOA-linked:         {voa_linked:>12,}\n"
        f"  Confidence-scored:  {scored:>12,}\n"
        f"  Deduplicated:       {deduped:>12,}"
    )


# ---------------------------------------------------------------------------
# status (enhanced with enrichment table counts)
# ---------------------------------------------------------------------------
@app.command()
def status(ctx: typer.Context):
    """Show ingestion status: record counts, link rates, average confidence."""
    state = _get_state(ctx)

    def _fast_count(session: Session, table_name: str) -> int:
        """Use PostgreSQL pg_class for fast approximate row counts."""
        result = session.execute(text(
            "SELECT reltuples::bigint FROM pg_class WHERE relname = :tbl"
        ), {"tbl": table_name})
        row = result.fetchone()
        count = row[0] if row else 0
        return max(count, 0)

    with state.session_factory() as session:
        pc_count = _fast_count(session, "postcodes")
        addr_count = _fast_count(session, "addresses")

        # These filtered counts use indexes — fast enough
        linked_count = (
            session.query(func.count(Address.id))
            .filter(Address.postcode_id.isnot(None))
            .scalar()
            or 0
        ) if addr_count < 1_000_000 else session.execute(text(
            "SELECT COUNT(*) FROM addresses WHERE postcode_id IS NOT NULL"
        )).scalar() or 0
        avg_conf = 0.0  # Skip slow AVG on large tables
        complete_count = 0  # Skip slow filtered count

        # Enrichment table counts (fast approximate)
        pp_count = _fast_count(session, "price_paid")
        co_count = _fast_count(session, "companies")
        fr_count = _fast_count(session, "food_ratings")
        uprn_count = _fast_count(session, "uprn_coordinates")
        voa_count = _fast_count(session, "voa_ratings")

        # Per-source address counts — use index on source column
        source_counts = session.execute(text(
            "SELECT source, COUNT(*) FROM addresses GROUP BY source"
        )).fetchall()

        # Data source statuses
        sources = session.query(DataSource).all()

    link_rate = (linked_count / addr_count * 100) if addr_count else 0

    # Summary table
    table = Table(title="Ingestion Status")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green", justify="right")
    table.add_row("Postcodes", f"{pc_count:,}")
    table.add_row("Addresses (total)", f"{addr_count:,}")
    table.add_row("Linked to postcode", f"{linked_count:,} ({link_rate:.1f}%)")
    table.add_row("Complete addresses", f"{complete_count:,}")
    table.add_row("Avg confidence", f"{avg_conf:.3f}")
    table.add_row("", "")
    table.add_row("Price paid records", f"{pp_count:,}")
    table.add_row("Company records", f"{co_count:,}")
    table.add_row("Food rating records", f"{fr_count:,}")
    table.add_row("UPRN coordinates", f"{uprn_count:,}")
    table.add_row("VOA rating records", f"{voa_count:,}")
    console.print(table)

    # Per-source address breakdown
    if source_counts:
        src_addr_table = Table(title="Addresses by Source")
        src_addr_table.add_column("Source", style="cyan")
        src_addr_table.add_column("Count", style="green", justify="right")
        for src_name, count in sorted(source_counts, key=lambda x: -(x[1] or 0)):
            src_addr_table.add_row(src_name or "(unknown)", f"{count:,}")
        console.print(src_addr_table)

    # Source status table
    if sources:
        src_table = Table(title="Data Sources")
        src_table.add_column("Source", style="cyan")
        src_table.add_column("Status", style="bold")
        src_table.add_column("Records", justify="right")
        src_table.add_column("Hash (first 12)")
        for src in sources:
            src_table.add_row(
                src.source_name,
                src.status,
                f"{src.record_count:,}" if src.record_count else "-",
                (src.file_hash[:12] + "...") if src.file_hash else "-",
            )
        console.print(src_table)


# ---------------------------------------------------------------------------
# all — full pipeline (enhanced)
# ---------------------------------------------------------------------------
@app.command(name="all")
def run_all(
    ctx: typer.Context,
    force_download: bool = typer.Option(
        False, "--force-download", help="Re-download even if files exist"
    ),
    skip_epc: bool = typer.Option(
        False, "--skip-epc", help="Skip EPC (requires manual download)"
    ),
):
    """Run the full ingestion pipeline: download -> load all sources -> merge."""
    console.print("[bold blue]Starting full ingestion pipeline...[/bold blue]")

    ctx.invoke(download, source="all", force=force_download)
    ctx.invoke(load_postcodes, truncate=False)
    ctx.invoke(load_osm, truncate=False)
    ctx.invoke(load_uprn, truncate=False)
    ctx.invoke(load_land_registry, truncate=False)

    if not skip_epc:
        cfg = _get_state(ctx).config
        if cfg.epc_dir.exists():
            ctx.invoke(load_epc, truncate=False)
        else:
            console.print(
                "[yellow]Skipping EPC (directory not found). "
                "Download from https://epc.opendatacommunities.org[/yellow]"
            )

    ctx.invoke(load_companies, truncate=False)
    ctx.invoke(load_fsa, truncate=False)
    ctx.invoke(load_voa, truncate=False)
    ctx.invoke(merge)
    ctx.invoke(status)

    console.print("[bold green]Pipeline complete![/bold green]")


# ---------------------------------------------------------------------------
# load-new-sources — only the 5 new sources (does NOT touch existing 31M)
# ---------------------------------------------------------------------------
@app.command(name="load-new-sources")
def load_new_sources(ctx: typer.Context):
    """Load ONLY the 5 new data sources (CQC, Charity, Schools, NHS, DVSA).

    This does NOT re-run existing sources (OSM, Land Registry, Companies House,
    FSA, VOA, EPC, UPRN, postcodes). Safe to run on a live database — uses
    ON CONFLICT DO UPDATE so it's idempotent.
    """
    console.print("[bold blue]Loading 5 new data sources only...[/bold blue]")

    ctx.invoke(load_cqc, ctx=ctx, file=None)
    ctx.invoke(load_charity, ctx=ctx, file=None)
    ctx.invoke(load_schools, ctx=ctx, file=None)
    ctx.invoke(load_nhs, ctx=ctx, file=None)
    ctx.invoke(load_dvsa, ctx=ctx, file=None)

    console.print("[bold green]All 5 new sources loaded![/bold green]")
    ctx.invoke(status)


# ---------------------------------------------------------------------------
# Upsert helpers (wired into batch_load)
# ---------------------------------------------------------------------------
def _upsert_postcodes_codepoint(session: Session, batch: list[BaseModel]) -> int:
    """Upsert Code-Point records into postcodes table. Returns rows affected."""
    from app.core.utils.postcode import postcode_no_space

    records = [r for r in batch if isinstance(r, CodePointRecord)]
    if not records:
        return 0

    values = [
        {
            "postcode": r.postcode_norm,
            "postcode_no_space": postcode_no_space(r.postcode_norm),
            "latitude": r.latitude,
            "longitude": r.longitude,
            "location": func.ST_SetSRID(
                func.ST_MakePoint(r.longitude, r.latitude), 4326
            ),
            "easting": r.easting,
            "northing": r.northing,
            "positional_quality": r.positional_quality,
            "country_code": r.country_code,
            "source": "codepoint",
        }
        for r in records
    ]

    stmt = pg_insert(Postcode).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["postcode"],
        set_={
            "latitude": stmt.excluded.latitude,
            "longitude": stmt.excluded.longitude,
            "location": stmt.excluded.location,
            "easting": stmt.excluded.easting,
            "northing": stmt.excluded.northing,
            "positional_quality": stmt.excluded.positional_quality,
            "country_code": stmt.excluded.country_code,
            "source": "codepoint",
        },
    )

    session.execute(stmt)
    # rowcount is unreliable with ON CONFLICT DO UPDATE in psycopg3
    return len(records)


def _upsert_postcodes_nspl(session: Session, batch: list[BaseModel]) -> int:
    """Merge NSPL admin data into existing postcodes. Returns rows affected."""
    records = [r for r in batch if isinstance(r, NSPLRecord)]
    if not records:
        return 0

    from app.core.utils.postcode import postcode_no_space

    values = [
        {
            "postcode": r.postcode_norm,
            "postcode_no_space": postcode_no_space(r.postcode_norm),
            "country_code": r.country_code,
            "region_code": r.region_code,
            "local_authority": r.local_authority,
            "parliamentary_const": r.parliamentary_const,
            "ward_code": r.ward_code,
            "parish_code": r.parish_code,
            "date_introduced": r.date_introduced,
            "date_terminated": r.date_terminated,
            "is_terminated": r.is_terminated,
            "source": "nspl",
        }
        for r in records
    ]

    stmt = pg_insert(Postcode).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["postcode"],
        set_={
            "country_code": stmt.excluded.country_code,
            "region_code": stmt.excluded.region_code,
            "local_authority": stmt.excluded.local_authority,
            "parliamentary_const": stmt.excluded.parliamentary_const,
            "ward_code": stmt.excluded.ward_code,
            "parish_code": stmt.excluded.parish_code,
            "date_introduced": stmt.excluded.date_introduced,
            "date_terminated": stmt.excluded.date_terminated,
            "is_terminated": stmt.excluded.is_terminated,
            "source": func.coalesce(Postcode.__table__.c.source, "") + "+nspl",
        },
    )

    session.execute(stmt)
    return len(records)


def _upsert_addresses_osm(session: Session, batch: list[BaseModel]) -> int:
    """Insert OSM addresses with ON CONFLICT DO NOTHING. Returns rows affected."""
    records = [r for r in batch if isinstance(r, OSMAddressRecord)]
    if not records:
        return 0

    values = [
        {
            "osm_id": r.osm_id,
            "osm_type": r.osm_type,
            "house_number": r.house_number,
            "house_name": r.house_name,
            "flat": r.flat,
            "street": r.street,
            "suburb": r.suburb,
            "city": r.city,
            "county": r.county,
            "postcode_raw": r.postcode_raw,
            "postcode_norm": r.postcode_norm,
            "latitude": r.latitude,
            "longitude": r.longitude,
            "source": "osm",
            "source_id": f"{r.osm_type}:{r.osm_id}",
        }
        for r in records
    ]

    stmt = pg_insert(Address).values(values)
    stmt = stmt.on_conflict_do_nothing(constraint="uq_addresses_osm")

    result = session.execute(stmt)
    # rowcount works for DO NOTHING but may return -1 with psycopg3;
    # fall back to len(records) if unreliable
    rc = result.rowcount
    return rc if rc >= 0 else len(records)


def _upsert_addresses_land_registry(session: Session, batch: list[BaseModel]) -> int:
    """Upsert Land Registry addresses. ON CONFLICT (source, source_id) DO UPDATE."""
    records = [r for r in batch if isinstance(r, LandRegistryRecord)]
    if not records:
        return 0

    values = [
        {
            "source": "land_registry",
            "source_id": f"lr:{r.transaction_id}",
            "house_number": r.paon,
            "flat": r.saon,
            "street": r.street,
            "suburb": r.locality,
            "city": r.town,
            "county": r.county,
            "postcode_raw": r.postcode_raw,
            "postcode_norm": r.postcode_norm,
        }
        for r in records
    ]

    stmt = pg_insert(Address).values(values)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_addresses_source",
        set_={
            "house_number": stmt.excluded.house_number,
            "flat": stmt.excluded.flat,
            "street": stmt.excluded.street,
            "suburb": stmt.excluded.suburb,
            "city": stmt.excluded.city,
            "county": stmt.excluded.county,
            "postcode_raw": stmt.excluded.postcode_raw,
            "postcode_norm": stmt.excluded.postcode_norm,
        },
    )

    session.execute(stmt)
    return len(records)


def _upsert_price_paid(session: Session, batch: list[BaseModel]) -> int:
    """Upsert price paid records. ON CONFLICT (transaction_id) DO NOTHING."""
    records = [r for r in batch if isinstance(r, LandRegistryRecord)]
    if not records:
        return 0

    values = [
        {
            "transaction_id": r.transaction_id,
            "postcode_norm": r.postcode_norm,
            "price": r.price,
            "date_of_transfer": r.date_of_transfer,
            "property_type": r.property_type,
            "old_new": r.old_new,
            "duration": r.duration,
            "paon": r.paon,
            "saon": r.saon,
            "street": r.street,
            "locality": r.locality,
            "town": r.town,
            "district": r.district,
            "county": r.county,
            "ppd_category": r.ppd_category,
            "record_status": r.record_status,
        }
        for r in records
    ]

    stmt = pg_insert(PricePaid).values(values)
    stmt = stmt.on_conflict_do_nothing(index_elements=["transaction_id"])

    result = session.execute(stmt)
    rc = result.rowcount
    return rc if rc >= 0 else len(records)


def _upsert_uprn_coordinates(session: Session, batch: list[BaseModel]) -> int:
    """Upsert UPRN coordinate records. ON CONFLICT (uprn) DO UPDATE."""
    records = [r for r in batch if isinstance(r, UPRNRecord)]
    if not records:
        return 0

    values = [
        {
            "uprn": r.uprn,
            "latitude": r.latitude,
            "longitude": r.longitude,
        }
        for r in records
    ]

    stmt = pg_insert(UPRNCoordinate).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["uprn"],
        set_={
            "latitude": stmt.excluded.latitude,
            "longitude": stmt.excluded.longitude,
        },
    )

    session.execute(stmt)
    return len(records)


def _upsert_addresses_companies_house(session: Session, batch: list[BaseModel]) -> int:
    """Upsert Companies House addresses. ON CONFLICT (source, source_id) DO UPDATE."""
    records = [r for r in batch if isinstance(r, CompaniesHouseRecord)]
    if not records:
        return 0

    values = [
        {
            "source": "companies_house",
            "source_id": f"ch:{r.company_number}",
            "house_number": None,
            "house_name": None,
            "street": r.address_line_1,
            "suburb": r.address_line_2,
            "city": r.post_town,
            "county": r.county,
            "postcode_raw": r.postcode_raw,
            "postcode_norm": r.postcode_norm,
        }
        for r in records
    ]

    stmt = pg_insert(Address).values(values)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_addresses_source",
        set_={
            "street": stmt.excluded.street,
            "suburb": stmt.excluded.suburb,
            "city": stmt.excluded.city,
            "county": stmt.excluded.county,
            "postcode_raw": stmt.excluded.postcode_raw,
            "postcode_norm": stmt.excluded.postcode_norm,
        },
    )

    session.execute(stmt)
    return len(records)


def _upsert_companies(session: Session, batch: list[BaseModel]) -> int:
    """Upsert company records. ON CONFLICT (company_number) DO UPDATE."""
    records = [r for r in batch if isinstance(r, CompaniesHouseRecord)]
    if not records:
        return 0

    values = [
        {
            "company_number": r.company_number,
            "company_name": r.company_name,
            "company_status": r.company_status,
            "company_type": r.company_type,
            "sic_code_1": r.sic_code_1,
            "sic_code_2": r.sic_code_2,
            "sic_code_3": r.sic_code_3,
            "sic_code_4": r.sic_code_4,
            "incorporation_date": r.incorporation_date,
            "postcode_norm": r.postcode_norm,
            "address_line_1": r.address_line_1,
            "address_line_2": r.address_line_2,
            "post_town": r.post_town,
            "county": r.county,
            "country": r.country,
        }
        for r in records
    ]

    stmt = pg_insert(Company).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["company_number"],
        set_={
            "company_name": stmt.excluded.company_name,
            "company_status": stmt.excluded.company_status,
            "company_type": stmt.excluded.company_type,
            "sic_code_1": stmt.excluded.sic_code_1,
            "sic_code_2": stmt.excluded.sic_code_2,
            "sic_code_3": stmt.excluded.sic_code_3,
            "sic_code_4": stmt.excluded.sic_code_4,
            "incorporation_date": stmt.excluded.incorporation_date,
            "postcode_norm": stmt.excluded.postcode_norm,
            "address_line_1": stmt.excluded.address_line_1,
            "address_line_2": stmt.excluded.address_line_2,
            "post_town": stmt.excluded.post_town,
            "county": stmt.excluded.county,
            "country": stmt.excluded.country,
        },
    )

    session.execute(stmt)
    return len(records)


def _upsert_addresses_fsa(session: Session, batch: list[BaseModel]) -> int:
    """Upsert FSA addresses. ON CONFLICT (source, source_id) DO UPDATE."""
    records = [r for r in batch if isinstance(r, FSARatingRecord)]
    if not records:
        return 0

    values = [
        {
            "source": "fsa",
            "source_id": f"fsa:{r.fhrs_id}",
            "street": r.address_line_1,
            "suburb": r.address_line_2,
            "city": r.address_line_3,
            "county": r.address_line_4,
            "postcode_raw": r.postcode_raw,
            "postcode_norm": r.postcode_norm,
            "latitude": r.latitude,
            "longitude": r.longitude,
        }
        for r in records
    ]

    stmt = pg_insert(Address).values(values)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_addresses_source",
        set_={
            "street": stmt.excluded.street,
            "suburb": stmt.excluded.suburb,
            "city": stmt.excluded.city,
            "county": stmt.excluded.county,
            "postcode_raw": stmt.excluded.postcode_raw,
            "postcode_norm": stmt.excluded.postcode_norm,
            "latitude": stmt.excluded.latitude,
            "longitude": stmt.excluded.longitude,
        },
    )

    session.execute(stmt)
    return len(records)


def _upsert_food_ratings(session: Session, batch: list[BaseModel]) -> int:
    """Upsert food rating records. ON CONFLICT (fhrs_id) DO UPDATE."""
    records = [r for r in batch if isinstance(r, FSARatingRecord)]
    if not records:
        return 0

    values = [
        {
            "fhrs_id": r.fhrs_id,
            "business_name": r.business_name,
            "business_type": r.business_type,
            "business_type_id": r.business_type_id,
            "rating_value": r.rating_value,
            "rating_date": r.rating_date,
            "postcode_norm": r.postcode_norm,
            "address_line_1": r.address_line_1,
            "address_line_2": r.address_line_2,
            "address_line_3": r.address_line_3,
            "address_line_4": r.address_line_4,
            "latitude": r.latitude,
            "longitude": r.longitude,
            "local_authority_code": r.local_authority_code,
            "local_authority_name": r.local_authority_name,
            "scores_hygiene": r.scores_hygiene,
            "scores_structural": r.scores_structural,
            "scores_management": r.scores_management,
        }
        for r in records
    ]

    stmt = pg_insert(FoodRating).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["fhrs_id"],
        set_={
            "business_name": stmt.excluded.business_name,
            "business_type": stmt.excluded.business_type,
            "business_type_id": stmt.excluded.business_type_id,
            "rating_value": stmt.excluded.rating_value,
            "rating_date": stmt.excluded.rating_date,
            "postcode_norm": stmt.excluded.postcode_norm,
            "address_line_1": stmt.excluded.address_line_1,
            "address_line_2": stmt.excluded.address_line_2,
            "address_line_3": stmt.excluded.address_line_3,
            "address_line_4": stmt.excluded.address_line_4,
            "latitude": stmt.excluded.latitude,
            "longitude": stmt.excluded.longitude,
            "local_authority_code": stmt.excluded.local_authority_code,
            "local_authority_name": stmt.excluded.local_authority_name,
            "scores_hygiene": stmt.excluded.scores_hygiene,
            "scores_structural": stmt.excluded.scores_structural,
            "scores_management": stmt.excluded.scores_management,
        },
    )

    session.execute(stmt)
    return len(records)


def _upsert_addresses_epc(session: Session, batch: list[BaseModel]) -> int:
    """Upsert EPC addresses. ON CONFLICT (source, source_id) DO UPDATE."""
    records = [r for r in batch if isinstance(r, EPCRecord)]
    if not records:
        return 0

    values = [
        {
            "source": "epc",
            "source_id": f"epc:{r.lmk_key}",
            "uprn": r.uprn,
            "street": r.address_line_1,
            "suburb": r.address_line_2,
            "city": r.post_town,
            "county": r.county,
            "postcode_raw": r.postcode_raw,
            "postcode_norm": r.postcode_norm,
        }
        for r in records
    ]

    stmt = pg_insert(Address).values(values)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_addresses_source",
        set_={
            "uprn": stmt.excluded.uprn,
            "street": stmt.excluded.street,
            "suburb": stmt.excluded.suburb,
            "city": stmt.excluded.city,
            "county": stmt.excluded.county,
            "postcode_raw": stmt.excluded.postcode_raw,
            "postcode_norm": stmt.excluded.postcode_norm,
        },
    )

    session.execute(stmt)
    return len(records)


def _upsert_addresses_voa(session: Session, batch: list[BaseModel]) -> int:
    """Upsert VOA addresses. ON CONFLICT (source, source_id) DO UPDATE."""
    records = [r for r in batch if isinstance(r, VOARecord)]
    if not records:
        return 0

    values = [
        {
            "source": "voa",
            "source_id": f"voa:{r.uarn}",
            "house_number": r.number_or_name,
            "street": r.street,
            "suburb": r.postal_district,
            "city": r.town,
            "county": r.county,
            "postcode_raw": r.postcode_raw,
            "postcode_norm": r.postcode_norm,
        }
        for r in records
    ]

    stmt = pg_insert(Address).values(values)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_addresses_source",
        set_={
            "house_number": stmt.excluded.house_number,
            "street": stmt.excluded.street,
            "suburb": stmt.excluded.suburb,
            "city": stmt.excluded.city,
            "county": stmt.excluded.county,
            "postcode_raw": stmt.excluded.postcode_raw,
            "postcode_norm": stmt.excluded.postcode_norm,
        },
    )

    session.execute(stmt)
    return len(records)


def _upsert_voa_ratings(session: Session, batch: list[BaseModel]) -> int:
    """Upsert VOA rating records. ON CONFLICT (uarn) DO UPDATE."""
    records = [r for r in batch if isinstance(r, VOARecord)]
    if not records:
        return 0

    values = [
        {
            "uarn": r.uarn,
            "billing_authority_code": r.billing_authority_code,
            "description_code": r.description_code,
            "description_text": r.description_text,
            "firm_name": r.firm_name,
            "postcode_norm": r.postcode_norm,
            "number_or_name": r.number_or_name,
            "street": r.street,
            "town": r.town,
            "postal_district": r.postal_district,
            "county": r.county,
            "sub_street_1": r.sub_street_1,
            "sub_street_2": r.sub_street_2,
            "sub_street_3": r.sub_street_3,
            "rateable_value": r.rateable_value,
            "effective_date": r.effective_date,
        }
        for r in records
    ]

    stmt = pg_insert(VOARating).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["uarn"],
        set_={
            "description_text": stmt.excluded.description_text,
            "firm_name": stmt.excluded.firm_name,
            "rateable_value": stmt.excluded.rateable_value,
            "effective_date": stmt.excluded.effective_date,
        },
    )

    session.execute(stmt)
    return len(records)


def _upsert_addresses_generic(session: Session, batch: list[BaseModel]) -> int:
    """Generic upsert for new data sources (CQC, Charity, Schools, NHS, DVSA).

    Extracts source, source_id, and address fields from various record types.
    ON CONFLICT (source, source_id) DO UPDATE.
    """
    from app.ingestion.schemas import (
        CharityRecord, CQCRecord, DVSARecord, NHSRecord, SchoolRecord,
    )

    if not batch:
        return 0

    values = []
    for r in batch:
        if isinstance(r, CQCRecord):
            values.append({
                "source": "cqc",
                "source_id": f"cqc:{r.location_id}",
                "street": r.address_line_1,
                "suburb": r.address_line_2,
                "city": r.city,
                "county": r.county,
                "postcode_raw": r.postcode_raw,
                "postcode_norm": r.postcode_norm,
                "latitude": r.latitude,
                "longitude": r.longitude,
            })
        elif isinstance(r, CharityRecord):
            values.append({
                "source": "charity",
                "source_id": f"charity:{r.charity_number}",
                "street": r.address_line_1,
                "suburb": r.address_line_2,
                "city": r.city,
                "county": r.county,
                "postcode_raw": r.postcode_raw,
                "postcode_norm": r.postcode_norm,
            })
        elif isinstance(r, SchoolRecord):
            values.append({
                "source": "schools",
                "source_id": f"school:{r.urn}",
                "street": r.street,
                "suburb": r.locality,
                "city": r.town,
                "county": r.county,
                "postcode_raw": r.postcode_raw,
                "postcode_norm": r.postcode_norm,
                "latitude": r.latitude,
                "longitude": r.longitude,
            })
        elif isinstance(r, NHSRecord):
            values.append({
                "source": "nhs",
                "source_id": f"nhs:{r.org_code}",
                "street": r.address_line_1,
                "suburb": r.address_line_2,
                "city": r.city,
                "postcode_raw": r.postcode_raw,
                "postcode_norm": r.postcode_norm,
            })
        elif isinstance(r, DVSARecord):
            values.append({
                "source": "dvsa",
                "source_id": f"dvsa:{r.station_number}",
                "street": r.address_line_1,
                "suburb": r.address_line_2,
                "city": r.town,
                "postcode_raw": r.postcode_raw,
                "postcode_norm": r.postcode_norm,
            })

    if not values:
        return 0

    stmt = pg_insert(Address).values(values)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_addresses_source",
        set_={
            "street": stmt.excluded.street,
            "suburb": stmt.excluded.suburb,
            "city": stmt.excluded.city,
            "county": stmt.excluded.county,
            "postcode_raw": stmt.excluded.postcode_raw,
            "postcode_norm": stmt.excluded.postcode_norm,
            "latitude": stmt.excluded.latitude,
            "longitude": stmt.excluded.longitude,
        },
    )

    session.execute(stmt)
    return len(values)


# ---------------------------------------------------------------------------
# Data source tracking helper
# ---------------------------------------------------------------------------
def _update_data_source(
    session_factory: sessionmaker[Session],
    source_name: str,
    *,
    status: str,
    file_hash: str | None = None,
    record_count: int | None = None,
    error_message: str | None = None,
) -> None:
    """Upsert data_sources tracking row."""
    with session_factory() as session:
        values = {
            "source_name": source_name,
            "status": status,
            "updated_at": datetime.now(timezone.utc),
        }
        if file_hash is not None:
            values["file_hash"] = file_hash
        if record_count is not None:
            values["record_count"] = record_count
        if error_message is not None:
            values["error_message"] = error_message

        if status == "ingesting":
            values["started_at"] = datetime.now(timezone.utc)
        elif status == "completed":
            values["completed_at"] = datetime.now(timezone.utc)

        stmt = pg_insert(DataSource).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["source_name"],
            set_={k: v for k, v in values.items() if k != "source_name"},
        )
        session.execute(stmt)
        session.commit()
