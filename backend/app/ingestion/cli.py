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
from app.core.db.loader import LoadResult, batch_load
from app.core.db.models import Address, DataSource, Postcode
from app.core.exceptions import PipelineError
from app.core.logging import get_logger, setup_logging
from app.ingestion.schemas import CodePointRecord, NSPLRecord, OSMAddressRecord

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
    source: str = typer.Argument(None, help="Source to download: codepoint, nspl, osm, or all"),
    force: bool = typer.Option(False, "--force", "-f", help="Re-download even if file exists"),
):
    """Download data sources (concurrently by default)."""
    from app.ingestion.downloader import run_download

    state = _get_state(ctx)
    log = get_logger("download")
    cfg = state.config

    all_sources = {
        "codepoint": (cfg.codepoint_download_url, cfg.codepoint_file),
        "nspl": (cfg.nspl_download_url, cfg.nspl_file),
        "osm": (cfg.osm_download_url, cfg.osm_file),
    }

    if source and source != "all":
        if source not in all_sources:
            console.print(f"[red]Unknown source: {source}. Choose: codepoint, nspl, osm[/red]")
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
# merge
# ---------------------------------------------------------------------------
@app.command()
def merge(ctx: typer.Context):
    """Link addresses to postcodes, compute confidence scores, deduplicate."""
    from app.ingestion.merge import (
        deduplicate,
        link_postcodes,
        score_confidence,
    )

    state = _get_state(ctx)

    # Check prerequisites
    with state.session_factory() as session:
        pc_count = session.query(func.count(Postcode.id)).scalar()
        addr_count = session.query(func.count(Address.id)).scalar()

    if not pc_count:
        raise PipelineError("No postcodes loaded. Run 'load-postcodes' first.")
    if not addr_count:
        raise PipelineError("No addresses loaded. Run 'load-osm' first.")

    console.print("[bold]Linking postcodes...[/bold]")
    linked = link_postcodes(state.session_factory)

    console.print("[bold]Computing confidence scores...[/bold]")
    scored = score_confidence(state.session_factory)

    console.print("[bold]Deduplicating...[/bold]")
    deduped = deduplicate(state.session_factory)

    console.print(
        f"[green]Merge complete:[/green] "
        f"{linked} linked, {scored} scored, {deduped} deduplicated"
    )


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------
@app.command()
def status(ctx: typer.Context):
    """Show ingestion status: record counts, link rates, average confidence."""
    state = _get_state(ctx)

    with state.session_factory() as session:
        pc_count = session.query(func.count(Postcode.id)).scalar() or 0
        addr_count = session.query(func.count(Address.id)).scalar() or 0
        linked_count = (
            session.query(func.count(Address.id))
            .filter(Address.postcode_id.isnot(None))
            .scalar()
            or 0
        )
        avg_conf = session.query(func.avg(Address.confidence)).scalar() or 0.0
        complete_count = (
            session.query(func.count(Address.id))
            .filter(Address.is_complete.is_(True))
            .scalar()
            or 0
        )

        # Data source statuses
        sources = session.query(DataSource).all()

    link_rate = (linked_count / addr_count * 100) if addr_count else 0

    # Summary table
    table = Table(title="Ingestion Status")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green", justify="right")
    table.add_row("Postcodes", f"{pc_count:,}")
    table.add_row("Addresses", f"{addr_count:,}")
    table.add_row("Linked to postcode", f"{linked_count:,} ({link_rate:.1f}%)")
    table.add_row("Complete addresses", f"{complete_count:,}")
    table.add_row("Avg confidence", f"{avg_conf:.3f}")
    console.print(table)

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
# all — full pipeline
# ---------------------------------------------------------------------------
@app.command(name="all")
def run_all(
    ctx: typer.Context,
    force_download: bool = typer.Option(
        False, "--force-download", help="Re-download even if files exist"
    ),
):
    """Run the full ingestion pipeline: download → load-postcodes → load-osm → merge."""
    console.print("[bold blue]Starting full ingestion pipeline...[/bold blue]")

    ctx.invoke(download, source="all", force=force_download)
    ctx.invoke(load_postcodes, truncate=False)
    ctx.invoke(load_osm, truncate=False)
    ctx.invoke(merge)
    ctx.invoke(status)

    console.print("[bold green]Pipeline complete![/bold green]")


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
