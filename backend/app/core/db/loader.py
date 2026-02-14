import signal
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field

from pydantic import BaseModel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from sqlalchemy.orm import Session, sessionmaker

from app.core.logging import get_logger

log = get_logger(__name__)


@dataclass
class LoadResult:
    source: str
    total: int = 0
    loaded: int = 0
    skipped: int = 0
    failed_batches: int = 0
    duration: float = 0.0
    errors: list[str] = field(default_factory=list)


def batch_load(
    session_factory: sessionmaker[Session],
    records: Iterator[list[BaseModel]],
    upsert_fn: Callable[[Session, list[BaseModel]], int],
    *,
    source: str,
    label: str = "Loading",
    total_hint: int | None = None,
) -> LoadResult:
    """Generic batch loader with Rich progress, error handling, and graceful shutdown.

    Args:
        session_factory: SQLAlchemy session factory.
        records: Iterator yielding batches of validated Pydantic models.
        upsert_fn: Function that takes (session, batch) and returns rows affected.
        source: Source name for logging context.
        label: Progress bar label.
        total_hint: Optional estimated total for progress bar.

    Returns:
        LoadResult with counts and timing.
    """
    result = LoadResult(source=source)
    start = time.monotonic()
    shutdown_requested = False

    original_handler = signal.getsignal(signal.SIGINT)

    def _handle_sigint(signum, frame):
        nonlocal shutdown_requested
        if shutdown_requested:
            # Second Ctrl+C: hard exit
            signal.signal(signal.SIGINT, original_handler)
            raise KeyboardInterrupt
        shutdown_requested = True
        log.warning("Graceful shutdown requested — finishing current batch", source=source)

    signal.signal(signal.SIGINT, _handle_sigint)

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("[cyan]{task.fields[status]}"),
        ) as progress:
            task = progress.add_task(
                label,
                total=total_hint,
                status=f"0 loaded, 0 skipped",
            )

            batch_num = 0
            for batch in records:
                if shutdown_requested:
                    log.info(
                        "Shutdown: stopping after batch",
                        source=source,
                        batch=batch_num,
                    )
                    break

                batch_num += 1
                result.total += len(batch)

                try:
                    with session_factory() as session:
                        loaded = upsert_fn(session, batch)
                        session.commit()
                        result.loaded += loaded
                        result.skipped += len(batch) - loaded
                except Exception as exc:
                    result.failed_batches += 1
                    error_msg = f"Batch {batch_num}: {exc}"
                    result.errors.append(error_msg)
                    log.error(
                        "Batch failed",
                        source=source,
                        batch=batch_num,
                        error=str(exc),
                    )

                progress.update(
                    task,
                    advance=len(batch),
                    status=f"{result.loaded} loaded, {result.skipped} skipped",
                )

    finally:
        signal.signal(signal.SIGINT, original_handler)
        result.duration = time.monotonic() - start

    log.info(
        "Load complete",
        source=source,
        total=result.total,
        loaded=result.loaded,
        skipped=result.skipped,
        failed_batches=result.failed_batches,
        duration=f"{result.duration:.1f}s",
    )
    return result
