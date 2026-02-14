import asyncio
import hashlib
from pathlib import Path

import httpx
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TransferSpeedColumn,
)

from app.core.exceptions import DownloadError
from app.core.logging import get_logger

log = get_logger(__name__)


async def _download_one(
    client: httpx.AsyncClient,
    url: str,
    dest: Path,
    *,
    source: str,
    force: bool = False,
    progress: Progress,
) -> str:
    """Download a single file with streaming, .tmp atomic rename, and SHA-256.

    Returns the SHA-256 hex digest of the downloaded file.
    """
    if dest.exists() and not force:
        log.info("File exists, skipping download", source=source, path=str(dest))
        return _hash_file(dest)

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")

    task_id = progress.add_task(f"Downloading {source}", total=None)

    try:
        async with client.stream("GET", url, follow_redirects=True) as response:
            if response.status_code != 200:
                raise DownloadError(
                    f"HTTP {response.status_code} for {source}",
                    source=source,
                    url=url,
                    status_code=response.status_code,
                )

            total = response.headers.get("content-length")
            if total:
                progress.update(task_id, total=int(total))

            sha256 = hashlib.sha256()
            with open(tmp, "wb") as f:
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    f.write(chunk)
                    sha256.update(chunk)
                    progress.advance(task_id, len(chunk))

        # Atomic rename
        tmp.rename(dest)
        digest = sha256.hexdigest()
        log.info("Download complete", source=source, path=str(dest), sha256=digest)
        return digest

    except httpx.HTTPError as exc:
        tmp.unlink(missing_ok=True)
        raise DownloadError(
            f"Network error downloading {source}: {exc}",
            source=source,
            url=url,
        ) from exc
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def _hash_file(path: Path) -> str:
    """Compute SHA-256 of an existing file."""
    sha256 = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


async def download_sources(
    sources: dict[str, tuple[str, Path]],
    *,
    force: bool = False,
    timeout: float = 3600.0,
) -> dict[str, str]:
    """Download multiple sources concurrently.

    Args:
        sources: Mapping of source_name → (url, destination_path).
        force: Re-download even if file exists.
        timeout: HTTP timeout in seconds.

    Returns:
        Mapping of source_name → SHA-256 hex digest.
    """
    results: dict[str, str] = {}

    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
    ) as progress:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
            tasks = [
                _download_one(
                    client,
                    url,
                    dest,
                    source=name,
                    force=force,
                    progress=progress,
                )
                for name, (url, dest) in sources.items()
            ]
            digests = await asyncio.gather(*tasks, return_exceptions=True)

    for (name, _), digest in zip(sources.items(), digests):
        if isinstance(digest, Exception):
            log.error("Download failed", source=name, error=str(digest))
            raise digest
        results[name] = digest

    return results


def run_download(
    sources: dict[str, tuple[str, Path]],
    *,
    force: bool = False,
) -> dict[str, str]:
    """Synchronous wrapper for download_sources."""
    return asyncio.run(download_sources(sources, force=force))
