"""OSM parser — .pbf path → Iterator of validated address record batches.

Uses osmium (pyosmium) to stream-parse the PBF file. Only extracts elements
with addr:* tags. Handles both nodes (direct lat/lon) and ways (centroid
via node location index).

Batches are streamed via a thread-safe queue so memory stays bounded.
"""

import queue
import threading
from collections.abc import Iterator
from pathlib import Path

import osmium

from app.core.exceptions import ParseError
from app.core.logging import get_logger
from app.core.utils.address import normalise_city, normalise_street
from app.core.utils.postcode import normalise_postcode
from app.ingestion.schemas import OSMAddressRecord

log = get_logger(__name__)

_SENTINEL = None  # signals end of stream


class _AddressCollector(osmium.SimpleHandler):
    """Collects addresses from OSM nodes and ways with addr:* tags.

    Completed batches are put into a queue for streaming consumption.
    """

    def __init__(self, batch_size: int, out_queue: queue.Queue):
        super().__init__()
        self.batch_size = batch_size
        self._queue = out_queue
        self._current_batch: list[OSMAddressRecord] = []
        self.skipped = 0
        self.total = 0

    def _process(self, osm_type: str, osm_id: int, tags: dict, lat: float, lon: float):
        self.total += 1

        raw_postcode = tags.get("addr:postcode", "")
        norm_postcode = normalise_postcode(raw_postcode) if raw_postcode else None

        try:
            record = OSMAddressRecord(
                osm_id=osm_id,
                osm_type=osm_type,
                house_number=tags.get("addr:housenumber"),
                house_name=tags.get("addr:housename"),
                flat=tags.get("addr:flats") or tags.get("addr:unit"),
                street=normalise_street(tags.get("addr:street")),
                suburb=tags.get("addr:suburb"),
                city=normalise_city(tags.get("addr:city")),
                county=tags.get("addr:county"),
                postcode_raw=raw_postcode or None,
                postcode_norm=norm_postcode,
                latitude=lat,
                longitude=lon,
            )
        except Exception:
            self.skipped += 1
            return

        self._current_batch.append(record)
        if len(self._current_batch) >= self.batch_size:
            self._queue.put(self._current_batch)
            self._current_batch = []

    def node(self, n):
        tags = {t.k: t.v for t in n.tags}
        if any(k.startswith("addr:") for k in tags):
            self._process("node", n.id, tags, n.location.lat, n.location.lon)

    def way(self, w):
        tags = {t.k: t.v for t in w.tags}
        if any(k.startswith("addr:") for k in tags):
            try:
                centroid = w.centroid()
                self._process("way", w.id, tags, centroid.lat, centroid.lon)
            except Exception:
                self.skipped += 1

    def flush(self):
        """Flush any remaining records in the current batch."""
        if self._current_batch:
            self._queue.put(self._current_batch)
            self._current_batch = []


def parse_osm(
    pbf_path: Path,
    *,
    batch_size: int = 2_000,
    index_type: str = "sparse_file_array",
) -> Iterator[list[OSMAddressRecord]]:
    """Parse an OSM .pbf file, yielding batches of validated address records.

    Args:
        pbf_path: Path to the .osm.pbf file.
        batch_size: Number of records per batch.
        index_type: Node location index type. 'flex_mem' (fast, ~4-6GB RAM)
                    or 'sparse_file_array' (slower, low RAM).

    Yields:
        Lists of OSMAddressRecord, each up to batch_size elements.
    """
    if not pbf_path.exists():
        raise ParseError(f"OSM file not found: {pbf_path}", source="osm")

    log.info(
        "Starting OSM parse",
        source="osm",
        path=str(pbf_path),
        index_type=index_type,
        batch_size=batch_size,
    )

    # Bounded queue: at most 4 batches buffered to limit memory
    out_queue: queue.Queue = queue.Queue(maxsize=4)
    error_holder: list[Exception] = []

    def _run_parser():
        collector = _AddressCollector(batch_size, out_queue)
        try:
            collector.apply_file(
                str(pbf_path),
                locations=True,
                idx=index_type,
            )
            collector.flush()
        except Exception as exc:
            error_holder.append(exc)
        finally:
            out_queue.put(_SENTINEL)
            log.info(
                "OSM parsing complete",
                source="osm",
                total=collector.total,
                skipped=collector.skipped,
            )

    thread = threading.Thread(target=_run_parser, daemon=True)
    thread.start()

    # Yield batches as they arrive from the parser thread
    while True:
        batch = out_queue.get()
        if batch is _SENTINEL:
            break
        yield batch

    thread.join()

    if error_holder:
        raise ParseError(
            f"OSM parsing failed: {error_holder[0]}",
            source="osm",
            detail=str(error_holder[0]),
        ) from error_holder[0]
