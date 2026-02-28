"""Post-ingestion merge operations: FK linking, confidence scoring, deduplication.

Enhanced for multi-source pipeline: includes UPRN geocoding, cross-source
deduplication, and enrichment table linking.

DATA SAFETY:
- Steps 1-8 (link, geocode, score) are NON-DESTRUCTIVE: they only fill NULL
  fields or set computed values. No source data is ever deleted or overwritten.
- Deduplication is the ONLY destructive step. It is OFF by default and must
  be explicitly requested via the --dedup flag on the merge command.

Optimised for 69M+ address rows:
- Enrichment linking uses exact source_id joins first (instant), then text fallback.
- Confidence scoring is split into phases to avoid O(N²) correlated subqueries.
- Deduplication reassigns enrichment FKs before deleting to avoid FK violations.
"""

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from app.core.logging import get_logger

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# 1. Postcode linking (NON-DESTRUCTIVE)
# ---------------------------------------------------------------------------

def link_postcodes(session_factory: sessionmaker[Session]) -> int:
    """Link addresses to postcodes via postcode_norm matching.

    Sets addresses.postcode_id where postcode_norm matches postcodes.postcode.
    Only touches rows WHERE postcode_id IS NULL (idempotent, non-destructive).

    Returns the number of rows updated.
    """
    with session_factory() as session:
        result = session.execute(
            text("""
                UPDATE addresses a
                SET postcode_id = p.id
                FROM postcodes p
                WHERE a.postcode_norm = p.postcode
                  AND a.postcode_id IS NULL
                  AND a.postcode_norm IS NOT NULL
            """)
        )
        count = result.rowcount
        session.commit()

    log.info("Postcodes linked", linked=count)
    return count


# ---------------------------------------------------------------------------
# 2. Geocoding (NON-DESTRUCTIVE — only fills NULL lat/lon)
# ---------------------------------------------------------------------------

def geocode_from_uprn(session_factory: sessionmaker[Session]) -> int:
    """Assign coordinates to addresses using UPRN → coordinate lookup.

    For addresses that have a UPRN but no coordinates, looks up the
    lat/lon from the uprn_coordinates table (OS Open UPRN data).
    Only touches rows WHERE latitude IS NULL (non-destructive).

    Returns the number of rows updated.
    """
    with session_factory() as session:
        result = session.execute(
            text("""
                UPDATE addresses a
                SET latitude = u.latitude,
                    longitude = u.longitude,
                    location = ST_SetSRID(ST_MakePoint(u.longitude, u.latitude), 4326)
                FROM uprn_coordinates u
                WHERE a.uprn = u.uprn
                  AND a.uprn IS NOT NULL
                  AND a.latitude IS NULL
            """)
        )
        count = result.rowcount
        session.commit()

    log.info("UPRN geocoding complete", geocoded=count)
    return count


def geocode_from_postcode(session_factory: sessionmaker[Session]) -> int:
    """Assign postcode centroid coordinates to addresses still lacking coords.

    For addresses that have a linked postcode but no lat/lon, copies the
    postcode centroid as an approximate location.
    Only touches rows WHERE latitude IS NULL (non-destructive).

    Returns the number of rows updated.
    """
    with session_factory() as session:
        result = session.execute(
            text("""
                UPDATE addresses a
                SET latitude = p.latitude,
                    longitude = p.longitude,
                    location = p.location
                FROM postcodes p
                WHERE a.postcode_id = p.id
                  AND a.latitude IS NULL
                  AND p.latitude IS NOT NULL
            """)
        )
        count = result.rowcount
        session.commit()

    log.info("Postcode centroid geocoding complete", geocoded=count)
    return count


# ---------------------------------------------------------------------------
# 3. Enrichment linking (NON-DESTRUCTIVE — only fills NULL address_id)
#    Exact source_id first, then text fallback.
# ---------------------------------------------------------------------------

def link_price_paid(session_factory: sessionmaker[Session]) -> int:
    """Link price_paid records to addresses.

    Phase 1: Exact match via transaction_id → source_id (fast, precise).
    Phase 2: Text fallback on postcode + PAON + street for remaining records.
    Only touches rows WHERE address_id IS NULL (non-destructive).

    Returns the total number of rows linked.
    """
    total = 0

    # Phase 1: Exact source_id join — Land Registry addresses have
    # source_id = 'lr:{transaction_id}', so we can join directly.
    with session_factory() as session:
        result = session.execute(
            text("""
                UPDATE price_paid pp
                SET address_id = a.id
                FROM addresses a
                WHERE a.source = 'land_registry'
                  AND a.source_id = 'lr:' || pp.transaction_id
                  AND pp.address_id IS NULL
            """)
        )
        exact = result.rowcount
        session.commit()
    log.info("Price paid linked (exact)", linked=exact)
    total += exact

    # Phase 2: Text fallback for any remaining unlinked records
    with session_factory() as session:
        result = session.execute(
            text("""
                UPDATE price_paid pp
                SET address_id = a.id
                FROM addresses a
                WHERE pp.postcode_norm = a.postcode_norm
                  AND pp.postcode_norm IS NOT NULL
                  AND UPPER(COALESCE(pp.paon, '')) = UPPER(COALESCE(a.house_number, a.house_name, ''))
                  AND UPPER(COALESCE(pp.street, '')) = UPPER(COALESCE(a.street, ''))
                  AND pp.address_id IS NULL
            """)
        )
        text_linked = result.rowcount
        session.commit()
    log.info("Price paid linked (text)", linked=text_linked)
    total += text_linked

    log.info("Price paid records linked", linked=total)
    return total


def link_companies(session_factory: sessionmaker[Session]) -> int:
    """Link company records to addresses.

    Phase 1: Exact match via company_number → source_id (fast, precise).
    Phase 2: Text fallback on postcode + address_line_1 for remaining records.
    Only touches rows WHERE address_id IS NULL (non-destructive).

    Returns the total number of rows linked.
    """
    total = 0

    # Phase 1: Exact source_id join — Companies House addresses have
    # source_id = 'ch:{company_number}'.
    with session_factory() as session:
        result = session.execute(
            text("""
                UPDATE companies c
                SET address_id = a.id
                FROM addresses a
                WHERE a.source = 'companies_house'
                  AND a.source_id = 'ch:' || c.company_number
                  AND c.address_id IS NULL
            """)
        )
        exact = result.rowcount
        session.commit()
    log.info("Companies linked (exact)", linked=exact)
    total += exact

    # Phase 2: Text fallback — match against any source's addresses
    with session_factory() as session:
        result = session.execute(
            text("""
                UPDATE companies c
                SET address_id = a.id
                FROM addresses a
                WHERE c.postcode_norm = a.postcode_norm
                  AND c.postcode_norm IS NOT NULL
                  AND UPPER(COALESCE(c.address_line_1, '')) = UPPER(
                      TRIM(COALESCE(a.house_number, '') || ' ' || COALESCE(a.street, ''))
                  )
                  AND c.address_id IS NULL
            """)
        )
        text_linked = result.rowcount
        session.commit()
    log.info("Companies linked (text)", linked=text_linked)
    total += text_linked

    log.info("Company records linked", linked=total)
    return total


def link_food_ratings(session_factory: sessionmaker[Session]) -> int:
    """Link food rating records to addresses.

    Phase 1: Exact match via fhrs_id → source_id (fast, precise).
    Phase 2: Text fallback on postcode + address_line_1 for remaining records.
    Only touches rows WHERE address_id IS NULL (non-destructive).

    Returns the total number of rows linked.
    """
    total = 0

    # Phase 1: Exact source_id join — FSA addresses have
    # source_id = 'fsa:{fhrs_id}'.
    with session_factory() as session:
        result = session.execute(
            text("""
                UPDATE food_ratings fr
                SET address_id = a.id
                FROM addresses a
                WHERE a.source = 'fsa'
                  AND a.source_id = 'fsa:' || fr.fhrs_id::text
                  AND fr.address_id IS NULL
            """)
        )
        exact = result.rowcount
        session.commit()
    log.info("Food ratings linked (exact)", linked=exact)
    total += exact

    # Phase 2: Text fallback — match against any source's addresses
    with session_factory() as session:
        result = session.execute(
            text("""
                UPDATE food_ratings fr
                SET address_id = a.id
                FROM addresses a
                WHERE fr.postcode_norm = a.postcode_norm
                  AND fr.postcode_norm IS NOT NULL
                  AND UPPER(COALESCE(fr.address_line_1, '')) = UPPER(
                      TRIM(COALESCE(a.house_number, '') || ' ' || COALESCE(a.street, ''))
                  )
                  AND fr.address_id IS NULL
            """)
        )
        text_linked = result.rowcount
        session.commit()
    log.info("Food ratings linked (text)", linked=text_linked)
    total += text_linked

    log.info("Food rating records linked", linked=total)
    return total


def link_voa_ratings(session_factory: sessionmaker[Session]) -> int:
    """Link VOA rating records to addresses.

    Phase 1: Exact match via UARN → source_id (fast, precise).
    Phase 2: Text fallback on postcode + number/name + street for remaining.
    Only touches rows WHERE address_id IS NULL (non-destructive).

    Returns the total number of rows linked.
    """
    total = 0

    # Phase 1: Exact source_id join — VOA addresses have
    # source_id = 'voa:{uarn}'.
    with session_factory() as session:
        result = session.execute(
            text("""
                UPDATE voa_ratings vr
                SET address_id = a.id
                FROM addresses a
                WHERE a.source = 'voa'
                  AND a.source_id = 'voa:' || vr.uarn::text
                  AND vr.address_id IS NULL
            """)
        )
        exact = result.rowcount
        session.commit()
    log.info("VOA ratings linked (exact)", linked=exact)
    total += exact

    # Phase 2: Text fallback
    with session_factory() as session:
        result = session.execute(
            text("""
                UPDATE voa_ratings vr
                SET address_id = a.id
                FROM addresses a
                WHERE vr.postcode_norm = a.postcode_norm
                  AND vr.postcode_norm IS NOT NULL
                  AND UPPER(COALESCE(vr.number_or_name, '')) = UPPER(COALESCE(a.house_number, ''))
                  AND UPPER(COALESCE(vr.street, '')) = UPPER(COALESCE(a.street, ''))
                  AND vr.address_id IS NULL
            """)
        )
        text_linked = result.rowcount
        session.commit()
    log.info("VOA ratings linked (text)", linked=text_linked)
    total += text_linked

    log.info("VOA rating records linked", linked=total)
    return total


# ---------------------------------------------------------------------------
# 4. Confidence scoring — phased to avoid O(N²) (NON-DESTRUCTIVE)
# ---------------------------------------------------------------------------

def score_confidence(session_factory: sessionmaker[Session]) -> int:
    """Compute weighted confidence score per address.

    Split into phases to avoid correlated subqueries on 69M+ rows:

    Phase 1 — Basic score (single-pass UPDATE, no subqueries):
        - postcode FK linked: 0.20
        - street present: 0.15
        - house number or name: 0.15
        - city present: 0.10
        - coordinates present: 0.10
        - suburb present: 0.05
        - UPRN present: 0.05

    Phase 2 — Multi-source bonus (+0.15) via CTE aggregation:
        Same address key (postcode + street + house_number) appears in 2+ sources.

    Phase 3 — Enrichment bonus (+0.05) via EXISTS:
        Address has at least one linked enrichment record.

    Also sets is_complete flag.
    Non-destructive: only writes computed metadata fields (confidence, is_complete).

    Returns the number of rows updated.
    """
    # Phase 1: Basic confidence — fast, no subqueries
    with session_factory() as session:
        result = session.execute(
            text("""
                UPDATE addresses a SET
                    confidence = (
                        CASE WHEN a.postcode_id IS NOT NULL THEN 0.20 ELSE 0.0 END
                        + CASE WHEN a.street IS NOT NULL AND a.street != '' THEN 0.15 ELSE 0.0 END
                        + CASE WHEN (a.house_number IS NOT NULL AND a.house_number != '')
                               OR (a.house_name IS NOT NULL AND a.house_name != '')
                          THEN 0.15 ELSE 0.0 END
                        + CASE WHEN a.city IS NOT NULL AND a.city != '' THEN 0.10 ELSE 0.0 END
                        + CASE WHEN a.latitude IS NOT NULL AND a.longitude IS NOT NULL
                          THEN 0.10 ELSE 0.0 END
                        + CASE WHEN a.suburb IS NOT NULL AND a.suburb != '' THEN 0.05 ELSE 0.0 END
                        + CASE WHEN a.uprn IS NOT NULL THEN 0.05 ELSE 0.0 END
                    ),
                    is_complete = (
                        a.postcode_norm IS NOT NULL
                        AND a.street IS NOT NULL AND a.street != ''
                        AND (
                            (a.house_number IS NOT NULL AND a.house_number != '')
                            OR (a.house_name IS NOT NULL AND a.house_name != '')
                        )
                    )
            """)
        )
        count = result.rowcount
        session.commit()
    log.info("Phase 1: basic confidence computed", updated=count)

    # Phase 2: Multi-source bonus — CTE aggregation instead of correlated subquery.
    # Finds address keys that appear across 2+ distinct sources, then adds 0.15 bonus.
    with session_factory() as session:
        result = session.execute(
            text("""
                WITH multi_source AS (
                    SELECT postcode_norm,
                           UPPER(COALESCE(street, '')) AS street_u,
                           UPPER(COALESCE(house_number, '')) AS hn_u
                    FROM addresses
                    WHERE postcode_norm IS NOT NULL
                      AND street IS NOT NULL AND street != ''
                    GROUP BY postcode_norm,
                             UPPER(COALESCE(street, '')),
                             UPPER(COALESCE(house_number, ''))
                    HAVING COUNT(DISTINCT source) >= 2
                )
                UPDATE addresses a
                SET confidence = COALESCE(a.confidence, 0) + 0.15
                FROM multi_source ms
                WHERE a.postcode_norm = ms.postcode_norm
                  AND UPPER(COALESCE(a.street, '')) = ms.street_u
                  AND UPPER(COALESCE(a.house_number, '')) = ms.hn_u
            """)
        )
        multi_count = result.rowcount
        session.commit()
    log.info("Phase 2: multi-source bonus applied", updated=multi_count)

    # Phase 3: Enrichment bonus — uses EXISTS with indexed address_id FKs.
    with session_factory() as session:
        result = session.execute(
            text("""
                UPDATE addresses a
                SET confidence = COALESCE(a.confidence, 0) + 0.05
                WHERE (
                    EXISTS (SELECT 1 FROM price_paid pp WHERE pp.address_id = a.id)
                    OR EXISTS (SELECT 1 FROM companies c WHERE c.address_id = a.id)
                    OR EXISTS (SELECT 1 FROM food_ratings fr WHERE fr.address_id = a.id)
                    OR EXISTS (SELECT 1 FROM voa_ratings vr WHERE vr.address_id = a.id)
                )
            """)
        )
        enrich_count = result.rowcount
        session.commit()
    log.info("Phase 3: enrichment bonus applied", updated=enrich_count)

    log.info("Confidence scores computed", updated=count)
    return count


# ---------------------------------------------------------------------------
# 5. Deduplication — DESTRUCTIVE, opt-in only via --dedup flag
# ---------------------------------------------------------------------------

_ENRICHMENT_TABLES = ("price_paid", "companies", "food_ratings", "voa_ratings")


def dedup_dry_run(session_factory: sessionmaker[Session]) -> dict:
    """Preview deduplication WITHOUT deleting anything.

    Returns a dict with counts of what WOULD be deleted per source, for both
    UPRN-based and text-based dedup phases.
    """
    stats: dict = {"uprn": {}, "text": {}}

    with session_factory() as session:
        # UPRN dedup preview
        rows = session.execute(
            text("""
                SELECT source, COUNT(*) as cnt
                FROM (
                    SELECT source, ROW_NUMBER() OVER (
                        PARTITION BY uprn
                        ORDER BY confidence DESC NULLS LAST, id ASC
                    ) AS rn
                    FROM addresses
                    WHERE uprn IS NOT NULL
                ) ranked
                WHERE rn > 1
                GROUP BY source
                ORDER BY cnt DESC
            """)
        ).fetchall()
        for src, cnt in rows:
            stats["uprn"][src] = cnt

        # Text dedup preview
        rows = session.execute(
            text("""
                SELECT source, COUNT(*) as cnt
                FROM (
                    SELECT source, ROW_NUMBER() OVER (
                        PARTITION BY postcode_norm,
                                     UPPER(COALESCE(street, '')),
                                     UPPER(COALESCE(house_number, '')),
                                     UPPER(COALESCE(flat, '')),
                                     UPPER(COALESCE(house_name, '')),
                                     UPPER(COALESCE(city, ''))
                        ORDER BY confidence DESC NULLS LAST, id ASC
                    ) AS rn
                    FROM addresses
                    WHERE postcode_norm IS NOT NULL
                      AND street IS NOT NULL AND street != ''
                ) ranked
                WHERE rn > 1
                GROUP BY source
                ORDER BY cnt DESC
            """)
        ).fetchall()
        for src, cnt in rows:
            stats["text"][src] = cnt

    return stats


def deduplicate(session_factory: sessionmaker[Session]) -> int:
    """Remove duplicate addresses across sources.

    DESTRUCTIVE — only call when explicitly requested.

    Phase 1: UPRN-based dedup (most reliable) — same UPRN = same property.
    Phase 2: Address text dedup — same postcode + street + house_number +
             flat + house_name + city (conservative: includes flat to avoid
             merging different flats at the same address).

    Before deleting, reassigns enrichment FK references (price_paid, companies,
    food_ratings, voa_ratings) from the duplicate to the keeper row to avoid
    FK violations.

    Keeps the row with the highest confidence. Deletes others.
    Returns the number of rows deleted.
    """
    total_deleted = 0

    # ---- Phase 1: UPRN-based dedup ----
    with session_factory() as session:
        # Reassign enrichment FKs from dupes to the keeper before deleting.
        for tbl in _ENRICHMENT_TABLES:
            session.execute(
                text(f"""
                    UPDATE {tbl} et
                    SET address_id = ranked.keeper_id
                    FROM (
                        SELECT id,
                               FIRST_VALUE(id) OVER (
                                   PARTITION BY uprn
                                   ORDER BY confidence DESC NULLS LAST, id ASC
                               ) AS keeper_id,
                               ROW_NUMBER() OVER (
                                   PARTITION BY uprn
                                   ORDER BY confidence DESC NULLS LAST, id ASC
                               ) AS rn
                        FROM addresses
                        WHERE uprn IS NOT NULL
                    ) ranked
                    WHERE et.address_id = ranked.id
                      AND ranked.rn > 1
                      AND ranked.keeper_id != ranked.id
                """)
            )
        session.commit()

        # Now safe to delete the duplicates
        result = session.execute(
            text("""
                DELETE FROM addresses a
                USING (
                    SELECT id, ROW_NUMBER() OVER (
                        PARTITION BY uprn
                        ORDER BY confidence DESC NULLS LAST, id ASC
                    ) AS rn
                    FROM addresses
                    WHERE uprn IS NOT NULL
                ) ranked
                WHERE a.id = ranked.id AND ranked.rn > 1
            """)
        )
        uprn_deleted = result.rowcount
        session.commit()

    log.info("UPRN deduplication", deleted=uprn_deleted)
    total_deleted += uprn_deleted

    # ---- Phase 2: Address text dedup ----
    # Conservative partition key includes flat + city to prevent merging
    # different flats or different cities with the same street address.
    with session_factory() as session:
        # Reassign enrichment FKs first
        for tbl in _ENRICHMENT_TABLES:
            session.execute(
                text(f"""
                    UPDATE {tbl} et
                    SET address_id = ranked.keeper_id
                    FROM (
                        SELECT id,
                               FIRST_VALUE(id) OVER (
                                   PARTITION BY postcode_norm,
                                               UPPER(COALESCE(street, '')),
                                               UPPER(COALESCE(house_number, '')),
                                               UPPER(COALESCE(flat, '')),
                                               UPPER(COALESCE(house_name, '')),
                                               UPPER(COALESCE(city, ''))
                                   ORDER BY confidence DESC NULLS LAST, id ASC
                               ) AS keeper_id,
                               ROW_NUMBER() OVER (
                                   PARTITION BY postcode_norm,
                                               UPPER(COALESCE(street, '')),
                                               UPPER(COALESCE(house_number, '')),
                                               UPPER(COALESCE(flat, '')),
                                               UPPER(COALESCE(house_name, '')),
                                               UPPER(COALESCE(city, ''))
                                   ORDER BY confidence DESC NULLS LAST, id ASC
                               ) AS rn
                        FROM addresses
                        WHERE postcode_norm IS NOT NULL
                          AND street IS NOT NULL AND street != ''
                    ) ranked
                    WHERE et.address_id = ranked.id
                      AND ranked.rn > 1
                      AND ranked.keeper_id != ranked.id
                """)
            )
        session.commit()

        # Now safe to delete
        result = session.execute(
            text("""
                DELETE FROM addresses a
                USING (
                    SELECT id, ROW_NUMBER() OVER (
                        PARTITION BY postcode_norm,
                                     UPPER(COALESCE(street, '')),
                                     UPPER(COALESCE(house_number, '')),
                                     UPPER(COALESCE(flat, '')),
                                     UPPER(COALESCE(house_name, '')),
                                     UPPER(COALESCE(city, ''))
                        ORDER BY confidence DESC NULLS LAST, id ASC
                    ) AS rn
                    FROM addresses
                    WHERE postcode_norm IS NOT NULL
                      AND street IS NOT NULL AND street != ''
                ) ranked
                WHERE a.id = ranked.id AND ranked.rn > 1
            """)
        )
        text_deleted = result.rowcount
        session.commit()

    log.info("Address text deduplication", deleted=text_deleted)
    total_deleted += text_deleted

    log.info("Deduplication complete", total_deleted=total_deleted)
    return total_deleted


# ---------------------------------------------------------------------------
# Helper: fix stale data source statuses
# ---------------------------------------------------------------------------

def fix_stale_statuses(session_factory: sessionmaker[Session]) -> int:
    """Fix data sources stuck in 'ingesting' status from interrupted runs.

    Checks actual data presence and marks as 'completed' if data exists.
    Returns number of statuses fixed.
    """
    fixed = 0
    stale_checks = {
        "osm": "SELECT COUNT(*) FROM addresses WHERE source = 'osm'",
        "voa": "SELECT COUNT(*) FROM voa_ratings",
        "epc": "SELECT COUNT(*) FROM addresses WHERE source = 'epc'",
        "fsa": "SELECT COUNT(*) FROM food_ratings",
    }

    with session_factory() as session:
        for source_name, count_sql in stale_checks.items():
            row = session.execute(
                text(
                    "SELECT status FROM data_sources WHERE source_name = :name"
                ),
                {"name": source_name},
            ).fetchone()
            if row and row[0] == "ingesting":
                count = session.execute(text(count_sql)).scalar() or 0
                if count > 0:
                    session.execute(
                        text(
                            "UPDATE data_sources SET status = 'completed', "
                            "record_count = :count "
                            "WHERE source_name = :name"
                        ),
                        {"name": source_name, "count": count},
                    )
                    fixed += 1
                    log.info(
                        "Fixed stale status",
                        source=source_name,
                        record_count=count,
                    )
        session.commit()

    return fixed
