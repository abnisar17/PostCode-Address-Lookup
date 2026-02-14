"""Post-ingestion merge operations: FK linking, confidence scoring, deduplication."""

from sqlalchemy import text, update
from sqlalchemy.orm import Session, sessionmaker

from app.core.db.models import Address
from app.core.logging import get_logger

log = get_logger(__name__)


def link_postcodes(session_factory: sessionmaker[Session]) -> int:
    """Link addresses to postcodes via postcode_norm matching.

    Sets addresses.postcode_id where postcode_norm matches postcodes.postcode.
    Only touches rows WHERE postcode_id IS NULL (idempotent).

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


def score_confidence(session_factory: sessionmaker[Session]) -> int:
    """Compute weighted confidence score per address.

    Weights:
        - postcode FK linked: 0.3
        - street present: 0.2
        - house number or name: 0.2
        - city present: 0.15
        - coordinates present: 0.1
        - suburb present: 0.05

    Also sets is_complete = true if address has postcode + street + (number or name).

    Returns the number of rows updated.
    """
    with session_factory() as session:
        result = session.execute(
            text("""
                UPDATE addresses SET
                    confidence = (
                        CASE WHEN postcode_id IS NOT NULL THEN 0.3 ELSE 0.0 END
                        + CASE WHEN street IS NOT NULL AND street != '' THEN 0.2 ELSE 0.0 END
                        + CASE WHEN (house_number IS NOT NULL AND house_number != '')
                               OR (house_name IS NOT NULL AND house_name != '')
                          THEN 0.2 ELSE 0.0 END
                        + CASE WHEN city IS NOT NULL AND city != '' THEN 0.15 ELSE 0.0 END
                        + CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL
                          THEN 0.1 ELSE 0.0 END
                        + CASE WHEN suburb IS NOT NULL AND suburb != '' THEN 0.05 ELSE 0.0 END
                    ),
                    is_complete = (
                        postcode_norm IS NOT NULL
                        AND street IS NOT NULL AND street != ''
                        AND (
                            (house_number IS NOT NULL AND house_number != '')
                            OR (house_name IS NOT NULL AND house_name != '')
                        )
                    )
            """)
        )
        count = result.rowcount
        session.commit()

    log.info("Confidence scores computed", updated=count)
    return count


def deduplicate(session_factory: sessionmaker[Session]) -> int:
    """Remove duplicate addresses (same postcode_norm + street + house_number/house_name).

    Keeps the row with the highest confidence. Deletes others.

    Returns the number of rows deleted.
    """
    with session_factory() as session:
        result = session.execute(
            text("""
                DELETE FROM addresses a
                USING (
                    SELECT id, ROW_NUMBER() OVER (
                        PARTITION BY postcode_norm,
                                     COALESCE(street, ''),
                                     COALESCE(house_number, ''),
                                     COALESCE(house_name, '')
                        ORDER BY confidence DESC NULLS LAST, id ASC
                    ) AS rn
                    FROM addresses
                    WHERE postcode_norm IS NOT NULL
                ) ranked
                WHERE a.id = ranked.id AND ranked.rn > 1
            """)
        )
        count = result.rowcount
        session.commit()

    log.info("Deduplication complete", deleted=count)
    return count
