-- ============================================================================
-- Data Quality Apply — Batched UPDATEs (processes 1M rows at a time)
-- ============================================================================
-- Previous single UPDATEs timed out on 31M rows.
-- This version processes in batches of 1M IDs to avoid timeouts.
-- ============================================================================

\timing on
SET statement_timeout = '0';  -- no timeout for this session

-- ============================================================================
-- Fix 1: Extract house numbers from street (batched)
-- ============================================================================
\echo '=== Fix 1: Extracting house numbers from street ==='

DO $$
DECLARE
    batch_size INT := 1000000;
    max_id INT;
    batch_start INT := 0;
    total_updated BIGINT := 0;
    batch_updated INT;
BEGIN
    SELECT MAX(id) INTO max_id FROM addresses;

    WHILE batch_start < max_id LOOP
        UPDATE addresses
        SET house_number = (regexp_match(TRIM(street), '^\s*(\d+[A-Za-z]?)\s*[,\s]'))[1],
            street = TRIM(regexp_replace(TRIM(street), '^\s*\d+[A-Za-z]?\s*[,\s]+\s*', ''))
        WHERE id > batch_start AND id <= batch_start + batch_size
          AND (house_number IS NULL OR TRIM(house_number) = '')
          AND street ~ '^\s*\d+[A-Za-z]?\s*[,\s]'
          AND TRIM(regexp_replace(TRIM(street), '^\s*\d+[A-Za-z]?\s*[,\s]+\s*', '')) != '';

        GET DIAGNOSTICS batch_updated = ROW_COUNT;
        total_updated := total_updated + batch_updated;
        batch_start := batch_start + batch_size;

        RAISE NOTICE 'Fix 1 — batch % to %: % rows updated (total: %)',
            batch_start - batch_size, batch_start, batch_updated, total_updated;
    END LOOP;

    RAISE NOTICE 'Fix 1 COMPLETE — total rows updated: %', total_updated;
END $$;

\echo 'Fix 1 done'

-- ============================================================================
-- Fix 2: Normalize street names (batched)
-- ============================================================================
\echo ''
\echo '=== Fix 2: Normalizing street names ==='

DO $$
DECLARE
    batch_size INT := 1000000;
    max_id INT;
    batch_start INT := 0;
    total_updated BIGINT := 0;
    batch_updated INT;
BEGIN
    SELECT MAX(id) INTO max_id FROM addresses;

    WHILE batch_start < max_id LOOP
        UPDATE addresses a
        SET street = sub.new_street
        FROM (
            SELECT id, _canonical_street(street) AS new_street
            FROM addresses
            WHERE id > batch_start AND id <= batch_start + batch_size
              AND street IS NOT NULL
              AND TRIM(street) != ''
        ) sub
        WHERE a.id = sub.id AND sub.new_street != a.street;

        GET DIAGNOSTICS batch_updated = ROW_COUNT;
        total_updated := total_updated + batch_updated;
        batch_start := batch_start + batch_size;

        RAISE NOTICE 'Fix 2 — batch % to %: % rows updated (total: %)',
            batch_start - batch_size, batch_start, batch_updated, total_updated;
    END LOOP;

    RAISE NOTICE 'Fix 2 COMPLETE — total rows updated: %', total_updated;
END $$;

\echo 'Fix 2 done'

-- ============================================================================
-- Fix 3: Clear suburb when it duplicates city (batched)
-- ============================================================================
\echo ''
\echo '=== Fix 3: Clearing duplicate suburb ==='

DO $$
DECLARE
    batch_size INT := 1000000;
    max_id INT;
    batch_start INT := 0;
    total_updated BIGINT := 0;
    batch_updated INT;
BEGIN
    SELECT MAX(id) INTO max_id FROM addresses;

    WHILE batch_start < max_id LOOP
        UPDATE addresses
        SET suburb = NULL
        WHERE id > batch_start AND id <= batch_start + batch_size
          AND suburb IS NOT NULL
          AND city IS NOT NULL
          AND UPPER(TRIM(suburb)) = UPPER(TRIM(city));

        GET DIAGNOSTICS batch_updated = ROW_COUNT;
        total_updated := total_updated + batch_updated;
        batch_start := batch_start + batch_size;

        RAISE NOTICE 'Fix 3 — batch % to %: % rows updated (total: %)',
            batch_start - batch_size, batch_start, batch_updated, total_updated;
    END LOOP;

    RAISE NOTICE 'Fix 3 COMPLETE — total rows updated: %', total_updated;
END $$;

\echo 'Fix 3 done'

-- ============================================================================
-- Verify SO23 0QD
-- ============================================================================
\echo ''
\echo '=== Verification: SO23 0QD ==='

SELECT id, source, house_number, street, suburb, city
FROM addresses
WHERE postcode_norm = 'SO23 0QD' AND duplicate_of IS NULL
ORDER BY
    regexp_replace(
        regexp_replace(
            UPPER(REPLACE(
                regexp_replace(street, '^\d+[a-zA-Z]?\s*[,\s]+\s*', '', 'g'),
                '.', ''
            )),
            '\mSTREET\s+(?=[A-Z])', 'SAINT ', 'g'
        ),
        '\mST\s+(?=[A-Z])', 'SAINT ', 'g'
    ),
    CAST(NULLIF(regexp_replace(
        COALESCE(house_number, (regexp_match(street, '^\d+'))[1], ''),
        '[^0-9]', '', 'g'
    ), '') AS INTEGER) NULLS LAST,
    house_number, street;

\echo ''
\echo '=== Unchanged rows check ==='
SELECT COUNT(*) AS still_unchanged
FROM addresses a
JOIN _dq_backup b ON a.id = b.address_id
WHERE a.street = b.street AND 'street_norm' = ANY(b.fix_applied);

\echo ''
\echo '============================================'
\echo 'ALL FIXES APPLIED'
\echo '============================================'
