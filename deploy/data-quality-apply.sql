-- ============================================================================
-- Data Quality Apply — Re-run the 3 UPDATEs (backup already exists)
-- ============================================================================
-- The backup INSERTs ran correctly but the UPDATEs didn't apply.
-- This script re-runs only the UPDATE statements.
-- ============================================================================

\timing on
SET statement_timeout = '3600s';

-- ============================================================================
-- Fix 1: Extract house numbers from street
-- ============================================================================
\echo '=== Fix 1: Extracting house numbers from street ==='

UPDATE addresses
SET house_number = (regexp_match(TRIM(street), '^\s*(\d+[A-Za-z]?)\s*[,\s]'))[1],
    street = TRIM(regexp_replace(TRIM(street), '^\s*\d+[A-Za-z]?\s*[,\s]+\s*', ''))
WHERE (house_number IS NULL OR TRIM(house_number) = '')
  AND street ~ '^\s*\d+[A-Za-z]?\s*[,\s]'
  AND TRIM(regexp_replace(TRIM(street), '^\s*\d+[A-Za-z]?\s*[,\s]+\s*', '')) != '';

\echo 'Fix 1 done'

-- ============================================================================
-- Fix 2: Normalize street names
-- ============================================================================
\echo ''
\echo '=== Fix 2: Normalizing street names ==='

UPDATE addresses a
SET street = sub.new_street
FROM (
    SELECT id, _canonical_street(street) AS new_street
    FROM addresses
    WHERE street IS NOT NULL
      AND TRIM(street) != ''
) sub
WHERE a.id = sub.id AND sub.new_street != a.street;

\echo 'Fix 2 done'

-- ============================================================================
-- Fix 3: Clear suburb when it duplicates city
-- ============================================================================
\echo ''
\echo '=== Fix 3: Clearing duplicate suburb ==='

UPDATE addresses
SET suburb = NULL
WHERE suburb IS NOT NULL
  AND city IS NOT NULL
  AND UPPER(TRIM(suburb)) = UPPER(TRIM(city));

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
