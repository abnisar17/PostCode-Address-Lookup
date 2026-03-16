-- ============================================================================
-- Data Quality Fix — Normalize addresses across sources
-- ============================================================================
-- Fixes 3 issues:
--   1. Extract house number from street (EPC: "15 St. Leonards Road" → 15 + "St Leonards Road")
--   2. Normalize street names across sources (Street Leonards → St Leonards, remove dots)
--   3. Clear suburb when it duplicates city (suburb=WINCHESTER, city=Winchester → suburb=NULL)
--
-- Safety: All original values backed up to _dq_backup table.
-- Undo:   See bottom of script for undo commands.
-- ============================================================================

\timing on

-- Prevent search endpoint timeout from killing this
SET statement_timeout = '3600s';

-- ============================================================================
-- PHASE 0: Create backup table
-- ============================================================================
\echo '=== PHASE 0: Creating backup table ==='

DROP TABLE IF EXISTS _dq_backup;

CREATE TABLE _dq_backup (
    address_id   BIGINT PRIMARY KEY,
    house_number TEXT,
    street       TEXT,
    suburb       TEXT,
    fix_applied  TEXT[],
    backed_up_at TIMESTAMPTZ DEFAULT now()
);

\echo 'Backup table created'

-- ============================================================================
-- PHASE 1: Create helper function for street normalization
-- ============================================================================
\echo ''
\echo '=== PHASE 1: Creating canonical street function ==='

CREATE OR REPLACE FUNCTION _canonical_street(s TEXT) RETURNS TEXT AS $$
DECLARE
    result TEXT;
BEGIN
    IF s IS NULL OR TRIM(s) = '' THEN RETURN s; END IF;

    result := TRIM(s);

    -- Remove dots (St. → St)
    result := REPLACE(result, '.', '');

    -- Uppercase for processing
    result := UPPER(result);

    -- Remove trailing commas
    result := regexp_replace(result, ',\s*$', '');

    -- Fix Land Registry bug: "STREET" at START of street name = always Saint
    -- No real UK street starts with the word "Street" — it's always a misexpansion of "St"
    -- e.g. "STREET LEONARDS ROAD" → "ST LEONARDS ROAD"
    result := regexp_replace(result, '^\mSTREET\s+', 'ST ');

    -- Convert to Title Case
    result := INITCAP(result);

    -- Collapse multiple spaces
    result := regexp_replace(result, '\s+', ' ', 'g');

    RETURN TRIM(result);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

\echo 'Function created'

-- ============================================================================
-- PHASE 2: Diagnostic — count affected rows
-- ============================================================================
\echo ''
\echo '=== PHASE 2: Diagnostics (counts) ==='

\echo ''
\echo 'Fix 1 — House numbers embedded in street (house_number IS NULL):'
SELECT COUNT(*) AS fix1_affected
FROM addresses
WHERE (house_number IS NULL OR TRIM(house_number) = '')
  AND street ~ '^\s*\d+[A-Za-z]?\s*[,\s]'
  AND TRIM(regexp_replace(TRIM(street), '^\s*\d+[A-Za-z]?\s*[,\s]+\s*', '')) != '';

\echo ''
\echo 'Fix 2 — Street names that would change after normalization:'
SELECT COUNT(*) AS fix2_affected
FROM addresses
WHERE street IS NOT NULL
  AND TRIM(street) != ''
  AND _canonical_street(street) != street;

\echo ''
\echo 'Fix 3 — Suburb duplicates city:'
SELECT COUNT(*) AS fix3_affected
FROM addresses
WHERE suburb IS NOT NULL
  AND city IS NOT NULL
  AND UPPER(TRIM(suburb)) = UPPER(TRIM(city));

-- ============================================================================
-- PHASE 3: Preview on SO23 0QD (before changes)
-- ============================================================================
\echo ''
\echo '=== PHASE 3: Preview SO23 0QD (before) ==='

\echo ''
\echo 'Current state:'
SELECT id, source, house_number, street, suburb, city
FROM addresses
WHERE postcode_norm = 'SO23 0QD' AND duplicate_of IS NULL
ORDER BY id
LIMIT 20;

\echo ''
\echo 'Fix 1 preview — house number extraction:'
SELECT id, source,
    house_number AS old_hnum,
    (regexp_match(TRIM(street), '^\s*(\d+[A-Za-z]?)\s*[,\s]'))[1] AS new_hnum,
    street AS old_street,
    TRIM(regexp_replace(TRIM(street), '^\s*\d+[A-Za-z]?\s*[,\s]+\s*', '')) AS new_street
FROM addresses
WHERE postcode_norm = 'SO23 0QD'
  AND (house_number IS NULL OR TRIM(house_number) = '')
  AND street ~ '^\s*\d+[A-Za-z]?\s*[,\s]';

\echo ''
\echo 'Fix 2 preview — street normalization:'
SELECT id, source, street AS old_street, _canonical_street(street) AS new_street
FROM addresses
WHERE postcode_norm = 'SO23 0QD'
  AND street IS NOT NULL AND TRIM(street) != ''
  AND _canonical_street(street) != street;

\echo ''
\echo 'Fix 3 preview — suburb = city:'
SELECT id, source, suburb, city
FROM addresses
WHERE postcode_norm = 'SO23 0QD'
  AND suburb IS NOT NULL AND city IS NOT NULL
  AND UPPER(TRIM(suburb)) = UPPER(TRIM(city));

-- ============================================================================
-- PHASE 4: Apply Fix 1 — Extract house number from street
-- ============================================================================
\echo ''
\echo '=== PHASE 4: Fix 1 — Extracting house numbers from street ==='

-- Backup affected rows
INSERT INTO _dq_backup (address_id, house_number, street, suburb, fix_applied)
SELECT id, house_number, street, suburb, ARRAY['hnum_extract']
FROM addresses
WHERE (house_number IS NULL OR TRIM(house_number) = '')
  AND street ~ '^\s*\d+[A-Za-z]?\s*[,\s]'
  AND TRIM(regexp_replace(TRIM(street), '^\s*\d+[A-Za-z]?\s*[,\s]+\s*', '')) != ''
ON CONFLICT (address_id) DO UPDATE SET
  fix_applied = _dq_backup.fix_applied || ARRAY['hnum_extract'];

\echo 'Rows backed up for Fix 1:'
SELECT COUNT(*) AS backed_up FROM _dq_backup WHERE 'hnum_extract' = ANY(fix_applied);

-- Apply the fix
UPDATE addresses
SET house_number = (regexp_match(TRIM(street), '^\s*(\d+[A-Za-z]?)\s*[,\s]'))[1],
    street = TRIM(regexp_replace(TRIM(street), '^\s*\d+[A-Za-z]?\s*[,\s]+\s*', ''))
WHERE (house_number IS NULL OR TRIM(house_number) = '')
  AND street ~ '^\s*\d+[A-Za-z]?\s*[,\s]'
  AND TRIM(regexp_replace(TRIM(street), '^\s*\d+[A-Za-z]?\s*[,\s]+\s*', '')) != '';

\echo 'Fix 1 applied'

-- ============================================================================
-- PHASE 5: Apply Fix 2 — Normalize street names
-- ============================================================================
\echo ''
\echo '=== PHASE 5: Fix 2 — Normalizing street names ==='

-- Backup affected rows (preserve original if not already backed up)
INSERT INTO _dq_backup (address_id, house_number, street, suburb, fix_applied)
SELECT id, house_number, street, suburb, ARRAY['street_norm']
FROM addresses
WHERE street IS NOT NULL
  AND TRIM(street) != ''
  AND _canonical_street(street) != street
ON CONFLICT (address_id) DO UPDATE SET
  fix_applied = _dq_backup.fix_applied || ARRAY['street_norm'];

\echo 'Rows backed up for Fix 2:'
SELECT COUNT(*) AS backed_up FROM _dq_backup WHERE 'street_norm' = ANY(fix_applied);

-- Apply using subquery (calls function once per row)
UPDATE addresses a
SET street = sub.new_street
FROM (
    SELECT id, _canonical_street(street) AS new_street
    FROM addresses
    WHERE street IS NOT NULL
      AND TRIM(street) != ''
) sub
WHERE a.id = sub.id AND sub.new_street != a.street;

\echo 'Fix 2 applied'

-- ============================================================================
-- PHASE 6: Apply Fix 3 — Clear suburb when it duplicates city
-- ============================================================================
\echo ''
\echo '=== PHASE 6: Fix 3 — Clearing duplicate suburb ==='

-- Backup affected rows
INSERT INTO _dq_backup (address_id, house_number, street, suburb, fix_applied)
SELECT id, house_number, street, suburb, ARRAY['suburb_clear']
FROM addresses
WHERE suburb IS NOT NULL
  AND city IS NOT NULL
  AND UPPER(TRIM(suburb)) = UPPER(TRIM(city))
ON CONFLICT (address_id) DO UPDATE SET
  fix_applied = _dq_backup.fix_applied || ARRAY['suburb_clear'];

\echo 'Rows backed up for Fix 3:'
SELECT COUNT(*) AS backed_up FROM _dq_backup WHERE 'suburb_clear' = ANY(fix_applied);

-- Apply the fix
UPDATE addresses
SET suburb = NULL
WHERE suburb IS NOT NULL
  AND city IS NOT NULL
  AND UPPER(TRIM(suburb)) = UPPER(TRIM(city));

\echo 'Fix 3 applied'

-- ============================================================================
-- PHASE 7: Verification
-- ============================================================================
\echo ''
\echo '=== PHASE 7: Verification ==='

\echo ''
\echo 'Total backup rows (all fixes):'
SELECT COUNT(*) AS total_backed_up FROM _dq_backup;

\echo ''
\echo 'Backup by fix type:'
SELECT
    CASE WHEN 'hnum_extract' = ANY(fix_applied) THEN 'Y' ELSE 'N' END AS hnum,
    CASE WHEN 'street_norm' = ANY(fix_applied) THEN 'Y' ELSE 'N' END AS street,
    CASE WHEN 'suburb_clear' = ANY(fix_applied) THEN 'Y' ELSE 'N' END AS suburb,
    COUNT(*) AS rows
FROM _dq_backup
GROUP BY 1, 2, 3
ORDER BY rows DESC;

\echo ''
\echo 'SO23 0QD after all fixes:'
SELECT id, source, house_number, street, suburb, city, confidence
FROM addresses
WHERE postcode_norm = 'SO23 0QD' AND duplicate_of IS NULL
ORDER BY
    _canonical_street(COALESCE(street, '')),
    CAST(NULLIF(regexp_replace(COALESCE(house_number, ''), '[^0-9]', '', 'g'), '') AS INTEGER) NULLS LAST,
    house_number, street;

\echo ''
\echo 'Sample of normalized streets (random 20):'
SELECT b.street AS original, a.street AS normalized, a.source
FROM _dq_backup b
JOIN addresses a ON a.id = b.address_id
WHERE 'street_norm' = ANY(b.fix_applied)
ORDER BY random()
LIMIT 20;

-- ============================================================================
-- DONE
-- ============================================================================
\echo ''
\echo '============================================'
\echo 'DATA QUALITY FIX COMPLETE'
\echo '============================================'
\echo ''
\echo 'Backup table: _dq_backup (DO NOT DROP until verified)'
\echo ''
\echo 'To undo ALL fixes:'
\echo '  UPDATE addresses a SET house_number = b.house_number, street = b.street, suburb = b.suburb FROM _dq_backup b WHERE a.id = b.address_id;'
\echo ''
\echo 'To undo only house number extraction:'
\echo '  UPDATE addresses a SET house_number = b.house_number, street = b.street FROM _dq_backup b WHERE a.id = b.address_id AND ''hnum_extract'' = ANY(b.fix_applied);'
\echo ''
\echo 'To undo only street normalization:'
\echo '  UPDATE addresses a SET street = b.street FROM _dq_backup b WHERE a.id = b.address_id AND ''street_norm'' = ANY(b.fix_applied);'
\echo ''
\echo 'To undo only suburb clearing:'
\echo '  UPDATE addresses a SET suburb = b.suburb FROM _dq_backup b WHERE a.id = b.address_id AND ''suburb_clear'' = ANY(b.fix_applied);'
\echo ''
\echo 'To cleanup after verification:'
\echo '  DROP TABLE IF EXISTS _dq_backup;'
\echo '  DROP FUNCTION IF EXISTS _canonical_street(TEXT);'
