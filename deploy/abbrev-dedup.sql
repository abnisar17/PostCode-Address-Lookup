-- ============================================================================
-- Abbreviation Dedup — Full Execution
-- ============================================================================
-- Removes duplicates caused by abbreviation differences:
--   "St. Leonards Road" vs "Street Leonards Road"
--   "High St" vs "High Street"
--   "Kings Rd" vs "Kings Road"
--
-- Safety: transaction-wrapped, targeted JOIN (not full window function)
-- Performance: Only processes addresses where normalization changes the street
-- ============================================================================

\timing on

-- ============================================================================
-- PHASE 1: Create normalization function
-- ============================================================================
\echo '=== PHASE 1: Creating normalization function ==='

CREATE OR REPLACE FUNCTION _norm_street_abbrev(s TEXT) RETURNS TEXT AS $$
DECLARE
    result TEXT;
BEGIN
    IF s IS NULL OR TRIM(s) = '' THEN
        RETURN '';
    END IF;

    result := UPPER(TRIM(s));

    -- Remove dots (St. → ST)
    result := REPLACE(result, '.', '');

    -- Normalize "STREET" when it appears BEFORE another word (= Saint)
    -- "STREET LEONARDS" → "SAINT LEONARDS"
    result := regexp_replace(result, '\bSTREET\s+(?=[A-Z])', 'SAINT ', 'g');

    -- Normalize "ST " before a word to "SAINT " (St James → SAINT JAMES)
    result := regexp_replace(result, '\bST\s+(?=[A-Z])', 'SAINT ', 'g');

    -- Normalize trailing abbreviations to full form
    result := regexp_replace(result, '\bST$', 'STREET');
    result := regexp_replace(result, '\bRD$', 'ROAD');
    result := regexp_replace(result, '\bAVE$', 'AVENUE');
    result := regexp_replace(result, '\bAV$', 'AVENUE');
    result := regexp_replace(result, '\bCRES$', 'CRESCENT');
    result := regexp_replace(result, '\bCR$', 'CRESCENT');
    result := regexp_replace(result, '\bDR$', 'DRIVE');
    result := regexp_replace(result, '\bDRV$', 'DRIVE');
    result := regexp_replace(result, '\bLN$', 'LANE');
    result := regexp_replace(result, '\bCT$', 'COURT');
    result := regexp_replace(result, '\bCL$', 'CLOSE');
    result := regexp_replace(result, '\bPL$', 'PLACE');
    result := regexp_replace(result, '\bSQ$', 'SQUARE');
    result := regexp_replace(result, '\bTERR$', 'TERRACE');
    result := regexp_replace(result, '\bGDNS$', 'GARDENS');
    result := regexp_replace(result, '\bGRV$', 'GROVE');
    result := regexp_replace(result, '\bPK$', 'PARK');
    result := regexp_replace(result, '\bMWS$', 'MEWS');

    -- Collapse multiple spaces
    result := regexp_replace(result, '\s+', ' ', 'g');

    RETURN TRIM(result);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

\echo 'Function created'

-- ============================================================================
-- PHASE 2: Build work tables
-- ============================================================================
\echo ''
\echo '=== PHASE 2: Building work tables ==='

DROP TABLE IF EXISTS _abbrev_norm;

CREATE UNLOGGED TABLE _abbrev_norm AS
SELECT
    id,
    postcode_norm,
    _norm_street_abbrev(street) AS norm_street,
    UPPER(TRIM(COALESCE(house_number, ''))) AS norm_hnum,
    UPPER(TRIM(COALESCE(flat, ''))) AS norm_flat,
    UPPER(TRIM(COALESCE(house_name, ''))) AS norm_hname
FROM addresses
WHERE postcode_norm IS NOT NULL
  AND street IS NOT NULL
  AND street != ''
  AND _norm_street_abbrev(street) != UPPER(TRIM(street));

\echo 'Candidates found:'
SELECT COUNT(*) AS changed_count FROM _abbrev_norm;

CREATE INDEX ON _abbrev_norm (postcode_norm, norm_street, norm_hnum, norm_flat, norm_hname);

-- ============================================================================
-- PHASE 3: Find duplicate pairs
-- ============================================================================
\echo ''
\echo '=== PHASE 3: Finding duplicate pairs ==='

DROP TABLE IF EXISTS _abbrev_pairs;

CREATE UNLOGGED TABLE _abbrev_pairs AS
SELECT DISTINCT ON (n.id)
    n.id AS dup_id,
    a.id AS keeper_id
FROM _abbrev_norm n
JOIN addresses a ON
    a.postcode_norm = n.postcode_norm
    AND _norm_street_abbrev(a.street) = n.norm_street
    AND UPPER(TRIM(COALESCE(a.house_number, ''))) = n.norm_hnum
    AND UPPER(TRIM(COALESCE(a.flat, ''))) = n.norm_flat
    AND UPPER(TRIM(COALESCE(a.house_name, ''))) = n.norm_hname
    AND a.id != n.id
    AND UPPER(TRIM(a.street)) = _norm_street_abbrev(a.street)
ORDER BY n.id, a.confidence DESC NULLS LAST, a.id ASC;

\echo 'Duplicate pairs found:'
SELECT COUNT(*) AS total_to_delete FROM _abbrev_pairs;

-- Sample preview
\echo ''
\echo 'Sample pairs:'
SELECT
    d.street AS dup_street,
    d.source AS dup_source,
    k.street AS keeper_street,
    k.source AS keeper_source,
    k.postcode_norm
FROM _abbrev_pairs p
JOIN addresses d ON d.id = p.dup_id
JOIN addresses k ON k.id = p.keeper_id
LIMIT 10;

-- Index pairs for fast FK updates
CREATE INDEX ON _abbrev_pairs (dup_id);
CREATE INDEX ON _abbrev_pairs (keeper_id);

-- ============================================================================
-- PHASE 4: FK reassignment + Delete (transaction)
-- ============================================================================
\echo ''
\echo '=== PHASE 4: Executing FK reassignment + deletion ==='
\echo 'Starting transaction...'

BEGIN;

\echo 'Reassigning price_paid FKs...'
UPDATE price_paid et
SET address_id = p.keeper_id
FROM _abbrev_pairs p
WHERE et.address_id = p.dup_id
  AND p.keeper_id != p.dup_id;

\echo 'Reassigning companies FKs...'
UPDATE companies et
SET address_id = p.keeper_id
FROM _abbrev_pairs p
WHERE et.address_id = p.dup_id
  AND p.keeper_id != p.dup_id;

\echo 'Reassigning food_ratings FKs...'
UPDATE food_ratings et
SET address_id = p.keeper_id
FROM _abbrev_pairs p
WHERE et.address_id = p.dup_id
  AND p.keeper_id != p.dup_id;

\echo 'Reassigning voa_ratings FKs...'
UPDATE voa_ratings et
SET address_id = p.keeper_id
FROM _abbrev_pairs p
WHERE et.address_id = p.dup_id
  AND p.keeper_id != p.dup_id;

\echo 'Deleting duplicate addresses...'
DELETE FROM addresses a
USING _abbrev_pairs p
WHERE a.id = p.dup_id;

COMMIT;

\echo ''
\echo '=== PHASE 4 COMPLETE ==='

-- ============================================================================
-- PHASE 5: Verify
-- ============================================================================
\echo ''
\echo '=== PHASE 5: Verification ==='

\echo 'Total addresses remaining:'
SELECT COUNT(*) AS total_addresses FROM addresses;

\echo ''
\echo 'Check SO230QD (St Leonards Road):'
SELECT house_number, street, city, source
FROM addresses WHERE postcode_norm = 'SO230QD'
ORDER BY street, house_number
LIMIT 20;

-- ============================================================================
-- Cleanup
-- ============================================================================
\echo ''
\echo '=== Cleanup ==='
DROP TABLE IF EXISTS _abbrev_norm, _abbrev_pairs;
DROP FUNCTION IF EXISTS _norm_street_abbrev(TEXT);

\echo 'Work tables and function dropped'
\echo ''
\echo 'To reclaim disk space:'
\echo '  VACUUM ANALYZE addresses;'
\echo ''
\echo '============================================'
\echo 'ABBREVIATION DEDUP COMPLETE'
\echo '============================================'
