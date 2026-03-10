-- ============================================================================
-- Abbreviation Dedup DIAGNOSTIC (no data changes)
-- ============================================================================
-- Finds duplicates caused by abbreviation differences:
--   "St. Leonards Road" vs "Street Leonards Road" vs "Saint Leonards Road"
--   "High St" vs "High Street"
--   "Kings Rd" vs "Kings Road"
-- ============================================================================

\timing on

-- ============================================================================
-- Helper: Create a function to normalize street abbreviations
-- ============================================================================
\echo '=== Creating normalization function ==='

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
-- Step 1: Find addresses where normalization changes the street name
-- ============================================================================
\echo ''
\echo '=== Step 1: Building normalized street comparison table ==='

DROP TABLE IF EXISTS _abbrev_norm;

-- Only include addresses where normalization actually changes the street
-- This keeps the working set small
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

\echo 'Addresses with abbreviation differences:'
SELECT COUNT(*) AS changed_count FROM _abbrev_norm;

CREATE INDEX ON _abbrev_norm (postcode_norm, norm_street, norm_hnum, norm_flat, norm_hname);
\echo 'Index created'

-- ============================================================================
-- Step 2: Find matching addresses (same normalized street, different original)
-- ============================================================================
\echo ''
\echo '=== Step 2: Finding duplicate pairs ==='

DROP TABLE IF EXISTS _abbrev_pairs;

-- Find addresses in _abbrev_norm that match addresses with already-normalized streets
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
    -- Prefer the address whose raw street already matches the normalized form
    AND UPPER(TRIM(a.street)) = _norm_street_abbrev(a.street)
ORDER BY n.id, a.confidence DESC NULLS LAST, a.id ASC;

\echo ''
\echo '========================================='
\echo 'TOTAL ABBREVIATION DUPLICATES:'
SELECT COUNT(*) AS total_to_delete FROM _abbrev_pairs;
\echo '========================================='

-- ============================================================================
-- Step 3: Sample pairs
-- ============================================================================
\echo ''
\echo '=== Step 3: Sample duplicate pairs ==='

SELECT
    p.dup_id,
    p.keeper_id,
    d.source AS dup_source,
    d.street AS dup_street,
    d.house_number AS dup_hnum,
    k.source AS keeper_source,
    k.street AS keeper_street,
    k.house_number AS keeper_hnum,
    k.postcode_norm
FROM _abbrev_pairs p
JOIN addresses d ON d.id = p.dup_id
JOIN addresses k ON k.id = p.keeper_id
LIMIT 20;

-- ============================================================================
-- Step 4: Breakdown by source
-- ============================================================================
\echo ''
\echo '=== Step 4: Breakdown by source ==='

\echo 'Records to DELETE by source:'
SELECT d.source, COUNT(*) AS count
FROM _abbrev_pairs p
JOIN addresses d ON d.id = p.dup_id
GROUP BY d.source ORDER BY count DESC;

\echo ''
\echo 'Records to KEEP by source:'
SELECT k.source, COUNT(*) AS count
FROM _abbrev_pairs p
JOIN addresses k ON k.id = p.keeper_id
GROUP BY k.source ORDER BY count DESC;

-- ============================================================================
-- Step 5: Check SO230QD specifically
-- ============================================================================
\echo ''
\echo '=== Step 5: Check SO230QD ==='
\echo 'Current state:'
SELECT id, house_number, street, city, source
FROM addresses WHERE postcode_norm = 'SO230QD'
ORDER BY street, house_number
LIMIT 30;

\echo ''
\echo 'Which would be deleted:'
SELECT d.id, d.house_number, d.street, d.source, p.keeper_id
FROM _abbrev_pairs p
JOIN addresses d ON d.id = p.dup_id
WHERE d.postcode_norm = 'SO230QD';

-- Cleanup
DROP TABLE IF EXISTS _abbrev_norm, _abbrev_pairs;
DROP FUNCTION IF EXISTS _norm_street_abbrev(TEXT);

\echo ''
\echo '============================================'
\echo 'ABBREVIATION DIAGNOSTIC COMPLETE — No data changed'
\echo '============================================'
