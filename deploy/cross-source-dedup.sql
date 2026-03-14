-- ============================================================================
-- Cross-Source Dedup — Soft-mark duplicates (no data deletion)
-- ============================================================================
-- Handles duplicates across different government sources where:
--   - EPC puts house number IN street: "5, St. Leonards Road"
--   - Land Registry separates: house_number="5", street="Street Leonards Road"
--   - Companies House: street="33 ST LEONARDS ROAD"
--   - Abbreviations: St./St/Street before names like "Leonards"
--
-- Safety: Sets duplicate_of = keeper_id instead of deleting.
-- Undo:   UPDATE addresses SET duplicate_of = NULL;
-- ============================================================================

\timing on

-- ============================================================================
-- PHASE 1: Helper functions
-- ============================================================================
\echo '=== PHASE 1: Creating helper functions ==='

-- Extract leading number from street field (e.g. "5, St. Leonards Road" → "5")
CREATE OR REPLACE FUNCTION _extract_street_number(s TEXT) RETURNS TEXT AS $$
BEGIN
    IF s IS NULL THEN RETURN NULL; END IF;
    -- Match leading digits, optionally followed by a letter (e.g. "38a")
    RETURN (regexp_match(TRIM(s), '^\s*(\d+[a-zA-Z]?)\s*[,\s]'))[1];
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Strip leading number from street field (e.g. "5, St. Leonards Road" → "St. Leonards Road")
CREATE OR REPLACE FUNCTION _strip_street_number(s TEXT) RETURNS TEXT AS $$
BEGIN
    IF s IS NULL THEN RETURN NULL; END IF;
    RETURN TRIM(regexp_replace(TRIM(s), '^\s*\d+[a-zA-Z]?\s*[,\s]+\s*', '', ''));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Normalize street abbreviations
CREATE OR REPLACE FUNCTION _norm_street(s TEXT) RETURNS TEXT AS $$
DECLARE
    result TEXT;
BEGIN
    IF s IS NULL OR TRIM(s) = '' THEN RETURN ''; END IF;

    result := UPPER(TRIM(s));

    -- Remove dots (St. → ST)
    result := REPLACE(result, '.', '');

    -- Remove trailing commas
    result := regexp_replace(result, ',\s*$', '');

    -- Normalize "STREET" before another word → "SAINT" (Street Leonards → SAINT LEONARDS)
    result := regexp_replace(result, '\bSTREET\s+(?=[A-Z])', 'SAINT ', 'g');

    -- Normalize "ST" before another word → "SAINT" (St James → SAINT JAMES)
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

\echo 'Functions created'

-- ============================================================================
-- PHASE 2: Build unified normalization table
-- ============================================================================
\echo ''
\echo '=== PHASE 2: Building normalization table ==='

DROP TABLE IF EXISTS _dedup_norm;

CREATE UNLOGGED TABLE _dedup_norm AS
SELECT
    id,
    postcode_norm,
    source,
    confidence,
    -- Normalized house number: explicit house_number OR extracted from street
    UPPER(TRIM(COALESCE(
        NULLIF(TRIM(COALESCE(house_number, '')), ''),
        _extract_street_number(street)
    , ''))) AS norm_hnum,
    -- Normalized street: apply normalization to street (strip number if no house_number)
    _norm_street(
        CASE WHEN house_number IS NOT NULL AND TRIM(house_number) != ''
             THEN street
             ELSE _strip_street_number(street)
        END
    ) AS norm_street,
    UPPER(TRIM(COALESCE(flat, ''))) AS norm_flat,
    UPPER(TRIM(COALESCE(house_name, ''))) AS norm_hname
FROM addresses
WHERE postcode_norm IS NOT NULL
  AND street IS NOT NULL AND TRIM(street) != ''
  AND duplicate_of IS NULL;

\echo 'Normalization table built:'
SELECT COUNT(*) AS total_rows FROM _dedup_norm;

CREATE INDEX ON _dedup_norm (postcode_norm, norm_street, norm_hnum, norm_flat, norm_hname);
CREATE INDEX ON _dedup_norm (id);
\echo 'Indexes created'

-- ============================================================================
-- PHASE 3: Find duplicate groups (keep best record)
-- ============================================================================
\echo ''
\echo '=== PHASE 3: Finding duplicate pairs ==='

DROP TABLE IF EXISTS _dedup_groups;

-- First, find which normalized keys have more than 1 address
CREATE UNLOGGED TABLE _dedup_groups AS
SELECT postcode_norm, norm_street, norm_hnum, norm_flat, norm_hname
FROM _dedup_norm
WHERE norm_street != ''
GROUP BY postcode_norm, norm_street, norm_hnum, norm_flat, norm_hname
HAVING COUNT(*) > 1;

\echo 'Duplicate groups found:'
SELECT COUNT(*) AS groups_with_duplicates FROM _dedup_groups;

CREATE INDEX ON _dedup_groups (postcode_norm, norm_street, norm_hnum, norm_flat, norm_hname);

-- Now find the keeper for each group (highest confidence, then lowest id)
DROP TABLE IF EXISTS _dedup_pairs;

CREATE UNLOGGED TABLE _dedup_pairs AS
WITH ranked AS (
    SELECT
        n.id,
        n.postcode_norm,
        n.norm_street,
        n.norm_hnum,
        n.norm_flat,
        n.norm_hname,
        ROW_NUMBER() OVER (
            PARTITION BY n.postcode_norm, n.norm_street, n.norm_hnum, n.norm_flat, n.norm_hname
            ORDER BY n.confidence DESC NULLS LAST, n.id ASC
        ) AS rn,
        FIRST_VALUE(n.id) OVER (
            PARTITION BY n.postcode_norm, n.norm_street, n.norm_hnum, n.norm_flat, n.norm_hname
            ORDER BY n.confidence DESC NULLS LAST, n.id ASC
        ) AS keeper_id
    FROM _dedup_norm n
    INNER JOIN _dedup_groups g USING (postcode_norm, norm_street, norm_hnum, norm_flat, norm_hname)
)
SELECT id AS dup_id, keeper_id
FROM ranked
WHERE rn > 1;

\echo ''
\echo '========================================='
\echo 'TOTAL DUPLICATES TO MARK:'
SELECT COUNT(*) AS total_to_mark FROM _dedup_pairs;
\echo '========================================='

-- ============================================================================
-- PHASE 4: Sample preview
-- ============================================================================
\echo ''
\echo '=== PHASE 4: Sample duplicate pairs ==='

SELECT
    d.street AS dup_street,
    d.house_number AS dup_hnum,
    d.source AS dup_source,
    k.street AS keeper_street,
    k.house_number AS keeper_hnum,
    k.source AS keeper_source,
    k.postcode_norm
FROM _dedup_pairs p
JOIN addresses d ON d.id = p.dup_id
JOIN addresses k ON k.id = p.keeper_id
LIMIT 20;

\echo ''
\echo 'Duplicates by source (to be marked):'
SELECT d.source, COUNT(*) AS count
FROM _dedup_pairs p
JOIN addresses d ON d.id = p.dup_id
GROUP BY d.source ORDER BY count DESC;

\echo ''
\echo 'Keepers by source:'
SELECT k.source, COUNT(DISTINCT k.id) AS count
FROM _dedup_pairs p
JOIN addresses k ON k.id = p.keeper_id
GROUP BY k.source ORDER BY count DESC;

-- ============================================================================
-- PHASE 5: Check SO23 0QD before marking
-- ============================================================================
\echo ''
\echo '=== PHASE 5: Check SO23 0QD (before) ==='
SELECT id, house_number, street, source, confidence
FROM addresses
WHERE postcode_norm = 'SO23 0QD' AND duplicate_of IS NULL
ORDER BY street, house_number
LIMIT 30;

\echo ''
\echo 'Which will be marked as duplicates:'
SELECT d.id, d.house_number, d.street, d.source, p.keeper_id
FROM _dedup_pairs p
JOIN addresses d ON d.id = p.dup_id
WHERE d.postcode_norm = 'SO23 0QD';

-- ============================================================================
-- PHASE 6: Mark duplicates (transaction-safe)
-- ============================================================================
\echo ''
\echo '=== PHASE 6: Marking duplicates ==='
\echo 'Starting transaction...'

BEGIN;

UPDATE addresses a
SET duplicate_of = p.keeper_id
FROM _dedup_pairs p
WHERE a.id = p.dup_id;

COMMIT;

\echo 'Duplicates marked!'

-- ============================================================================
-- PHASE 7: Verify
-- ============================================================================
\echo ''
\echo '=== PHASE 7: Verification ==='

\echo 'Total addresses (all):'
SELECT COUNT(*) AS total FROM addresses;

\echo ''
\echo 'Total unique (non-duplicate) addresses:'
SELECT COUNT(*) AS unique_addresses FROM addresses WHERE duplicate_of IS NULL;

\echo ''
\echo 'Total marked as duplicate:'
SELECT COUNT(*) AS duplicates FROM addresses WHERE duplicate_of IS NOT NULL;

\echo ''
\echo 'SO23 0QD after dedup (visible addresses):'
SELECT id, house_number, street, source, confidence
FROM addresses
WHERE postcode_norm = 'SO23 0QD' AND duplicate_of IS NULL
ORDER BY street, house_number
LIMIT 30;

-- ============================================================================
-- Cleanup
-- ============================================================================
\echo ''
\echo '=== Cleanup ==='
DROP TABLE IF EXISTS _dedup_norm, _dedup_groups, _dedup_pairs;
DROP FUNCTION IF EXISTS _extract_street_number(TEXT);
DROP FUNCTION IF EXISTS _strip_street_number(TEXT);
DROP FUNCTION IF EXISTS _norm_street(TEXT);
\echo 'Work tables and functions dropped'

\echo ''
\echo 'To undo all dedup marks (instant):'
\echo '  UPDATE addresses SET duplicate_of = NULL;'
\echo ''
\echo '============================================'
\echo 'CROSS-SOURCE DEDUP COMPLETE'
\echo '============================================'
