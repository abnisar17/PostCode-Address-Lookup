-- ============================================================================
-- Cross-Format Address Deduplication
-- ============================================================================
-- Problem: Same address stored differently across sources:
--   EPC:           house_number=NULL,  street="1, Fewston Crescent"
--   Land Registry: house_number="1",   street="Fewston Crescent"
--
-- Strategy: Targeted JOIN (NOT full window function over 42M rows)
--   1. Find addresses where number is embedded in street (small subset)
--   2. JOIN against addresses with explicit house_number
--   3. Delete the less-structured record, keep the proper one
--
-- Safety: backup table, diagnostic counts, transaction-wrapped mutations
-- Performance: Uses indexes, no window function over full table
-- ============================================================================

\timing on

-- ============================================================================
-- PHASE 1: CREATE WORK TABLE (candidates with number embedded in street)
-- ============================================================================
\echo '=== PHASE 1: Finding addresses with number embedded in street ==='

DROP TABLE IF EXISTS _xfmt_embedded;

CREATE UNLOGGED TABLE _xfmt_embedded AS
SELECT
    id,
    postcode_norm,
    -- Extract leading number: "1, Fewston Crescent" → "1", "10A, High St" → "10A"
    UPPER(TRIM(substring(street from '^\s*(\d+[A-Za-z]?)\s*[,\s]'))) AS xnum,
    -- Strip leading number+separator: "1, Fewston Crescent" → "FEWSTON CRESCENT"
    UPPER(TRIM(regexp_replace(street, '^\s*\d+[A-Za-z]?\s*[,\s]+\s*', ''))) AS xstreet,
    UPPER(TRIM(COALESCE(flat, ''))) AS xflat,
    UPPER(TRIM(COALESCE(house_name, ''))) AS xhname
FROM addresses
WHERE postcode_norm IS NOT NULL
  AND (house_number IS NULL OR TRIM(house_number) = '')
  AND street ~ '^\s*\d+[A-Za-z]?\s*[,\s]'
  AND TRIM(regexp_replace(street, '^\s*\d+[A-Za-z]?\s*[,\s]+\s*', '')) != '';

\echo 'Embedded candidates found:'
SELECT COUNT(*) AS embedded_count FROM _xfmt_embedded;

-- Index for efficient JOIN
CREATE INDEX ON _xfmt_embedded (postcode_norm, xnum, xstreet, xflat, xhname);

\echo 'Index created on work table'

-- ============================================================================
-- PHASE 2: FIND MATCHING PAIRS (embedded record → proper record)
-- ============================================================================
\echo ''
\echo '=== PHASE 2: Finding matching proper-format addresses ==='

DROP TABLE IF EXISTS _xfmt_pairs;

-- For each embedded record, find the best matching proper record
-- DISTINCT ON (e.id) ensures each duplicate maps to exactly one keeper
CREATE UNLOGGED TABLE _xfmt_pairs AS
SELECT DISTINCT ON (e.id)
    e.id AS dup_id,
    a.id AS keeper_id
FROM _xfmt_embedded e
JOIN addresses a ON
    a.postcode_norm = e.postcode_norm
    AND UPPER(TRIM(a.house_number)) = e.xnum
    AND UPPER(TRIM(a.street)) = e.xstreet
    AND UPPER(TRIM(COALESCE(a.flat, ''))) = e.xflat
    AND UPPER(TRIM(COALESCE(a.house_name, ''))) = e.xhname
    AND a.house_number IS NOT NULL
    AND TRIM(a.house_number) != ''
    AND a.id != e.id
ORDER BY e.id, a.confidence DESC NULLS LAST, a.id ASC;

\echo 'Cross-format duplicate pairs found:'
SELECT COUNT(*) AS pairs_to_delete FROM _xfmt_pairs;

-- Also handle embedded-vs-embedded duplicates (e.g. "1, Fewston Crescent" vs "1 Fewston Crescent")
-- Both have house_number=NULL, but after normalization they match
\echo ''
\echo 'Checking for embedded-vs-embedded duplicates...'

INSERT INTO _xfmt_pairs (dup_id, keeper_id)
SELECT DISTINCT ON (e1.id)
    e1.id AS dup_id,
    e2.id AS keeper_id
FROM _xfmt_embedded e1
JOIN _xfmt_embedded e2 ON
    e1.postcode_norm = e2.postcode_norm
    AND e1.xstreet = e2.xstreet
    AND e1.xnum = e2.xnum
    AND e1.xflat = e2.xflat
    AND e1.xhname = e2.xhname
    AND e2.id < e1.id  -- keep the lower id
WHERE NOT EXISTS (
    -- Skip if already marked as duplicate from Phase 2
    SELECT 1 FROM _xfmt_pairs p WHERE p.dup_id = e1.id
)
ORDER BY e1.id, e2.id ASC;

\echo 'Total duplicates after both checks:'
SELECT COUNT(*) AS total_to_delete FROM _xfmt_pairs;

-- ============================================================================
-- PHASE 3: DIAGNOSTIC — Preview samples (READ-ONLY, no changes)
-- ============================================================================
\echo ''
\echo '=== PHASE 3: Sample preview of 15 duplicate pairs ==='
\echo '(dup = will be deleted, keeper = will be kept)'
\echo ''

SELECT
    p.dup_id,
    p.keeper_id,
    d.source AS dup_source,
    d.house_number AS dup_house_num,
    d.street AS dup_street,
    d.city AS dup_city,
    k.source AS keeper_source,
    k.house_number AS keeper_house_num,
    k.street AS keeper_street,
    k.city AS keeper_city,
    k.postcode_norm
FROM _xfmt_pairs p
JOIN addresses d ON d.id = p.dup_id
JOIN addresses k ON k.id = p.keeper_id
LIMIT 15;

-- Show which sources are affected
\echo ''
\echo 'Duplicates by source (these will be DELETED):'
SELECT d.source, COUNT(*) AS count
FROM _xfmt_pairs p
JOIN addresses d ON d.id = p.dup_id
GROUP BY d.source
ORDER BY count DESC;

\echo ''
\echo 'Keepers by source (these will be KEPT):'
SELECT k.source, COUNT(*) AS count
FROM _xfmt_pairs p
JOIN addresses k ON k.id = p.keeper_id
GROUP BY k.source
ORDER BY count DESC;

-- Check enrichment data on duplicates (would be reassigned, not lost)
\echo ''
\echo 'Enrichment records on duplicate addresses (will be REASSIGNED to keeper):'
SELECT
    (SELECT COUNT(*) FROM price_paid pp JOIN _xfmt_pairs p ON pp.address_id = p.dup_id) AS price_paid_to_reassign,
    (SELECT COUNT(*) FROM companies c JOIN _xfmt_pairs p ON c.address_id = p.dup_id) AS companies_to_reassign,
    (SELECT COUNT(*) FROM food_ratings fr JOIN _xfmt_pairs p ON fr.address_id = p.dup_id) AS food_ratings_to_reassign,
    (SELECT COUNT(*) FROM voa_ratings vr JOIN _xfmt_pairs p ON vr.address_id = p.dup_id) AS voa_ratings_to_reassign;

-- ============================================================================
-- PHASE 4: BACKUP (save all rows that will be deleted)
-- ============================================================================
\echo ''
\echo '=== PHASE 4: Creating backup of addresses to be deleted ==='

DROP TABLE IF EXISTS _xfmt_backup;

CREATE TABLE _xfmt_backup AS
SELECT a.*, p.keeper_id
FROM addresses a
JOIN _xfmt_pairs p ON a.id = p.dup_id;

\echo 'Backup created:'
SELECT COUNT(*) AS backed_up_rows FROM _xfmt_backup;
\echo ''
\echo '>>> To RESTORE if needed: INSERT INTO addresses SELECT id,postcode_id,postcode_raw,postcode_norm,house_number,house_name,flat,street,suburb,city,county,location,latitude,longitude,source,source_id,uprn,confidence,is_complete,created_at FROM _xfmt_backup;'

-- ============================================================================
-- PHASE 5: EXECUTE — FK reassignment + Delete (all in transaction)
-- ============================================================================
\echo ''
\echo '=== PHASE 5: Executing FK reassignment + deletion ==='
\echo 'Starting transaction...'

BEGIN;

-- Reassign enrichment FKs from duplicate → keeper
\echo 'Reassigning price_paid FKs...'
UPDATE price_paid et
SET address_id = p.keeper_id
FROM _xfmt_pairs p
WHERE et.address_id = p.dup_id
  AND p.keeper_id != p.dup_id;

\echo 'Reassigning companies FKs...'
UPDATE companies et
SET address_id = p.keeper_id
FROM _xfmt_pairs p
WHERE et.address_id = p.dup_id
  AND p.keeper_id != p.dup_id;

\echo 'Reassigning food_ratings FKs...'
UPDATE food_ratings et
SET address_id = p.keeper_id
FROM _xfmt_pairs p
WHERE et.address_id = p.dup_id
  AND p.keeper_id != p.dup_id;

\echo 'Reassigning voa_ratings FKs...'
UPDATE voa_ratings et
SET address_id = p.keeper_id
FROM _xfmt_pairs p
WHERE et.address_id = p.dup_id
  AND p.keeper_id != p.dup_id;

\echo 'Deleting duplicate addresses...'
DELETE FROM addresses a
USING _xfmt_pairs p
WHERE a.id = p.dup_id;

COMMIT;

\echo ''
\echo '=== PHASE 5 COMPLETE ==='

-- ============================================================================
-- PHASE 6: VERIFY
-- ============================================================================
\echo ''
\echo '=== PHASE 6: Verification ==='

\echo 'Total addresses remaining:'
SELECT COUNT(*) AS total_addresses FROM addresses;

\echo ''
\echo 'Checking a known duplicate postcode (HG1 2BP) — should have fewer entries now:'
SELECT house_number, street, city, source
FROM addresses
WHERE postcode_norm = 'HG12BP'
ORDER BY street, house_number
LIMIT 20;

-- ============================================================================
-- CLEANUP (optional — run after verifying everything is correct)
-- ============================================================================
\echo ''
\echo '=== Cleanup ==='
\echo 'Work tables kept for safety. To clean up later, run:'
\echo '  DROP TABLE IF EXISTS _xfmt_embedded, _xfmt_pairs, _xfmt_backup;'
\echo ''
\echo 'To reclaim disk space, run:'
\echo '  VACUUM ANALYZE addresses;'
\echo ''
\echo '============================================'
\echo 'CROSS-FORMAT DEDUP COMPLETE'
\echo '============================================'
