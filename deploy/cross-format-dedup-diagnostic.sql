-- ============================================================================
-- Cross-Format Dedup DIAGNOSTIC ONLY (no data changes)
-- ============================================================================
-- Run this FIRST to see what would be affected before running the full script
-- ============================================================================

\timing on

\echo '=== Step 1: Finding addresses with number embedded in street ==='

DROP TABLE IF EXISTS _xfmt_embedded;

CREATE UNLOGGED TABLE _xfmt_embedded AS
SELECT
    id,
    postcode_norm,
    UPPER(TRIM(substring(street from '^\s*(\d+[A-Za-z]?)\s*[,\s]'))) AS xnum,
    UPPER(TRIM(regexp_replace(street, '^\s*\d+[A-Za-z]?\s*[,\s]+\s*', ''))) AS xstreet,
    UPPER(TRIM(COALESCE(flat, ''))) AS xflat,
    UPPER(TRIM(COALESCE(house_name, ''))) AS xhname
FROM addresses
WHERE postcode_norm IS NOT NULL
  AND (house_number IS NULL OR TRIM(house_number) = '')
  AND street ~ '^\s*\d+[A-Za-z]?\s*[,\s]'
  AND TRIM(regexp_replace(street, '^\s*\d+[A-Za-z]?\s*[,\s]+\s*', '')) != '';

\echo 'Candidates with number embedded in street:'
SELECT COUNT(*) AS embedded_count FROM _xfmt_embedded;

CREATE INDEX ON _xfmt_embedded (postcode_norm, xnum, xstreet, xflat, xhname);

-- ============================================================================
\echo ''
\echo '=== Step 2: Finding matching proper-format addresses ==='

DROP TABLE IF EXISTS _xfmt_pairs;

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

\echo 'Embedded-vs-proper duplicates:'
SELECT COUNT(*) AS embedded_vs_proper FROM _xfmt_pairs;

-- Embedded-vs-embedded
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
    AND e2.id < e1.id
WHERE NOT EXISTS (
    SELECT 1 FROM _xfmt_pairs p WHERE p.dup_id = e1.id
)
ORDER BY e1.id, e2.id ASC;

\echo ''
\echo '========================================='
\echo 'TOTAL DUPLICATES THAT WOULD BE DELETED:'
SELECT COUNT(*) AS total_to_delete FROM _xfmt_pairs;
\echo '========================================='

-- ============================================================================
\echo ''
\echo '=== Step 3: Sample duplicate pairs ==='

SELECT
    p.dup_id,
    p.keeper_id,
    d.source AS dup_source,
    d.house_number AS dup_housenum,
    d.street AS dup_street,
    k.source AS keeper_source,
    k.house_number AS keeper_housenum,
    k.street AS keeper_street,
    k.postcode_norm
FROM _xfmt_pairs p
JOIN addresses d ON d.id = p.dup_id
JOIN addresses k ON k.id = p.keeper_id
LIMIT 20;

-- ============================================================================
\echo ''
\echo '=== Step 4: Breakdown by source ==='

\echo 'Records to DELETE by source:'
SELECT d.source, COUNT(*) AS count
FROM _xfmt_pairs p
JOIN addresses d ON d.id = p.dup_id
GROUP BY d.source ORDER BY count DESC;

\echo ''
\echo 'Records to KEEP by source:'
SELECT k.source, COUNT(*) AS count
FROM _xfmt_pairs p
JOIN addresses k ON k.id = p.keeper_id
GROUP BY k.source ORDER BY count DESC;

-- ============================================================================
\echo ''
\echo '=== Step 5: Enrichment data on duplicates ==='
SELECT
    (SELECT COUNT(*) FROM price_paid pp JOIN _xfmt_pairs p ON pp.address_id = p.dup_id) AS price_paid_to_reassign,
    (SELECT COUNT(*) FROM companies c JOIN _xfmt_pairs p ON c.address_id = p.dup_id) AS companies_to_reassign,
    (SELECT COUNT(*) FROM food_ratings fr JOIN _xfmt_pairs p ON fr.address_id = p.dup_id) AS food_ratings_to_reassign,
    (SELECT COUNT(*) FROM voa_ratings vr JOIN _xfmt_pairs p ON vr.address_id = p.dup_id) AS voa_ratings_to_reassign;

-- ============================================================================
\echo ''
\echo '=== Step 6: Check specific postcode (HG1 2BP) ==='
\echo 'Current state:'
SELECT id, house_number, street, city, source
FROM addresses WHERE postcode_norm = 'HG12BP'
ORDER BY street, house_number;

\echo ''
\echo 'Which of these would be deleted:'
SELECT d.id, d.house_number, d.street, d.source, p.keeper_id
FROM _xfmt_pairs p
JOIN addresses d ON d.id = p.dup_id
WHERE d.postcode_norm = 'HG12BP';

-- Cleanup work tables
DROP TABLE IF EXISTS _xfmt_embedded, _xfmt_pairs;

\echo ''
\echo '============================================'
\echo 'DIAGNOSTIC COMPLETE — No data was changed'
\echo '============================================'
