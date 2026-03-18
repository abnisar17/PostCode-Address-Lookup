-- ============================================================================
-- Fix EPC house names — Move house names from street to house_name field
-- ============================================================================
-- EPC records have house/building names in the wrong fields:
--   Current:  street="Pebbles", suburb="St. Leonards Road", house_name=null
--   Correct:  house_name="Pebbles", street="St Leonards Road", suburb=null
--
-- Pattern: suburb looks like a street (ends with Road/Lane/etc),
--          street does NOT look like a street (it's a building name),
--          house_name is empty.
--
-- Safety: All original values backed up. Fully reversible.
-- ============================================================================

\timing on
SET statement_timeout = '0';

-- ============================================================================
-- PHASE 1: Backup affected rows
-- ============================================================================
\echo '=== PHASE 1: Backing up affected rows ==='

CREATE TABLE IF NOT EXISTS _epc_hname_backup (
    address_id   BIGINT PRIMARY KEY,
    house_name   TEXT,
    street       TEXT,
    suburb       TEXT,
    backed_up_at TIMESTAMPTZ DEFAULT now()
);

INSERT INTO _epc_hname_backup (address_id, house_name, street, suburb)
SELECT id, house_name, street, suburb
FROM addresses
WHERE source = 'epc'
  AND suburb IS NOT NULL
  AND suburb ~ '(Road|Street|Lane|Avenue|Drive|Close|Way|Place|Crescent|Court|Terrace|Grove|Gardens|Mews|Rise|Hill|Park|Square|Walk)\s*$'
  AND street !~ '(Road|Street|Lane|Avenue|Drive|Close|Way|Place|Crescent|Court|Terrace|Grove|Gardens|Mews|Rise|Hill|Park|Square|Walk)\s*$'
  AND (house_name IS NULL OR TRIM(house_name) = '')
ON CONFLICT (address_id) DO NOTHING;

\echo 'Rows backed up:'
SELECT COUNT(*) AS backed_up FROM _epc_hname_backup;

-- ============================================================================
-- PHASE 2: Preview SO23 0QD before fix
-- ============================================================================
\echo ''
\echo '=== PHASE 2: Preview SO23 0QD (before) ==='
SELECT id, house_name, street, suburb
FROM addresses
WHERE postcode_norm = 'SO23 0QD'
  AND source = 'epc'
  AND suburb IS NOT NULL
  AND (house_name IS NULL OR TRIM(house_name) = '');

-- ============================================================================
-- PHASE 3: Apply fix (batched)
-- ============================================================================
\echo ''
\echo '=== PHASE 3: Applying fix (batched) ==='

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
        SET house_name = street,
            street = _canonical_street(suburb),
            suburb = NULL
        WHERE id > batch_start AND id <= batch_start + batch_size
          AND source = 'epc'
          AND suburb IS NOT NULL
          AND suburb ~ '(Road|Street|Lane|Avenue|Drive|Close|Way|Place|Crescent|Court|Terrace|Grove|Gardens|Mews|Rise|Hill|Park|Square|Walk)\s*$'
          AND street !~ '(Road|Street|Lane|Avenue|Drive|Close|Way|Place|Crescent|Court|Terrace|Grove|Gardens|Mews|Rise|Hill|Park|Square|Walk)\s*$'
          AND (house_name IS NULL OR TRIM(house_name) = '');

        GET DIAGNOSTICS batch_updated = ROW_COUNT;
        total_updated := total_updated + batch_updated;
        batch_start := batch_start + batch_size;

        RAISE NOTICE 'Batch % to %: % rows updated (total: %)',
            batch_start - batch_size, batch_start, batch_updated, total_updated;
    END LOOP;

    RAISE NOTICE 'COMPLETE — total rows updated: %', total_updated;
END $$;

\echo 'Fix applied'

-- ============================================================================
-- PHASE 4: Verify SO23 0QD
-- ============================================================================
\echo ''
\echo '=== PHASE 4: Verification SO23 0QD ==='
SELECT id, house_name, street, suburb, source
FROM addresses
WHERE postcode_norm = 'SO23 0QD' AND duplicate_of IS NULL
ORDER BY
    street,
    CAST(NULLIF(regexp_replace(COALESCE(house_number, ''), '[^0-9]', '', 'g'), '') AS INTEGER) NULLS LAST,
    house_name;

\echo ''
\echo '============================================'
\echo 'EPC HOUSE NAME FIX COMPLETE'
\echo '============================================'
\echo ''
\echo 'To undo:'
\echo '  UPDATE addresses a SET house_name = b.house_name, street = b.street, suburb = b.suburb FROM _epc_hname_backup b WHERE a.id = b.address_id;'
\echo ''
\echo 'To cleanup after verification:'
\echo '  DROP TABLE IF EXISTS _epc_hname_backup;'
