-- ============================================================================
-- Attribute API-added addresses: add addresses.added_by_key_id.
-- Records which API key inserted a row (source='api'), so a partner's data can
-- be traced and, if needed, bulk-removed. Safe to run more than once.
-- Adding a nullable column with no default is an instant metadata change.
-- ============================================================================

\timing on

ALTER TABLE addresses ADD COLUMN IF NOT EXISTS added_by_key_id INTEGER REFERENCES api_keys(id);

-- Partial index stays tiny (only api-added rows have a value).
CREATE INDEX IF NOT EXISTS ix_addresses_added_by_key
    ON addresses (added_by_key_id) WHERE added_by_key_id IS NOT NULL;

\echo ''
\echo '============================================'
\echo 'addresses.added_by_key_id ready'
\echo '============================================'
