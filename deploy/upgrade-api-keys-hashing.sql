-- ============================================================================
-- Upgrade api_keys to hashed storage.
-- Adds key_hash + key_prefix, backfills from any existing plaintext keys,
-- and relaxes the old NOT NULL/UNIQUE on the legacy plaintext `key` column.
-- Safe to run more than once.
-- ============================================================================

\timing on

-- pgcrypto provides digest() for the one-time backfill of existing keys.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS key_hash   VARCHAR(64);
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS key_prefix VARCHAR(16);

-- Backfill existing plaintext keys into the new hashed form.
UPDATE api_keys
   SET key_hash   = encode(digest(key, 'sha256'), 'hex'),
       key_prefix = left(key, 12)
 WHERE key_hash IS NULL
   AND key IS NOT NULL;

-- The legacy plaintext column is no longer required.
ALTER TABLE api_keys ALTER COLUMN key DROP NOT NULL;

-- Look-ups are by hash now.
CREATE UNIQUE INDEX IF NOT EXISTS ix_api_keys_key_hash ON api_keys (key_hash);

\echo ''
\echo '============================================'
\echo 'api_keys upgraded to hashed storage'
\echo '============================================'
