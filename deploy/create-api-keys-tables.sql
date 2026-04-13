-- ============================================================================
-- Create API key management tables
-- ============================================================================

\timing on

\echo '=== Creating api_keys table ==='
CREATE TABLE IF NOT EXISTS api_keys (
    id SERIAL PRIMARY KEY,
    key VARCHAR(64) NOT NULL UNIQUE,
    user_name VARCHAR(100) NOT NULL,
    email VARCHAR(200),
    is_active BOOLEAN NOT NULL DEFAULT true,
    rate_limit_per_day INTEGER NOT NULL DEFAULT 10000,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_api_keys_key ON api_keys (key);

\echo '=== Creating api_usage table ==='
CREATE TABLE IF NOT EXISTS api_usage (
    id BIGSERIAL PRIMARY KEY,
    api_key_id INTEGER NOT NULL REFERENCES api_keys(id),
    endpoint VARCHAR(200) NOT NULL,
    method VARCHAR(10) NOT NULL,
    query_params TEXT,
    status_code INTEGER NOT NULL,
    response_time_ms INTEGER,
    ip_address VARCHAR(45),
    timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_api_usage_key_timestamp ON api_usage (api_key_id, timestamp);
CREATE INDEX IF NOT EXISTS ix_api_usage_api_key_id ON api_usage (api_key_id);

\echo ''
\echo '============================================'
\echo 'API KEY TABLES CREATED'
\echo '============================================'
