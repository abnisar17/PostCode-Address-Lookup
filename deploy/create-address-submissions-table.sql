-- ============================================================================
-- Create the address_submissions moderation queue.
-- User-submitted "missing addresses" land here as 'pending' and only enter the
-- live addresses table once an admin approves them. Safe to run more than once.
-- ============================================================================

\timing on

CREATE TABLE IF NOT EXISTS address_submissions (
    id            BIGSERIAL PRIMARY KEY,
    postcode_raw  VARCHAR(20),
    postcode_norm VARCHAR(10),
    house_number  VARCHAR(100),
    house_name    VARCHAR(200),
    flat          VARCHAR(50),
    street        VARCHAR(200),
    city          VARCHAR(100),
    county        VARCHAR(100),
    status        VARCHAR(20) NOT NULL DEFAULT 'pending',
    review_note   TEXT,
    submitter_ip  VARCHAR(45),
    address_id    INTEGER REFERENCES addresses(id),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    reviewed_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_address_submissions_status
    ON address_submissions (status);
CREATE INDEX IF NOT EXISTS ix_address_submissions_postcode_norm
    ON address_submissions (postcode_norm);

\echo ''
\echo '============================================'
\echo 'address_submissions table ready'
\echo '============================================'
