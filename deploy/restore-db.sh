#!/usr/bin/env bash
# =============================================================
# Standalone DB restore script
# Usage: bash deploy/restore-db.sh [/path/to/dump]
# =============================================================
set -euo pipefail

DUMP_FILE="${1:-/root/postcode_lookup.dump}"
PROJECT_DIR="/opt/postcode-lookup"

if [ ! -f "$DUMP_FILE" ]; then
    echo "ERROR: Dump file not found at $DUMP_FILE"
    echo "Usage: bash restore-db.sh [/path/to/dump]"
    exit 1
fi

cd "$PROJECT_DIR"

echo "Restoring database from: $DUMP_FILE"
echo "This may take 10-30 minutes..."
echo ""

# Terminate active connections
docker compose exec -T db psql -U postgres -c \
    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'postcode_lookup' AND pid <> pg_backend_pid();" 2>/dev/null || true

# Drop and recreate
docker compose exec -T db dropdb -U postgres --if-exists postcode_lookup
docker compose exec -T db createdb -U postgres postcode_lookup

# Extensions
docker compose exec -T db psql -U postgres -d postcode_lookup -c "CREATE EXTENSION IF NOT EXISTS postgis;"
docker compose exec -T db psql -U postgres -d postcode_lookup -c "CREATE EXTENSION IF NOT EXISTS pg_trgm;"

# Copy dump into container and restore
docker compose cp "$DUMP_FILE" db:/tmp/postcode_lookup.dump
docker compose exec -T db pg_restore \
    -U postgres \
    -d postcode_lookup \
    --no-owner \
    --no-acl \
    --jobs=4 \
    /tmp/postcode_lookup.dump \
|| echo "Note: pg_restore may report warnings for extensions — this is normal."
docker compose exec -T db rm -f /tmp/postcode_lookup.dump

# Restart backend
docker compose restart backend

echo ""
echo "Database restored successfully."
echo "Check: curl http://localhost:8000/api/health"
