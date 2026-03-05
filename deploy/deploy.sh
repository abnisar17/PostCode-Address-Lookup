#!/usr/bin/env bash
# =============================================================
# PostcodeAddressLookup — Server Deployment Script
# Run this on the server: bash deploy.sh
# =============================================================
set -euo pipefail

SERVER_IP=$(hostname -I | awk '{print $1}')
PROJECT_DIR="/opt/postcode-lookup"
DUMP_FILE="/root/postcode_lookup.dump"

echo "==========================================="
echo " PostcodeAddressLookup Deployment"
echo " Server: $SERVER_IP"
echo "==========================================="

# ----------------------------------------------------------
# 1. Install Docker if not present
# ----------------------------------------------------------
if ! command -v docker &>/dev/null; then
    echo ""
    echo "[1/7] Installing Docker..."
    apt-get update
    apt-get install -y ca-certificates curl gnupg
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" > /etc/apt/sources.list.d/docker.list
    apt-get update
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
    systemctl enable docker
    systemctl start docker
    echo "    Docker installed."
else
    echo ""
    echo "[1/7] Docker already installed. Skipping."
fi

# ----------------------------------------------------------
# 2. Install Nginx (host reverse proxy) if not present
# ----------------------------------------------------------
if ! command -v nginx &>/dev/null; then
    echo ""
    echo "[2/7] Installing Nginx..."
    apt-get update
    apt-get install -y nginx
    systemctl enable nginx
    echo "    Nginx installed."
else
    echo ""
    echo "[2/7] Nginx already installed. Skipping."
fi

# ----------------------------------------------------------
# 3. Configure Nginx reverse proxy
# ----------------------------------------------------------
echo ""
echo "[3/7] Configuring Nginx reverse proxy (port 80 → Docker services)..."

# Remove default site if it exists
rm -f /etc/nginx/sites-enabled/default

# Copy our proxy config
cp "$PROJECT_DIR/deploy/nginx-proxy.conf" /etc/nginx/sites-available/postcode-lookup.conf 2>/dev/null \
    || cp "$PROJECT_DIR/deploy/nginx-proxy.conf" /etc/nginx/conf.d/postcode-lookup.conf

# Enable site (sites-available/sites-enabled style)
if [ -d /etc/nginx/sites-enabled ]; then
    ln -sf /etc/nginx/sites-available/postcode-lookup.conf /etc/nginx/sites-enabled/postcode-lookup.conf
fi

nginx -t
systemctl restart nginx
echo "    Nginx configured: port 80 → frontend:3000 + /api → backend:8000"

# ----------------------------------------------------------
# 4. Create production .env
# ----------------------------------------------------------
echo ""
echo "[4/7] Creating production .env..."

cat > "$PROJECT_DIR/.env" <<'ENVEOF'
POSTGRES_PASSWORD=postgres
VITE_API_BASE_URL=http://PLACEHOLDER_IP
LOG_LEVEL=INFO
LOG_FORMAT=json
ENVEOF

# Replace placeholder with actual server IP
sed -i "s|PLACEHOLDER_IP|${SERVER_IP}|g" "$PROJECT_DIR/.env"

echo "    .env created with VITE_API_BASE_URL=http://${SERVER_IP}"

# ----------------------------------------------------------
# 5. Stop old containers (if any)
# ----------------------------------------------------------
echo ""
echo "[5/7] Stopping old containers..."
cd "$PROJECT_DIR"
docker compose down 2>/dev/null || true
echo "    Old containers stopped."

# ----------------------------------------------------------
# 6. Build and start services
# ----------------------------------------------------------
echo ""
echo "[6/7] Building and starting Docker services (production mode)..."
docker compose -f docker-compose.yml up --build -d

echo "    Waiting for database to be healthy..."
for i in $(seq 1 30); do
    if docker compose exec -T db pg_isready -U postgres &>/dev/null; then
        echo "    Database is ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "    ERROR: Database did not become healthy in 30 seconds."
        exit 1
    fi
    sleep 2
done

# ----------------------------------------------------------
# 7. Restore database from dump (if dump file exists)
# ----------------------------------------------------------
if [ -f "$DUMP_FILE" ]; then
    echo ""
    echo "[7/7] Restoring database from dump file..."
    echo "    This may take 10-30 minutes for a large database."
    echo ""

    # Drop and recreate database
    docker compose exec -T db psql -U postgres -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'postcode_lookup' AND pid <> pg_backend_pid();" 2>/dev/null || true
    docker compose exec -T db dropdb -U postgres --if-exists postcode_lookup
    docker compose exec -T db createdb -U postgres postcode_lookup

    # Enable extensions
    docker compose exec -T db psql -U postgres -d postcode_lookup -c "CREATE EXTENSION IF NOT EXISTS postgis;"
    docker compose exec -T db psql -U postgres -d postcode_lookup -c "CREATE EXTENSION IF NOT EXISTS pg_trgm;"

    # Copy dump file into the DB container, then restore inside it
    echo "    Copying dump into db container..."
    docker compose cp "$DUMP_FILE" db:/tmp/postcode_lookup.dump

    echo "    Restoring... (this is the slow part)"
    docker compose exec -T db pg_restore \
        -U postgres \
        -d postcode_lookup \
        --no-owner \
        --no-acl \
        --jobs=4 \
        /tmp/postcode_lookup.dump \
    || echo "    Note: pg_restore may report warnings for extensions — this is normal."

    # Clean up dump inside container
    docker compose exec -T db rm -f /tmp/postcode_lookup.dump

    echo "    Database restored."

    # Restart backend so it picks up the restored data
    docker compose restart backend
    sleep 3
else
    echo ""
    echo "[7/7] No dump file found at $DUMP_FILE — skipping DB restore."
    echo "    To restore later, place the dump at $DUMP_FILE and run:"
    echo "    cd $PROJECT_DIR && bash deploy/restore-db.sh"
fi

# ----------------------------------------------------------
# Done
# ----------------------------------------------------------
echo ""
echo "==========================================="
echo " Deployment complete!"
echo "==========================================="
echo ""
echo " Frontend:  http://${SERVER_IP}/"
echo " API:       http://${SERVER_IP}/api/health"
echo ""
echo " Useful commands:"
echo "   cd $PROJECT_DIR"
echo "   docker compose logs -f          # View logs"
echo "   docker compose ps               # Check status"
echo "   docker compose exec db psql -U postgres -d postcode_lookup  # DB shell"
echo ""
