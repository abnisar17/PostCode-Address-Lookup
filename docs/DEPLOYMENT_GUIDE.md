# UK Postcode & Address Lookup — Deployment Guide

Complete step-by-step guide for running the project on a local machine and deploying it to a production server.

---

## Table of Contents

- [Part A: Running Locally on Another Machine](#part-a-running-locally-on-another-machine)
  - [A1. Prerequisites](#a1-prerequisites)
  - [A2. Preparing the Project ZIP](#a2-preparing-the-project-zip)
  - [A3. Extracting and Setting Up](#a3-extracting-and-setting-up)
  - [A4. Starting the Services](#a4-starting-the-services)
  - [A5. Restoring the Database](#a5-restoring-the-database)
  - [A6. Verifying Everything Works](#a6-verifying-everything-works)
  - [A7. Stopping and Restarting](#a7-stopping-and-restarting)
- [Part B: Deploying to a Production Server](#part-b-deploying-to-a-production-server)
  - [B1. Server Requirements](#b1-server-requirements)
  - [B2. Exporting the Database from Local](#b2-exporting-the-database-from-local)
  - [B3. Connecting to the Server via SSH](#b3-connecting-to-the-server-via-ssh)
  - [B4. Investigating the Existing Setup](#b4-investigating-the-existing-setup)
  - [B5. Stopping the Old Deployment](#b5-stopping-the-old-deployment)
  - [B6. Uploading the Project to the Server](#b6-uploading-the-project-to-the-server)
  - [B7. Uploading the Database Dump](#b7-uploading-the-database-dump)
  - [B8. Installing Docker on the Server](#b8-installing-docker-on-the-server)
  - [B9. Installing Nginx on the Server](#b9-installing-nginx-on-the-server)
  - [B10. Configuring the Nginx Reverse Proxy](#b10-configuring-the-nginx-reverse-proxy)
  - [B11. Creating the Production Environment File](#b11-creating-the-production-environment-file)
  - [B12. Building and Starting Containers](#b12-building-and-starting-containers)
  - [B13. Restoring the Database on the Server](#b13-restoring-the-database-on-the-server)
  - [B14. Verifying the Production Deployment](#b14-verifying-the-production-deployment)
  - [B15. Cleaning Up](#b15-cleaning-up)
  - [B16. Server Management Commands](#b16-server-management-commands)
- [Part C: Troubleshooting](#part-c-troubleshooting)

---

## Part A: Running Locally on Another Machine

### A1. Prerequisites

Before you begin, install the following on the target machine:

#### Docker Desktop (Required)

Docker runs the entire application (database, backend, frontend) in containers. No need to install Python, Node.js, or PostgreSQL manually.

- **Windows**: Download from https://www.docker.com/products/docker-desktop/
  - During installation, enable "Use WSL 2 instead of Hyper-V" if prompted
  - After installation, open Docker Desktop and wait for it to show "Docker Desktop is running"
  - Minimum requirements: Windows 10 64-bit (Build 19041+), 4 GB RAM

- **macOS**: Download from https://www.docker.com/products/docker-desktop/
  - Drag to Applications and open
  - Allow permissions when prompted
  - Wait for Docker icon in menu bar to show "Docker Desktop is running"

- **Linux (Ubuntu/Debian)**:
  ```bash
  sudo apt-get update
  sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
  sudo systemctl enable docker && sudo systemctl start docker
  ```

#### Verify Docker is working

Open a terminal (PowerShell on Windows, Terminal on macOS/Linux) and run:

```bash
docker --version
docker compose version
```

Both commands should print version numbers. If either fails, Docker Desktop is not running or not installed correctly.

#### Git Bash (Windows Only, Recommended)

The Makefile and shell commands in this project use Unix syntax. On Windows, install Git which includes Git Bash:

- Download from https://git-scm.com/download/win
- During installation, select "Use Git from Git Bash only"
- After installation, you can right-click any folder and select "Open Git Bash here"

---

### A2. Preparing the Project ZIP

On the machine that currently has the project running:

#### What to include in the ZIP

```
PostcodeAddressLookup/
  backend/                    # Backend source code
    app/                      # Python application code
    alembic.ini               # Database migration config
    pyproject.toml            # Python dependencies
    uv.lock                   # Locked dependency versions
    Dockerfile                # Backend Docker build instructions
    data/                     # Keep this folder but it should be empty (only .gitkeep inside)
  frontend/                   # Frontend source code
    src/                      # Svelte components and routes
    package.json              # Node.js dependencies
    package-lock.json         # Locked dependency versions
    Dockerfile                # Production frontend Docker build
    Dockerfile.dev            # Development frontend Docker build
    nginx.conf                # Production Nginx config for frontend
    svelte.config.js          # SvelteKit configuration
    vite.config.ts            # Vite build tool configuration
    tsconfig.json             # TypeScript configuration
  deploy/                     # Deployment scripts
    deploy.sh                 # Automated server deployment script
    restore-db.sh             # Standalone database restore script
    nginx-proxy.conf          # Server Nginx reverse proxy config
  docs/                       # Documentation
  docker-compose.yml          # Production Docker Compose config
  docker-compose.override.yml # Development Docker Compose overrides
  .env.example                # Example environment variables
  Makefile                    # Shortcut commands
  CLAUDE.md                   # Project documentation
  postcode_lookup.dump        # Database dump file (~17 GB)
```

#### What to EXCLUDE from the ZIP

These folders are auto-generated and Docker rebuilds them:

```
node_modules/          # ~108 MB — npm rebuilds this
.venv/                 # ~160 MB — uv/pip rebuilds this
__pycache__/           # Python bytecode cache
.svelte-kit/           # SvelteKit build cache
.git/                  # Git history (not needed for running)
backend/data/*.csv     # Raw source data files (already in database)
backend/data/*.zip     # Raw source data archives (already in database)
backend/data/*.pbf     # OpenStreetMap data file (already in database)
```

#### Creating the ZIP

**On Windows (PowerShell):**

```powershell
cd C:\Users\YourName\Downloads\Praveen-POC

# Create ZIP excluding unnecessary folders
Compress-Archive -Path PostcodeAddressLookup\* -DestinationPath PostcodeAddressLookup.zip
```

Note: Windows built-in ZIP does not support exclude patterns. If the ZIP is too large because of `node_modules` or `.venv`, delete those folders first:

```powershell
cd PostcodeAddressLookup
Remove-Item -Recurse -Force frontend\node_modules -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force frontend\.svelte-kit -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force backend\.venv -ErrorAction SilentlyContinue
cd ..
Compress-Archive -Path PostcodeAddressLookup\* -DestinationPath PostcodeAddressLookup.zip
```

**On macOS/Linux:**

```bash
cd ~/Downloads/Praveen-POC
zip -r PostcodeAddressLookup.zip PostcodeAddressLookup/ \
    -x "*/node_modules/*" \
    -x "*/.venv/*" \
    -x "*/__pycache__/*" \
    -x "*/.svelte-kit/*" \
    -x "*/.git/*"
```

The resulting ZIP will be approximately **17 GB** (mostly the database dump file).

---

### A3. Extracting and Setting Up

1. **Copy the ZIP** to the new machine (via USB drive, external hard drive, network share, etc.)

2. **Extract the ZIP** to a folder of your choice, for example:
   - Windows: `C:\Projects\PostcodeAddressLookup\`
   - macOS/Linux: `~/Projects/PostcodeAddressLookup/`

3. **Open a terminal** in the extracted folder:
   - Windows: Open Git Bash, then `cd /c/Projects/PostcodeAddressLookup`
   - macOS: Open Terminal, then `cd ~/Projects/PostcodeAddressLookup`
   - Linux: Open Terminal, then `cd ~/Projects/PostcodeAddressLookup`

4. **Create the environment file:**

   ```bash
   cp .env.example .env
   ```

   The default `.env` values work for local development. No changes needed.

---

### A4. Starting the Services

Make sure **Docker Desktop is running** (check the system tray icon on Windows, or menu bar on macOS).

#### Option 1: Using Make (Git Bash on Windows, or macOS/Linux terminal)

```bash
make up
```

This starts all 3 services in development mode with hot-reload.

#### Option 2: Using Docker Compose directly (works in any terminal)

```bash
docker compose up --build -d
```

The `-d` flag runs containers in the background. The `--build` flag rebuilds images.

#### What happens during the first run

1. Docker downloads base images (Python 3.12, Node 22, PostgreSQL 16 + PostGIS) — this takes 5-10 minutes on first run
2. Backend image builds: installs Python dependencies via `uv`
3. Frontend image builds: installs Node dependencies via `npm ci`, builds the Svelte app
4. PostgreSQL starts and creates the `postcode_lookup` database
5. Backend starts and connects to the database
6. Frontend starts (Vite dev server on port 3000)

#### Monitor the build progress

```bash
docker compose logs -f
```

Press `Ctrl+C` to stop watching logs (containers keep running).

#### Verify all containers are running

```bash
docker compose ps
```

You should see output like:

```
NAME                    STATUS
postcodelookup-db-1       Up (healthy)
postcodelookup-backend-1  Up
postcodelookup-frontend-1 Up
```

All three must show "Up". The database should show "Up (healthy)".

**If a container shows "Restarting" or "Exit":**

```bash
# Check what went wrong
docker compose logs db
docker compose logs backend
docker compose logs frontend
```

---

### A5. Restoring the Database

The containers are running but the database is empty. You need to restore from the dump file.

#### Step 1: Copy the dump file into the database container

```bash
docker compose cp postcode_lookup.dump db:/tmp/postcode_lookup.dump
```

This copies the 17 GB file into the running PostgreSQL container. It takes a few minutes.

#### Step 2: Enable required PostgreSQL extensions

```bash
docker compose exec -T db psql -U postgres -d postcode_lookup -c "CREATE EXTENSION IF NOT EXISTS postgis;"
docker compose exec -T db psql -U postgres -d postcode_lookup -c "CREATE EXTENSION IF NOT EXISTS pg_trgm;"
```

These enable:
- **PostGIS**: Geographic/spatial data support (coordinates, location queries)
- **pg_trgm**: Trigram text matching (powers the ILIKE fuzzy search)

#### Step 3: Restore the database

```bash
docker compose exec -T db pg_restore \
    -U postgres \
    -d postcode_lookup \
    --no-owner \
    --no-acl \
    --jobs=4 \
    /tmp/postcode_lookup.dump
```

**This takes 10-30 minutes** depending on the machine's disk speed and CPU.

Explanation of flags:
- `-U postgres` — Connect as the postgres user
- `-d postcode_lookup` — Restore into the postcode_lookup database
- `--no-owner` — Don't try to set original ownership (avoids permission errors)
- `--no-acl` — Don't restore access privileges (not needed for local dev)
- `--jobs=4` — Use 4 parallel workers for faster restore

**You will see some warning messages** like:
```
pg_restore: warning: errors ignored on restore: X
```

This is normal. These warnings are about PostGIS extension objects that already exist. The data restores correctly.

#### Step 4: Clean up the dump inside the container

```bash
docker compose exec -T db rm -f /tmp/postcode_lookup.dump
```

#### Step 5: Restart the backend

```bash
docker compose restart backend
```

The backend reconnects to the now-populated database.

---

### A6. Verifying Everything Works

#### Check the API health endpoint

```bash
curl http://localhost:8000/api/health
```

You should see a JSON response with record counts:

```json
{
  "status": "connected",
  "postcodes": 2700000,
  "addresses": 69000000,
  "price_paid": 31000000,
  "companies": 5500000,
  "food_ratings": 505000,
  "voa_ratings": 2300000
}
```

(Numbers are approximate.)

#### Test the postcode autocomplete

```bash
curl "http://localhost:8000/api/postcodes/autocomplete?q=SW1A"
```

Should return matching postcodes like SW1A 1AA, SW1A 2AA, etc.

#### Open the frontend in your browser

Go to: **http://localhost:3000**

You should see the UK Postcode & Address Lookup interface with:
- A search bar for postcode lookup
- A tab for address search
- A status bar at the bottom showing "API connected" with record counts

#### Test a postcode lookup

Type "SW1A 1AA" in the search bar. You should see addresses at that postcode with enrichment data (price history, companies, etc.).

---

### A7. Stopping and Restarting

#### Stop all services

```bash
docker compose down
```

This stops and removes the containers but **preserves the database data** (stored in a Docker volume).

#### Restart services (database data is preserved)

```bash
docker compose up -d
```

No need to restore the database again — the `pgdata` volume persists between restarts.

#### Full reset (deletes all data, starts from scratch)

```bash
docker compose down -v
```

The `-v` flag removes volumes, including the database. You would need to restore the dump again.

---

---

## Part B: Deploying to a Production Server

This section covers deploying to a Linux VPS (e.g., Hetzner Cloud, DigitalOcean, AWS EC2).

### B1. Server Requirements

#### Minimum Specifications

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 2 vCPUs | 4 vCPUs |
| RAM | 4 GB | 8 GB |
| Disk | 80 GB SSD | 120 GB SSD |
| OS | Ubuntu 22.04+ | Ubuntu 24.04 LTS |
| Network | Port 80 (HTTP) open | Ports 80, 443, SSH open |

The database with 69M+ addresses, indexes, and enrichment tables uses approximately 50-60 GB of disk space.

#### Required open ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 80 | TCP | HTTP (public web access) |
| 22 (or custom) | TCP | SSH (server management) |
| 443 | TCP | HTTPS (optional, if adding SSL later) |

Check with your hosting provider's firewall settings (e.g., Hetzner Cloud Firewall, AWS Security Groups) that these ports are open for inbound traffic.

---

### B2. Exporting the Database from Local

On your local machine where the project is currently running with data:

#### Start local Docker services (if not already running)

```bash
cd /path/to/PostcodeAddressLookup
docker compose up -d
```

#### Wait for the database to be healthy

```bash
docker compose exec -T db pg_isready -U postgres
```

Should print: `localhost:5432 - accepting connections`

#### Create the database dump

```bash
docker compose exec db pg_dump -U postgres -Fc postcode_lookup > postcode_lookup.dump
```

**This takes 15-45 minutes** and produces a file of approximately 17 GB.

The `-Fc` flag creates a compressed custom-format dump, which is:
- Smaller than plain SQL
- Supports parallel restore (`--jobs=4`)
- Supports selective restore (individual tables)

There is no progress indicator. To verify the dump is progressing:

**Windows (PowerShell):**
```powershell
Get-Item postcode_lookup.dump | Select-Object Length, LastWriteTime
```

**macOS/Linux:**
```bash
ls -lh postcode_lookup.dump
```

Run this command multiple times — the file size should keep growing. When the `docker compose exec` command finishes and returns you to the prompt, the dump is complete.

---

### B3. Connecting to the Server via SSH

#### Standard SSH connection

```bash
ssh root@YOUR_SERVER_IP
```

Replace `YOUR_SERVER_IP` with your server's IP address (e.g., `5.75.139.132`).

#### If SSH uses a non-standard port

Some servers use a custom SSH port for security:

```bash
ssh -p PORT_NUMBER root@YOUR_SERVER_IP
```

For example:
```bash
ssh -p 38052 root@YOUR_SERVER_IP
```

#### If root login is disabled

Many servers disable direct root login. Log in as a regular user and escalate:

```bash
ssh -p PORT_NUMBER admin@YOUR_SERVER_IP

# Once logged in, become root:
sudo su -
```

#### SSH connection troubleshooting

| Error | Cause | Solution |
|-------|-------|----------|
| `Connection timed out` | Port 22 is blocked by firewall | Check hosting provider's firewall rules, or try other ports |
| `Connection refused` | SSH service not running | Use hosting provider's web console to start SSH |
| `Permission denied` | Wrong password or user | Try different user (admin vs root), check password carefully |

---

### B4. Investigating the Existing Setup

If the server already has a previous deployment, investigate before making changes:

```bash
# What Docker containers are currently running?
docker ps

# List all Docker Compose projects
docker compose ls

# Find where the old project is located
find / -name "docker-compose.yml" -maxdepth 5 2>/dev/null

# Check if Nginx is installed and running
systemctl status nginx 2>/dev/null
which nginx

# View existing Nginx configuration
cat /etc/nginx/sites-enabled/* 2>/dev/null
cat /etc/nginx/conf.d/* 2>/dev/null

# Check server resources (RAM, disk, CPU)
free -h        # Available memory
df -h          # Available disk space
nproc          # Number of CPU cores
```

**Save or note the output** — you'll need to know:
- Where the old project directory is
- Whether Nginx is already installed and configured
- Whether there's enough disk space (need ~80 GB free)

---

### B5. Stopping the Old Deployment

Navigate to the old project directory and stop it:

```bash
# Go to the old project directory (found in step B4)
cd /path/to/old/project

# Stop all containers
docker compose down

# Verify nothing is running
docker ps
```

**Important:** Do NOT use `docker compose down -v` unless you want to delete the old database volume. If you want to keep the old data as a backup, just use `docker compose down`.

---

### B6. Uploading the Project to the Server

From your **local machine** (not the SSH session), upload the project code:

#### Create the project directory on the server

In your SSH session:
```bash
mkdir -p /opt/postcode-lookup
```

#### Upload using scp (from local terminal)

```bash
cd /path/to/PostcodeAddressLookup

# Upload project files (small, just code)
scp -P PORT_NUMBER -r \
    backend \
    frontend \
    deploy \
    docs \
    docker-compose.yml \
    docker-compose.override.yml \
    .env.example \
    Makefile \
    CLAUDE.md \
    root@YOUR_SERVER_IP:/opt/postcode-lookup/
```

Replace `PORT_NUMBER` with your SSH port (e.g., 38052) and `YOUR_SERVER_IP` with the server IP.

Note: The `-P` flag (capital P) specifies the SSH port for `scp`. This is different from `ssh` which uses lowercase `-p`.

If uploading as a non-root user:
```bash
scp -P PORT_NUMBER -r \
    backend frontend deploy docs \
    docker-compose.yml docker-compose.override.yml \
    .env.example Makefile CLAUDE.md \
    admin@YOUR_SERVER_IP:/tmp/postcode-lookup/

# Then in SSH session:
sudo mv /tmp/postcode-lookup/* /opt/postcode-lookup/
```

This upload is small (~1-2 MB of code) and completes in seconds.

---

### B7. Uploading the Database Dump

This is the large file (~17 GB). From your **local machine**:

```bash
scp -P PORT_NUMBER postcode_lookup.dump root@YOUR_SERVER_IP:/root/postcode_lookup.dump
```

**Estimated upload times:**

| Upload Speed | Time for 17 GB |
|-------------|----------------|
| 5 Mbps | ~8 hours |
| 10 Mbps | ~4 hours |
| 25 Mbps | ~1.5 hours |
| 50 Mbps | ~45 minutes |
| 100 Mbps | ~25 minutes |

**Tip:** You can proceed with Steps B8-B12 in your SSH session while this uploads. The database dump is only needed for Step B13.

**If the upload is interrupted**, `scp` does not support resume. Use `rsync` instead, which can resume partial transfers:

```bash
rsync -avP --progress -e "ssh -p PORT_NUMBER" \
    postcode_lookup.dump root@YOUR_SERVER_IP:/root/postcode_lookup.dump
```

The `-P` flag in rsync (or `--partial --progress`) enables resume on reconnect.

---

### B8. Installing Docker on the Server

In your SSH session (as root):

#### Check if Docker is already installed

```bash
docker --version
docker compose version
```

If both commands return version numbers, skip to Step B9.

#### Install Docker (Ubuntu/Debian)

```bash
# Update package index
apt-get update

# Install prerequisites
apt-get install -y ca-certificates curl gnupg

# Add Docker's official GPG key
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg

# Add Docker repository
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" > /etc/apt/sources.list.d/docker.list

# Install Docker
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Start Docker and enable on boot
systemctl enable docker
systemctl start docker
```

#### Verify installation

```bash
docker --version
# Expected: Docker version 27.x.x or similar

docker compose version
# Expected: Docker Compose version v2.x.x or similar

docker run hello-world
# Expected: "Hello from Docker!" message
```

---

### B9. Installing Nginx on the Server

Nginx acts as a reverse proxy on the host machine, routing incoming HTTP traffic (port 80) to the Docker containers.

#### Check if Nginx is already installed

```bash
which nginx
nginx -v
```

If Nginx is installed, skip to Step B10.

#### Install Nginx

```bash
apt-get update
apt-get install -y nginx

# Start and enable on boot
systemctl enable nginx
systemctl start nginx

# Verify it's running
systemctl status nginx
```

---

### B10. Configuring the Nginx Reverse Proxy

#### Remove the default Nginx site

```bash
rm -f /etc/nginx/sites-enabled/default
```

#### Copy the project's proxy configuration

```bash
cp /opt/postcode-lookup/deploy/nginx-proxy.conf /etc/nginx/sites-available/postcode-lookup.conf
```

#### Enable the configuration

```bash
ln -sf /etc/nginx/sites-available/postcode-lookup.conf /etc/nginx/sites-enabled/postcode-lookup.conf
```

#### Test the configuration is valid

```bash
nginx -t
```

Expected output:
```
nginx: the configuration file /etc/nginx/nginx.conf syntax is ok
nginx: configuration file /etc/nginx/nginx.conf test is successful
```

If you see errors, check the config file for typos.

#### Restart Nginx

```bash
systemctl restart nginx
```

#### What this configuration does

The `nginx-proxy.conf` file routes traffic as follows:

```
Internet (port 80)
    |
    ├── /api/*     →  127.0.0.1:8000 (FastAPI backend container)
    |
    └── /*         →  127.0.0.1:3000 (SvelteKit frontend container)
```

This means:
- Users visit `http://YOUR_SERVER_IP/` and see the frontend
- The frontend calls `http://YOUR_SERVER_IP/api/...` for data
- Nginx proxies the `/api/` requests to the backend container

---

### B11. Creating the Production Environment File

```bash
cd /opt/postcode-lookup

cat > .env << EOF
POSTGRES_PASSWORD=postgres
VITE_API_BASE_URL=http://YOUR_SERVER_IP
LOG_LEVEL=INFO
LOG_FORMAT=json
EOF
```

**Replace `YOUR_SERVER_IP`** with your actual server IP address. For example:

```bash
cat > .env << EOF
POSTGRES_PASSWORD=postgres
VITE_API_BASE_URL=http://5.75.139.132
LOG_LEVEL=INFO
LOG_FORMAT=json
EOF
```

**Important notes:**
- `VITE_API_BASE_URL` is baked into the frontend at build time. It tells the browser where to send API requests. It MUST be the public IP or domain name of your server.
- `POSTGRES_PASSWORD` should be changed to a strong password in production. If you change it here, it will only take effect on a fresh database volume. To change it on an existing database, you must also update the password inside PostgreSQL.
- `LOG_FORMAT=json` produces structured JSON logs suitable for production log aggregation.

---

### B12. Building and Starting Containers

```bash
cd /opt/postcode-lookup

# Build and start in production mode (no dev overrides)
docker compose -f docker-compose.yml up --build -d
```

The `-f docker-compose.yml` flag explicitly uses only the production config, skipping the development override file.

**This takes 3-10 minutes** on first run as it:
1. Downloads base images (PostgreSQL, Python, Node.js, Nginx)
2. Builds the backend image (installs Python dependencies)
3. Builds the frontend image (installs Node dependencies, builds static assets)
4. Starts all three containers

#### Monitor the build and startup

```bash
docker compose logs -f
```

Press `Ctrl+C` to stop watching (containers keep running).

#### Verify containers are running

```bash
docker compose ps
```

All three services should show "Up":

```
NAME                          STATUS
postcode-lookup-db-1          Up (healthy)
postcode-lookup-backend-1     Up
postcode-lookup-frontend-1    Up
```

#### Wait for the database to be healthy

```bash
docker compose exec -T db pg_isready -U postgres
```

Expected: `localhost:5432 - accepting connections`

---

### B13. Restoring the Database on the Server

**Make sure the dump file upload (Step B7) has completed before starting this step.**

Verify the dump file exists:

```bash
ls -lh /root/postcode_lookup.dump
```

#### Step 1: Copy the dump into the database container

```bash
cd /opt/postcode-lookup
docker compose cp /root/postcode_lookup.dump db:/tmp/postcode_lookup.dump
```

This copies the file from the server's filesystem into the running PostgreSQL container.

#### Step 2: Drop and recreate the database

```bash
# Terminate any existing connections
docker compose exec -T db psql -U postgres -c \
    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'postcode_lookup' AND pid <> pg_backend_pid();"

# Drop the empty database
docker compose exec -T db dropdb -U postgres --if-exists postcode_lookup

# Create a fresh empty database
docker compose exec -T db createdb -U postgres postcode_lookup
```

#### Step 3: Enable required extensions

```bash
docker compose exec -T db psql -U postgres -d postcode_lookup -c "CREATE EXTENSION IF NOT EXISTS postgis;"
docker compose exec -T db psql -U postgres -d postcode_lookup -c "CREATE EXTENSION IF NOT EXISTS pg_trgm;"
```

#### Step 4: Restore the database

```bash
docker compose exec -T db pg_restore \
    -U postgres \
    -d postcode_lookup \
    --no-owner \
    --no-acl \
    --jobs=4 \
    /tmp/postcode_lookup.dump
```

**This takes 10-30 minutes** depending on server disk speed.

You will see warning messages about existing extensions — these are normal and harmless:
```
pg_restore: warning: errors ignored on restore: X
```

#### Step 5: Clean up and restart

```bash
# Remove the dump from inside the container
docker compose exec -T db rm -f /tmp/postcode_lookup.dump

# Restart the backend to reconnect with populated data
docker compose restart backend

# Wait a few seconds for backend to start
sleep 5
```

---

### B14. Verifying the Production Deployment

#### Test the API from the server

```bash
# Health check (should show record counts)
curl http://localhost:8000/api/health

# Same through Nginx
curl http://YOUR_SERVER_IP/api/health

# Test postcode autocomplete
curl "http://YOUR_SERVER_IP/api/postcodes/autocomplete?q=SW1A"

# Test postcode lookup
curl "http://YOUR_SERVER_IP/api/postcodes/SW1A%201AA"
```

#### Test from your browser

Open: **http://YOUR_SERVER_IP/**

You should see:
- The UK Postcode & Address Lookup interface
- The status bar at the bottom showing "API connected" with record counts
- Postcode search working with autocomplete
- Address search returning results

---

### B15. Cleaning Up

After verifying everything works:

```bash
# Delete the dump file from the server (frees ~17 GB)
rm -f /root/postcode_lookup.dump

# Remove unused Docker images (frees disk space)
docker image prune -f

# Check remaining disk usage
df -h
```

---

### B16. Server Management Commands

After deployment, use these commands for ongoing management:

```bash
# Always navigate to project directory first
cd /opt/postcode-lookup

# --- Status ---
docker compose ps                    # Check container status
docker compose logs -f               # Live logs (all services)
docker compose logs backend          # Backend logs only
docker compose logs frontend         # Frontend logs only
docker compose logs db               # Database logs only

# --- Restart ---
docker compose restart               # Restart all services
docker compose restart backend       # Restart backend only
docker compose restart frontend      # Restart frontend only

# --- Stop / Start ---
docker compose stop                  # Stop all (preserves data)
docker compose start                 # Start all
docker compose down                  # Stop and remove containers (preserves DB volume)

# --- Rebuild (after code changes) ---
docker compose -f docker-compose.yml up --build -d

# --- Database shell ---
docker compose exec db psql -U postgres -d postcode_lookup

# --- Check database record counts ---
docker compose exec -T db psql -U postgres -d postcode_lookup -c \
    "SELECT 'postcodes' as table_name, count(*) FROM postcodes
     UNION ALL SELECT 'addresses', count(*) FROM addresses
     UNION ALL SELECT 'price_paid', count(*) FROM price_paid
     UNION ALL SELECT 'companies', count(*) FROM companies
     UNION ALL SELECT 'food_ratings', count(*) FROM food_ratings
     UNION ALL SELECT 'voa_ratings', count(*) FROM voa_ratings;"

# --- View disk usage ---
docker system df                     # Docker disk usage
df -h                                # Server disk usage
```

---

## Part C: Troubleshooting

### Container won't start

```bash
# Check which container is failing
docker compose ps

# Read the error logs
docker compose logs <service_name>
# e.g., docker compose logs backend
```

### Backend shows "database is temporarily unavailable"

The database container might not be ready yet:

```bash
# Check database health
docker compose exec -T db pg_isready -U postgres

# Restart backend after database is healthy
docker compose restart backend
```

### Frontend shows blank page or "API unavailable"

1. Check that the backend is running: `curl http://localhost:8000/api/health`
2. Check that Nginx is routing correctly: `curl http://YOUR_SERVER_IP/api/health`
3. Verify `VITE_API_BASE_URL` in `.env` matches your server IP
4. If you changed `.env`, rebuild the frontend: `docker compose -f docker-compose.yml up --build -d frontend`

### Database restore fails

```bash
# Check if extensions are enabled
docker compose exec -T db psql -U postgres -d postcode_lookup -c "\dx"

# If not, enable them
docker compose exec -T db psql -U postgres -d postcode_lookup -c "CREATE EXTENSION IF NOT EXISTS postgis;"
docker compose exec -T db psql -U postgres -d postcode_lookup -c "CREATE EXTENSION IF NOT EXISTS pg_trgm;"

# Retry the restore
docker compose exec -T db pg_restore -U postgres -d postcode_lookup --no-owner --no-acl --jobs=4 /tmp/postcode_lookup.dump
```

### Port 80 not accessible from internet

- Check hosting provider's firewall (Hetzner Cloud Firewall, AWS Security Groups)
- Check server's firewall: `ufw status` or `iptables -L`
- Open port 80: `ufw allow 80/tcp && ufw reload`

### SSH connection issues

| Problem | Solution |
|---------|----------|
| `Connection timed out` | Port 22 blocked — check cloud firewall, try other ports |
| `Connection refused` | SSH not running — use hosting provider's web console |
| `Permission denied` | Wrong password — try admin user, then `sudo su -` |

### "Disk full" errors

```bash
# Check disk usage
df -h

# Clean Docker caches
docker system prune -af

# Remove old images
docker image prune -af
```

### Rebuilding after code changes

If you update the code and need to redeploy:

```bash
cd /opt/postcode-lookup

# Upload new code from local machine (run from local terminal):
# scp -P PORT_NUMBER -r backend frontend root@YOUR_SERVER_IP:/opt/postcode-lookup/

# On the server, rebuild and restart:
docker compose -f docker-compose.yml up --build -d
```

The database does not need to be restored again — it persists in the Docker volume.

---

*Last updated: March 2026*
