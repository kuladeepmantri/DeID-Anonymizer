#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
cd "$PROJECT_ROOT"

function info(){ echo -e "[INFO] $*"; }
function warn(){ echo -e "[WARN] $*"; }
function err(){ echo -e "[ERROR] $*"; }

info "De-identification Platform setup starting..."

# Check Apple Silicon
ARCH=$(uname -m || true)
if [[ "$ARCH" != "arm64" && "$ARCH" != "aarch64" ]]; then
  warn "This setup is optimized for Apple Silicon (arm64). You are on: $ARCH"
fi

# Check Docker
if ! command -v docker >/dev/null 2>&1; then
  err "Docker not found. Install Docker Desktop for Mac and re-run."
  exit 1
fi

# Check Docker is running
if ! docker info >/dev/null 2>&1; then
  err "Docker daemon is not running. Open Docker Desktop and wait until it starts."
  exit 1
fi

# Copy env if missing
if [[ ! -f .env ]]; then
  info "Creating .env from .env.example"
  cp .env.example .env
fi

# Build and start
info "Building and starting Docker services (this may take a few minutes)..."
docker compose -f deploy/docker-compose.yml --project-directory . up -d --build

# Wait for services
function wait_http(){
  local url="$1"; local name="$2"; local max=60; local i=0
  until curl -fsS "$url" >/dev/null 2>&1; do
    ((i++)) || true
    if (( i > max )); then err "Timeout waiting for $name at $url"; return 1; fi
    sleep 2
  done
  info "$name is ready"
}

info "Waiting for backend health..."
wait_http "http://localhost:8000/health/live" "backend"

info "Waiting for MinIO..."
wait_http "http://localhost:9000/minio/health/live" "minio"

info "All services are up."

cat <<EOF

Open these URLs:
- Frontend: http://localhost:5173
- Backend API docs: http://localhost:8000/docs
- MinIO Console: http://localhost:9001 (user: minioadmin, pass: minioadmin)

Next steps:
- Try uploading a file in the UI and run text de-identification
- See logs: make logs
- Stop stack: docker compose -f deploy/docker-compose.yml --project-directory . down -v
EOF