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

# Copy or generate env if missing
if [[ ! -f .env ]]; then
  if [[ -f .env.example ]]; then
    info "Creating .env from .env.example"
    cp .env.example .env
  else
    info ".env.example not found. Generating default .env"
    cat > .env <<'EOF'
APP_ENV=development
SECRET_KEY=changeme-super-secret
JWT_ALGORITHM=HS256
API_BASE_URL=http://localhost:8000
FRONTEND_BASE_URL=http://localhost:5173
ALLOWED_ORIGINS=http://localhost:5173
POSTGRES_DB=deid
POSTGRES_USER=deid_user
POSTGRES_PASSWORD=deid_password
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
DATABASE_URL=postgresql+asyncpg://deid_user:deid_password@postgres:5432/deid
REDIS_URL=redis://redis:6379/0
CELERY_BROKER_URL=redis://redis:6379/1
CELERY_RESULT_BACKEND=redis://redis:6379/2
STORAGE_BACKEND=s3
S3_ENDPOINT_URL=http://minio:9000
S3_REGION=us-east-1
S3_BUCKET=deid-bucket
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
OIDC_PROVIDER_URL=
OIDC_CLIENT_ID=
OIDC_CLIENT_SECRET=
OIDC_REDIRECT_URI=http://localhost:8000/auth/callback
ENABLE_GRAPHQL=true
ENABLE_AUDIT_LEDGER=true
ENABLE_OCR=true
ENABLE_AI_DETECTION=true
ENABLE_METAL_ACCELERATION=false
EOF
  fi
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