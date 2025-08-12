#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
cd "$PROJECT_ROOT"

function section(){ echo -e "\n=== $* ==="; }

section "System"
uname -a || true
arch || true

section "Docker"
if ! command -v docker >/dev/null 2>&1; then
  echo "Docker not installed. Install Docker Desktop for Mac."
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon not running. Start Docker Desktop."
  exit 1
fi

docker compose -f "$PROJECT_ROOT/deploy/docker-compose.yml" --project-directory "$PROJECT_ROOT" ps || true

section "Ports"
for p in 5173 8000 9000 9001 5432 6379; do
  echo -n "Port $p: "; (lsof -i :$p -sTCP:LISTEN -nP || true) | head -n 1 || true
done

section "Health checks"
set +e
curl -fsS http://localhost:8000/health/live && echo "\nBackend OK" || echo "Backend not healthy"
curl -fsS http://localhost:9000/minio/health/live && echo "\nMinIO OK" || echo "MinIO not healthy"
set -e

section "Recent logs"
docker compose -f "$PROJECT_ROOT/deploy/docker-compose.yml" --project-directory "$PROJECT_ROOT" logs --tail=100 backend worker postgres redis minio | tail -n 200 || true

section "API probe"
set +e
curl -fsS http://localhost:8000/ | jq . || curl -fsS http://localhost:8000/ || true
set -e

section "Suggestions"
echo "- If backend is not healthy, try: docker compose -f \"$PROJECT_ROOT/deploy/docker-compose.yml\" --project-directory \"$PROJECT_ROOT\" restart backend"
echo "- If ports are in use, stop the conflicting processes or change ports in .env/compose"
echo "- If MinIO bucket missing, rerun: docker compose -f \"$PROJECT_ROOT/deploy/docker-compose.yml\" --project-directory \"$PROJECT_ROOT\" up -d createbuckets"
echo "- To rebuild from scratch: docker compose -f \"$PROJECT_ROOT/deploy/docker-compose.yml\" --project-directory \"$PROJECT_ROOT\" down -v && docker compose -f \"$PROJECT_ROOT/deploy/docker-compose.yml\" --project-directory \"$PROJECT_ROOT\" up -d --build"
