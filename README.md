### De-identification & Data Privacy Platform (Enterprise)

FastAPI backend, Celery workers, React + Vite + MUI frontend, Redis, Postgres, and MinIO. Optimized for Apple Silicon (M1/M2/M3) with linux/arm64 images. Optional Metal acceleration on host for local AI when not using Docker.

#### Prereqs
- Docker Desktop 4.30+ (Apple Silicon)
- Node.js 20+
- Python 3.12+ (optional for direct host runs)

#### Quickstart (Docker)
- Copy `.env.example` to `.env` and adjust if needed
- Start stack:
```bash
docker compose -f deploy/docker-compose.yml --project-directory . up -d --build
```
- Backend: `http://localhost:8000/docs`
- Frontend: `http://localhost:5173`
- MinIO Console: `http://localhost:9001` (minioadmin/minioadmin)

#### Apple Silicon Notes
- Images are pinned to linux/arm64 and use slim/alpine bases
- No GPU pass-through is required. AI runs CPU-first by default
- For local host-run AI with Metal MPS (no Docker): set `ENABLE_METAL_ACCELERATION=true` and run backend via `make dev-backend-host`

#### Makefile targets
```bash
make init            # install frontend deps
make up              # docker compose up (arm64)
make down            # docker compose down -v
make logs            # tail service logs
make dev-backend     # run FastAPI via uvicorn in Docker
make dev-frontend    # run Vite dev server
make test            # backend tests
```

#### Security & Compliance
- JWT auth with RBAC
- At-rest AES-256 via storage provider, TLS 1.3 in ingress (prod)
- Audit logs with append-only table, exportable to external ledger

#### License
Proprietary. All rights reserved.




