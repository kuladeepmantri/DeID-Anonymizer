### De-identification & Data Privacy Platform (Enterprise)

An enterprise-grade platform to detect and de-identify PII/PHI across structured and unstructured data with policy-driven controls, API access, auditability, and modern UI.

What it does (high level):
- Detects sensitive data in text and files and applies masking/tokenization/anonymization according to policies
- Supports uploads to object storage and batch background processing
- Exposes REST APIs (and GraphQL optional) for programmatic use
- Provides a clean React UI with drag-and-drop, dark mode, and real-time feedback

What’s included (stack):
- Backend: FastAPI + Celery + Redis + Postgres + MinIO (S3-compatible)
- Frontend: React + Vite + MUI
- CI: Lint, tests, Docker build for linux/arm64 (Apple Silicon)
- Apple Silicon: All images target linux/arm64; CPU-first AI by default

### Quick Start for complete beginners (M3 MacBook Pro/Max)

0) Install prerequisites
- Docker Desktop for Mac (Apple Silicon)
- Node.js 20 (install via `nvm`, `fnm`, or NodeJS pkg)
- Git (Xcode Command Line Tools or Homebrew)

1) Clone the repository and open Terminal
```bash
# In Terminal
cd ~
git clone <your-repo-url> deid-platform
cd deid-platform
```

2) Create environment file
```bash
cp .env.example .env
```

3) One-command setup and run (recommended)
```bash
bash scripts/setup.sh
```
What it does:
- Verifies Docker, Node, and Apple Silicon environment
- Builds and starts the stack via docker compose
- Waits until Postgres, Redis, MinIO, and the Backend are ready
- Prints the service URLs and basic next steps

4) Open the app
- Frontend: http://localhost:5173
- Backend API docs: http://localhost:8000/docs
- MinIO Console: http://localhost:9001 (user: minioadmin, pass: minioadmin)

5) Try de-identification
- In the UI, paste text containing emails/phones and click Run
- Or via API: `POST http://localhost:8000/deid/text` with `{ "text": "john.doe@example.com" }`

### Alternate run commands (manual)
- Start: `docker compose -f deploy/docker-compose.yml --project-directory . up -d --build`
- Stop: `docker compose -f deploy/docker-compose.yml --project-directory . down -v`
- Logs: `make logs`
- Tests: `make test`
- Frontend dev only: `make dev-frontend`
- Backend dev only: `make dev-backend`
- Backend on host (optional Metal AI later): `ENABLE_METAL_ACCELERATION=true make dev-backend-host`

### Troubleshooting (run this first)
```bash
bash scripts/debug.sh
```
This will:
- Check Docker is running, arch is arm64, and ports are free
- Show container status and last logs
- Probe health endpoints (backend, Postgres, Redis, MinIO)
- Attempt simple API calls to verify routing

If Docker isn’t found, install/restart Docker Desktop and re-run `scripts/setup.sh`.

### Common issues on M3 Macs
- Docker build stuck or slow: ensure Docker Desktop has adequate CPU/RAM in Settings > Resources
- Port in use (5173, 8000, 9000, 9001, 5432, 6379): stop any app using those ports or change them in `.env`/compose
- MinIO bucket not found: the stack includes an init job (`createbuckets`) that creates the bucket; re-run `docker compose up -d`

### Security & Compliance (baseline)
- JWT auth scaffold and RBAC-ready structure
- AES-256 at rest via storage provider; TLS 1.3 in ingress (prod)
- Append-only audit trail planned; exportable to external ledger
- SSO/OIDC placeholders in `.env` for future wiring

### Roadmap options
- AI-driven NER (spaCy/Presidio) behind a feature flag
- Policy authoring assistant and rule suggestions
- Distributed batch processing (Spark) for massive datasets
- Helm charts for Kubernetes deployment and GitOps CD

### Development
- Backend code: `apps/backend/app`
- Frontend code: `apps/frontend/src`
- Compose stack: `deploy/docker-compose.yml`
- CI: `.github/workflows/ci.yml`

### License
Proprietary. All rights reserved.




