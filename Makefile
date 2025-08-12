SHELL := /usr/bin/bash
PROJECT_NAME := deid-platform

export COMPOSE_FILE := deploy/docker-compose.yml

init:
	cd apps/frontend && corepack enable && pnpm install || npm install

up:
	docker compose -p $(PROJECT_NAME) up -d --build

down:
	docker compose -p $(PROJECT_NAME) down -v

logs:
	docker compose -p $(PROJECT_NAME) logs -f | cat

ps:
	docker compose -p $(PROJECT_NAME) ps

dev-backend:
	docker compose -p $(PROJECT_NAME) up -d backend worker

dev-frontend:
	docker compose -p $(PROJECT_NAME) up -d frontend

restart:
	docker compose -p $(PROJECT_NAME) restart

test:
	docker compose -p $(PROJECT_NAME) run --rm backend pytest -q

lint:
	docker compose -p $(PROJECT_NAME) run --rm backend ruff check app

format:
	docker compose -p $(PROJECT_NAME) run --rm backend ruff format app

# Host run options (Metal MPS possible if ENABLE_METAL_ACCELERATION=true)
dev-backend-host:
	cd apps/backend && python -m venv .venv && source .venv/bin/activate && pip install -U pip && pip install -r requirements.txt && uvicorn app.main:app --reload --host 0.0.0.0 --port 8000