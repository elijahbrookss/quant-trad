# Makefile — fast local dev: bring up infra, run apps, and test

.DEFAULT_GOAL := help

COMPOSE_FILE := docker/docker-compose.local.yml
SERVICES     := timescaledb pgadmin grafana loki
FRONTEND_DIR := portal/frontend
BACKEND_APP  := portal.backend.main:app
PY_SRC       := src
VENV_DIR     := env

VENV_CHECK := [ -f "$(VENV_DIR)/bin/activate" ] || { echo '❌ No venv. Run make bootstrap.'; exit 1; }

.PHONY: help bootstrap \
        infra-up infra-down infra-restart infra-logs infra-status infra-clean db-shell \
        api-up api-stop web-up web-stop \
        dev-up dev-down \
        test test-unit test-int

## Show this help
help:
	@awk 'BEGIN{FS":.*##"; printf "\nCommands:\n"} /^[a-zA-Z0-9_.-]+:.*##/{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

## One-time setup (venv, pip, npm)
bootstrap:
	@python3 -m venv $(VENV_DIR)
	@. $(VENV_DIR)/bin/activate && pip install -U pip && pip install -r requirements.txt
	@cd $(FRONTEND_DIR) && npm install
	@echo "✅ Bootstrap complete"

## Start infra containers (DB, pgAdmin, Grafana, Loki)
infra-up:
	docker compose -f $(COMPOSE_FILE) up -d $(SERVICES)
	@echo "⏳ Waiting for TimescaleDB..."
	@while ! docker exec tsdb pg_isready -U postgres >/dev/null 2>&1; do \
		echo "… still waiting"; sleep 2; \
	done
	@echo "✅ TimescaleDB ready"
	@echo "➡ Grafana http://localhost:3000 | Loki http://localhost:3100 | pgAdmin http://localhost:8080"

## Stop infra containers (keep volumes)
infra-down:
	docker compose -f $(COMPOSE_FILE) stop $(SERVICES)
	@echo "🛑 Infra stopped"

## Restart infra containers
infra-restart:
	$(MAKE) infra-down
	$(MAKE) infra-up

## Tail infra logs
infra-logs:
	docker compose -f $(COMPOSE_FILE) logs -f $(SERVICES)

## Show running infra
infra-status:
	docker compose -f $(COMPOSE_FILE) ps --status=running

## Teardown infra with volumes (DANGER)
infra-clean:
	docker compose -f $(COMPOSE_FILE) down -v
	@echo "🧹 Removed containers and volumes"

## Open psql shell to TimescaleDB
db-shell:
	psql "postgresql://postgres:postgres@localhost:5432/postgres"

## Run FastAPI (reload) with venv
api-up:
	@echo "🚀 API dev server"
	@bash -c "$(VENV_CHECK) && . $(VENV_DIR)/bin/activate && export PYTHONPATH=$(PY_SRC) && uvicorn $(BACKEND_APP) --reload"

## (Optional) stop API if you background it (placeholder)
api-stop:
	@pkill -f "uvicorn $(BACKEND_APP)" || true
	@echo "🛑 API stopped"

## Run frontend dev server (Vite)
web-up:
	@echo "🎨 Frontend dev server"
	@cd $(FRONTEND_DIR) && npm run dev

## (Optional) stop frontend if you background it (placeholder)
web-stop:
	@pkill -f "vite" || true
	@echo "🛑 Frontend stopped"

## Bring up infra + API + Web for full dev
dev-up: infra-up
	@$(MAKE) -j2 api-up web-up

## Stop everything (apps need stop only if backgrounded)
dev-down: infra-down
	@echo "🧯 Dev stack down"

## Run all tests
test:
	@bash -c "$(VENV_CHECK) && . $(VENV_DIR)/bin/activate && pytest -v tests/"

## Unit tests only
test-unit:
	@bash -c "$(VENV_CHECK) && . $(VENV_DIR)/bin/activate && pytest -v -m 'not integration' tests/"

## Integration tests only
test-int:
	@bash -c "$(VENV_CHECK) && . $(VENV_DIR)/bin/activate && pytest -v -m integration tests/"
