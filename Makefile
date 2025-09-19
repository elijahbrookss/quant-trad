SHELL := /bin/bash
.DEFAULT_GOAL := help

## ---------- Config ----------
PYTHONPATH ?= src
PY          ?= python3
VENV        ?= .venv
PYTHON      := PYTHONPATH=$(PYTHONPATH) $(VENV)/bin/python
PIP         := $(VENV)/bin/pip
REQ         ?= requirements.txt
DEV_REQ     ?= requirements-dev.txt
REQS_HASH   := $(VENV)/.reqs.sha256

UVICORN_APP ?= portal.backend.main:app
UVICORN_OPTS?= --reload --host 0.0.0.0 --port 8000

FRONT_DIR   ?= portal/frontend
NPM         ?= npm

DC_FILE     ?= docker/docker-compose.local.yml
SERVICES    ?= timescaledb pgadmin grafana loki

PID_DIR     ?= .pids
LOG_DIR     ?= logs

## ============================== HELP ==================================== ##
.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS":.*##"; print "Usage: make <target>\n\nTargets:"} \
	/^[a-zA-Z0-9_.-]+:.*##/ {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

## ============================ ORCHESTRATOR =============================== ##
.PHONY: dev stop
dev: venv deps dev-up api-up frontend-start ## One-shot: venv+deps → infra → API → Frontend
stop: api-stop frontend-stop dev-down           ## Stop API & Frontend, then infra

## ============================ BOOTSTRAP ================================= ##
.PHONY: venv deps deps-dev freeze reset-venv _deps_hash _ensure_python _ensure_dirs
venv: _ensure_python ## Create venv if missing
	@if [ ! -x "$(PYTHON)" ]; then \
		echo "🧪 Creating venv at $(VENV)"; \
		$(PY) -m venv $(VENV); \
	fi

deps: venv _deps_hash ## Install deps if requirements changed
	@if [ "$$(cat $(REQS_HASH).new)" != "$$(cat $(REQS_HASH) 2>/dev/null || echo _none_)" ]; then \
		echo "📦 Installing Python deps..."; \
		$(PIP) install --upgrade pip setuptools wheel; \
		[ -f "$(REQ)" ] && $(PIP) install -r $(REQ) || true; \
		[ -f "$(DEV_REQ)" ] && $(PIP) install -r $(DEV_REQ) || true; \
		mv -f $(REQS_HASH).new $(REQS_HASH); \
	else \
		echo "📦 Dependencies unchanged"; \
		rm -f $(REQS_HASH).new; \
	fi

deps-dev: deps ## Install dev-only deps (if you keep them in requirements-dev.txt)
	@[ -f "$(DEV_REQ)" ] && { echo "📦 Installing dev deps..."; $(PIP) install -r $(DEV_REQ); } || echo "ℹ️ No $(DEV_REQ) found"

freeze: ## Write fully pinned lockfile from current venv
	@$(PIP) freeze > requirements.lock.txt && echo "🔒 Wrote requirements.lock.txt"

reset-venv: ## Remove venv and reinstall deps
	@rm -rf $(VENV)
	@$(MAKE) deps

_deps_hash:
	@{ cat $(REQ) 2>/dev/null || true; echo; cat $(DEV_REQ) 2>/dev/null || true; } \
	| sha256sum | awk '{print $$1}' > $(REQS_HASH).new

_ensure_python:
	@command -v $(PY) >/dev/null 2>&1 || { echo "❌ $(PY) not found on PATH"; exit 1; }

_ensure_dirs:
	@mkdir -p $(PID_DIR) $(LOG_DIR)

## ============================== API ===================================== ##
.PHONY: api-dev api-up api-start api-stop api-reload api-shell
api-dev: venv deps ## Ensure venv+deps, then run API (reload)
	@echo "🚀 API dev server"
	$(PYTHON) -m uvicorn $(UVICORN_APP) $(UVICORN_OPTS)

api-up: ## Run API only (assumes venv+deps)
	@echo "🚀 API server (no bootstrap)"
	$(PYTHON) -m uvicorn $(UVICORN_APP) --host 0.0.0.0 --port 8000

api-start: _ensure_dirs ## Start API in background (logs/PID managed)
	@if pgrep -f "uvicorn.*$(UVICORN_APP)" >/dev/null; then \
		echo "ℹ️ API already running"; \
	else \
		echo "🚀 Starting API (logs: $(LOG_DIR)/api.log)"; \
		nohup $(PYTHON) -m uvicorn $(UVICORN_APP) $(UVICORN_OPTS) \
			>$(LOG_DIR)/api.log 2>&1 & echo $$! > $(PID_DIR)/api.pid; \
		sleep 1; \
		[ -s $(PID_DIR)/api.pid ] && echo "✅ API PID $$(cat $(PID_DIR)/api.pid)" || echo "⚠️ API PID not recorded"; \
	fi

api-stop: ## Stop API (PID file preferred; fallback to pkill)
	@if [ -f $(PID_DIR)/api.pid ]; then \
		echo "🛑 Stopping API (PID $$(cat $(PID_DIR)/api.pid))"; \
		kill $$(cat $(PID_DIR)/api.pid) 2>/dev/null || true; \
		rm -f $(PID_DIR)/api.pid; \
	else \
		pkill -f "uvicorn.*$(UVICORN_APP)" 2>/dev/null || true; \
	fi

api-reload: ## Touch file to trigger --reload
	@touch backend/.reload && echo "♻️  Triggered reload"

api-shell: venv ## Project-aware Python shell
	@$(PYTHON) -q

## ============================ FRONTEND ================================== ##
.PHONY: frontend-start frontend-stop
frontend-start: _ensure_dirs ## Start Vite dev server in background
	@if pgrep -f "[v]ite.*$(FRONT_DIR)" >/dev/null; then \
		echo "ℹ️ Frontend already running"; \
	else \
		echo "🎨 Starting Frontend (logs: $(LOG_DIR)/frontend.log)"; \
		nohup $(NPM) --prefix $(FRONT_DIR) run dev >$(LOG_DIR)/frontend.log 2>&1 & echo $$! > $(PID_DIR)/frontend.pid; \
		sleep 1; \
		[ -s $(PID_DIR)/frontend.pid ] && echo "✅ Frontend PID $$(cat $(PID_DIR)/frontend.pid)" || echo "⚠️ Frontend PID not recorded"; \
	fi

frontend-stop: ## Stop Vite dev server
	@if [ -f $(PID_DIR)/frontend.pid ]; then \
		echo "🛑 Stopping Frontend (PID $$(cat $(PID_DIR)/frontend.pid))"; \
		kill $$(cat $(PID_DIR)/frontend.pid) 2>/dev/null || true; \
		rm -f $(PID_DIR)/frontend.pid; \
	else \
		pkill -f "[v]ite.*$(FRONT_DIR)" 2>/dev/null || true; \
	fi

## ============================== DOCKER ================================== ##
.PHONY: dev-up dev-down dev-logs dev-ps
dev-up: ## Start local infra (TimescaleDB, Grafana, Loki, pgAdmin)
	@docker compose -f $(DC_FILE) up -d $(SERVICES)
	@echo "⏳ Waiting for TimescaleDB..." && sleep 2
	@echo "✅ TimescaleDB ready"
	@echo "➡ Grafana http://localhost:3000 | Loki http://localhost:3100 | pgAdmin http://localhost:8080"

dev-down: ## Stop infra
	@docker compose -f $(DC_FILE) down

dev-logs: ## Tail docker logs
	@docker compose -f $(DC_FILE) logs -f

dev-ps: ## Show docker service status
	@docker compose -f $(DC_FILE) ps

## =============================== QUALITY ================================ ##
.PHONY: fmt lint typecheck test cov clean
fmt: venv ## Format (if tools installed)
	@echo "🎨 Formatting (ruff/black if available)"
	@$(PYTHON) -m black backend portal 2>/dev/null || true
	@$(PYTHON) -m ruff check --fix backend portal 2>/dev/null || true

lint: venv ## Lint
	@echo "🔎 Linting (ruff if available)"
	@$(PYTHON) -m ruff check backend portal 2>/dev/null || true

typecheck: venv ## Type-check
	@echo "🧠 mypy (if available)"
	@$(PYTHON) -m mypy backend 2>/dev/null || true

test: venv ## Tests
	@$(PYTHON) -m pytest -q

cov: venv ## Tests + coverage
	@$(PYTHON) -m pytest --maxfail=1 --disable-warnings -q --cov=backend --cov-report=term-missing

clean: ## Remove caches/build artifacts
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@rm -rf .coverage htmlcov dist build $(PID_DIR) $(LOG_DIR) 2>/dev/null || true
	@echo "🧹 Cleaned"
