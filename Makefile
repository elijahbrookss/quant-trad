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

COMPOSE_FILE ?= docker/docker-compose.yml
COMPOSE_CMD  ?= docker compose -f $(COMPOSE_FILE)
COMPOSE_BAKE ?= true
export COMPOSE_BAKE

TSDB_PORT ?= 15432
export TSDB_PORT

# Docker stack profile helpers
STACK_PRESET_all           := core,database,observability
STACK_PRESET_core          := core,database
STACK_PRESET_database      := database
STACK_PRESET_observability := observability
STACK_PROFILES             ?= all

define _resolve_profiles
$(strip $(foreach item,$(subst ',', ,$(1)),$(if $(STACK_PRESET_$(item)),$(STACK_PRESET_$(item)),$(item))))
endef

STACK_PROFILE_LIST := $(call _resolve_profiles,$(STACK_PROFILES))
STACK_PROFILE_ARGS := $(foreach profile,$(sort $(subst ',', ,$(STACK_PROFILE_LIST))),--profile $(profile))

# Allow "make stack-up BUILD=1" to trigger docker compose --build
STACK_BUILD_FLAG := $(if $(filter 1 true yes on,$(BUILD)),--build,)

PID_DIR     ?= .pids
LOG_DIR     ?= logs

## ============================== HELP ==================================== ##
.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS":.*##"; print "Usage: make <target>\n\nTargets:"} \
	/^[a-zA-Z0-9_.-]+:.*##/ {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

## ============================ ORCHESTRATOR =============================== ##
.PHONY: dev stop
dev: venv deps dev-up frontend-start api-up  ## One-shot: venv+deps → infra → API → Frontend
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
.PHONY: dev-up dev-down dev-logs dev-ps \
stack-up stack-stop stack-down stack-restart stack-logs stack-ps stack-build stack-rebuild \
stack-up-all stack-up-core stack-up-database stack-up-observability \
stack-stop-all stack-stop-core stack-stop-database stack-stop-observability \
stack-restart-all stack-restart-core stack-restart-database stack-restart-observability \
compose-up compose-down compose-logs compose-ps compose-core compose-db compose-observability

dev-up: stack-up-all ## Backwards-compatible alias for docker stack startup

dev-down: stack-down-all ## Backwards-compatible alias for docker stack shutdown

dev-logs: stack-logs ## Backwards-compatible alias for docker stack logs

dev-ps: stack-ps ## Backwards-compatible alias for docker stack status

stack-up: ## Start selected docker compose profiles (STACK_PROFILES=all|core|database|observability)
	@echo "🚢 Starting stack (profiles: $(STACK_PROFILE_LIST))"
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) up $(STACK_BUILD_FLAG) -d
	@profiles="$(STACK_PROFILE_LIST)"; \
	  if echo "$$profiles" | grep -qw core; then \
	    echo "➡ Frontend http://localhost:5173 | Backend http://localhost:8000"; \
	  fi; \
	  if echo "$$profiles" | grep -qw database; then \
	    echo "➡ TimescaleDB tcp://localhost:$(TSDB_PORT) | pgAdmin http://localhost:8080"; \
	  fi; \
	  if echo "$$profiles" | grep -qw observability; then \
	    echo "➡ Grafana http://localhost:3000 | Loki http://localhost:3100"; \
	  fi

stack-stop: ## Stop running services for selected profiles (containers remain)
	@echo "🛑 Stopping stack (profiles: $(STACK_PROFILE_LIST))"
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) stop

stack-down: ## Remove containers for selected profiles
	@echo "🧹 Removing stack (profiles: $(STACK_PROFILE_LIST))"
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) down --remove-orphans

stack-restart: ## Restart services for selected profiles (use BUILD=1 to rebuild)
	@echo "♻️  Restarting stack (profiles: $(STACK_PROFILE_LIST))"
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) up $(STACK_BUILD_FLAG) --force-recreate -d

stack-logs: ## Follow logs for selected profiles (SERVICE=name to filter)
	@echo "📜 Tailing logs (profiles: $(STACK_PROFILE_LIST))"
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) logs -f $(SERVICE)

stack-ps: ## Show status for selected profiles
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) ps

stack-build: ## Build images for selected profiles
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) build

stack-rebuild: ## Rebuild images (no cache) and restart selected profiles
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) build --no-cache
	@$(MAKE) stack-up STACK_PROFILES="$(STACK_PROFILES)"

stack-up-all: ## Start entire stack (all profiles)
	@$(MAKE) stack-up STACK_PROFILES=all

stack-up-core: ## Start application services (frontend/backend + database)
	@$(MAKE) stack-up STACK_PROFILES=core

stack-up-database: ## Start database profile only
	@$(MAKE) stack-up STACK_PROFILES=database

stack-up-observability: ## Start observability tooling only
	@$(MAKE) stack-up STACK_PROFILES=observability

stack-stop-all: ## Stop every profile without removing containers
	@$(MAKE) stack-stop STACK_PROFILES=all

stack-stop-core: ## Stop application services (frontend/backend + database)
	@$(MAKE) stack-stop STACK_PROFILES=core

stack-stop-database: ## Stop database profile only
	@$(MAKE) stack-stop STACK_PROFILES=database

stack-stop-observability: ## Stop observability tooling only
	@$(MAKE) stack-stop STACK_PROFILES=observability

stack-restart-all: ## Restart entire stack (all profiles)
	@$(MAKE) stack-restart STACK_PROFILES=all BUILD=$(BUILD)

stack-restart-core: ## Restart application services (frontend/backend + database)
	@$(MAKE) stack-restart STACK_PROFILES=core BUILD=$(BUILD)

stack-restart-database: ## Restart database profile only
	@$(MAKE) stack-restart STACK_PROFILES=database BUILD=$(BUILD)

stack-restart-observability: ## Restart observability tooling only
	@$(MAKE) stack-restart STACK_PROFILES=observability BUILD=$(BUILD)

# Legacy aliases retained for compatibility
compose-up: ## Start frontend, backend, database, and observability stacks
	@$(MAKE) stack-up-all BUILD=$(BUILD)

compose-down: ## Stop all docker stacks
	@$(MAKE) stack-down STACK_PROFILES=all

compose-logs: ## Tail logs from the active docker stack
	@$(MAKE) stack-logs STACK_PROFILES=all SERVICE=$(SERVICE)

compose-ps: ## Show status of running docker services
	@$(MAKE) stack-ps STACK_PROFILES=all

compose-core: ## Start the frontend and backend (database included for dependencies)
	@$(MAKE) stack-up STACK_PROFILES=core BUILD=$(BUILD)

compose-db: ## Start only the database services
	@$(MAKE) stack-up STACK_PROFILES=database BUILD=$(BUILD)

compose-observability: ## Start only Grafana, Loki, and Promtail
	@$(MAKE) stack-up STACK_PROFILES=observability BUILD=$(BUILD)

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
