SHELL := /bin/bash
.DEFAULT_GOAL := help

## ---------- Config ----------
PYTHONPATH ?= .:src
PY          ?= python3
VENV        ?= .venv
PYTHON      := PYTHONPATH=$(PYTHONPATH) $(VENV)/bin/python
PIP         := $(VENV)/bin/pip
REQ         ?= requirements.txt
DEV_REQ     ?= requirements-dev.txt
REQS_HASH   := $(VENV)/.reqs.sha256
CHANGELOG_MODEL ?= gpt-oss:20b

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
STACK_PRESET_all           := core,database,observability # ,brokers removed brokers
STACK_PRESET_core          := core,database
STACK_PRESET_database      := database
STACK_PRESET_observability := observability
STACK_PRESET_brokers       := brokers
STACK_PROFILES             ?= all

empty :=
space := $(empty) $(empty)
comma := ,

STACK_PROFILES_EFFECTIVE := $(if $(strip $(STACK_PROFILES)),$(STACK_PROFILES),all)
STACK_PROFILE_EXPANDED   := $(foreach token,$(subst $(comma), ,$(STACK_PROFILES_EFFECTIVE)),$(if $(strip $(token)),$(if $(STACK_PRESET_$(strip $(token))),$(STACK_PRESET_$(strip $(token))),$(strip $(token))),))
STACK_PROFILE_WORDS      := $(filter-out ,$(subst $(comma), ,$(STACK_PROFILE_EXPANDED)))
STACK_PROFILE_ARGS       := $(foreach profile,$(STACK_PROFILE_WORDS),--profile $(profile))
STACK_PROFILE_DISPLAY    := $(if $(STACK_PROFILE_WORDS),$(subst $(space),$(comma) ,$(strip $(STACK_PROFILE_WORDS))),all)

# Allow "make stack-up BUILD=1" to trigger docker compose --build
STACK_BUILD_FLAG := $(if $(filter 1 true yes on,$(BUILD)),--build,)

PID_DIR     ?= .pids
LOG_DIR     ?= logs

## ============================== HELP ==================================== ##
.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS=":.*##"; print "Usage: make <target>\n\nTargets:"} \
	/^[a-zA-Z0-9_.-]+:.*##/ {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

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

## ============================ LOCAL SETUP ================================ ##
.PHONY: frontend-install local-setup local-up local-stop local-restart local-db-up local-db-stop

frontend-install: ## Install frontend dependencies (npm install)
	@[ -f "$(FRONT_DIR)/package.json" ] && { \
		echo "📦 Installing frontend deps..."; \
		$(NPM) --prefix $(FRONT_DIR) install; \
	} || echo "ℹ️ No frontend package.json found at $(FRONT_DIR)"

local-setup: venv deps frontend-install ## Prepare dependencies for a local (non-Docker) dev run
	@echo "✅ Local dependencies ready"

local-db-up: ## Start TimescaleDB (and companions) for local dev via Docker
	@echo "🐘 Starting TimescaleDB via docker compose"
	@$(COMPOSE_CMD) --profile database up -d tsdb pgadmin
	@echo "⏳ Waiting for TimescaleDB to accept connections..."
	@attempt=0; \
	until $(COMPOSE_CMD) --profile database exec -T tsdb bash -c 'PGPASSWORD=quanttrad pg_isready -q -h localhost -p 5432 -d quanttrad -U quanttrad' >/dev/null 2>&1; do \
		attempt=$$((attempt+1)); \
		if [ $$attempt -ge 30 ]; then \
			echo "❌ TimescaleDB readiness check failed"; \
			exit 1; \
		fi; \
		echo "  ⏱️  waiting for TimescaleDB (attempt $$attempt)..."; \
		sleep 1; \
	done
	@echo "✅ TimescaleDB ready on localhost:$(TSDB_PORT)"

local-up: local-setup local-db-up api-start frontend-start ## Start backend & frontend locally, provisioning TimescaleDB via Docker
	@echo "🚀 Portal running locally (API: http://localhost:8000, Frontend: http://localhost:5173, DB: localhost:$(TSDB_PORT))"

local-db-stop: ## Stop TimescaleDB containers used for local dev
	@echo "🛑 Stopping TimescaleDB containers"
	@$(COMPOSE_CMD) --profile database stop tsdb pgadmin >/dev/null 2>&1 || true

local-stop: api-stop frontend-stop local-db-stop ## Stop locally started backend, frontend, and TimescaleDB
	@echo "🛑 Local portal processes stopped (including TimescaleDB)"

local-restart: local-stop local-up ## Restart locally started backend, frontend, and TimescaleDB

## ============================== API ===================================== ##
.PHONY: api-start api-stop

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
.PHONY: stack-up stack-stop stack-down stack-restart stack-logs stack-ps stack-build stack-rebuild

stack-up: ## Start selected docker compose profiles (STACK_PROFILES=all|core|database|observability)
	@echo "🚢 Starting stack (profiles: $(STACK_PROFILE_DISPLAY))"
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) up $(STACK_BUILD_FLAG) -d
	@profiles="$(STACK_PROFILE_WORDS)"; \

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
	@echo "🛑 Stopping stack (profiles: $(STACK_PROFILE_DISPLAY))"
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) stop

stack-down: ## Remove containers for selected profiles
	@echo "🧹 Removing stack (profiles: $(STACK_PROFILE_DISPLAY))"
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) down --remove-orphans

stack-restart: ## Restart services for selected profiles (use BUILD=1 to rebuild)
	@echo "♻️  Restarting stack (profiles: $(STACK_PROFILE_DISPLAY))"
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) up $(STACK_BUILD_FLAG) --force-recreate -d

stack-logs: ## Follow logs for selected profiles (SERVICE=name to filter)
	@echo "📜 Tailing logs (profiles: $(STACK_PROFILE_DISPLAY))"

	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) logs -f $(SERVICE)

stack-ps: ## Show status for selected profiles
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) ps

stack-build: ## Build images for selected profiles
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) build

stack-rebuild: ## Rebuild images (no cache) and restart selected profiles
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) build --no-cache
	@$(MAKE) stack-up STACK_PROFILES=$(STACK_PROFILES)

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

## ============================ GRAFANA =================================== ##
.PHONY: grafana-backup grafana-restore grafana-list
grafana-backup: ## Backup all Grafana dashboards to JSON files
	@echo "📊 Backing up Grafana dashboards..."
	@bash scripts/backup-grafana-dashboards.sh

grafana-restore: ## Restart Grafana to reload provisioned dashboards
	@echo "🔄 Restarting Grafana to reload dashboards..."
	@$(COMPOSE_CMD) --profile observability restart grafana
	@echo "✅ Grafana restarted - dashboards will be provisioned from JSON files"

grafana-list: ## List all current Grafana dashboards
	@echo "📋 Current Grafana dashboards:"
	@curl -s -u admin:admin http://localhost:3000/api/search?type=dash-db | jq -r '.[] | "  - \(.title) (uid: \(.uid))"' || echo "⚠️  Could not connect to Grafana"

## ============================= AUTOMATION ============================== ##
.PHONY: changelog-pr
changelog-pr: ## Generate changelog using the first open PR for the current branch (requires gh CLI)
	@set -euo pipefail; \
	command -v gh >/dev/null 2>&1 || { echo "❌ GitHub CLI (gh) is required"; exit 1; }; \
	branch=$$(git branch --show-current); \
	# Capture the first open PR for the current branch as TSV: number, title, head, base
	pr_line=$$(gh pr list --state open --head "$$branch" --limit 1 --json number,title,headRefName,baseRefName --jq 'if length > 0 then [.[0].number, .[0].title, .[0].headRefName, .[0].baseRefName] | @tsv else "" end'); \
	if [ -z "$$pr_line" ]; then echo "ℹ️ No open PR found for branch $$branch"; exit 1; fi; \
	IFS=$$'\t' read -r pr_number pr_title head_ref base_ref <<<"$$pr_line"; \
	diff_file=$${DIFF_FILE:-/tmp/changelog.diff}; \
	model=$${CHANGELOG_MODEL:-$(CHANGELOG_MODEL)}; \
	if [ -z "$$model" ]; then echo "❌ CHANGELOG_MODEL is empty"; exit 1; fi; \
	release_name=$${RELEASE_NAME:-$$pr_title}; \
	dry_flag=$${DRY_RUN:+--dry-run}; \
	config_path=$${CHANGELOG_CONFIG:-scripts/automation/config/prompts.yaml}; \
	echo "📝 Writing diff for $$base_ref..$$head_ref to $$diff_file"; \
	git log  --pretty=format:'%h%n%s%n%b%n---' "$$base_ref..$$head_ref" > "$$diff_file"; \
	if [ ! -s "$$diff_file" ]; then echo "⚠️ Generated diff is empty"; exit 1; fi; \
	echo "🚀 Generating changelog for PR $$pr_number (head: $$head_ref, base: $$base_ref)"; \
	PYTHONPATH=scripts $(PY) scripts/automation/llm_changelog.py --diff-file "$$diff_file" --branch "$$head_ref" --release-name "$$release_name" --model "$$model" --config "$$config_path" $$dry_flag

.PHONY: changelog-pr-batch
changelog-pr-batch: ## Generate changelog entries for merged PRs since BASE_BRANCH (requires gh CLI, e.g., BASE_BRANCH=develop)
	@set -euo pipefail; \
	if [ -z "$${BASE_BRANCH:-}" ]; then echo "❌ BASE_BRANCH is required (e.g., BASE_BRANCH=develop)"; exit 1; fi; \
	scripts/automation/changelog_pr_batch.sh "$$BASE_BRANCH"
