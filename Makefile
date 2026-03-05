SHELL := /bin/bash
.DEFAULT_GOAL := help

## ---------- Config ----------
-include .sync-docs.mk

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
COMPOSE_BAKE ?= false
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

# Docs sync (Obsidian/Windows rsync friendly)
SYNC_DOCS_SRC         ?= docs/
SYNC_DOCS_DEST        ?= $(or $(OBSIDIAN_SYNC_DOCS_DEST),$(OBSIDIAN_SYNC_DEST),)
SYNC_DOCS_RSYNC       ?= rsync
SYNC_DOCS_RSYNC_FLAGS ?= -az
SYNC_DOCS_DELETE      ?= 0
SYNC_DOCS_DELETE_FLAG := $(if $(filter 1 true yes on,$(SYNC_DOCS_DELETE)),--delete,)

## ============================== HELP ==================================== ##
.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS=":.*##"; print "Usage: make <target>\n\nTargets:"} \
	/^[a-zA-Z0-9_.-]+:.*##/ {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

## ========================== QUICK COMMANDS ============================== ##
.PHONY: up down restart logs build rebuild ps sync-docs sync-logs

up: stack-up sync-docs ## Start stack (alias for stack-up, use STACK_PROFILES= to customize)
down: stack-down ## Stop and remove stack (alias for stack-down)
restart: stack-restart ## Restart stack (alias for stack-restart, use BUILD=1 to rebuild)
logs: stack-logs ## Tail logs (alias for stack-logs, use SERVICE= to filter)
build: stack-build sync-docs ## Build images (alias for stack-build)
rebuild: stack-rebuild ## Rebuild images from scratch (alias for stack-rebuild)
ps: stack-ps ## Show running containers (alias for stack-ps)

sync-docs: ## Sync ./docs to external path via rsync (set SYNC_DOCS_DEST or OBSIDIAN_SYNC_DOCS_DEST)
	@set -euo pipefail; \
	if ! command -v "$(SYNC_DOCS_RSYNC)" >/dev/null 2>&1; then \
		echo "✗ rsync not found on PATH"; exit 1; \
	fi; \
	src_raw="$(SYNC_DOCS_SRC)"; \
	dest_raw="$(SYNC_DOCS_DEST)"; \
	if [ -z "$$dest_raw" ]; then \
		echo "ℹ sync-docs skipped: set SYNC_DOCS_DEST (or OBSIDIAN_SYNC_DOCS_DEST)"; \
		exit 0; \
	fi; \
	src="$$(cd "$$src_raw" >/dev/null 2>&1 && pwd)/"; \
	dest="$$dest_raw"; \
	dest="$${dest//\{HOME\}/$$HOME}"; \
	dest="$${dest//\{USER\}/$$USER}"; \
	dest="$${dest//\{REPO\}/$(CURDIR)}"; \
	if [[ "$$dest" =~ ^[A-Za-z]:\\\\ ]]; then \
		drive="$${dest:0:1}"; \
		rest="$${dest:2}"; \
		rest="$${rest//\\//}"; \
		drive="$$(echo "$$drive" | tr '[:upper:]' '[:lower:]')"; \
		dest="/mnt/$$drive/$${rest#/}"; \
	fi; \
	is_remote=0; \
	if [[ "$$dest" == *:* ]] && [[ ! "$$dest" =~ ^/mnt/[a-z]/ ]]; then \
		is_remote=1; \
	fi; \
	if [ "$$is_remote" -eq 0 ]; then \
		mkdir -p "$$dest"; \
	fi; \
	echo "► Syncing docs: $$src -> $$dest"; \
	"$(SYNC_DOCS_RSYNC)" $(SYNC_DOCS_RSYNC_FLAGS) $(SYNC_DOCS_DELETE_FLAG) "$$src" "$$dest"; \
	echo "✓ Docs synced"

sync-logs: sync-docs ## Alias for docs sync (backward-compatible target name)

## ============================ BOOTSTRAP ================================= ##
.PHONY: deps _deps_hash _ensure_python _ensure_dirs

deps: _ensure_python _deps_hash ## Install Python dependencies
	@if [ ! -x "$(PYTHON)" ]; then \
		echo "► Creating venv at $(VENV)"; \
		$(PY) -m venv $(VENV); \
	fi
	@if [ "$$(cat $(REQS_HASH).new)" != "$$(cat $(REQS_HASH) 2>/dev/null || echo _none_)" ]; then \
		echo "► Installing Python deps..."; \
		$(PIP) install --upgrade pip setuptools wheel; \
		[ -f "$(REQ)" ] && $(PIP) install -r $(REQ) || true; \
		[ -f "$(DEV_REQ)" ] && $(PIP) install -r $(DEV_REQ) || true; \
		mv -f $(REQS_HASH).new $(REQS_HASH); \
	else \
		echo "✓ Dependencies unchanged"; \
		rm -f $(REQS_HASH).new; \
	fi

_deps_hash:
	@{ cat $(REQ) 2>/dev/null || true; echo; cat $(DEV_REQ) 2>/dev/null || true; } \
	| sha256sum | awk '{print $$1}' > $(REQS_HASH).new

_ensure_python:
	@command -v $(PY) >/dev/null 2>&1 || { echo "✗ $(PY) not found on PATH"; exit 1; }

_ensure_dirs:
	@mkdir -p $(PID_DIR) $(LOG_DIR)

## ============================ COINBASE ============================ ##
.PHONY: coinbase-jwt
coinbase-jwt: ## Generate Coinbase JWT (reads secrets.env)
	@$(PYTHON) scripts/coinbase_jwt.py --path /api/v3/brokerage/products


## ============================== DOCKER ================================== ##
.PHONY: stack-up stack-stop stack-down stack-restart stack-logs stack-ps stack-build stack-rebuild fetch-ddl

stack-up: ## Start selected docker compose profiles (STACK_PROFILES=all|core|database|observability)
	@echo "► Starting stack [$(STACK_PROFILE_DISPLAY)]"
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) up $(STACK_BUILD_FLAG) -d 2>&1 | grep -E '^(\s+(✔|⠿)|Container)' || true
	@profiles="$(STACK_PROFILE_WORDS)"; \
		endpoints=""; \
		if echo "$$profiles" | grep -qw core; then \
		        endpoints="$$endpoints\n  ► Frontend   http://localhost:5173\n  ► Backend    http://localhost:8000"; \
		fi; \
		if echo "$$profiles" | grep -qw database; then \
		        endpoints="$$endpoints\n  ► TimescaleDB tcp://localhost:$(TSDB_PORT)\n  ► pgAdmin     http://localhost:8080"; \
		fi; \
		if echo "$$profiles" | grep -qw observability; then \
		        endpoints="$$endpoints\n  ► Grafana     http://localhost:3000\n  ► Loki        http://localhost:3100"; \
		fi; \
		if [ -n "$$endpoints" ]; then \
		        echo -e "\n✓ Stack ready$$endpoints"; \
		fi

stack-stop: ## Stop running services for selected profiles (containers remain)
	@echo "► Stopping stack [$(STACK_PROFILE_DISPLAY)]"
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) stop 2>&1 | grep -E '^(\s+(✔|⠿)|Container)' || true
	@echo "✓ Stack stopped"

stack-down: ## Remove containers for selected profiles
	@echo "► Removing stack [$(STACK_PROFILE_DISPLAY)]"
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) down --remove-orphans 2>&1 | grep -E '^(\s+(✔|⠿)|Container|Network)' || true
	@echo "✓ Stack removed"

stack-restart: ## Restart services for selected profiles (use BUILD=1 to rebuild)
	@echo "► Restarting stack [$(STACK_PROFILE_DISPLAY)]"
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) up $(STACK_BUILD_FLAG) --force-recreate -d 2>&1 | grep -E '^(\s+(✔|⠿)|Container)' || true
	@echo "✓ Stack restarted"

stack-logs: ## Follow logs for selected profiles (SERVICE=name to filter)
	@echo "► Tailing logs [$(STACK_PROFILE_DISPLAY)]$(if $(SERVICE), → $(SERVICE),)"
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) logs -f $(SERVICE)

stack-ps: ## Show status for selected profiles
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) ps

stack-build: ## Build images for selected profiles
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) build

stack-rebuild: ## Rebuild images (no cache) and restart selected profiles
	@$(COMPOSE_CMD) $(STACK_PROFILE_ARGS) build --no-cache
	@$(MAKE) stack-up STACK_PROFILES=$(STACK_PROFILES)

fetch-ddl: ## Copy TSDB schema DDL to clipboard
	@COMPOSE_FILE="$(COMPOSE_FILE)" TSDB_SERVICE=tsdb bash scripts/fetch-tsdb-ddl.sh

## =============================== QUALITY ================================ ##
.PHONY: fmt lint typecheck test cov clean
fmt: venv ## Format (if tools installed)
	@echo "► Formatting (ruff/black if available)"
	@$(PYTHON) -m black backend portal 2>/dev/null || true
	@$(PYTHON) -m ruff check --fix backend portal 2>/dev/null || true

lint: venv ## Lint
	@echo "► Linting (ruff if available)"
	@$(PYTHON) -m ruff check backend portal 2>/dev/null || true

typecheck: venv ## Type-check
	@echo "► Type-checking (mypy if available)"
	@$(PYTHON) -m mypy backend 2>/dev/null || true

test: venv ## Tests
	@$(PYTHON) -m pytest -q

cov: venv ## Tests + coverage
	@$(PYTHON) -m pytest --maxfail=1 --disable-warnings -q --cov=backend --cov-report=term-missing

clean: ## Remove caches/build artifacts
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@rm -rf .coverage htmlcov dist build $(PID_DIR) $(LOG_DIR) 2>/dev/null || true
	@echo "✓ Cleaned"

## ============================ GRAFANA =================================== ##
.PHONY: grafana-backup grafana-restore grafana-list
grafana-backup: ## Backup all Grafana dashboards to JSON files
	@echo "► Backing up Grafana dashboards..."
	@bash scripts/backup-grafana-dashboards.sh

grafana-restore: ## Restart Grafana to reload provisioned dashboards
	@echo "► Restarting Grafana to reload dashboards..."
	@$(COMPOSE_CMD) --profile observability restart grafana
	@echo "✓ Grafana restarted - dashboards will be provisioned from JSON files"

grafana-list: ## List all current Grafana dashboards
	@echo "► Current Grafana dashboards:"
	@curl -s -u admin:admin http://localhost:3000/api/search?type=dash-db | jq -r '.[] | "  - \(.title) (uid: \(.uid))"' || echo "⚠ Could not connect to Grafana"

## ============================= AUTOMATION ============================== ##
.PHONY: changelog-pr
changelog-pr: ## Generate changelog using the first open PR for the current branch (requires gh CLI)
	@set -euo pipefail; \
	command -v gh >/dev/null 2>&1 || { echo "✗ GitHub CLI (gh) is required"; exit 1; }; \
	branch=$$(git branch --show-current); \
	# Capture the first open PR for the current branch as TSV: number, title, head, base
	pr_line=$$(gh pr list --state open --head "$$branch" --limit 1 --json number,title,headRefName,baseRefName --jq 'if length > 0 then [.[0].number, .[0].title, .[0].headRefName, .[0].baseRefName] | @tsv else "" end'); \
	if [ -z "$$pr_line" ]; then echo "ℹ No open PR found for branch $$branch"; exit 1; fi; \
	IFS=$$'\t' read -r pr_number pr_title head_ref base_ref <<<"$$pr_line"; \
	diff_file=$${DIFF_FILE:-/tmp/changelog.diff}; \
	model=$${CHANGELOG_MODEL:-$(CHANGELOG_MODEL)}; \
	if [ -z "$$model" ]; then echo "✗ CHANGELOG_MODEL is empty"; exit 1; fi; \
	release_name=$${RELEASE_NAME:-$$pr_title}; \
	dry_flag=$${DRY_RUN:+--dry-run}; \
	config_path=$${CHANGELOG_CONFIG:-scripts/automation/config/prompts.yaml}; \
	echo "► Writing diff for $$base_ref..$$head_ref to $$diff_file"; \
	git log  --pretty=format:'%h%n%s%n%b%n---' "$$base_ref..$$head_ref" > "$$diff_file"; \
	if [ ! -s "$$diff_file" ]; then echo "⚠ Generated diff is empty"; exit 1; fi; \
	echo "► Generating changelog for PR $$pr_number (head: $$head_ref, base: $$base_ref)"; \
	PYTHONPATH=scripts $(PY) scripts/automation/llm_changelog.py --diff-file "$$diff_file" --branch "$$head_ref" --release-name "$$release_name" --model "$$model" --config "$$config_path" $$dry_flag

.PHONY: changelog-pr-batch
changelog-pr-batch: ## Generate changelog entries for merged PRs since BASE_BRANCH (requires gh CLI, e.g., BASE_BRANCH=develop)
	@set -euo pipefail; \
	if [ -z "$${BASE_BRANCH:-}" ]; then echo "✗ BASE_BRANCH is required (e.g., BASE_BRANCH=develop)"; exit 1; fi; \
	scripts/automation/changelog_pr_batch.sh "$$BASE_BRANCH"

.PHONY: branch-artifact
branch-artifact: ## Build artifact with all commits and merged PRs for a branch (defaults to current branch)
	@set -euo pipefail; \
	branch=$${BRANCH:-$$(git branch --show-current)}; \
	if [ -z "$$branch" ]; then echo "✗ Could not determine branch (not on a branch?)"; exit 1; fi; \
	scripts/automation/branch_artifact_builder.sh "$$branch"


BOTS_COMPOSE_FILE ?= docker/docker-compose.bots.yml
BOTS_COMPOSE_CMD  ?= docker compose -f $(BOTS_COMPOSE_FILE)

.PHONY: bots-up bots-down bots-ps bots-logs

bots-up: ## Start isolated bot containers stack
	@$(BOTS_COMPOSE_CMD) up -d

bots-down: ## Stop isolated bot containers stack
	@$(BOTS_COMPOSE_CMD) down --remove-orphans

bots-ps: ## List isolated bot containers
	@$(BOTS_COMPOSE_CMD) ps

bots-logs: ## Tail isolated bot containers logs
	@$(BOTS_COMPOSE_CMD) logs -f
