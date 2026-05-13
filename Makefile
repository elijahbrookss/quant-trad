SHELL := /bin/bash
.DEFAULT_GOAL := help

## ---------- Config ----------
-include .sync-docs.mk

PYTHONPATH ?= .:src
PY          ?= python3
VENV        ?= .venv
VENV_PYTHON := $(VENV)/bin/python
PYTHON      := PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON)
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
COMPOSE_BAKE ?= false
export COMPOSE_BAKE

BOTS_COMPOSE_FILE ?= docker/docker-compose.bots.yml
BOTS_COMPOSE_CMD  ?= docker compose -f $(BOTS_COMPOSE_FILE)

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
LOG_TAIL    ?= 240

DB_SERVICE      ?= tsdb
BACKEND_SERVICE ?= backend
BOT_SERVICE     ?= bot-runtime
REPORT_EXPORT_DIR ?= $(LOG_DIR)/reports
GOLDEN_OUT_DIR    ?= $(REPORT_EXPORT_DIR)/golden-repeatability
BACKEND_API_URL   ?= http://127.0.0.1:8000
RUN_WAIT_INTERVAL ?= 30
RUN_WAIT_TIMEOUT  ?= 3600

LOCAL_PG_ENV = if [ -f secrets.env ]; then \
	while IFS='=' read -r key value; do \
		key="$${key%%[[:space:]]*}"; \
		value="$${value%$$'\r'}"; \
		case "$$key" in PG_DSN|POSTGRES_USER|POSTGRES_PASSWORD|POSTGRES_DB) \
			if [ -z "$${!key:-}" ]; then \
				export "$$key=$$value"; \
			fi; \
		esac; \
	done < secrets.env; \
	fi; \
	export QT_LOGGING_LOKI_URL=""; \
	export QT_LOGGING_DEBUG=false; \
	export QT_LOGGING_LEVEL=WARNING; \
	export MPLCONFIGDIR="$${MPLCONFIGDIR:-/tmp/matplotlib}"; \
	mkdir -p "$$MPLCONFIGDIR"; \
	if [ -z "$${PG_DSN:-}" ]; then \
		: "$${POSTGRES_USER:?POSTGRES_USER or PG_DSN is required}"; \
		: "$${POSTGRES_PASSWORD:?POSTGRES_PASSWORD or PG_DSN is required}"; \
		: "$${POSTGRES_DB:?POSTGRES_DB or PG_DSN is required}"; \
		export PG_DSN="postgresql+psycopg2://$${POSTGRES_USER}:$${POSTGRES_PASSWORD}@localhost:$(TSDB_PORT)/$${POSTGRES_DB}"; \
	fi
PYTEST_ENV = QT_LOGGING_LOKI_URL= QT_LOGGING_DEBUG=false QT_LOGGING_LEVEL=WARNING MPLCONFIGDIR=/tmp/matplotlib
REPORT_API_TEST_TIMEOUT ?= 90s

# Docs sync (Obsidian/Windows rsync friendly)
SYNC_DOCS_SRC         ?= docs/
SYNC_DOCS_DEST        ?= $(or $(OBSIDIAN_SYNC_DOCS_DEST),$(OBSIDIAN_SYNC_DEST),)
SYNC_DOCS_RSYNC       ?= rsync
SYNC_DOCS_RSYNC_FLAGS ?= -az
SYNC_DOCS_DELETE      ?= 0
SYNC_DOCS_DELETE_FLAG := $(if $(filter 1 true yes on,$(SYNC_DOCS_DELETE)),--delete,)

# Mermaid diagram rendering
MERMAID_SRC         ?= docs/architecture
MERMAID_CLI         ?= mmdc
MERMAID_RENDER_ARGS ?=
MERMAID_CLI_ARGS    ?=

## ============================== HELP ==================================== ##
.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS=":.*##"; print "Usage: make <target>\n\nTargets:"} \
	/^[a-zA-Z0-9_.-]+:.*##/ {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

## ========================== QUICK COMMANDS ============================== ##
.PHONY: up down restart logs build rebuild ps sync-docs architecture-svgs mermaid-svgs

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

architecture-svgs: mermaid-svgs ## Render docs/architecture .mmd files to sibling .svg files

mermaid-svgs: ## Render .mmd files to sibling .svg files (MERMAID_SRC=path)
	@$(PY) scripts/docs/render_mermaid_svgs.py --root "$(MERMAID_SRC)" --mmdc "$(MERMAID_CLI)" $(MERMAID_RENDER_ARGS) $(if $(strip $(MERMAID_CLI_ARGS)),-- $(MERMAID_CLI_ARGS),)

## ============================ BOOTSTRAP ================================= ##
.PHONY: deps venv _deps_hash _ensure_python _ensure_dirs

deps: _ensure_python _deps_hash ## Install Python dependencies
	@if [ ! -x "$(VENV_PYTHON)" ]; then \
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

venv: deps ## Ensure virtualenv and Python dependencies

_deps_hash:
	@{ cat $(REQ) 2>/dev/null || true; echo; cat $(DEV_REQ) 2>/dev/null || true; } \
	| sha256sum | awk '{print $$1}' > $(REQS_HASH).new

_ensure_python:
	@command -v $(PY) >/dev/null 2>&1 || { echo "✗ $(PY) not found on PATH"; exit 1; }

_ensure_dirs:
	@mkdir -p $(PID_DIR) $(LOG_DIR)

## ============================== DOCKER ================================== ##
.PHONY: stack-up stack-stop stack-down stack-restart stack-logs stack-ps stack-build stack-rebuild \
	bots-up bots-down bots-ps bots-logs

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

bots-up: ## Start isolated bot containers stack
	@$(BOTS_COMPOSE_CMD) up -d

bots-down: ## Stop isolated bot containers stack
	@$(BOTS_COMPOSE_CMD) down --remove-orphans

bots-ps: ## List isolated bot containers
	@$(BOTS_COMPOSE_CMD) ps

bots-logs: ## Tail isolated bot containers logs
	@$(BOTS_COMPOSE_CMD) logs -f

## ============================ DEV / AUDIT =============================== ##
.PHONY: status logs-backend logs-bots backend-shell bot-shell dbshell db-query db-file \
	bot-active bot-start bot-stop run-status run-wait golden-compare \
	report-export report-readiness report-dataset report-summary report-diagnostics report-manifest \
	run-ordering run-throughput run-event-summary run-seq-gaps run-write-latency observability-storage-budget \
	botlens-check wallet-diagnostics report-wallet-diagnostics \
	test-reporting test-reporting-api test-botlens test-runtime validate-docs frontend-test frontend-build frontend-check \
	git-status git-diff git-check check commit

status: ## Show service status without docker compose ps sandbox friction
	@echo "Core stack:"
	@docker ps --filter "name=quant-trad" --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}\t{{.Ports}}'
	@echo ""
	@echo "Bot runtimes:"
	@docker ps --filter "name=quant-trad-bots" --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}\t{{.Ports}}'

logs-backend: ## Tail backend logs (LOG_TAIL=240)
	@$(COMPOSE_CMD) --profile core logs --tail $(LOG_TAIL) -f $(BACKEND_SERVICE)

logs-bots: ## Tail bot runtime logs (bot=<container> for a spawned runtime container)
	@set -euo pipefail; \
	if [ -n "$(strip $(bot))" ]; then \
		docker logs --tail $(LOG_TAIL) -f "$(bot)"; \
	else \
		$(BOTS_COMPOSE_CMD) logs --tail $(LOG_TAIL) -f $(BOT_SERVICE); \
	fi

backend-shell: ## Open a shell in the backend service container
	@$(COMPOSE_CMD) --profile core exec $(BACKEND_SERVICE) bash

bot-shell: ## Open a shell in a bot runtime container (bot=<container> for spawned runtimes)
	@set -euo pipefail; \
	if [ -n "$(strip $(bot))" ]; then \
		docker exec -it "$(bot)" bash; \
	else \
		$(BOTS_COMPOSE_CMD) exec $(BOT_SERVICE) bash; \
	fi

dbshell: ## Open psql inside the TimescaleDB container
	@$(COMPOSE_CMD) --profile database exec $(DB_SERVICE) bash -lc 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

db-query: ## Run one SQL statement against TimescaleDB (sql="select 1")
	@set -euo pipefail; \
	if [ -z "$(strip $(sql))" ]; then echo '✗ sql="..." is required'; exit 1; fi; \
	$(COMPOSE_CMD) --profile database exec -T -e SQL="$(sql)" $(DB_SERVICE) bash -lc 'psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "$$SQL"'

db-file: ## Run a SQL file against TimescaleDB (file=scripts/db/example.sql)
	@set -euo pipefail; \
	if [ -z "$(strip $(file))" ]; then echo "✗ file= is required"; exit 1; fi; \
	test -f "$(file)" || { echo "✗ SQL file not found: $(file)"; exit 1; }; \
	$(COMPOSE_CMD) --profile database exec -T $(DB_SERVICE) bash -lc 'psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"' < "$(file)"

bot-active: venv ## Print active run state for a bot (bot=<bot_id>)
	@set -euo pipefail; \
	if [ -z "$(strip $(bot))" ]; then echo "✗ bot=<bot_id> is required"; exit 1; fi; \
	$(PYTEST_ENV) $(PYTHON) scripts/reporting/bot_runtime_control.py --api-url "$(BACKEND_API_URL)" active --bot-id "$(bot)"

bot-start: venv ## Start a bot and print the accepted run id (bot=<bot_id> request=<optional>)
	@set -euo pipefail; \
	if [ -z "$(strip $(bot))" ]; then echo "✗ bot=<bot_id> is required"; exit 1; fi; \
	$(PYTEST_ENV) $(PYTHON) scripts/reporting/bot_runtime_control.py --api-url "$(BACKEND_API_URL)" start --bot-id "$(bot)" $(if $(strip $(request)),--request-id "$(request)",)

bot-stop: venv ## Stop a bot run through the backend API (bot=<bot_id> run=<optional> preserve=1)
	@set -euo pipefail; \
	if [ -z "$(strip $(bot))" ]; then echo "✗ bot=<bot_id> is required"; exit 1; fi; \
	$(PYTEST_ENV) $(PYTHON) scripts/reporting/bot_runtime_control.py --api-url "$(BACKEND_API_URL)" stop --bot-id "$(bot)" $(if $(strip $(run)),--run-id "$(run)",) $(if $(strip $(request)),--request-id "$(request)",) $(if $(filter 1 true yes on,$(preserve)),--preserve-container,)

run-status: venv ## Print persisted run status (run=<run_id>)
	@set -euo pipefail; \
	if [ -z "$(strip $(run))" ]; then echo "✗ run=<run_id> is required"; exit 1; fi; \
	$(LOCAL_PG_ENV); \
	$(PYTHON) scripts/reporting/bot_runtime_control.py status --run-id "$(run)"

run-wait: venv ## Wait for a run to reach terminal DB status (run=<run_id> timeout=3600 interval=30)
	@set -euo pipefail; \
	if [ -z "$(strip $(run))" ]; then echo "✗ run=<run_id> is required"; exit 1; fi; \
	$(LOCAL_PG_ENV); \
	$(PYTHON) scripts/reporting/bot_runtime_control.py --timeout "$(if $(strip $(timeout)),$(timeout),$(RUN_WAIT_TIMEOUT))" wait --run-id "$(run)" --interval "$(if $(strip $(interval)),$(interval),$(RUN_WAIT_INTERVAL))" $(if $(filter 1 true yes on,$(print_each)),--print-each,) $(if $(filter 1 true yes on,$(allow_non_completed)),--allow-non-completed,)

report-export: venv _ensure_dirs ## Export a run report bundle (run=<run_id> out=logs/reports include_candles=0)
	@set -euo pipefail; \
	if [ -z "$(strip $(run))" ]; then echo "✗ run=<run_id> is required"; exit 1; fi; \
	$(LOCAL_PG_ENV); \
	$(PYTHON) scripts/reporting/inspect_report.py export --run-id "$(run)" --out-dir "$(if $(strip $(out)),$(out),$(REPORT_EXPORT_DIR))" $(if $(filter 1 true yes on,$(include_candles)),--include-candles,)

report-readiness: venv ## Inspect report readiness for a run (run=<run_id>)
	@set -euo pipefail; \
	if [ -z "$(strip $(run))" ]; then echo "✗ run=<run_id> is required"; exit 1; fi; \
	$(LOCAL_PG_ENV); \
	$(PYTHON) scripts/reporting/inspect_report.py readiness --run-id "$(run)"

report-dataset: venv ## Print the canonical report dataset for a run (run=<run_id>)
	@set -euo pipefail; \
	if [ -z "$(strip $(run))" ]; then echo "✗ run=<run_id> is required"; exit 1; fi; \
	$(LOCAL_PG_ENV); \
	$(PYTHON) scripts/reporting/inspect_report.py dataset --run-id "$(run)"

report-summary: venv ## Print the compact report summary for a run (run=<run_id>)
	@set -euo pipefail; \
	if [ -z "$(strip $(run))" ]; then echo "✗ run=<run_id> is required"; exit 1; fi; \
	$(LOCAL_PG_ENV); \
	$(PYTHON) scripts/reporting/inspect_report.py summary --run-id "$(run)"

report-diagnostics: venv ## Print report diagnostics for a run (run=<run_id>)
	@set -euo pipefail; \
	if [ -z "$(strip $(run))" ]; then echo "✗ run=<run_id> is required"; exit 1; fi; \
	$(LOCAL_PG_ENV); \
	$(PYTHON) scripts/reporting/inspect_report.py diagnostics --run-id "$(run)"

report-manifest: venv ## Print the report export manifest for a run (run=<run_id> include_candles=0)
	@set -euo pipefail; \
	if [ -z "$(strip $(run))" ]; then echo "✗ run=<run_id> is required"; exit 1; fi; \
	$(LOCAL_PG_ENV); \
	$(PYTHON) scripts/reporting/inspect_report.py manifest --run-id "$(run)" $(if $(filter 1 true yes on,$(include_candles)),--include-candles,)

run-ordering: venv ## Check runtime event ordering health for a run (run=<run_id>)
	@set -euo pipefail; \
	if [ -z "$(strip $(run))" ]; then echo "✗ run=<run_id> is required"; exit 1; fi; \
	$(LOCAL_PG_ENV); \
	$(PYTHON) scripts/reporting/check_run_ordering.py --run-id "$(run)"

run-throughput: venv ## Summarize runtime event throughput by minute (run=<run_id>)
	@set -euo pipefail; \
	if [ -z "$(strip $(run))" ]; then echo "✗ run=<run_id> is required"; exit 1; fi; \
	$(LOCAL_PG_ENV); \
	$(PYTHON) scripts/reporting/runtime_event_diagnostics.py throughput --run-id "$(run)"

run-event-summary: venv ## Summarize runtime event counts by event type/name (run=<run_id>)
	@set -euo pipefail; \
	if [ -z "$(strip $(run))" ]; then echo "✗ run=<run_id> is required"; exit 1; fi; \
	$(LOCAL_PG_ENV); \
	$(PYTHON) scripts/reporting/runtime_event_diagnostics.py event-summary --run-id "$(run)"

run-storage-budget: venv ## Estimate runtime event storage budget by tier (run=<optional_run_id>)
	@set -euo pipefail; \
	$(LOCAL_PG_ENV); \
	$(PYTHON) scripts/reporting/runtime_event_diagnostics.py storage-budget $(if $(strip $(run)),--run-id "$(run)",)

run-seq-gaps: venv ## Check runtime run_seq gaps and duplicates (run=<run_id>)
	@set -euo pipefail; \
	if [ -z "$(strip $(run))" ]; then echo "✗ run=<run_id> is required"; exit 1; fi; \
	$(LOCAL_PG_ENV); \
	$(PYTHON) scripts/reporting/runtime_event_diagnostics.py seq-gaps --run-id "$(run)" $(if $(strip $(limit)),--limit "$(limit)",)

run-write-latency: venv ## Summarize runtime event DB write latency metrics (run=<run_id>)
	@set -euo pipefail; \
	if [ -z "$(strip $(run))" ]; then echo "✗ run=<run_id> is required"; exit 1; fi; \
	$(LOCAL_PG_ENV); \
	$(PYTHON) scripts/reporting/runtime_event_diagnostics.py write-latency --run-id "$(run)"

observability-storage-budget: venv ## Summarize durable observability rollup storage budget (run=<optional_run_id>)
	@set -euo pipefail; \
	$(LOCAL_PG_ENV); \
	$(PYTHON) scripts/reporting/runtime_event_diagnostics.py observability-storage-budget $(if $(strip $(run)),--run-id "$(run)",) $(if $(strip $(limit)),--limit "$(limit)",)

botlens-check: venv ## Replay BotLens projection state from the ledger (run=<run_id> symbol=<optional>)
	@set -euo pipefail; \
	if [ -z "$(strip $(run))" ]; then echo "✗ run=<run_id> is required"; exit 1; fi; \
	$(LOCAL_PG_ENV); \
	$(PYTHON) scripts/reporting/check_botlens_projection.py --run-id "$(run)" $(if $(strip $(symbol)),--symbol-key "$(symbol)",) $(if $(strip $(max_seq)),--max-seq "$(max_seq)",)

wallet-diagnostics: venv ## Check wallet trace/replay diagnostics for a run (run=<run_id> compare=<optional>)
	@set -euo pipefail; \
	if [ -z "$(strip $(run))" ]; then echo "✗ run=<run_id> is required"; exit 1; fi; \
	$(LOCAL_PG_ENV); \
	$(PYTHON) scripts/reporting/check_wallet_determinism.py --run-id "$(run)" $(if $(strip $(compare)),--compare-run-id "$(compare)",)

report-wallet-diagnostics: wallet-diagnostics ## Alias for wallet-diagnostics

golden-compare: venv _ensure_dirs ## Compare two completed runs as a golden candidate (left=<run_id> right=<run_id>)
	@set -euo pipefail; \
	if [ -z "$(strip $(left))" ] || [ -z "$(strip $(right))" ]; then echo "✗ left=<run_id> and right=<run_id> are required"; exit 1; fi; \
	$(LOCAL_PG_ENV); \
	$(PYTHON) scripts/reporting/golden_repeatability.py --left-run-id "$(left)" --right-run-id "$(right)" --out-dir "$(if $(strip $(out)),$(out),$(GOLDEN_OUT_DIR))" $(if $(filter 1 true yes on,$(check_prior)),--check-prior,) $(if $(filter 1 true yes on,$(allow_fail)),--no-fail,)

test-reporting: venv ## Run focused reporting service tests
	@$(PYTEST_ENV) $(PYTHON) -m pytest -q tests/test_reports/test_report_artifacts.py tests/test_reports/test_report_metrics.py tests/test_portal/test_report_data.py tests/test_portal/test_report_artifact_bundle_workers.py tests/test_portal/test_report_export_bundle.py tests/test_portal/test_report_execution_mode_contract.py tests/test_portal/test_run_research_dataset.py

test-reporting-api: venv ## Run report API route tests with a bounded timeout
	@PYTHONPATH=$(PYTHONPATH) $(PYTEST_ENV) timeout $(REPORT_API_TEST_TIMEOUT) $(VENV_PYTHON) -m pytest -q tests/test_reports/test_report_contract_routes.py tests/test_reports/test_reports_endpoints.py

test-botlens: venv ## Run focused BotLens and runtime projection tests
	@$(PYTEST_ENV) $(PYTHON) -m pytest -q tests/test_portal/test_botlens_*.py tests/test_portal/test_bot_run_diagnostics_projection.py tests/test_portal/test_runtime_events_repo.py tests/integration/runtime

test-runtime: test-botlens ## Alias for focused BotLens/runtime tests

validate-docs: venv ## Refresh architecture index and run docs contract validation
	@$(PYTHON) scripts/docs/build_architecture_index.py
	@$(PYTEST_ENV) $(PYTHON) -m pytest -q tests/contract/test_architecture_docs_index.py

frontend-test: ## Run frontend unit tests
	@$(NPM) --prefix $(FRONT_DIR) test

frontend-build: ## Build frontend assets
	@$(NPM) --prefix $(FRONT_DIR) run build

frontend-check: frontend-test frontend-build ## Run frontend tests and build

git-status: ## Show short git status
	@git status --short

git-diff: ## Show git diff summary and changed file status
	@git diff --stat
	@git diff --name-status

git-check: ## Show status and run git diff whitespace checks
	@git status --short
	@git diff --check

check: git-check validate-docs test-reporting test-botlens frontend-check ## Run standard developer/audit checks

commit: ## Stage all repo changes and commit (msg="area: core change")
	@set -euo pipefail; \
	msg="$(msg)"; \
	if [ -z "$$msg" ]; then echo '✗ msg="area: core change" is required'; exit 1; fi; \
	if [[ "$$msg" == *$$'\n'* || "$$msg" == *$$'\r'* ]]; then echo "✗ commit message must be one line"; exit 1; fi; \
	if [[ ! "$$msg" =~ ^[^:\ ]([^:]*)?:\ .+ ]]; then echo '✗ commit message must match "<area>: <core change>"'; exit 1; fi; \
	if [ "$${#msg}" -gt 72 ]; then echo "✗ commit message is $${#msg} chars; keep it at 72 or less"; exit 1; fi; \
	git diff --check; \
	git add -A; \
	if git diff --cached --quiet; then echo "✗ no changes staged for commit"; exit 1; fi; \
	git diff --cached --check; \
	git commit -m "$$msg"; \
	echo "✓ commit $$(git rev-parse --short HEAD)"

## =============================== QUALITY ================================ ##
.PHONY: test clean
test: venv ## Run the Python test suite
	@$(PYTHON) -m pytest -q

clean: ## Remove caches/build artifacts; keep audit logs
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@rm -rf .coverage htmlcov dist build $(PID_DIR) 2>/dev/null || true
	@echo "✓ Cleaned"
