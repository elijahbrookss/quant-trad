# Makefile for local dev: setup containers, run app, and run tests

.PHONY: setup shutdown db_cli test test-integration test-unit run status

# Ensure virtual environment is set up
VENV_CHECK=test -f env/bin/activate || { echo \"❌ Virtualenv not found. Run 'make dev' first.\"; exit 1; }

## Start all required containers (TimescaleDB, pgAdmin, Grafana, Loki)
setup:
	docker compose -f docker/docker-compose.local.yml up -d timescaledb pgadmin grafana loki
	@echo "⏳ Waiting for TimescaleDB to be ready..."
	@while ! docker exec tsdb pg_isready -U postgres >/dev/null 2>&1; do \
		echo "Waiting for TimescaleDB..."; \
		sleep 2; \
	done
	@echo "TimescaleDB is ready"
	@echo "pgAdmin → http://localhost:8080"
	@echo "Grafana → http://localhost:3000"

## Stop all containers
shutdown:
	docker compose -f docker/docker-compose.local.yml stop timescaledb pgadmin grafana loki
	@echo "All containers stopped"

## Open a psql shell to TimescaleDB
db_cli:
	psql "postgresql://postgres:postgres@localhost:5432/postgres"
	@echo "Use \\q to exit the shell"

## Run the main program with virtual environment and PYTHONPATH=project root
run:
	@echo "Running application with PYTHONPATH=$(pwd)"
	@bash -c "$(VENV_CHECK) && source env/bin/activate && export PYTHONPATH=$(pwd) && python3 src/main.py"

## Run all tests with virtual environment
test:
	@bash -c "$(VENV_CHECK) && source env/bin/activate && pytest -v tests/"

## Run only unit tests
test-unit:
	@bash -c "$(VENV_CHECK) && source env/bin/activate && pytest -v -m 'not integration' tests/"

## Run only integration tests
test-integration:
	@bash -c "$(VENV_CHECK) && source env/bin/activate && pytest -v -m integration tests/"


## Show running container status
status:
	@docker compose ps --status=running


## Run development startup script
dev:
	@echo "Running dev startup script..."
	@./scripts/dev_startup.sh