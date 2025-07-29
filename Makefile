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
	@echo "Containers started successfully"
	@echo "Access the following services:"

	@echo "TimescaleDB → postgresql://postgres:postgres@localhost:5432/postgres"
	@echo "pgAdmin → http://localhost:8080"
	@echo "Grafana → http://localhost:3000"
	@echo "Loki → http://localhost:3100"

	@echo "Starting frontend server..."
	@cd portal/frontend && npm run dev &
	@echo "Frontend server started"


## Stop all containers
shutdown:
	docker compose -f docker/docker-compose.local.yml stop timescaledb pgadmin grafana loki
	@echo "All containers stopped"

## Open a psql shell to TimescaleDB
db_cli:
	psql "postgresql://postgres:postgres@localhost:5432/postgres"
	@echo "Use \\q to exit the shell"

## Run FastAPI app with virtual environment and PYTHONPATH=src/
run:
	@echo "Running FastAPI app with PYTHONPATH=src"
	@bash -c "$(VENV_CHECK) && source env/bin/activate && export PYTHONPATH=src && uvicorn portal.backend.main:app --reload"

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