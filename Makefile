# Makefile for local dev: setup containers, run app, and run tests

.PHONY: setup shutdown db_cli test test-integration test-unit run status

## Start all required containers (TimescaleDB, pgAdmin, Grafana, Loki)
setup:
	docker compose up -d timescaledb pgadmin grafana loki
	@echo "⏳ Waiting for TimescaleDB to be ready..."
	@while ! docker exec tsdb pg_isready -U postgres >/dev/null 2>&1; do \
		echo "Waiting for TimescaleDB..."; \
		sleep 2; \
	done
	@echo "✅ TimescaleDB is ready"
	@echo "pgAdmin → http://localhost:8080"
	@echo "Grafana → http://localhost:3000"

## Stop all containers
shutdown:
	docker compose stop timescaledb pgadmin grafana loki
	@echo "⛔ All containers stopped"

## Open a psql shell to TimescaleDB
db_cli:
	psql "postgresql://postgres:postgres@localhost:5432/postgres"
	@echo "Use \\q to exit the shell"

## Run all tests
test:
	pytest -v tests/

## Run only unit tests
test-unit:
	pytest -v -m "not integration" tests/

## Run only integration tests
test-integration:
	pytest -v -m integration tests/

## Run the main program with virtual environment and PYTHONPATH=src
run:
	@echo "Running application with PYTHONPATH=src"
	@bash -c "source env/bin/activate && PYTHONPATH=src python3 src/main.py"

## Show running container status
status:
	@docker compose ps --status=running


## Run development startup script
dev:
	@echo "Running dev startup script..."
	@./scripts/dev_startup.sh