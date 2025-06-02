# Makefile
.PHONY: db_up db_down db_logs db_cli test test-integration test-unit

db_up:
	docker compose up -d timescaledb
	@echo "Waiting for TimescaleDB to start..."
	@while ! docker exec -it tsdb pg_isready -U postgres; do \
		echo "Waiting for TimescaleDB to be ready..."; \
		sleep 2; \
	done
	@echo "TimescaleDB is ready!"
	docker compose up -d pgadmin
	@echo Use the following command to view the IP address of the TimescaleDB container:
	@echo "docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' tsdb"
	@echo "You can also connect using pgAdmin at http://localhost:8080"

db_down:
	docker compose stop timescaledb
	docker compose stop pgadmin

db_logs:
	docker compose logs -f timescaledb

# quick psql shell (requires psql client installed inside WSL/Windows)
db_cli:
	psql "postgresql://postgres:postgres@localhost:5432/postgres"
	@echo "Connected to TimescaleDB. Use \q to exit."

env:
	@echo "To activate the virtual environment, run:"
	@echo "source .venv/bin/activate"

test:  ## Run all tests
	pytest -v tests/

test-unit:  ## Run only unit tests (if you tag with @pytest.mark.unit)
	pytest -v -m "not integration" tests/

test-integration:  ## Run only integration tests
	pytest -v -m integration tests/
