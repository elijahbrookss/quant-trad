# Makefile
.PHONY: db_up db_down db_logs db_cli 

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