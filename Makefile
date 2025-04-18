# Makefile
.PHONY: db_up db_down db_logs db_cli

db_up:
	docker compose up -d timescaledb

db_down:
	docker compose stop timescaledb

db_logs:
	docker compose logs -f timescaledb

# quick psql shell (requires psql client installed inside WSL/Windows)
db_cli:
	psql "postgresql://postgres:postgres@localhost:5432/postgres"
