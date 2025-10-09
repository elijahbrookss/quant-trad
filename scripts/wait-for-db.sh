#!/bin/bash

set -e

# Configurable timeout (default 60 seconds)
TIMEOUT=${DB_TIMEOUT:-120}
START_TIME=$(date +%s)

DB_HOST=${TSDB_HOST:-timescaledb}
DB_PORT=${TSDB_PORT:-5432}
DB_USER=${POSTGRES_USER:-postgres}

echo "Waiting for TimescaleDB to be ready on ${DB_HOST}:${DB_PORT} (timeout: ${TIMEOUT}s)..."

until pg_isready -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}"; do
  NOW=$(date +%s)
  ELAPSED=$((NOW - START_TIME))

  if [ "$ELAPSED" -ge "$TIMEOUT" ]; then
    echo "TimescaleDB did not become ready within ${TIMEOUT}s. Exiting."
    exit 1
  fi

  echo "⏳ Still waiting... (${ELAPSED}s)"
  sleep 2
done

echo "TimescaleDB is ready. Running command: $@"
exec "$@"
