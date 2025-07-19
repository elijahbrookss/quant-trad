#!/bin/bash

set -e

# Configurable timeout (default 60 seconds)
TIMEOUT=${DB_TIMEOUT:-120}
START_TIME=$(date +%s)

echo "Waiting for TimescaleDB to be ready (timeout: ${TIMEOUT}s)..."

until pg_isready -h timescaledb -p 5432 -U postgres; do
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
