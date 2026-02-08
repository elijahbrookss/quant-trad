#!/usr/bin/env bash
set -e

SYSTEM_STATS_CONTROL=$(ls /usr/share/postgresql/*/extension/system_stats.control 2>/dev/null || true)
if [ -z "$SYSTEM_STATS_CONTROL" ]; then
  echo "system_stats extension not available in this Postgres image; pgAdmin system_stats panel will remain unavailable. Use container stats / cAdvisor instead."
fi

exec docker-entrypoint.sh "$@"
