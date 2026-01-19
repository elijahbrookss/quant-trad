#!/usr/bin/env bash
set -euo pipefail

: "${COMPOSE_FILE:?COMPOSE_FILE is required (e.g., docker/docker-compose.yml).}"
: "${TSDB_SERVICE:=tsdb}"

compose_cmd=(docker compose -f "$COMPOSE_FILE")

tmp_file="$(mktemp /tmp/tsdb_ddl.XXXXXX.sql)"
cleanup() {
  rm -f "$tmp_file"
}
trap cleanup EXIT

"${compose_cmd[@]}" exec -T "$TSDB_SERVICE" bash -lc \
  'pg_dump --schema-only --no-owner --no-privileges -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  > "$tmp_file"

clipboard_cmd=()
if [ -n "${CLIPBOARD_CMD:-}" ]; then
  clipboard_cmd=(${CLIPBOARD_CMD})
elif command -v pbcopy >/dev/null 2>&1; then
  clipboard_cmd=(pbcopy)
elif command -v xclip >/dev/null 2>&1; then
  clipboard_cmd=(xclip -selection clipboard)
elif command -v wl-copy >/dev/null 2>&1; then
  clipboard_cmd=(wl-copy)
else
  echo "ERROR: No clipboard tool found. Set CLIPBOARD_CMD or install pbcopy/xclip/wl-copy." >&2
  exit 1
fi

"${clipboard_cmd[@]}" < "$tmp_file"
bytes="$(wc -c < "$tmp_file" | tr -d ' ')"
echo "OK: DDL copied to clipboard (${bytes} bytes)."
