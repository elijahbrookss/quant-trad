#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="${repo_root}/docker/test/grafana-email-alerting.compose.yml"
project_name="quanttrad-alerting-test-$$"
mailpit_port="$(${PYTHON:-python3} - <<'PY'
import socket

with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
)"

export QT_ALERT_TEST_PROJECT_NAME="${project_name}"
export QT_ALERT_TEST_MAILPIT_PORT="${mailpit_port}"

cleanup() {
  docker compose --project-name "${project_name}" --file "${compose_file}" down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

if ! docker compose --project-name "${project_name}" --file "${compose_file}" up --detach --wait --wait-timeout 120; then
  echo "Disposable Grafana email proof failed to start." >&2
  docker compose --project-name "${project_name}" --file "${compose_file}" logs --tail 200 --no-color >&2
  exit 1
fi

deadline=$((SECONDS + 120))
while (( SECONDS < deadline )); do
  messages="$(curl --fail --silent --show-error "http://127.0.0.1:${mailpit_port}/api/v1/messages" || true)"
  if [[ -n "${messages}" ]] && printf '%s' "${messages}" | "${PYTHON:-python3}" -c '
import json
import sys

payload = json.load(sys.stdin)
messages = payload.get("messages") or payload.get("Messages") or []
expected = {"owner@quanttrad.test", "backup@quanttrad.test"}
recipients = set()
subjects = []

for message in messages:
    subjects.append(message.get("Subject") or message.get("subject") or "")
    addressed = message.get("To") or message.get("to") or []
    if isinstance(addressed, dict):
        addressed = [addressed]
    for entry in addressed:
        if isinstance(entry, dict):
            address = entry.get("Address") or entry.get("address")
            if address:
                recipients.add(address)
        elif isinstance(entry, str):
            recipients.add(entry)

if not expected.issubset(recipients):
    raise SystemExit(1)
if not any("QT email delivery proof" in subject for subject in subjects):
    raise SystemExit(1)
'; then
    echo "Grafana email alerting proof captured for 2 recipients."
    exit 0
  fi
  sleep 2
done

echo "Timed out waiting for Grafana to deliver the email alert." >&2
docker compose --project-name "${project_name}" --file "${compose_file}" logs --tail 200 --no-color >&2
exit 1
