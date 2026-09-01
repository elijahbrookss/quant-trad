#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="${repo_root}/docker/test/grafana-email-alerting.compose.yml"
cleanup_compose_file="${repo_root}/docker/test/grafana-email-alerting-cleanup.compose.yml"
project_name="quanttrad-alerting-test-$$"
ports="$(${PYTHON:-python3} - <<'PY'
import socket

with socket.socket() as first, socket.socket() as second:
    first.bind(("127.0.0.1", 0))
    second.bind(("127.0.0.1", 0))
    print(first.getsockname()[1], second.getsockname()[1])
PY
)"
read -r mailpit_port grafana_port <<<"${ports}"

export QT_ALERT_TEST_PROJECT_NAME="${project_name}"
export QT_ALERT_TEST_MAILPIT_PORT="${mailpit_port}"
export QT_ALERT_TEST_GRAFANA_PORT="${grafana_port}"

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
delivery_proven=false
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
    delivery_proven=true
    break
  fi
  sleep 2
done

if test "${delivery_proven}" != "true"; then
  echo "Timed out waiting for Grafana to deliver the email alert." >&2
  docker compose --project-name "${project_name}" --file "${compose_file}" logs --tail 200 --no-color >&2
  exit 1
fi

if ! docker compose \
  --project-name "${project_name}" \
  --file "${cleanup_compose_file}" \
  up --detach --force-recreate --wait --wait-timeout 120 grafana; then
  echo "Disposable Grafana alert cleanup proof failed to start." >&2
  docker compose --project-name "${project_name}" --file "${cleanup_compose_file}" logs --tail 200 --no-color >&2
  exit 1
fi

contact_points="$(
  curl --fail --silent --show-error \
    --user admin:alert-test-password \
    "http://127.0.0.1:${grafana_port}/api/v1/provisioning/contact-points"
)"
policies="$(
  curl --fail --silent --show-error \
    --user admin:alert-test-password \
    "http://127.0.0.1:${grafana_port}/api/v1/provisioning/policies"
)"
CONTACT_POINTS="${contact_points}" POLICIES="${policies}" \
  "${PYTHON:-python3}" -c '
import json
import os

contact_points = json.loads(os.environ["CONTACT_POINTS"])
policies = json.loads(os.environ["POLICIES"])
if any(point.get("uid") == "qt-operator-email" for point in contact_points):
    raise SystemExit("qt-operator-email survived cleanup provisioning")
if "qt-operator-email" in json.dumps(policies):
    raise SystemExit("qt-operator-email policy survived cleanup provisioning")
'
echo "Grafana alerting cleanup proof removed the preview contact point and policy."
