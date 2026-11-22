#!/usr/bin/env bash
set -euo pipefail

printf 'ci_proof_step Prove Grafana email notification path\n'
bash scripts/ci/test_grafana_email_alerting.sh

printf 'ci_proof_step Prove physical storage alert queries and thresholds\n'
python3 scripts/ci/test_storage_alerts.py
