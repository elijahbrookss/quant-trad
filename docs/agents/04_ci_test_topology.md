# CI Test Topology (Phased Suite Routing)

This document captures the suite-routing strategy for test reliability without semantic drift.

## Goals

- Keep a single full integration run as the final safety net.
- Fail fast on core/runtime contract regressions.
- Isolate optional-provider and web import boundary failures.
- Avoid hidden coupling by centralizing suite definitions.

## Current CI Flow

1. **targeted-suites** (matrix): `core`, `provider`, `web`
2. **integration-tests** (depends on targeted suites)

Suite commands are centralized in `scripts/ci/run_test_suite.sh` and run directly on the GitHub runner by default.

## Why this structure

- The matrix catches breakages earlier and with clearer ownership.
- The final integration run ensures no suite is silently skipped.
- A single suite-runner script reduces drift between workflow YAML and actual test commands.

## Safe Landing Path

### Phase 1 (now)
- Matrix routing with explicit test-file groups.
- Full integration run remains mandatory.

### Phase 2
- Expand `@pytest.mark.core|provider|web|smoke` coverage.
- Move suite selection from file lists to marker expressions.

### Phase 3
- Add provider matrix variants (e.g., alpaca/ccxt/ib) so third-party SDK regressions are isolated.
- Keep core/runtime contract checks as unconditional required status checks.

## Operational Rule

If you change suite contents, update only `scripts/ci/run_test_suite.sh`.
Workflow jobs should continue to call that script and avoid duplicating pytest arguments.


## Local CI Reproduction

To mimic GitHub Actions locally (default host mode):

1. Install dependencies:
   - `python -m pip install --upgrade pip`
   - `pip install -r requirements.txt`
2. Run targeted suites exactly as CI does:
   - `./scripts/ci/run_test_suite.sh core`
   - `./scripts/ci/run_test_suite.sh provider`
   - `./scripts/ci/run_test_suite.sh web`
3. Run full integration suite:
   - `./scripts/ci/run_test_suite.sh integration`

## Optional Container Reproduction

If you want to debug container-specific issues locally, run with:

- `CI_USE_DOCKER=1 ./scripts/ci/run_test_suite.sh core`
- `CI_USE_DOCKER=1 ./scripts/ci/run_test_suite.sh integration`

### Troubleshooting: wait script permission errors

If you see `Permission denied` for `wait-for-db.sh` inside a bind-mounted container, invoke it through bash:
- `bash /app/scripts/wait-for-db.sh ...`

This avoids dependency on executable bits from host mounts and matches the robust path used in the container wrapper path.


## Agent CI Preflight Checklist

Before attempting local CI reproduction, run:

- `bash -n scripts/ci/run_test_suite.sh`
- `python -m pip --version`

Before optional container reproduction, also run:

- `command -v docker`
