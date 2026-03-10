# CI Test Topology

This document describes the current GitHub Actions test topology as it actually exists today.

## Goals

- Fail fast on core/runtime contract regressions.
- Isolate optional-provider and web import boundary failures.
- Avoid hidden coupling by centralizing suite definitions.
- Keep the PR gate fast enough that it stays on.

## Current CI Flow

1. **targeted-suites** (matrix): `core`, `provider`, `web`

That is the whole GitHub PR gate right now.

Suite commands are centralized in `scripts/ci/run_test_suite.sh` and run directly on the GitHub runner host by default.

## Current Boundary

- GitHub PR CI is host-run.
- Product/runtime behavior is still container-first.
- These are not the same thing.

The current CI job is intentionally a fast regression screen, not a full runtime-faithful environment.

## Why this structure

- The matrix catches breakages earlier and with clearer ownership.
- A single suite-runner script reduces drift between workflow YAML and actual test commands.
- GitHub-host execution avoids spending time fighting container orchestration for every PR.

## What CI Is And Is Not Protecting

Current GitHub PR CI is good at catching:

- import/bootstrap breakage
- pure logic regressions
- provider wiring regressions
- web/service regressions that do not depend on full runtime environment fidelity

Current GitHub PR CI is not the source of truth for:

- container networking behavior
- runtime composition across real services
- database wiring inside the Docker network
- full backtest/runtime orchestration

If a bug only appears once services are inside the Docker network, host-run PR CI may miss it.

## Safe Landing Path

### Phase 1 (now)
- Matrix routing with explicit test-file groups.
- Host-run PR checks only.

### Phase 2
- Expand `@pytest.mark.core|provider|web|smoke` coverage.
- Move suite selection from file lists to marker expressions.

### Phase 3
- Add a separate container-backed runtime/integration job if runtime orchestration bugs become a recurring problem worth paying for.
- Add provider matrix variants (e.g., alpaca/ccxt/ib) only if third-party SDK regressions justify the cost.

## Operational Rule

If you change suite contents, update only `scripts/ci/run_test_suite.sh`.
Workflow jobs should continue to call that script and avoid duplicating pytest arguments.


## Local Reproduction

To mimic the current GitHub job locally:

1. Install dependencies:
   - `python -m pip install --upgrade pip`
   - `pip install -r requirements.txt`
2. Run targeted suites exactly as CI does:
   - `./scripts/ci/run_test_suite.sh core`
   - `./scripts/ci/run_test_suite.sh provider`
   - `./scripts/ci/run_test_suite.sh web`
## Optional Container Reproduction

If you want to debug container-specific issues locally, use the optional Docker path:

- `CI_USE_DOCKER=1 ./scripts/ci/run_test_suite.sh core`
- `CI_USE_DOCKER=1 ./scripts/ci/run_test_suite.sh integration`

This is a debugging path, not the current default PR gate.

### Troubleshooting: wait script permission errors

If you see `Permission denied` for `wait-for-db.sh` inside a bind-mounted container, invoke it through bash:
- `bash /app/scripts/wait-for-db.sh ...`

This avoids dependency on executable bits from host mounts and matches the robust path used in the container wrapper path.


## Practical Rule

Do not assume GitHub PR CI proves container/runtime correctness.

It proves the selected fast suites passed on a clean runner host. That is still useful. It is just a smaller claim.

## Agent CI Preflight Checklist

Before attempting local CI reproduction, run:

- `bash -n scripts/ci/run_test_suite.sh`
- `python -m pip --version`

Before optional container reproduction, also run:

- `command -v docker`
