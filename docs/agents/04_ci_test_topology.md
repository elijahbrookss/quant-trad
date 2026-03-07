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

Suite commands are centralized in `scripts/ci/run_test_suite.sh`.

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
