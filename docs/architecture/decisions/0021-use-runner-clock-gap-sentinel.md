---
component: adr-runner-clock-gap-sentinel
subsystem: observability
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - observability
  - watchdog
  - runtime
  - runner
code_paths:
  - portal/backend/main.py
  - portal/backend/service/bots/runner_observability.py
  - portal/backend/service/bots/bot_watchdog.py
  - src/core/settings.py
  - config/defaults.yaml
---
# ADR 0021: Use Runner Clock Gap Sentinel

## Status

Accepted on 2026-05-19.

## Context

Paper/live bot ownership is proven by backend runner heartbeats. If the host,
WSL VM, Docker daemon, or backend process is paused, the next watchdog scan sees
a stale heartbeat, but the stale row alone does not explain whether the bot
logic failed or the runner stopped observing time.

Quant-Trad needs a cheap signal that says "this runner stopped waking up on
time" without changing execution semantics or inventing strategy facts.

## Decision

Run a tiny clock-gap sentinel inside each backend runner process.

The sentinel compares wall-clock delta and monotonic-clock delta against its
expected wake interval. When either observed delta exceeds the configured
threshold, it emits `runner_clock_gap_detected` with:

- `runner_id`,
- `detected_at`,
- `expected_interval_seconds`,
- `threshold_seconds`,
- wall and monotonic deltas,
- wall and monotonic gap seconds,
- max gap seconds.

The signal is runner-specific because runner ownership is represented by
`runner_id`. The pattern is globally reusable: every future process that owns
bot heartbeats can start the same sentinel and record gaps under its own
runner id.

## Consequences

- Watchdog degradation can include nearby runner pause evidence.
- Host/VM/Docker suspension becomes distinguishable from strategy/runtime logic
  failure.
- The signal remains diagnostic observability; it is not execution truth,
  wallet truth, trade truth, or report certification evidence.
- False positives are bounded by a configurable threshold and only affect
  diagnostics.

## References

- [Observability Boundary](../observability/OBSERVABILITY_BOUNDARY.md)
- [Execution Runtime Boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [Runtime Contract](../../contracts/platform/01_runtime_contract.md)
