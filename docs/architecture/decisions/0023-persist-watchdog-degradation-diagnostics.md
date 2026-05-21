---
component: adr-watchdog-degradation-diagnostics
subsystem: execution-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - watchdog
  - lifecycle
  - observability
  - runtime
code_paths:
  - portal/backend/service/bots/bot_watchdog.py
  - portal/backend/service/bots/runner_observability.py
  - portal/backend/service/storage/repos/bots.py
  - portal/backend/service/bots/startup_lifecycle.py
---
# ADR 0023: Persist Watchdog Degradation Diagnostics

## Status

Accepted on 2026-05-19.

## Context

Watchdog stale-heartbeat detection is already classified as recoverable
lifecycle degradation unless independent evidence proves a terminal runtime
failure. That classification is correct, but the persisted lifecycle row did
not carry enough evidence to explain why the watchdog degraded a run.

After a host/WSL/Docker pause, operators needed to correlate backend logs,
bot logs, WSL journal output, Docker events, and lifecycle rows manually.

## Decision

When the watchdog records stale heartbeat or container-not-running conditions,
persist bounded diagnostics in lifecycle metadata and failure payloads under
`watchdog_diagnostics`.

Stale-heartbeat diagnostics include:

- detecting runner id and previous runner id,
- heartbeat timestamp or explicit missing heartbeat flag,
- stale age and stale threshold,
- active run id when available,
- recent runner clock-gap evidence,
- recent Docker lifecycle evidence for the bot container.

Container-not-running diagnostics include:

- detecting runner id,
- active run id,
- inspected container status, exit code, OOM flag, error, and runtime run id,
- recent Docker lifecycle evidence for the bot container.

## Consequences

- Degraded lifecycle rows become useful incident evidence without changing
  execution semantics.
- Recoverable watchdog conditions still remain degraded, not failed, unless
  independent terminal process/container evidence exists.
- Reports can continue to classify recoverable watchdog rows separately from
  terminal runtime failures.
- Diagnostic payloads stay bounded and nested so they do not become a new
  persistence schema boundary.

## References

- [Execution Runtime Boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [Observability Boundary](../observability/OBSERVABILITY_BOUNDARY.md)
- [Runtime Contract](../../contracts/platform/01_runtime_contract.md)
