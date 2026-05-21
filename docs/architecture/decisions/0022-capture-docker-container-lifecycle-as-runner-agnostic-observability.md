---
component: adr-docker-container-lifecycle-observability
subsystem: observability
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - observability
  - docker
  - watchdog
  - runtime
code_paths:
  - docker/docker-compose.yml
  - portal/backend/main.py
  - portal/backend/service/bots/runner_observability.py
  - portal/backend/service/bots/runner.py
  - portal/backend/service/bots/bot_watchdog.py
  - src/core/settings.py
  - config/defaults.yaml
---
# ADR 0022: Capture Docker Container Lifecycle as Runner-Agnostic Observability

## Status

Accepted on 2026-05-19.

## Context

When a backend or bot container exits, Docker knows the lifecycle fact before
the backend can infer it from stale heartbeats or missing containers. During
incident investigation, those facts were available through ad hoc
`docker events` queries, not durable Quant-Trad observability.

Docker-specific facts should be captured without making the watchdog depend on
one bot-runner implementation or treating Docker as execution truth.

## Decision

When `bot_runtime.target=docker`, start one Docker lifecycle observer from the
backend runner lifecycle. Also run a tiny observability-profile Docker event
sidecar so backend-container exits are visible even when the backend process is
the process that dies.

The observer streams Docker container events, filters them to Quant-Trad
containers, and emits `docker_lifecycle_event` with bounded context:

- `runner_id`,
- `container_name`,
- `container_id`,
- `container_family`,
- `action`,
- `exit_code`,
- `image`,
- Docker and observer timestamps,
- derived `bot_id` when the container name uses the bot runtime prefix.

The observer is runner-agnostic at the Quant-Trad layer: it watches container
lifecycle facts for backend, bot, database, and observability containers rather
than calling a specific bot runner API. The implementation remains explicitly
Docker-specific and is skipped for non-Docker runtime targets.

Spawned bot containers carry bounded labels (`loki.job=quanttrad`,
`loki.service=bot-runtime`, `quanttrad.bot_id`, and `quanttrad.run_id`) so both
the backend observer and the sidecar can correlate events without inspecting
mutable container internals.

## Consequences

- Container exits, kills, OOM events, starts, stops, destroys, and health
  transitions become visible as lifecycle diagnostics.
- Backend container exits are captured by the Docker event sidecar when the
  observability profile is running.
- Watchdog degradation can include the latest nearby bot-container event.
- Docker facts explain operational state, but they do not replace canonical
  runtime lifecycle, wallet, order, trade, or report facts.
- Missing Docker CLI/socket access is reported as a WARN-level observability
  gap instead of crashing the backend.

## References

- [Observability Boundary](../observability/OBSERVABILITY_BOUNDARY.md)
- [Execution Runtime Boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [Runtime Contract](../../contracts/platform/01_runtime_contract.md)
