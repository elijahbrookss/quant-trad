---
component: adr-canonical-collector-operations
subsystem: data
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - market-data
  - collectors
  - operations
  - diagnostics
  - frontend
  - audit
code_paths:
  - src/market_data/collector_operations.py
  - portal/backend/service/market/collector_operations.py
  - portal/backend/service/storage/repos/collector_operations.py
  - portal/backend/controller/market_data.py
  - portal/backend/workers/market_data_collector.py
  - portal/frontend/src/features/collectors
  - portal/frontend/src/v2
  - cli/main.py
  - cli/mcp_server.py
---
# ADR 0064: Use One Code-Owned Collector Operations Contract

## Status

Accepted on 2026-08-10.

## Context

QT has one canonical Fact path, but its collector operations are split between
scheduled Fact definitions and continuous market-structure streams. Each path
has its own desired-state fields, lifecycle language, health reads, controls,
and evidence. Frontend V2 currently derives scheduled health in JavaScript and
has no safe control path or continuous-collector detail surface.

The split hid a production restart storm: reviewed continuous definitions were
enabled without all series mappings required by their finalizer. Existing logs
and tables contained the cause, but no canonical diagnostic folded that
evidence into a failing boundary and safe operator action.

Collectors are implementation assets, not user-authored automation. Provider
adapters, Fact schemas, subjects, cadence, safety policy, and recovery behavior
must remain code- or reviewed-manifest-owned.

Database rows are durable configuration/evidence, but do not define executable
code. Historical integration-test and retired definitions may remain pinned by
immutable Dataset/archive evidence. Treating every persisted row as an active
collector would incorrectly grant those rows runtime authority and turn fleet
health into a database-hygiene report.

## Decision

QT will expose one `CollectorOperationsService` over every durable registered
collector. Scheduled polls and continuous streams retain their fit-for-purpose
definition/evidence tables, but no UI, CLI, or MCP consumer may infer lifecycle
or mutate those tables directly.

Each definition retains a code-owned configured gate and receives the same
typed operator-control fields:

- `desired_state`: `running`, `stopped`, or `paused`;
- monotonic `control_generation`;
- last request identity, actor, and time.

`enabled` is the configured/code-owned gate. Operator start/stop/pause/resume
does not redefine or reconfigure a collector. Workers run only a configured
definition whose desired state is `running`. A restart advances the control
generation so a continuous task drains and is replaced even when its desired
state remains `running`.

The canonical actual lifecycle vocabulary is:

- `DISABLED` — the reviewed definition is not configured for operation;
- `STOPPED` — configured, desired stopped, and no live owner;
- `PAUSED` — configured, desired paused, and no live owner;
- `STARTING` — desired running and the worker is acquiring ownership/readiness;
- `HEALTHY` — ownership, delivery, freshness, and validation evidence agree;
- `DEGRADED` — collection continues with actionable quality/freshness trouble;
- `RETRYING` — a bounded retry or supervisor restart delay is active;
- `RECOVERING` — durable recovery is actively reconciling retained evidence;
- `FAILED` — desired work cannot proceed or a terminal invariant failed;
- `STOPPING` — desired work was withdrawn while an owner is draining.

Only the backend projector assigns these states. It publishes the decisive
evidence and state reason. Absence and failed reads are explicit; consumers do
not substitute a healthy state.

Every collector projection includes identity, provider, collector type,
schemas, subjects, configured/desired/actual state, worker and heartbeat,
uptime, last attempt/provider success/accepted Fact, recent throughput,
accept/noop/reject counts, freshness, retry, gaps, errors, restart count, and
recovery state. Provider-specific diagnostics are typed extensions and cannot
choose generic UI workflow.

One diagnostic operation evaluates ordered boundaries:

`worker -> scheduler -> ownership -> provider -> canonicalization -> schema -> persistence -> freshness -> gaps/recovery`

It returns structured checks, a likely failing boundary, evidence, and one of a
bounded set of recommendations. Diagnostics do not autonomously remediate.

The registered action catalog is `start`, `stop`, `restart`, `pause`, `resume`,
`health_probe`, `diagnose`, and capability-gated bounded `recover`. Unsupported
actions are absent and rejected server-side. Disruptive and acquisition actions
require explicit confirmation. Collector creation/deletion, arbitrary
configuration, credentials, SQL, schema creation, code execution, and unbounded
acquisition are not actions.

Every mutation appends one immutable `collector_operation.v1` result containing
request/action/collector, request time, actor/context, prior state, resulting
state, success/failure, and evidence/error. Operation success means the desired
transition was durably accepted; subsequent worker readiness remains visible
through lifecycle evidence.

Frontend V2 consumes only the canonical fleet/detail/diagnostic/action API. It
may lay out topology and format values, but it may not derive collector health,
join operational tables, interpret provider payloads, or create collector
definitions.

The code-owned registry is the operational admission boundary. A persisted
definition is included only when the deployed runtime recognizes its kind,
adapter, configuration version, provider binding, and produced schemas.
Non-admitted durable rows remain countable and inspectable as migration/audit
debt but are not projected as failed fleet members.

## Consequences

Operators get one vocabulary and command path while the scheduled and streaming
runtimes keep their different acquisition mechanics. Restart, pause, worker
loss, gaps, retries, schema failures, and recovery become comparable without
flattening their evidence.

Existing direct enable/disable and continuous start/stop controls must be moved
to the canonical service and retired as public operational paths. Definition
installation remains a separate code-reviewed/admin boundary.

The CLI, MCP adapter, and Frontend V2 all invoke the same backend contract.
Failed operation preconditions and successful mutations share one immutable
audit ledger, and request IDs make retries safe without dual transitions.

The schema change requires an explicit out-of-band migration with collector
writers stopped. Existing configured intent is migrated once; there is no
runtime fallback from missing control fields.

## Rejected alternatives

- Put provider-specific state machines in Frontend V2.
- Treat `enabled`, a worker heartbeat, or a recent Fact as sufficient health by
  itself.
- Build a second generic collector table and dual-write every runtime event.
- Infer pause/restart from the latest log message.
- Let the browser create definitions or edit provider/runtime JSON.
- Add autonomous restart/recovery based on diagnostic recommendations.
- Treat paper Bot streams or bounded provider reads as fleet collectors.

## Evidence

- [Collector operations discovery](../../engineering/collector-operations-discovery.md)
- [Collector operations control plane](../data/COLLECTOR_OPERATIONS_CONTROL_PLANE.md)
