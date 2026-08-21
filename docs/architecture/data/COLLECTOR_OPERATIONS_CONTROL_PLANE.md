---
component: collector-operations-control-plane
subsystem: data
layer: service
doc_type: architecture
status: active
tags:
  - market-data
  - collectors
  - operations
  - lifecycle
  - diagnostics
  - audit
  - frontend
code_paths:
  - src/market_data/collector_operations.py
  - portal/backend/db/market_data_models.py
  - portal/backend/service/market/collector_operations_service.py
  - portal/backend/service/storage/repos/collector_operations.py
  - portal/backend/service/market/collector_service.py
  - portal/backend/service/market/collector_supervisor.py
  - portal/backend/service/market/continuous_stream_collector.py
  - portal/backend/workers/market_data_collector.py
  - portal/backend/controller/market_data.py
  - cli/main.py
  - cli/mcp_server.py
  - portal/frontend/src/features/collectors
  - portal/frontend/src/v2
---
# Collector Operations Control Plane

## Boundary

```text
Frontend V2 / qt / MCP
  -> canonical Collector API
  -> CollectorOperationsService
  -> scheduled worker or continuous supervisor
  -> provider adapters
  -> canonical Facts and operational evidence
```

The service operates registered collectors. It does not register adapters,
schemas, sources, subjects, credentials, or definitions. Those remain code and
reviewed-manifest concerns.

Persistence alone does not make a definition operational. The registry admits
only definitions whose provider, adapter, configuration schema, Fact schemas,
and runtime kind are recognized by the deployed code. Durable rows outside
that registry are reported as `unregistered_definition_count` and omitted from
fleet health rather than being mistaken for failed collectors. This keeps
historical evidence visible without giving old test or retired definitions new
runtime authority.

## Identity and configuration

A canonical collector identity contains:

- stable definition ID and kind (`scheduled_fact` or `continuous_stream`);
- provider/venue and registered adapter;
- canonical Fact schema IDs;
- canonical instrument/subject identities;
- cadence or stream trigger semantics;
- read-only reviewed configuration and capability catalog.

The per-kind definition table remains configuration authority. `enabled` means
the reviewed definition is configured. Operator desired state is a distinct,
typed field and may be `running`, `stopped`, or `paused`. The worker requires
both configuration and desired intent.

No frontend request accepts provider URLs, product IDs, schedules, retry
policies, credentials, schema IDs, adapter IDs, or arbitrary configuration.

## Operational snapshot

`market.collector_operational_snapshot.v1` contains one bounded row per
definition:

```text
identity
configured_state + desired_state + actual_state + state_reason
worker identity + heartbeat + uptime
last acquisition attempt + provider success + accepted Fact
throughput + accepted/noop/rejected observations
freshness + provider/QT delay
retry + gap + error + restart + recovery
registered capabilities and safe actions
```

The backend derives the row from one consistent database observation. Metrics
carry their window and observed time. A missing component is `unknown` or
`unavailable`, never zero or healthy. Each row also exposes a presentation
projection: `operational_state`, `health_status`, `needs_attention`, and a
stable `attention_reason`.

The full fleet response remains available for exact tools and diagnostics. The
Frontend V2 fleet uses `market.collector_provider_summary.v1`: one compact
provider rollup with state counts, health, throughput, running-stream
freshness, schema counts, and a bounded list of actionable exceptions. Its SSE
endpoint evaluates periodically but emits only when the material summary
changes. Collector rows are fetched in bounded pages only for the expanded
provider or the explicit all-collector search.

## Lifecycle and health projection

Operational state and health are separate backend-owned dimensions.

Operational state is `DISABLED`, `STOPPING`, `STOPPED`, `PAUSED`, or
`RUNNING`. It represents configuration, operator intent, and whether active
ownership is still draining. Health is meaningful only for `RUNNING`
collectors and is `HEALTHY`, `DELAYED`, `FAILED`, or `UNKNOWN`.
Non-running collectors report `NOT_APPLICABLE`; their age must not be rendered
as continuously worsening freshness.

The detailed `actual_state` and `state_reason` remain diagnostic evidence for
startup, retry, recovery, and degradation. They do not replace the simpler
operational state/health projection. Frontends and clients render the backend
projection and must not infer replacement semantics from heartbeat, freshness,
or a single metric.

## Telemetry and events

Scheduled attempts already preserve stage timings and insertion/noop evidence.
Continuous streams preserve raw archive ranges, mappings, session events,
coverage, quality, and Facts. The operations projector normalizes those sources;
it does not copy them into a competing event ledger.

Continuous health keeps transport liveness separate from canonical Fact flow.
The latest archived non-subscription provider frame, including an enrolled
heartbeat, establishes provider activity and drives the running-stream freshness
projection. `last_accepted_fact_at` and Fact throughput remain separate
persistence evidence. A valid quiet stream therefore stays healthy when its
provider heartbeat is current even if its primary domain channel has emitted no
new canonical Fact. The freshness threshold accounts for the reviewed segment
rotation policy so asynchronous archive finalization does not create a false
delay.

The common recent-activity contract maps evidence to stable event kinds such as
`fact_accepted`, `provider_succeeded`, `retry_scheduled`, `provider_failed`,
`gap_opened`, `gap_recovered`, `ownership_acquired`, `restart`, `pause`, and
`resume`. Each normalized event retains its authoritative evidence reference.

Operation results are separate immutable audit records. They never replace
runtime attempts, session events, gaps, or Facts.

## Diagnostics

Each diagnostic check has:

- boundary and stable code;
- `pass`, `warning`, `fail`, or `unknown` status;
- readable summary;
- bounded structured evidence and authoritative references;
- optional typed provider extension.

The result names the highest-confidence failing boundary and safe recommended
actions. A recommendation is advisory. Only an explicit, confirmed action
request may mutate state or acquire data.

Examples:

| Evidence | Boundary | Recommendation |
| --- | --- | --- |
| desired running, expired worker heartbeat | worker | inspect worker, then restart the worker service |
| scheduled failure in provider-request stage with retry available | provider | no action or health probe |
| continuous finalizer missing a required series mapping | canonicalization | stop collector and deploy reviewed definition fix |
| schema rejection with current worker/ownership | schema | inspect rejected observation; do not restart-loop |
| stale accepted Fact after provider successes | persistence/freshness | inspect canonical persistence |
| active safety latch | ownership/safety | inspect provider/storage and acknowledge only after correction |

## Safe actions

| Action | Semantics |
| --- | --- |
| `start` | Move a configured stopped collector to desired running |
| `stop` | Withdraw desired work and allow an owner to drain |
| `restart` | Advance the control generation; drain and reacquire without redefining the collector |
| `pause` | Withdraw work while retaining an explicit paused intent |
| `resume` | Move an explicitly paused collector to desired running |
| `health_probe` | Re-evaluate bounded health evidence; no Fact acquisition |
| `diagnose` | Return the canonical structured diagnostic |
| `recover` | Invoke only a registered bounded recovery capability with explicit limits and confirmation |

State/action validation is strict and idempotent by request ID. A configured
disabled or invalid definition exposes no start/recovery action. A safety latch
blocks start/resume/restart until the separate acknowledgement contract is
satisfied.

Failed preconditions are audited too. An unknown collector, missing disruptive
confirmation, invalid registration, or unsupported action appends a failed
operation result with the unchanged prior/resulting state. Replaying the same
request ID returns the original result and does not advance control generation
or append a second audit row.

## API shape

The canonical surface is organized by collector identity:

- full fleet snapshot and change-only stream for exact tooling;
- compact provider-summary snapshot and material-change stream;
- bounded provider pages and a bounded all-collector search;
- exact collector detail;
- bounded facts, gaps, retries/errors, events, operations, and schemas;
- diagnostics and health probe;
- one action route with a closed action enum and typed bounded parameters;
- one aggregate market-data-plane snapshot.

Definition installation and bounded provider acquisition remain separate admin
or acquisition routes. No frontend adapter imports them.

The same surface is available through `qt data collectors ...` and
`qt mcp serve`. MCP reads use `quanttrad://market-data/...` resources. MCP
mutations are planned by default and require `apply=true`, `confirm=true`, a
request ID, actor context, and reason before delegating to the canonical `qt`
command.

## Frontend V2

The collector console provides:

- a provider-first fleet summary with explicit state counts and separate health;
- lazy, paginated collector rows for one expanded provider at a time;
- an all-collector search as a secondary precise inspection view;
- an exact detail view for runtime, acquisition, data quality, event history,
  canonical Facts, diagnostics, and operation audit;
- state-relevant primary actions, with probe, restart, and diagnostics in the
  overflow surface;
- backend aggregate metrics without frontend-only health calculations.

Structured payloads and raw evidence are available in an Advanced/Raw section.
Provider-specific diagnostic extensions may have a bounded typed renderer, but
provider identity never selects generic lifecycle or actions.

The operator route is **Operations -> Market**. It does not subscribe to every
collector. One provider-summary stream communicates material fleet changes;
details refresh only while the selected page is visible. At 200 or more
collectors, the default workload therefore remains proportional to the number
of providers and the one bounded page the operator opens.

The detail view keeps routine evidence out of the primary control surface,
shows only actions valid for the current state, and keeps runtime, activity,
Facts, data quality, diagnostics, configuration, and operation history behind
one collector identity.

## Operational migration

The control columns and immutable operation table are introduced by an
out-of-band migration while collector writers are stopped. The migration maps
existing configured intent once, removes superseded mutable runtime-mode data,
and validates that no active claim or stream lease exists. Runtime has no
missing-column fallback.

See [ADR 0064](../decisions/0064-use-one-code-owned-collector-operations-contract.md)
and the [discovery report](../../engineering/collector-operations-discovery.md).
Operator commands, action guards, and failure procedures are documented in the
[collector operations guide](../../guides/collector-operations.md).
