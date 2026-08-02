---
component: operator-console-v2
subsystem: frontend
layer: projection
doc_type: architecture
status: active
tags:
  - frontend
  - operator-console
  - overview
  - operations
  - botlens
  - read-model
  - market-data
  - research
code_paths:
  - portal/frontend/src/main-v2.jsx
  - portal/frontend/src/v2
  - portal/frontend/src/features/overview
  - portal/frontend/src/features/operations
  - portal/frontend/src/features/collectors
  - portal/frontend/src/features/market-structure
  - portal/frontend/src/features/bots/botlens
  - portal/frontend/src/adapters/marketData.adapter.js
  - portal/frontend/src/adapters/research.adapter.js
  - portal/backend/controller/bots.py
  - portal/backend/controller/reports.py
  - portal/backend/controller/research.py
  - portal/backend/service/bots/bot_service.py
  - portal/backend/service/bots/botlens_bootstrap_service.py
  - portal/backend/service/reports
  - portal/backend/service/research
---
# Operator Console v2

## Purpose

Operator Console v2 is Quant Trad's bounded human inspection surface. It answers
what needs attention, what is actually running, whether market evidence is
usable, and what persisted research outcomes exist. It does not start, stop,
configure, import, promote, or otherwise mutate platform state.

The initial information architecture is deliberately limited to two primary
rooms:

- **Overview** is the evidence-first entry point.
- **Operations** is the searchable inventory of run instances, data-plane
  evidence, research evidence, and their definitions.

BotLens, collector evidence, and research evidence are contextual lenses reached
from those rooms. They are not additional top-level products.

## Boundary Contract

The console owns:

- browser-local filters, sort order, selected tabs, and safe navigation origin;
- view models over typed backend read contracts;
- explicit loading, partial, unavailable, stale, invalid, and empty states;
- bounded refresh and polling of read-only projections;
- run-scoped navigation to persisted supporting evidence.

The console does not own:

- runtime, collector, or stream lifecycle;
- scheduling, retries, leases, fencing, or liveness;
- report generation, research mutation, or strategy authoring;
- market-data reconstruction, admission, normalization, or quality truth;
- alternate run identity or report semantics.

The v2 adapters used by the primary rooms issue GET requests only. The v1
application remains a separate entrypoint while the operator console matures.

## Evidence Flow

```text
durable runtime / market / research truth
  -> typed backend read projections
  -> GET-only frontend adapters
  -> source-specific hooks with explicit partial failure
  -> pure evidence view models
  -> Overview / Operations
  -> exact run, collector, or research lens
```

Frontend state may improve first paint, but it is never authoritative. A run
link carries a matching navigation hint for immediate rendering and then fetches
the exact route `run_id`. A mismatched response is rejected rather than rendered
under the requested identity.

## Overview Contract

Overview preserves this order:

1. attention;
2. current operations;
3. market-data posture;
4. research activity;
5. recent outcomes.

Attention uses a fixed 72-hour lookback, deduplicates by canonical evidence
identity, and sorts by severity followed by evidence recency. Healthy evidence,
disabled schedules, and terminal failures outside the lookback do not appear.

Current operations contains actual active run instances, in-flight collector
attempts, and currently leased stream sessions. Enabled bot or collector
definitions are configuration and are never counted as running work.

Market posture exposes collection, coverage, book validity, archive state,
normalization, admission, quality evidence, and latest observation separately.
It does not collapse them into a platform-health claim. In particular, a
configured or recently collected BIP/BTC pair is not treated as production
admitted without explicit persisted admission evidence.

The activity heatmap uses backend aggregates, zero-filled UTC days, a named
timestamp field, and explicit qualifying lifecycle states. The frontend offers
only activity types supported by those contracts. Top result considers
completed backtests only and uses deterministic metric, completion-time, and
`run_id` ordering.

## Operations Contract

Operations keeps four concepts separate:

- persisted run instances;
- collector schedule definitions and their delivery attempts;
- continuous stream definitions and session evidence;
- bot and research definitions/evidence.

The initial run inventory is bounded to the latest 50 persisted runs per bot
definition. That bound is visible operationally and can later become a paged
backend projection; it must not be described as complete history.

Collector health is delivery evidence, not process liveness. An enabled
definition with no attempt evidence is unknown. A recent successful attempt can
be on schedule, while process liveness still remains unknown because the current
contract has no heartbeat.

Continuous stream sessions are shown from persisted session events and fenced
lease evidence. Schedule attempts and stream sessions never share a synthetic
"running" state.

## Read Projections

The console composes existing read APIs with these bounded additions:

| Projection | Purpose |
| --- | --- |
| `GET /api/bots/runs/{run_id}` | Exact persisted run, lifecycle, lease, report, and definition evidence |
| `GET /api/bots/runs/{run_id}/botlens/bootstrap` | Exact run-scoped BotLens bootstrap; historical transport remains closed |
| `GET /api/reports/activity` | Zero-filled completed-backtest activity by UTC day |
| `GET /api/reports?sort=...` | Deterministic completed-result ordering using supported metric sorts |
| `GET /api/research/activity` | Zero-filled activity for supported persisted research evidence types |
| Existing market-structure reads | Definitions, sessions, status, normalization specifications, and latest facts |

A missing bot definition does not erase a persisted historical run. Exact run
and BotLens projections fall back to the stored run identity and mark definition
availability explicitly.

## Refresh, Failure, And Trust

- API reachability is labeled connectivity only and is not a platform-health
  summary.
- Independent sources may fail independently; successful evidence remains
  visible alongside actionable error text.
- Polling never calls watchdog or mutation-bearing endpoints.
- Empty means the authoritative read returned no matching evidence. Unknown
  means the required evidence or contract is absent. Unavailable means the read
  failed.
- Client clocks are used only for presentation and bounded age/lookback
  evaluation over persisted timestamps.

## Routing

Primary routes are `/overview` and `/operations`. Contextual evidence routes
are:

- `/operations/runs/:runId`;
- `/operations/collectors/:definitionId`;
- `/operations/research/:itemId`.

Legacy v2 room routes redirect into the bounded information architecture.
Mutation-oriented legacy rooms are not mounted in the v2 entrypoint.

## Deliberate Limits

- The console has no authoritative platform-wide heartbeat.
- Run inventory is per-definition and bounded rather than globally paginated.
- Historical BotLens uses persisted/rebuildable evidence and does not open live
  transport.
- Backend report activity uses `ended_at`; supported research memory activity
  uses `created_at`, and research-check completion currently uses persisted
  check creation as the documented completion proxy.
- Bundle splitting is a performance follow-up; it does not change projection
  ownership or evidence semantics.

## Related Docs

- [System architecture model](../system/SYSTEM_MODEL.md)
- [BotLens projection boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
- [Reporting boundary](../reporting/REPORTING_BOUNDARY.md)
- [Research memory boundary](../research-memory/RESEARCH_MEMORY_BOUNDARY.md)
- [Data boundary](../data/DATA_BOUNDARY.md)
