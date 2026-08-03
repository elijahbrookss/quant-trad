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
  - portal/frontend/src/components/bots
  - portal/frontend/src/adapters/marketData.adapter.js
  - portal/frontend/src/adapters/research.adapter.js
  - portal/backend/controller/bots.py
  - portal/backend/controller/reports.py
  - portal/backend/controller/research.py
  - portal/backend/service/bots/bot_service.py
  - portal/backend/service/storage/repos/runs.py
  - portal/backend/service/storage/repos/market_structure.py
  - portal/backend/workers/market_data_collector.py
  - portal/backend/service/bots/botlens_bootstrap_service.py
  - portal/backend/service/reports
  - portal/backend/service/research
---
# Operator Console v2

## Purpose

Operator Console v2 is Quant Trad's bounded human inspection surface. It is a
dashboard and evidence browser, not an alternate workflow engine.

The information architecture has two primary rooms:

- **Overview** answers whether anything needs the operator now.
- **Operations** locates run, collector, market-data, and research evidence.

BotLens, collector attempts, and research trails are contextual lenses opened
from those rooms. The primary rooms stay sparse; technical identity, attempts,
timestamps, diagnostics, and raw payloads belong in lenses or context menus.

## Boundary Contract

The console owns:

- browser-local filters, sort order, pagination, tabs, and safe navigation;
- view models over typed backend read contracts;
- explicit loading, partial, unavailable, stale, invalid, and empty states;
- bounded snapshots, cursors, and change-only streams over read-only projections;
- run-scoped navigation to persisted supporting evidence;
- readable operator summaries with copyable technical error details.

The console does not own:

- runtime, container, collector, or stream lifecycle;
- scheduling, retries, leases, fencing, or process liveness;
- report generation, research mutation, or strategy authoring;
- market-data reconstruction, admission, normalization, or quality truth;
- alternate run identity or report semantics.

The v2 adapters used by the primary rooms issue GET requests only. A rerun
option copies the canonical `qt bots start ... --dataset-id ...` command; it
does not start a run from the browser.

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

Frontend state may improve first paint, but it is never authoritative. Exact
run routes refresh the navigation hint from `run_id` and reject mismatched
scope.

## Overview Contract

Overview is intentionally dashboard-shaped:

- four rollups: attention, active runs, collector delivery, and market pairs;
- at most three current attention items;
- at most four active evidence rows;
- one bounded activity heatmap;
- one top completed result.

There are no numbered report sections and no complete operational tables on
Overview.

Attention uses a fixed 72-hour lookback, deduplicates by canonical evidence
identity, and sorts by severity followed by recency. Active work contains real
run instances, in-flight attempts, and leased stream sessions. Enabled
definitions are never counted as running.

Collector rollups are healthy only when the durable worker heartbeat is current, an active attempt is not stalled, the schedule is current, and recent successful delivery evidence is fresh. Liveness, schedule, and delivery remain separately inspectable. Market posture keeps collection, coverage, book
validity, archive, normalization, admission, and quality evidence independent.

## Operations Contract

Operations has four inventories:

| Inventory | Primary row grain | Deep detail |
| --- | --- | --- |
| Runs | persisted run instance | BotLens modal, report/research evidence, copy-only rerun command |
| Collectors | provider + provider product/instrument | individual fact schedule and recent attempts |
| Market data | configured futures/spot pair | independent quality and admission states |
| Research | persisted research item | relationship trail and raw provider-free evidence |

Collector rows group facts such as `derivatives.open_interest` and
`derivatives.funding_rate` under the same provider and provider product.
Canonical instrument identity remains visible but is not the primary label.

Run inventory uses `bot_run_inventory.v1`: one reverse-chronological, 100-row server window with a stable `(before_sort_at, before_run_id)` continuation cursor. It does not fan out by bot definition. Market inventory uses `market_structure_operator_snapshot.v1` plus a change-only SSE stream; the compact list projection is built in one database round trip while full per-definition archive, coverage, book, and quality forensics stay lazy behind the lens. Research remains bounded to 200 records. Client tables may window these results, but numbered pages are not the primary historical retrieval contract.

## BotLens Modal And Replay

Run inspection is a routed modal with a blurred background and a four-field
evidence header. Closing the modal returns to its originating inventory.

Eligibility rules:

- active eligible runs are labeled **Open BotLens**;
- terminal persisted projections are labeled **Open replay** and
  **Rebuildable**;
- runs without reported BotLens evidence render a disabled action;
- direct routes remain safe and render explicit unavailable/error states.

Historical bootstrap is bounded to 30 seconds. A timeout states that historical
replay did not become ready and offers retry/report evidence; it never spins
indefinitely. Chart history is fetched in 240-bar pages. Panning to the left
edge triggers one guarded page request; moving away rearms the trigger. The
manual **Load earlier** action remains as an accessible fallback.

This does not claim that the full historical decision ledger is already
available in BotLens. Persisted report datasets remain authoritative for
completed decisions and trades until cold BotLens reconstruction proves equal,
bounded, and operationally reliable.

## Liveness And Freshness Language

The console may say:

- API reachable: the health request succeeded;
- collectors live: the durable collector worker heartbeat is current;
- market updates live: the market-structure snapshot stream is connected;
- BotLens live: an eligible run projection is sequenced and resynchronizable;
- on schedule: collector attempts satisfy delivery timing.

The console may not collapse those facts into “platform healthy” or “all containers live.” Collector-worker heartbeat authority is deliberately scoped to scheduled market-data collection; bot container state, stream leases, and API reachability retain their own evidence boundaries.

## Failure Presentation

Known technical failures are translated into operator language while preserving
the exact detail behind a disclosure and copy action. For example,
`market_normalization_spec_storage_corrupt: hash mismatch` becomes a
normalization-integrity failure with the raw message available for forensics.

Independent sources fail independently. Successful evidence stays visible.
Unknown means a contract/evidence fact is absent; unavailable means a read
failed; invalid means evidence explicitly failed a validity contract.

## Deliberate Limits

- no single synthetic platform-wide heartbeat; collector workers, bot containers, streams, and API reachability retain separate authority;
- no browser mutation or run-control commands;
- no complete historical inventory beyond the visible read bounds;
- no claim that every completed run has usable BotLens evidence;
- no claim that hot BotLens projection equals complete persisted replay until
  the deterministic reconciliation tests pass;
- no frontend suppression of backend collision, gap, quality, or readiness
  evidence.

## Related Docs

- [Operator validation](../../engineering/frontend-v2-operator-validation.md)
- [System architecture model](../system/SYSTEM_MODEL.md)
- [BotLens projection boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
- [Reporting boundary](../reporting/REPORTING_BOUNDARY.md)
- [Research memory boundary](../research-memory/RESEARCH_MEMORY_BOUNDARY.md)
- [Data boundary](../data/DATA_BOUNDARY.md)
