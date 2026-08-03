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
  - portal/backend/controller/market_data.py
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
- **Operations** locates run, market, and research evidence.

BotLens, Market Lens, and research trails are contextual lenses opened
from those rooms. The primary rooms stay sparse; technical identity, attempts,
timestamps, diagnostics, and raw payloads belong in lenses or context menus.

## Boundary Contract

The console owns:

- browser-local filters, sort order, cursor-window state, tabs, and safe navigation;
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

## Navigation And Visual Language

The shell uses a persistent left rail with only **Overview** and **Operations**.
The rail is keyboard-operable, collapsible, and remembers the browser-local
preference. Collapsing navigation changes presentation only; it never changes
route or evidence state. On narrow screens it becomes an icon rail instead of
covering the workspace.

The visual hierarchy is intentionally restrained: matte work surfaces carry
tables and charts; glass and backdrop blur are reserved for the navigation
shell, routed lenses, menus, and modal overlays. Warm ivory and brass establish
the luxury tone, while cyan is reserved for live evidence. Motion is short,
functional, and disabled by the existing reduced-motion preference. Loading
uses component-shaped skeletons so stale values are not painted as current.

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

Operations has three task domains rather than exposing backend subsystem names:

| Domain | Primary row grain | Deep detail |
| --- | --- | --- |
| Runs | persisted run instance | BotLens modal, report/research evidence, copy-only rerun command |
| Market | grouped scheduled facts and configured structure pairs | Market Lens facts, attempts, latency, and quality |
| Research | persisted research item | relationship trail and raw provider-free evidence |

The Market domain keeps two explicit sections. **Scheduled facts** groups facts
such as `derivatives.open_interest` and `derivatives.funding_rate` by provider
and provider product/instrument. **Structure streams** preserves independent
coverage, book-validity, archive, normalization, admission, and quality states
for configured futures/spot relationships. The grouping reduces scan cost but
does not merge their typed backend contracts.

Run inventory uses `bot_run_inventory.v1`: one reverse-chronological server
window with a stable `(before_sort_at, before_run_id)` continuation cursor.
**Load older runs** appends and deduplicates the next bounded window; numbered
pages are not the historical retrieval contract. Market inventory uses
`market_structure_operator_snapshot.v1` plus a change-only SSE stream, while
the scheduled-fact projection has its own durable snapshot and stream. Research
remains bounded to 200 records. Only the selected task domain performs its
secondary inventory reads.

The market snapshot is a partial-success contract. `component_errors` is keyed
by `definitions`, `sessions`, `normalization_specs`, or `status_by_definition`;
each value contains a stable code, readable message, and exact technical detail.
A failed component returns its typed empty fallback while healthy components
remain visible. The backend emits one warning per distinct component failure
and one recovery event rather than logging every five-second stream poll. The
frontend marks normalization unavailable when its component read failed and
renders each error inside Structure Streams; it never reinterprets a failed read
as evidence that no specifications exist.

Market Lens is a routed, blurred modal over Operations. It exposes four
component-owned views: Status, Facts, Attempts, and Quality. Facts are read from
a bounded canonical typed-fact window (maximum seven days and 1,000 samples);
the lens never calls the provider or reconstructs missing history. Attempt bars
show wall time and provider request time independently when provider timing
evidence is present. Missing provider timing is labeled unavailable rather than
interpreted as zero.

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
indefinitely. Terminal run bootstrap reconstructs run/catalog scope only and
marks selected-symbol state as a required secondary read. That lets the frozen
240-bar chart and the first durable forensic page load in parallel with cold
symbol projection instead of blocking all useful content behind it. Run
inventory determines replay eligibility from either a hot
projection or compact durable BotLens-ledger evidence; the inventory read never
reconstructs the run.

Dataset-bound backtest charts read only the run's frozen dataset series at its
recorded commit boundary. They never fall through to later canonical revisions.
The initial view requests the latest 240 bars, left-edge movement requests one
guarded older page, and the manual **Load earlier** action remains as an
accessible fallback. Every chart response names its evidence source.

Selected-symbol snapshots are latest-tail views: 32 signals, 32 decisions, 64
trade states, 32 logs, and 160 overlays at most. Each concern reports included,
available, ordering, and truncation metadata. These windows make initial state
responsive; they are not completeness claims.

Decision replay uses the typed durable event ledger in ascending 200-event
pages keyed by after_seq and after_row_id and scoped to the selected instrument.
The UI deduplicates those documents against the bounded projection snapshot,
streams the next page near the scroll edge, and retains **Continue replay** as
an accessible fallback. Signals, decisions, fills, and trade lifecycle events
keep their domain identity and context. A component-local failure preserves
already loaded evidence and exposes copyable details.

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

Errors render beside the component and action they affect, not in a page-level
stack. Summary cards, attention, current operations, scheduled facts, structure
streams, research evidence, and lenses each own their loading and failure state.
A readable summary is primary; a disclosure and copy action preserve exact
technical details. Independent sources fail independently. Successful evidence stays visible.
Unknown means a contract/evidence fact is absent; unavailable means a read
failed; invalid means evidence explicitly failed a validity contract.

## Deliberate Limits

- no single synthetic platform-wide heartbeat; collector workers, bot containers, streams, and API reachability retain separate authority;
- no browser mutation or run-control commands;
- no unbounded historical inventory; run history advances only through the stable cursor;
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
