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
  - portal/backend/service/storage/repos/candles.py
  - portal/backend/workers/market_data_collector.py
  - portal/backend/service/bots/botlens_bootstrap_service.py
  - portal/backend/service/bots/botlens_domain_events.py
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
| Runs / Current | one fresh leased run instance from the active-run stream | BotLens modal and exact-run actions |
| Runs / History | persisted terminal or prior run instance | replay, report/research evidence, copy-only rerun command |
| Market | grouped scheduled facts and configured structure pairs | Market Lens facts, attempts, latency, and quality |
| Research | persisted research item | relationship trail and raw provider-free evidence |

The Market domain keeps two explicit sections. **Scheduled facts** groups facts
such as `derivatives.open_interest` and `derivatives.funding_rate` by provider
and provider product/instrument. **Structure streams** preserves independent
coverage, book-validity, archive, normalization, admission, and quality states
for configured futures/spot relationships. The grouping reduces scan cost but
does not merge their typed backend contracts.

Runs opens on **Current**. Current run cards consume `active_run_list.v1` from
`GET /api/bots/runs/active` and its `GET /api/bots/runs/stream` SSE companion.
The stream's initial snapshot is primary; the browser performs one bounded HTTP
fallback only when the first stream snapshot does not arrive. Runtime deltas
update an exact `run_id` locally, while membership changes trigger one bounded
resynchronization read. There is no interval polling.

The active projection begins with fresh run leases, joins run/lifecycle truth in
batches, and includes only evidenced starting, running, paused, or degraded run
instances. Two active runs of one bot definition remain two cards because
definition identity and run identity are not interchangeable. Overview uses the
same active-run projection and never opens the global historical inventory
endpoint. A configured or terminal bot definition is not an active run.

**History** is an explicit secondary view. Only after the operator selects it
does the console read `bot_run_inventory.v1`: one 20-run
reverse-chronological server window with a stable
`(before_sort_at, before_run_id)` continuation cursor. **Load older runs**
appends and deduplicates the next bounded window; numbered pages are not the
historical retrieval contract, and the console never attempts to preload every
completed run or replay. Market inventory uses
`market_structure_operator_snapshot.v1` plus a change-only SSE stream, while
the scheduled-fact projection has its own durable snapshot and stream. Research
remains bounded to 200 records. Only the selected task domain performs its
secondary inventory reads.

The history repository selects only list-card scalar fields, summary, hashes,
execution semantics, and compact dataset identity. It does not read or return a
run's full `config_snapshot`; opening the exact run is the boundary that may
load that configuration. This keeps thousands of historical runs cheap to scan
without weakening exact-run provenance.

Scheduled-fact and market-structure hooks are also stream-first. When an SSE
surface exists they wait up to four seconds for its initial snapshot before one
HTTP fallback. Manual refresh remains an explicit resynchronization action;
mounting a page does not launch an HTTP snapshot and SSE snapshot in parallel.

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

Run inspection is a routed modal with a blurred background and a compact
dashboard header. The header shows strategy, lifecycle/mode, exact backtest
range, execution semantics, selected market, open trades, warning count, and
last-event freshness. Generated IDs and raw contract detail stay behind the
fixed, body-portal **Run details** lens with a copy action. That compact lens
fits in the current viewport and scrolls only its bounded raw-detail region,
not the underlying page. Closing BotLens returns to its originating inventory.

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
240-bar chart and first 100-record decision, trade, and diagnostic pages load in
parallel with cold symbol projection instead of blocking all useful content
behind it. Historical run scope recovers instrument ID and timeframe from the
canonical series key when older catalog rows omit those routing fields; it does
not invent a human symbol. Run inventory determines replay eligibility from
either a hot projection or compact durable BotLens-ledger evidence; the
inventory read never reconstructs the run.

Chart-history reconstruction is a blocking database/replay read and therefore
runs in FastAPI's worker threadpool, never on the shared async event loop that
owns health, live streams, and navigation reads.

Active BotLens opens are bootstrap-first. When the hot selected-symbol
bootstrap already contains a bounded candle window, the console renders that
window and attaches the live stream without issuing the initial cold
chart-history request or scanning durable forensic history. Cold chart
reconstruction remains available when bootstrap has no candles or the operator
moves left. Completed runs request their initial durable 240-bar chart window
and independent decision, trade, and diagnostic pages because terminal replay,
not a live projection, owns their historical completeness evidence.

Run identity, selected-symbol bootstrap, and initial chart reads are abortable
and scheduled after the mount commits. Research evidence is not requested for
an active run; the compact header uses exact-run inspection and labels research
as deferred. A terminal run loads research evidence only after its authoritative
run read confirms terminal state. React development StrictMode probe mounts are
therefore canceled before durable reads begin, while a real mount still fails
visibly if any component read fails.

Bot controller handlers that call synchronous SQL, Docker inspection, dataset
preparation, or forensic services execute in FastAPI's worker threadpool. The
SSE initial fleet snapshot and live-WebSocket run resolution explicitly offload
the same blocking work. A slow container inspection or replay may delay its own
component, but it must not occupy the async event loop that serves health,
navigation, or unrelated live sockets.

This is an API-wide boundary, not a BotLens-only optimization. Synchronous HTTP
handlers use `def` so FastAPI owns their worker-thread dispatch. Routes remain
`async def` only when they await cooperative stream, queue, WebSocket, or job
work; any synchronous lookup or replay inside such a route crosses an explicit
thread offload seam. See [ADR 0054](../decisions/0054-keep-blocking-api-work-off-the-event-loop.md).

Dataset-bound backtest charts read only the run's frozen dataset series at its
recorded commit boundary. They never fall through to later canonical revisions.
The initial view requests the latest 240 bars. Actual logical-range movement
near the left edge requests one guarded older page; a focused interior window
can likewise request newer pages at its right edge. Frozen responses compute
before/after flags against dataset bounds rather than merely the requested
interval, and long-lived chart subscriptions invoke the current range callback.
Viewport pan is the only history-navigation control; there is no separate
**Load earlier** action. The browser retains a sliding window of at most 3,840
candles. Prepend and focused-window append preserve the prior visible time
range. Selecting a trade replaces unrelated chart history with at most 320 bars
spanning 72 bars before entry through 72 bars after exit, then centers the chart
on entry. Every chart response names its evidence source.

Trade-marker layers are deduplicated by evidence identity and refreshed after
camera setup, so initial, focused, prepended, appended, and resized views index
labels against the loaded candle range without changing causal bar projection.

New runs may also return bounded historical overlay pages replayed from retained
overlay deltas. The browser merges at most 16 page payloads and uses durable
history for terminal runs while active runs continue to prefer the live
projection. Each page reports ordering, cadence, window, terminal-checkpoint,
truncation, and fingerprint evidence. Old runs without retained deltas say
**overlays not retained for this run**; incomplete pages say **bounded replay**;
only proven pages say **ledger verified**.

Terminal overlay reconstruction avoids repeated presentation serialization
inside the delta loop, but it still verifies ordering and every typed tail-patch
result before emitting the same stable page fingerprint. Performance work must
not turn a corrupt overlay chain into apparently complete chart evidence.

Overlay validity is component-owned. A missing overlay commit or fingerprint
mismatch marks only `overlay_validity` invalid, clears/suppresses overlay
geometry, and displays a readable chart-local notice with copyable technical
detail. Frozen candles, decisions, trade markers, and forensic paging remain
available. A later typed full-state overlay checkpoint may recover that layer;
the UI never mutates an old snapshot to pretend the missing evidence arrived.

Selected-symbol snapshots are latest-tail views: 16 signals, 16 decisions, 32
trade states, 16 logs, and 160 overlays at most. Runtime warning detail is also
limited to the latest 16 entries while the total count, type/severity summary,
and durable forensic/report paths remain available. Each concern reports included,
available, ordering, and truncation metadata. These windows make initial state
responsive; they are not completeness claims.

Terminal decision, trade, and diagnostic inspection uses separate typed
100-record pages. Decision and trade pages are scoped to the selected canonical
instrument when that identity is available, with symbol fallback only for old
evidence. Their tab badges and page counts use durable totals rather than the
bounded projection tail. The browser retains only the current page and uses
explicit previous/next navigation; it does not grow an infinite list.
Diagnostics are grouped by severity, source, stable code, and stable affected
identity so repeated fallback evidence becomes one inspectable condition. A
detail lens preserves every raw occurrence and offers a copyable troubleshooting
payload. Component-local failures preserve the chart and other healthy tabs.

Active runs continue to consume the typed live decision/trade/diagnostic tail.
The older ascending event-ledger cursor remains a forensic service boundary,
but the terminal operator workspace does not use that growing cursor as its
complete-history index.

Live WebSocket packets are queued in arrival order and reduced on a bounded
100-ms visual cadence in chunks of at most 24 messages and 256 KiB. Each ordered
chunk commits projection state once. Contiguous overlay commits combine their
ordered ops only when base clocks match; polyline tail patches update geometry
immediately and carry their result fingerprint forward. A pending client queue
is capped at 256 messages and 2 MiB; renderer backlog closes and resumes the
socket from its last committed cursor after accepted backlog drains. Packets
already queued behind an overflow boundary are not parsed. It does not force a
bootstrap unless the server reports
a mismatched session, ahead-of-stream cursor, or expired replay window.
Reconnect attempts use capped exponential backoff and reset only after a stable
connection. Server fanout sends to viewers concurrently with a configurable
1,500-ms default send deadline. A slow viewer is evicted and cannot serially
delay healthy viewers. See [ADR 0055](../decisions/0055-separate-bounded-botlens-hot-state-from-durable-inspection.md).

The terminal lifecycle message closes live eligibility in the projection
store. The existing routed room then performs one exact-run refresh and starts
research plus durable evidence only after that read proves the run inactive.
The chart/controller remains mounted across this handoff.

## Liveness And Freshness Language

The console may say:

- API reachable: the health request succeeded;
- collectors live: the durable collector worker heartbeat is current;
- market updates live: the market-structure snapshot stream is connected;
- BotLens live: an eligible run projection is sequenced and resynchronizable;
- active run alive: a fresh run lease has a hot projected runtime update;
- on schedule: collector attempts satisfy delivery timing.

`run_live` is derived through the canonical lifecycle normalizer from phase and
status; an omitted redundant boolean may not contradict `phase=live` and
`status=running`. Terminal lifecycle evidence clears run-level live readiness.

The console may not collapse those facts into “platform healthy” or “all
containers live.” Collector-worker heartbeat authority is deliberately scoped
to scheduled market-data collection; bot container state, stream leases, and API
reachability retain their own evidence boundaries.

An active lease without a hot projected runtime update is labeled **Awaiting
telemetry**, not alive. The console therefore promises near-real-time run
projection only after that typed evidence exists; it does not infer container
liveness from a configured definition or an open browser connection.

## Failure Presentation

Known technical failures are translated into operator language while preserving
the exact detail behind a disclosure and copy action. For example,
`market_normalization_spec_storage_corrupt: hash mismatch` becomes a
normalization-integrity failure with the raw message available for forensics.

Errors render beside the component and action they affect, not in a page-level
stack. Summary cards, attention, current operations, scheduled facts, structure
streams, research evidence, and lenses each own their loading and failure state.
A readable summary is primary; a disclosure and copy action preserve exact
technical details. Independent sources fail independently. Successful evidence
stays visible.
Overview never concatenates independent failures into a page-level warning.
Composite dashboard panels expose one collapsed, source-labeled availability
disclosure; valid snapshot values remain visible and are labeled partial when a
stream or sibling read fails.
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
- [ADR 0055: Bounded BotLens hot state and durable inspection](../decisions/0055-separate-bounded-botlens-hot-state-from-durable-inspection.md)
- [Research memory boundary](../research-memory/RESEARCH_MEMORY_BOUNDARY.md)
- [Data boundary](../data/DATA_BOUNDARY.md)
