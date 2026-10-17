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

Frontend V2 is the bounded human research browser. The agent remains the primary
work interface. Browser state never owns research, runtime, collector or
scientific history.

## Navigation and presentation

Primary destinations are **Overview** and **Studies**. **Bot runs** and
**Collectors** are named secondary links, not nested under an Operations room.
The bottom **Settings** menu contains Storage and Appearance on desktop and
phone. Existing appearance, density and motion preferences remain owned by
PortalSettingsContext; the charcoal surfaces use existing v2 semantic tokens
and a sage default accent. Previously saved accent choices remain respected.

Routes are `/overview`, `/studies`, `/studies/:itemId`, `/bot-runs` and
`/collectors`. The exact `/operations/runs/:runId`,
`/operations/market/:collectorKind/:collectorId` and
`/operations/research/:itemId` routes remain inspectable. Legacy Operations tab
links redirect deliberately to their named destination; research and memory
aliases reach Studies. No historical evidence identities are rewritten.

Matte charcoal work panels, restrained navigation glass, readable focus rings
and the existing responsive shell carry the approved palette. Scientific
conclusions and limitations are separate from record lifecycle status.

## Read boundaries and Overview

`GET /api/research/overview` aggregates saved Study counts in PostgreSQL. It does
not fetch all Study payloads or relationship trails into Python or the browser.
Symbol, timeframe and exact stored-window distributions count Study IDs;
windows are not continuous source-data coverage. Linked Check definition
groups count each Study once per actual linked definition, may overlap, and
retain unclassified Studies. Only the largest 12 groups are returned, with
explicit remainder counts. No count implies independent scientific evidence.

Overview has three inventory rollups, one compact bot/collector attention area,
one existing ActivityHeatmap and recent compact Study rows. Activity explicitly
selects completed Checks, completed backtests, created hypotheses or recorded
Observations. Backtests retain ended_at UTC semantics; research activity retains
the producing contract's created_at semantics. Activity is not market time.
Operational details stay in Bot runs and Collectors. Missing/partial evidence
never becomes zero or an all-clear. No full research inventory is loaded for
attention or activity.

Bot runs reuses the existing active-run projection, stream-first behavior,
Current/History separation, stable historical cursor, BotLens, replay and run
report access. Live/paper/backtest executions remain distinct from research
Checks. Collectors reuses the provider/collector inventory, material-change
stream, health/freshness/ingestion evidence and separately owned audited
lifecycle operations. Collector detail retains Runtime, Activity, Facts, Data
quality, Diagnostics and Configuration. Recent Facts are not a historical
market browser or proof that a Study's source inputs remain available.

## Study browsing and inspection

`GET /api/research/browse` returns scalar metadata plus publication descriptors,
not bodies, full payloads or base64 files. Search is title/symbol/ID, with exact
kind, state, symbol, timeframe and publication filters. Pages contain 1..100
rows (UI 25); continuation is stable `(created_at,id)` descending and scoped to
the exact filters. Total means all matching saved records, not the loaded page.
URL filters and browser back/forward retain navigation state. A direct cursor
link can return to First page without assuming a local cursor history.

Studies defaults to Study records. **All research records** retains standalone
Checks, Observations and hypotheses; unrelated records are not forced into a
new Study entity. `summary` reads compact scalar fields, body and verified
question/publication history. Selecting a revision pins its hash and reference
identities. A generic interpretation uses emitted conclusion/scope/limitations;
heterogeneous records without interpretation remain inspectable. No universal
scientific chart renderer or hardcoded result template is introduced.

Interpretation, Evidence, Files and History are separate views. Evidence loads
a bounded 50-link one-hop compact `relationships` projection only on intent;
it labels truncation and separates current links from frozen publication
references. Related bot runs retain exact BotLens links and origin-aware return.
Two linked Checks can use the existing comparison contract on explicit intent,
with bounded/access-checked records and a 256 KiB response cap. Scope, Dataset
and population remain inspectable; numeric deltas do not establish independent
evidence. Full raw payload is an explicit bounded load. Files loads existing
native image metadata and lazy original image bytes, filtering Study images by
the selected publication hash. Existing image custody revalidates bindings,
checksums and access. Legacy encoded reports are preserved in downloadable
original record JSON; extracted report-file retrieval needs an explicitly
reviewed typed field/MIME/checksum mapping, not heuristic payload traversal.

## Request lifecycle and limits

Research reads coalesce identical in-flight requests. Each consumer owns its
AbortSignal; only the last consumer's departure aborts the physical fetch.
Navigation invalidates rendering ownership immediately, and obsolete success
or error cannot overwrite another record. Abort is a cancelled section state,
with an explicit reload action; it is not a technical failure. A bounded
15-second, 20-entry/2 MiB memory cache holds compact metadata only (individual
entries at most 256 KiB). Raw records, export preflights and ZIP bytes are not
cached. No private evidence is persisted in localStorage.

Read handlers are synchronous threadpool work with the existing ExecutionControl
20-second total deadline for new reads and reused research activity/image GETs.
New compact sessions also use a 5-second per-statement PostgreSQL timeout;
other SQL inside the controlled scope uses the existing remaining-deadline hook.
The reused backtest activity endpoint belongs to reporting and is unchanged; its
latency/SQL-release behavior needs separate qualification, not an assumed new
research timeout. New
compact sessions use repeatable-read/read-only connection options before their
first statement. Summary and relationship release project nested dependency
IDs and apply existing protocol custody checks without transferring full
payloads; access verification fails closed above its 512-identity bound.

The real local HTTP fixture test proves that closing a client socket does not
stop the synchronous read worker. Browser AbortController is not a claim of
server cancellation. Deadlines independently bound remaining work; actual
PostgreSQL disconnect-to-release timing is not measured here. No navigation,
page cancellation, cache eviction or export cancellation calls the scientific
job cancel route. Research jobs retain their own admission/lifecycle authority.

## Saved-record evidence export

Export review is explicit. Preflight and content use a selected revision/hash,
recheck access and referenced identities, preserve historical reasoning
snapshots, and reuse immutable Check definitions/requests/plans/bindings/results.
Original root-current JSON is labeled separately from the exact selected
historical publication. Native image bytes come only from custody, filtered to
included interpretation hashes or exact Check evidence. Later interpretation
images cannot enter a historical snapshot bundle.

The ZIP contains records, selected publication, native visuals and manifests,
README reproduction limits, and SHA-256 checksums. It is always labeled
`saved_record_verification_only`; no separate raw-source files are included.
Exact record payloads are preserved, including embedded content whose source
completeness and redistribution rights are not established by export. Record checksums do not establish detector recomputation, statistical
validity, causality or profitability. Embedded reports remain unmodified inside
their record JSON. Export never acquires, replays, executes attachments or
grants source redistribution rights.

Bounds: 8 MiB per record, 32 MiB complete uncompressed bundle, 160 files including
metadata, at most 100 pinned references and one concurrent export preparation
per process. Oversize, inaccessible, invalid or changed evidence fails explicitly;
no partial archive silently passes. The existing database model and image
durability are unchanged; there is no new persistence layer or migration.

## Validation scope

`make frontend-check` runs Node model/adapter/source tests, React/jsdom component
tests and a Vite build. Focused backend tests exercise compact projections,
cursor scoping, sealed dependencies, frozen reference export, bounds/checksums
and a real local HTTP disconnect with synthetic work. Disposable DB tests cover
large paginated inventories/aggregates when the isolated Docker prerequisite is
available. Browser QA uses installed Edge, isolated profile and synthetic API
fixtures; it must not be described as a production performance measurement.

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

- [Historical operator validation](../../engineering/frontend-v2-operator-validation.md)
- [System architecture model](../system/SYSTEM_MODEL.md)
- [BotLens projection boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
- [Reporting boundary](../reporting/REPORTING_BOUNDARY.md)
- [ADR 0055: Bounded BotLens hot state and durable inspection](../decisions/0055-separate-bounded-botlens-hot-state-from-durable-inspection.md)
- [Research memory boundary](../research-memory/RESEARCH_MEMORY_BOUNDARY.md)
- [Data boundary](../data/DATA_BOUNDARY.md)

## System storage settings

The sidebar Storage entry opens a separately owned settings surface. Compact
drive rows show backend capacity and availability; role selectors and collapsed
advanced controls produce a durable backend review. Enrollment selects a
host-prepared target ID. The page does not accept filesystem paths or execute
storage work itself. Missing status and blocked plans stay explicit.

Physical policy activation remains unavailable until its executor and migration
are implemented. See [storage management](../persistence/STORAGE_MANAGEMENT.md).


## Human Narrative Readability Review

The one-time authoring and visual-review guidance is in
[Writing research for human inspection](../../guides/research-workflow.md#writing-research-for-human-inspection).
The current narrative surface renders escaped plain text, not Markdown: conclusion
and body preserve newlines; scope and limitations currently collapse them. A
small semantic block renderer and consistent readable spacing are proposed for
review, not implemented by this documentation change. Human explanations belong
in existing title/body and versioned conclusion/scope/limitations fields; immutable
Check evidence, technical identities and operational receipts retain their owners.
Missing top-level Study metadata must not be presented as unknown underlying
scientific evidence, nor automatically inferred from free text.
