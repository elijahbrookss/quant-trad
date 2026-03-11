---
component: botlens-debugger-architecture
subsystem: portal-runtime
layer: reference
doc_type: architecture
status: active
tags:
  - runtime
  - botlens
  - debugger
  - read-model
  - playback
code_paths:
  - portal/backend/service/bots/botlens_series_service.py
  - portal/backend/service/bots/telemetry_stream.py
  - portal/backend/service/storage/repos/runtime_events.py
  - portal/frontend/src/components/bots/BotLensLiveModal.jsx
  - portal/frontend/src/components/bots/BotLensChart.jsx
  - portal/frontend/src/components/bots/chartDataUtils.js
---
# BotLens Runtime Inspection Architecture

## Purpose

BotLens is the run-scoped runtime inspection surface for a bot run.

It exists to make one run explainable through a coherent inspection model that supports:

- debugging,
- replay,
- diagnosis,
- and operator understanding.

BotLens is not decorative UI.
It is the inspection surface for runtime behavior and runtime meaning.

## Why It Exists

Quant-Trad requires explainability across runtime execution.

A bot run must be inspectable in terms of:

- known-at timing,
- state transitions,
- decision context,
- execution realism,
- and resulting trades.

BotLens exists to expose that story without inventing a second execution path.
It is the inspection surface over the runtime event stream and its derived projections.

## What BotLens Promises

BotLens promises a coherent run-scoped inspection view of runtime behavior.

That means:

- one run-scoped continuity model,
- one canonical series identity model,
- one canonical candle timeline per selected series,
- one run-scoped event timeline not anchored solely to candles,
- one coherent set of derived projections for overlays, trades, warnings, logs, decisions, and runtime status,
- and one consistent interpretation across bootstrap, replay, paging, and live delivery.

BotLens does not promise convenience over correctness.
If the runtime story is inconsistent, BotLens must surface that inconsistency rather than mask it.

## What BotLens Does Not Promise

BotLens is not the trading engine and does not replace the canonical execution record.

It does not promise:

- legal or accounting completeness beyond the authoritative runtime record,
- perfect tick-level reconstruction when the authoritative event record is bar-scoped,
- zero-latency rendering,
- exhaustive exposure of every engine internal,
- hidden cleanup inside chart or render primitives,
- or an alternate semantics path separate from the authoritative runtime event model.

If the runtime did not know something yet, BotLens must not project it.
If BotLens cannot reproduce what the runtime knew, that is a contract violation.

## Core Position

BotLens is an inspection surface, not a demo surface.

The goal is not to make charts move.
The goal is to make a bot run intelligible.

That means semantic integrity beats animation polish.
Correctness beats convenience.
Fail-loud diagnostics beat silent repair.

## Authority Model

Runtime events are authoritative.

The BotLens read model is derived from the runtime event stream.
Snapshots and checkpoints are derived materializations of that same stream.

Authority is therefore:

- runtime events define semantics,
- BotLens projections derive from runtime events,
- snapshots/checkpoints materialize derived state for replay, bootstrap, restart efficiency, and read performance,
- frontend state consumes the canonical BotLens contract,
- frontend state does not define an independent truth.

Snapshots and checkpoints must never define alternate semantics.
If a snapshot cannot be reproduced from authoritative runtime events, the system has drifted.

## Canonical Runtime Story

BotLens is trustworthy only if every delivery mode is transport over the same underlying run story.

Those delivery modes include:

- bootstrap reads,
- history paging,
- snapshot replay,
- and live delivery.

These are not separate semantic modes.
They are separate access paths over the same run-scoped inspection contract.

## Read Model Contract

The BotLens read model is the run-scoped inspection projection of a bot run.

It contains:

- a canonical candle timeline for the selected series,
- a run-scoped event timeline that is not reducible to candle order alone,
- and derived projections such as overlays, trades, warnings, logs, decisions, and runtime status.

The read model must preserve continuity across those projections.
It must not require consumers to invent alternate reconstruction rules.

## Snapshot And Checkpoint Semantics

Snapshots and checkpoints are derived materializations only.

They exist to support:

- replay,
- bootstrap,
- restart efficiency,
- and read efficiency.

They are not an independent semantic authority.

Required properties:

- a snapshot/checkpoint is derived from the authoritative runtime event stream,
- a snapshot/checkpoint preserves the same identity and ordering contracts as the event stream,
- a snapshot/checkpoint does not redefine merge rules,
- and replay from a snapshot/checkpoint must remain semantically consistent with replay from authoritative runtime events.

## Identity Contracts

### Run Identity

Every BotLens payload belongs to one run identity.

All continuity, ordering, paging, replay, and live application semantics are scoped to that run identity.
No payload may be merged across run boundaries.

### Series Identity

Series identity is backend-owned canonical identity.

A series identity must be stable across:

- bootstrap,
- history paging,
- live delivery,
- snapshot materialization,
- and frontend consumption.

The frontend may maintain UI-local selection keys, but it must not redefine canonical series identity.

### Candle Identity

Candle identity is one canonical normalized bar-time rule.

For a given series:

- every candle time is normalized through one canonical rule,
- candle times are strictly increasing in the projected series timeline,
- a canonical candle time may appear at most once,
- and a same-identity candle replaces the prior candle rather than appending a duplicate.

Transport-specific, chart-specific, or UI-local candle identity rules are not allowed.

### Event Ordering And Continuity

BotLens event ordering is defined by the authoritative runtime ordering/cursor contract for a run.

The delivery layer must preserve:

- monotonic run-scoped ordering,
- explicit baseline establishment,
- continuity checks between baseline and live delivery,
- and explicit handling of stale, overlapping, or discontinuous updates.

### Overlay Identity

Overlay identity is explicit and stable within the BotLens projection model.

An overlay identity must support:

- revision,
- merge,
- supersession,
- retirement,
- and projection into renderable current state.

Overlay identity must not depend on chart-layer heuristics.

## Known-At Timing Contract

BotLens must respect the same known-at timing rules as the runtime and playback contracts.

That means:

- overlays do not appear before the runtime could have emitted the relevant overlay domain outputs/events,
- trades do not appear before the execution state exists,
- warnings and status changes appear when they became known,
- decisions appear according to the runtime event stream rather than retrospective convenience,
- and historical reads do not rewrite the meaning of later already-known state.

History may reveal older context.
It must not retroactively alter the semantics of later projected state.

## Delivery And Continuity Contract

BotLens delivery/state application is continuity-sensitive.

### Bootstrap

Bootstrap establishes the baseline cursor/sequence and the baseline projected state for a run/series selection.

No live application is trusted until baseline continuity is known.

### Live Application

Live application begins only after baseline continuity is established.

Incoming live payloads must be applied against the known baseline cursor/sequence.

### Overlap And Older Payloads

Overlapping, duplicate, or older live payloads must be:

- buffered,
- rejected,
- or treated as continuity failure.

They must not silently produce a competing timeline.

### Resync

Resync is a controlled rebuild from a fresh canonical baseline.

It is not an ad hoc chart repair path.
It is an explicit recovery path when continuity is no longer trusted.

### Stale Behavior

Stale state must be explicit.

BotLens may continue to show the last trusted projected state while marked stale, but it must not imply live continuity that is no longer trusted.

## Transport Trust States

BotLens delivery/state application uses a small explicit trust-state model:

- `bootstrapping`: baseline is being established
- `live`: baseline continuity is known and live payloads are being applied
- `stale`: the last projected state is retained but live continuity is not trusted
- `resyncing`: a fresh canonical baseline is being rebuilt
- `completed`: the run is terminal and live continuity is no longer expected

These are inspection-surface delivery states.
They are not trading business logic states.

## Projection Model

BotLens is more than a candle stream.

Its projection model consists of:

- selected-series candle timeline,
- run-scoped event timeline,
- overlay projection,
- trade projection,
- warning projection,
- log projection,
- decision projection,
- and runtime status projection.

Those projections may be rendered in different surfaces, but they must remain semantically aligned to the same run-scoped authority and continuity model.

## Merge Semantics

BotLens merge behavior is deterministic.

### Bootstrap Window

Bootstrap establishes the initial canonical visible window for the selected series.

Rules:

- normalize candle identity,
- preserve strict ascending order,
- collapse same-identity duplicates,
- seed the canonical baseline state.

### History Paging

History extends the visible window backward without altering the meaning of later already-known state.

Rules:

- merge by canonical candle identity,
- preserve strict ascending order,
- tolerate overlap without duplication,
- and preserve the same series identity and continuity contract as bootstrap/live delivery.

### Live Updates

Live delivery advances or revises the tail of the same canonical series timeline.

Rules:

- a newer candle appends,
- a same-identity candle replaces,
- an older or overlapping payload does not silently create a second timeline,
- and continuity failure triggers explicit stale/resync behavior.

### Example

Given canonical candle times:

- bootstrap window: `[10:00, 10:01, 10:02]`
- overlapping history page: `[09:58, 09:59, 10:00]`

The merged canonical history becomes:

- `[09:58, 09:59, 10:00, 10:01, 10:02]`

If live delivery then carries:

- replacement candle at `10:02`
- later candle at `10:03`

The canonical series becomes:

- `[09:58, 09:59, 10:00, 10:01, 10:02(revised), 10:03]`

At no point may `10:00` or `10:02` exist twice.

## Overlay Semantics

Indicators and runtime components emit overlay-related domain outputs/events.
BotLens owns the projection semantics that turn those immutable overlay events into renderable current state.

BotLens overlay semantics therefore include:

- explicit overlay identity,
- immutable overlay event records,
- revision semantics,
- merge semantics,
- supersession semantics,
- retirement semantics,
- and projection of renderable current overlay state.

Overlay lifecycle semantics belong to the BotLens projection layer, not to indicator-specific chart logic.

The chart/render layer consumes current projected overlay state.
It does not invent overlay lifecycle rules.

## Frontend And Backend Alignment

The backend and frontend must agree on the same BotLens contract.

That contract is:

- backend owns canonical BotLens semantics and delivery contracts,
- backend transport surfaces preserve those semantics across bootstrap/history/live access paths,
- frontend consumes and applies the canonical BotLens contract,
- frontend validates continuity and identity at the state boundary,
- chart/render components consume already-canonical projected state.

The frontend must not invent a competing truth about run, series, candle, overlay, or continuity semantics.

## Normalization Ownership

Normalization belongs as early as possible in the BotLens ingestion and projection path.

That means:

- runtime and backend services emit canonical BotLens payload semantics,
- bootstrap/history/live access paths do not invent different identity rules,
- and state-application boundaries preserve the canonical contracts.

The frontend still enforces those contracts at the state boundary to protect continuity and fail loudly on violations.

The chart layer is not a normalization layer.
If cleanup first happens inside a chart primitive, contract enforcement happened too late.

## State Application Boundary

BotLens state application must preserve one authoritative projected state per selected run/series.

That state is the basis for:

- bootstrap baseline,
- history expansion,
- live application,
- replay continuity,
- and render consumption.

Queued transport state, rendered state, and inspection controls may exist as operational layers, but they must all derive from the same canonical projection rules.

## Failure Semantics

BotLens must fail loudly when inspection invariants are broken.

Examples:

- duplicate canonical candle identities,
- descending candle order,
- run or series identity mismatch,
- overlay projection contradictions,
- continuity gaps,
- or snapshot materializations that cannot preserve authoritative event semantics.

Required response:

- log with run and series context,
- surface stale/resync state explicitly,
- and refuse silent corruption of the inspection model.

Forbidden response:

- silent chart-layer cleanup,
- arbitrary data discard without explanation,
- or transport-specific repair that leaves the inspection contract inconsistent.

## Required Structural Invariants

BotLens must satisfy these structural invariants:

- one authoritative semantic source: runtime events
- one derived BotLens read model over that source
- one canonical identity contract for run, series, candle, and overlay state
- one continuity model across bootstrap, replay, paging, and live delivery
- one projection model for candles, overlays, trades, warnings, logs, decisions, and runtime status
- and one render contract that consumes canonical projected state

The UI may still be sophisticated.
The semantics underneath it must remain boring.

## What This Implies For File Boundaries

BotLens code should be partitioned by responsibility, not convenience.

Good boundaries:

- runtime event ingestion and materialization,
- transport adapters and websocket wiring,
- canonical BotLens projection/state application,
- candle identity and merge logic,
- overlay projection logic,
- chart rendering primitives.

Bad boundaries:

- duplicate merge logic across reducers, modal helpers, and chart components,
- transport-specific candle or overlay identity rules,
- hidden lifecycle repair inside visualization components,
- or backend/frontend each maintaining different semantic definitions for the same BotLens entities.

## Non-Negotiable Rule

BotLens must never invent a second truth about a bot run.

If the runtime story is unclear, the answer is to clarify the authoritative event and read-model contract.
It is not to patch over inconsistency in the chart layer.
