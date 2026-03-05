# BotLens Telemetry Flow (Plain-English Walkthrough)

## Why this exists

This document explains, in simple terms, what telemetry is, what happens between candles, and how BotLens gets live updates without replacing DB history.

If you are deciding what to change in snapshots/streaming, this is the mental model to use.

## One-line definition

Telemetry is the bot runtime's live "now state" feed to BotLens.

- DB = historical record (what happened over the whole run)
- Telemetry = current changing state (what is happening right now)

Both are needed, but they have different jobs.

## What telemetry is actually sending

Each live update contains a compact view of the current run state, mainly:

- recent candle window per series (not full history)
- recent overlays for chart display
- active/recent trades window
- recent logs/decisions/warnings window
- runtime status summary

Important:
- it is not meant to carry full run history
- it is not the source of truth for historical replay

## High-level run walkthrough

### 1) Run starts

- Bot runtime starts workers for symbol/timeframe processing.
- Backend marks run as running.
- BotLens can load the latest known state from DB immediately.

### 2) Engine processes bars internally

For each new bar, engine logic runs:

- update indicators/state
- evaluate strategy rules
- update positions/trades
- update runtime stats/status

This is execution logic, not frontend logic.

### 3) Worker emits live view updates

When meaningful state changes (new bar, trade change, terminal status), workers emit a compact view update to the container process.

This avoids emitting on every tiny internal change.

### 4) Container builds one outbound telemetry payload

Container loop merges worker updates into one bot-level view and sends it to backend ingest.

At this point:
- engine work is already done
- telemetry is packaging + transport

### 5) Backend ingest handles the update (two lanes)

Backend receives the telemetry payload and splits work:

- Live lane (fast): in-memory ring + websocket broadcast
- Durable lane (async): `view_state` checkpoint upsert in DB

So viewers are not blocked by checkpoint DB writes.

### 6) Frontend rendering

BotLens renders:

- historical context from DB-backed APIs
- live "tail" from telemetry stream

This gives you continuity: deep history from DB + up-to-date live movement from stream.

### 7) Run ends

- final state is emitted
- run status flips to stopped/completed/failed
- BotLens can still read full history from DB afterwards

## What happens "between candles"

Between two candles, the system is typically doing:

- engine state updates and rule checks
- local merge/prepare of view payload
- network send to backend
- backend checkpoint upsert + live fan-out
- frontend apply/render

If delay grows over time, the problem is usually in the live transport/ingest/render side, not bar math itself.

## Overlay runtime mode (new)

Regime overlays now have two runtime modes:

- default (`runtime_regime_overlay_rebuild=false`): build once at prepare-time, reuse during bar loop.
- optional (`runtime_regime_overlay_rebuild=true`): rebuild from visible candles each bar.

Default mode removes growing-window overlay rebuild cost from the hot bar path while preserving frontend visibility rules.

## What `telemetry_emit_ms` means

`telemetry_emit_ms` is the time spent trying to hand one telemetry payload from container runtime to backend ingest.

In plain terms:
- "How long did it take to push this live update out?"

It includes send wait/backpressure time.
It does not mean indicator math itself got slower.

If your dashboard shows rising "Selected Series Bar Latency", see:
- `docs/architecture/STEP_SERIES_STATE_METRICS_SIMPLE.md`
- that metric comes from `step_series_state` (bar execution path), not container telemetry emit.

## New telemetry health metrics

- `payload_bytes`: how large each emitted update is
- `ingest_queue_depth`: backend ingest backlog
- `persist_queue_depth`: DB checkpoint backlog
- `persist_seq_lag`: how far durable checkpoint is behind live seq
- `persist_lag_ms`: time for a queued checkpoint to become durable

## What `view_state` checkpoints are

A `view_state` checkpoint is the latest durable live state for a run in DB.

It is used for:

- fast BotLens bootstrap (open screen, get latest immediately)
- reconnect recovery when stream continuity is lost
- operational read models that need current run state

It is not a full event history table.

## Ownership boundaries (what to change safely)

Use this rule when refactoring:

- Engine path owns execution correctness:
  - initialize -> apply_bar -> snapshot
- BotLens path owns viewing:
  - latest checkpoint + live tail stream
- DB owns historical replay

So:
- do not make engine depend on frontend needs
- do not make frontend depend on full-history snapshots
- keep stream payload focused on recent/live window only

## Why bars can look "batchy" in UI

Even with streaming, updates can appear in small bursts when:

- worker emits only on meaningful changes
- container loop cadence groups close updates
- stream carries overlay deltas and frontend applies them to the current snapshot
- frontend render buffer smooths playback

That is a rendering/stream cadence behavior, not necessarily missing bar processing.

## Practical decision guide

If you ask "should this be in telemetry or DB?", use:

- Needed for immediate live screen reaction: telemetry
- Needed for full historical analysis/replay: DB
- Needed by both: DB for history + telemetry for latest window

This keeps performance predictable and architecture clean.
