# Bot Runtime Latency Drift (2026-02-28)

## Incident
- Run IDs observed:
  - `bb73b09f-2f15-46f1-9dec-eaa9ab4243a8`
  - `b2e2bf5f-f6c0-4e99-ae6a-a46b938e0246`
- Symptom: runtime latency and cycle latency increased as run progressed.
- User-facing effect: BotLens update cadence degraded over time, with later bars taking significantly longer.

## What We Observed
- `container_snapshot_cycle` p95 rose from low hundreds of ms to ~1s+.
- `step_series_state` and `step_finalize_bar` trended upward with bar index.
- `step_push_update` payload bytes rose continuously over time.
- Snapshot payload size in `portal_bot_run_snapshots` grew from tiny initial rows to multi-MB rows as history grew.

## Root Cause
Latency drift was not one bug. It was compounded repeated work on growing state:

1. Full chart snapshots were generated and pushed frequently from worker callbacks.
2. Container telemetry payloads included full snapshot bodies every cycle.
3. Snapshot persistence path did extra DB reads (`existing` + `max(seq)`) per write.
4. Stream payload build ran even with zero subscribers.
5. Candle visibility path re-sorted an already monotonic candle prefix each step.
6. Delta streaming recalculated log/decision/trade fingerprints from growing objects each push.
7. Payload-size instrumentation used full `json.dumps(...)` on each observed step.

None of the above used hard caps; each became more expensive as run state grew.

## Fixes Applied (No Hard Data Caps)

### Runtime / Engine
- Skip heavy stream payload assembly for `bar`/`intrabar` events when there are no subscribers.
  - File: `src/engines/bot_runtime/runtime/mixins/state_streaming.py`
- Removed redundant per-step candle sorting in visible-candle building.
  - File: `src/engines/bot_runtime/runtime/components/chart_state.py`
- Added mutation-driven revisions for log and decision streams; delta payload now ships these blocks only when revision changes.
  - Files:
    - `src/engines/bot_runtime/runtime/components/event_sink.py`
    - `src/engines/bot_runtime/runtime/mixins/setup_prepare.py`
    - `src/engines/bot_runtime/runtime/mixins/state_streaming.py`
- Added trade-state revision counter in `LadderRiskEngine` and switched stream cache invalidation to that counter.
  - Files:
    - `src/engines/bot_runtime/core/domain/engine.py`
    - `src/engines/bot_runtime/runtime/mixins/state_streaming.py`
- Payload-size telemetry now counts UTF-8 bytes with chunked encoding and configurable sampling cadence (instead of full-string `json.dumps` each step).
  - Files:
    - `src/engines/bot_runtime/runtime/mixins/state_streaming.py`
    - `src/engines/bot_runtime/runtime/mixins/setup_prepare.py`

### Worker / Container
- Worker full-snapshot emission is now cadence-based via `BOT_WORKER_FULL_SNAPSHOT_INTERVAL_MS` (default `1000` ms), instead of full snapshot emission on every state callback.
  - File: `portal/backend/service/bots/container_runtime.py`
- Telemetry payloads are lightweight by default; full snapshot is optional via `BOT_TELEMETRY_INCLUDE_SNAPSHOT=1`.
  - File: `portal/backend/service/bots/container_runtime.py`
- Added runtime env passthrough for worker snapshot cadence:
  - `BOT_WORKER_FULL_SNAPSHOT_INTERVAL_MS`
  - File: `portal/backend/service/bots/runner.py`

### Storage
- Removed per-write snapshot pre-check reads (`existing` + `max(snapshot_seq)`) in snapshot persistence hot path.
  - File: `portal/backend/service/storage/repos/runtime_events.py`

### Telemetry Hub Compatibility
- Ingest now supports lightweight telemetry payloads (without embedded full snapshot) and reconstructs broadcast payload from latest DB snapshot when viewers are present.
  - File: `portal/backend/service/bots/telemetry_stream.py`

### Settings Catalog
- Exposed worker full-snapshot cadence in bot settings catalog defaults/templates.
- Exposed payload-byte sampling cadence knob in bot settings catalog defaults/templates.
  - File: `portal/backend/service/bots/config_service.py`

## Non-Goals / Explicitly Avoided
- No hard truncation/capping of historical run data was introduced as part of this incident fix.
- No silent fallback behavior was added.

## Operational Controls
- `BOT_WORKER_FULL_SNAPSHOT_INTERVAL_MS`:
  - Lower value = fresher full snapshots, higher runtime overhead.
  - Higher value = less drift pressure, coarser full-snapshot frequency.
- `BOT_TELEMETRY_INCLUDE_SNAPSHOT`:
  - `0` (default): lightweight telemetry envelopes.
  - `1`: include full snapshot in websocket telemetry payloads.
- `BOT_RUNTIME_PUSH_PAYLOAD_BYTES_SAMPLE_EVERY` (or `push_payload_bytes_sample_every` runtime config):
  - `1`: measure payload bytes every observed push (highest instrumentation overhead).
  - `N>1`: measure every Nth push to reduce profiler self-overhead while preserving trend visibility.

## Follow-Up Checks
1. Track p95 over time for:
   - `step_series_state`
   - `step_finalize_bar`
   - `step_push_update`
   - `container_snapshot_cycle`
2. Add regression query: correlation of step duration with bar index.
3. Keep an eye on `snapshot_write_ms` and `telemetry_emit_ms` slopes across long runs.
