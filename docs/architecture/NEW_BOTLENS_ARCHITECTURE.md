# New BotLens Architecture (Canonical)

BotLens is a two-input viewer scoped by `(run_id, series_key)`.

## Inputs

1. **REST history/window API**
   - `GET /api/bots/{bot_id}/active-run`
   - `GET /api/bots/runs/{run_id}/series`
   - `GET /api/bots/runs/{run_id}/series/{series_key}/window?to=now&limit=N`
   - `GET /api/bots/runs/{run_id}/series/{series_key}/history?before_ts=<iso>&limit=N`

2. **WS live-tail API**
   - `WS /api/bots/ws/runs/{run_id}/series/{series_key}/live?after_seq=<seq>`
   - Emits delta-only messages (`bar_append`, `bar_update`, `status`).

## Rules

- WS is live tail only; it never backfills history.
- History replace/window behavior comes from REST.
- On seq gaps/out-of-order, client triggers re-bootstrap via REST and re-opens live WS.
- On unmount/hidden, UI closes WS and clears pending queue to prevent backlog replay.
- Telemetry publish remains non-blocking to runtime loop; no viewer should affect trading timing.
