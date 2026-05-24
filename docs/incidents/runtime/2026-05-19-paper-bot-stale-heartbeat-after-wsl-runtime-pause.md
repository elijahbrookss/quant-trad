# Paper Bot Stale Heartbeat After WSL Runtime Pause (2026-05-19)

## Incident

- Scope: paper bot runtime, backend runner heartbeat, Docker/WSL host runtime,
  watchdog lifecycle classification, Loki/container diagnostics
- Bot: `7bd70fd4-dd70-421d-8dfe-e0530d42b758` (`bot1`)
- Run: `77522c67-472b-4f49-a4f6-b13c2def3027`
- Strategy: `56e28c19-5d1d-4769-9d7d-919c63004998`
- Run type: `paper`
- User-facing effect: the paper run showed degraded after an overnight run, and
  the backend was later found stopped/restarted during manual recovery

This was an operational runner/host incident, not a strategy, indicator, wallet,
order, fee, slippage, or playback semantic failure.

## Timeline

- `2026-05-18T22:16:37Z`: paper run started.
- `2026-05-19T06:25:02Z`: last recorded backend watchdog heartbeat for the run.
- `2026-05-19T06:25:07Z` to `2026-05-19T07:57:53Z`: backend and bot logs were
  effectively silent; WSL journal entries showed repeated clock-change records
  before the gap and resumed at the end of the gap.
- `2026-05-19T07:57:55Z` (`2026-05-19 02:57:55 Central`): watchdog marked the run
  degraded with `stale_heartbeat:prev=backend.quanttrad`.
- `2026-05-19T13:39:49Z` (`2026-05-19 08:39:49 Central`): backend received
  `SIGTERM` and exited with code `1`.
- `2026-05-19T13:41:55Z`: backend was restarted.
- `2026-05-19T13:42:32Z` and `2026-05-19T13:43:03Z`: watchdog observed stale
  ownership again with missing prior runner context.
- `2026-05-19T13:43:23Z`: run cancellation/stop completed and the bot container
  was removed; the bot container exit was observed as `137`.

## What We Observed

The first degradation happened immediately after a long runtime silence. The
same silence appeared across backend and bot logs, and WSL reported clock-change
activity around the gap. That pattern is consistent with host sleep, WSL VM
pause, Docker Desktop pause, or an equivalent host runtime suspension.

The paper market stream later logged websocket disconnects with:

```text
no close frame received or sent
```

Loki also returned a transient `500` with ring health text:

```text
at least 1 live replicas required
```

Those were useful symptoms but not the primary cause of the first degraded
state. The first degraded state was explained by the backend heartbeat going
stale after the runner stopped producing heartbeats during the runtime gap.

## Root Cause

The backend runner stopped observing time long enough for its bot heartbeat to
become stale. On resume, the watchdog correctly classified the bot as degraded
because the previous runner heartbeat was too old.

The missing capability was not the watchdog action. The missing capability was
durable operational evidence tying the stale heartbeat to a runner/host pause
and nearby container lifecycle facts.

## What Was Not The Root Cause

This incident did not require changing:

- strategy decision logic,
- indicator state semantics,
- wallet/order/trade behavior,
- fee or slippage behavior,
- playback,
- or report certification semantics.

## Corrective Action

The follow-up implementation added:

- runner clock-gap detection via `runner_clock_gap_detected`,
- Docker container lifecycle capture via `docker_lifecycle_event`,
- an observability-profile Docker event sidecar for backend/container exits
  that occur after the backend process is gone,
- persisted `watchdog_diagnostics` on degraded/crashed watchdog lifecycle rows,
- configuration for the sentinel and Docker observer under
  `bot_runtime.watchdog`,
- focused tests for sentinel detection, Docker event normalization, settings,
  and watchdog persistence.

## Architectural Decisions

- [ADR 0021: Use Runner Clock Gap Sentinel](../../architecture/decisions/0021-use-runner-clock-gap-sentinel.md)
- [ADR 0022: Capture Docker Container Lifecycle as Runner-Agnostic Observability](../../architecture/decisions/0022-capture-docker-container-lifecycle-as-runner-agnostic-observability.md)
- [ADR 0023: Persist Watchdog Degradation Diagnostics](../../architecture/decisions/0023-persist-watchdog-degradation-diagnostics.md)

## Permanent Lessons

- Runner liveness needs its own operational evidence.
- Host/VM/Docker pause detection should be tied to `runner_id`, not to one bot.
- Docker lifecycle capture should be Quant-Trad runner-agnostic but explicitly
  Docker-specific.
- Watchdog degradation is much easier to trust when the lifecycle row contains
  stale age, previous runner, current runner, nearby runner clock gap, and
  nearby container lifecycle facts.
