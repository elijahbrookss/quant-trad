---
component: botlens-message-flow-troubleshooting
subsystem: botlens-projections
layer: troubleshooting
doc_type: architecture
status: active
tags:
  - botlens
  - projections
  - troubleshooting
  - performance
  - observability
code_paths:
  - src/engines/bot_runtime/core/domain/engine.py
  - src/engines/bot_runtime/runtime/mixins/runtime_events.py
  - src/engines/bot_runtime/runtime/mixins/runtime_push_stream.py
  - src/engines/bot_runtime/runtime/components/canonical_facts.py
  - portal/backend/service/bots/container_runtime_telemetry.py
  - portal/backend/service/bots/botlens_intake_router.py
  - portal/backend/service/bots/botlens_mailbox.py
  - portal/backend/service/bots/botlens_symbol_projector.py
  - portal/backend/service/bots/botlens_run_projector.py
  - portal/backend/service/bots/botlens_projector_registry.py
  - portal/backend/service/bots/botlens_transport.py
  - portal/frontend/src/components/bots/botlensProjection.js
  - portal/frontend/src/components/bots/BotLensChart.jsx
  - portal/frontend/src/components/bots/chartArtifactRefreshPolicy.js
  - portal/frontend/src/features/bots/botlens/hooks/useBotLensController.js
---
# BotLens Message Flow Troubleshooting

## Purpose

This document is the standard investigation path for BotLens runtime-to-frontend
performance issues. It maps the flow by architectural hop, not by function name,
so it should stay useful while implementation details evolve.

Use it when a run is slow, BotLens appears behind, warnings look duplicated, or
a queue reports pressure. The goal is to answer where the message enters, where
it waits, what drains it, why the drain is slow, and which change reduces work
without changing runtime truth.

## Flow

| Hop | Owner | Boundary | Drain | Primary Questions |
| --- | --- | --- | --- | --- |
| Runtime fact construction | execution runtime | bar-step snapshot boundary | synchronous bar step | Is fact construction expensive, or is time spent after the fact exists? |
| Runtime projection handoff | runtime projection dispatcher | bounded producer-side projection queue | dispatcher thread | Is BotLens fanout pressure affecting execution timing, or degrading as projection-only work? |
| Backend ingest | telemetry websocket/intake | websocket receive and ingest routing | backend event loop | Are payload bytes, JSON decode, or routing time growing? |
| Symbol intake | per-symbol mailbox | bounded symbol fact queue | symbol projector | Is queue depth rising faster than the projector drains? |
| Symbol projection | symbol projector | symbol read-model state | bounded batch drain | Is apply time high, or are too many drain turns being scheduled? |
| Run aggregation | run projector | run notification queue | run projector | Are symbol summaries, open trades, or run health updates creating fanout pressure? |
| Fanout | projector registry and run stream | fanout queue and stream ring | viewer transport loop | Are viewer payloads being built/sent, or dropped because no viewer needs them? |
| Frontend projection | BotLens frontend store | websocket message application | browser event loop | Is the client applying stale deltas, missing a base snapshot, or rendering too much per message? |

## Investigation Rules

- Start at the first backed-up queue, not the loudest downstream symptom.
- Separate queue wait from apply time. A deep queue with low apply time means
  the drain is not getting enough turns or emits too much downstream work per
  turn. A shallow queue with high apply time means the projector work itself is
  expensive.
- Treat BotLens projection as degradable. Projection pressure can drop or
  coalesce visual/debug transport and require resync, but it must not corrupt
  canonical runtime truth.
- Treat canonical persistence as non-degradable. Durable fact overflow, writer
  failure, and terminal persistence drain timeout are runtime failures.
- Keep frontend delivery separate from backend projection. Fanout pressure is
  downstream of symbol projection; it should not explain symbol fact queue depth
  unless projector work is waiting on fanout.

## Runtime-To-Backend Checks

Use runtime timing to classify the producer side:

- `build_state_ms` and fact construction timings answer whether runtime work is
  expensive before projection handoff.
- Runtime projection queue depth, lag, overflow, and degraded counts answer
  whether BotLens fanout is keeping up with already committed live facts.
- Canonical persistence queue metrics answer durable DB pressure. These belong
  to persistence health, not BotLens live projection health.

If runtime fact construction is cheap but projection handoff is pressured, fix
the projection handoff or downstream drain. Do not gate or slow the execution
loop to protect a debug surface.

Within runtime fact construction, look for work that scales with retained
history instead of the current mutation. Trade revision checks should inspect
only trades touched by the current bar. Log and wallet facts should advance by
revision or append cursor. Repeated warning occurrences may update diagnostic
aggregation without rebuilding an unchanged compact warning projection. A
per-bar full scan of any of these retained collections is a performance defect.
For recurring fallback conditions, verify the compact UI warning identity is
series/reason scoped while the durable runtime event remains bar scoped. A bar
timestamp in the compact warning ID turns every occurrence into a material
health mutation and defeats the revision gate.

## Backend Queue Checks

For symbol projection pressure, compare these signals in the same time bucket:

- symbol fact enqueued count,
- symbol fact drain envelope/event count,
- symbol fact queue wait and oldest age,
- symbol projector apply time,
- run notification queue wait,
- fanout queue wait and dropped stale count.

Interpretation:

- High enqueue, low drain count, low apply time: increase drain efficiency or
  reduce downstream emits per drain turn.
- High apply time: reduce projector work, payload size, or per-event mutation
  cost.
- High run-notification wait after symbol drains: run aggregation is the next
  bottleneck.
- High fanout wait with stable symbol/run queues: viewer transport is the next
  bottleneck.

## Warning Summary Checks

Run health warnings are run-level state. The run health payload carries one
warning summary with per-symbol buckets and grouped warning conditions. The
frontend may show symbol badges from those buckets, but it must not copy the
entire run warning list into every symbol runtime panel.

If warning counts look duplicated:

- inspect the run health warning summary first,
- confirm per-symbol buckets match the active warnings,
- confirm frontend symbol runtime state is using only the matching symbol bucket,
- only then inspect warning identity generation inside runtime.

## Browser Rendering Checks

If orders or active-trade rays update while candles appear frozen, the transport
may still be healthy. Check the browser rendering path before attributing the
symptom to missing runtime bars:

- confirm candle count and latest candle time advance in frontend state,
- confirm the chart series receives the accepted candle independently of trade
  and overlay artifact reconstruction,
- confirm follow-latest remains enabled only while the operator is at the live
  edge,
- check whether trade/overlay arrays are being recreated on a candle-only store
  update,
- inspect browser memory and long tasks while multiple run lenses are open.

The normal chart contract applies candle appends immediately. Historical trade
and overlay artifacts may refresh on their bounded render cadence unless their
own material input changed. Do not solve renderer pressure by dropping candle
facts or making execution viewer-aware.

At DEBUG level, protocol libraries must not print every WebSocket frame and
canonical fact workers must sample successful batch/dispatch diagnostics.
Errors, overflow, drain timeouts, and lifecycle boundaries remain unsampled.
Use metrics for exact queue and throughput counts rather than reconstructing
them from one stdout line per message.

## Telemetry Storage Checks

The step rollup table stores timing and payload metrics for execution analysis.
It must not store repeated queue-depth, queue-lag, or worker-count gauges on
every bar step. Queue pressure belongs in queue observability and low-rate
health, not repeated per-step rollups.

When storage grows quickly:

- count rows by table and metric name,
- separate durable runtime events from observability rollups,
- remove low-value repeated gauges before adding new indexes,
- add indexes only for confirmed query patterns.

## Reporting Pattern

A BotLens performance investigation should report:

- selected run or aggregate window,
- first queue or boundary where wait grows,
- drain rate versus enqueue rate,
- apply time versus queue wait,
- largest payload category if payload size is implicated,
- code or contract change that reduces work, coalesces visual state, or makes
  projection degrade without touching canonical runtime truth.
