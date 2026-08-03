---
component: operator-console-v2-validation
subsystem: frontend
layer: validation
doc_type: validation
status: active
tags:
  - frontend
  - operator-console
  - botlens
  - backtest
  - validation
code_paths:
  - portal/frontend/src/v2
  - portal/frontend/src/features/bots/botlens
  - portal/backend/service/bots/botlens_bootstrap_service.py
  - portal/backend/service/reports
---
# Frontend v2 Operator Validation

## Scope

This validation used a dedicated bot definition and did not modify an existing
strategy or bot. It tested:

- a one-year provider-free backtest;
- active BotLens delivery and rendering;
- completed-run report readiness;
- terminal BotLens reconstruction;
- the honesty of liveness and availability labels.

## Run Evidence

| Fact | Result |
| --- | --- |
| Evaluation window | 2024-01-01 through 2025-01-01 |
| Dataset | `mds_d4ae8f22d4bc493eac88cbe5db80cd01` |
| Hourly bars | 8,804 including 20 warmup bars |
| Runtime duration | about 591 seconds |
| Trades | 508 |
| Net P&L | -2,200 |
| Final state | completed |
| Runtime fact high-water sequence | 12,916 before the terminal lifecycle event |

The first attempted trailing-year dataset was correctly rejected because two
five-hour candle gaps violated dataset admission. The clean 2024 dataset had
continuity 1.0.

## Active BotLens Finding

During the run, the selected chart, decisions, trade markers, and active-trade
state advanced with the simulated clock. Requests stayed run-scoped.

The observed client path was not pristine:

- duplicate/stale diagnostic deltas were dropped;
- one overlay delta was rejected for a base-commit mismatch;
- verbose diagnostic configuration produced hundreds of console messages;
- chart resets occurred as bounded windows advanced.

The sequence and mismatch guards protected the rendered state, but these facts
prevent a claim that active BotLens delivery is noise-free. Frontend v2 disables
verbose BotLens debug logging by default.

## Completed Evidence Finding

The persisted report read model reached `ready_with_caveats`, was safe to
compare, and reported clean dataset quality. It was not a golden candidate:
market-state evidence was unavailable and observability evidence was truncated.

The diagnostic summary contained:

- 623 critical `botlens_ingest_persist_failed` events caused by one runtime
  event identity colliding with divergent material;
- 23 slow database-write diagnostics;
- a pessimistic intrabar fallback because the frozen dataset did not contain
  one-minute candles;
- transport retry/loss diagnostics.

Although the run inventory later marked historical BotLens evidence
rebuildable, a cold BotLens bootstrap did not produce a chart within a bounded
70-second browser observation. This means the current completed-run experience
does not yet satisfy a promise of full decision/overlay replay.

## Honest Product Boundary

The frontend can promise:

- near-real-time updates for one eligible active BotLens projection, with
  sequence and resynchronization contracts;
- explicit on-schedule collector delivery evidence;
- API connectivity and definition-stream connectivity as separate facts;
- completed persisted report and dataset evidence when readiness says so.

The frontend cannot promise:

- near-real-time status for every container;
- collector process liveness;
- complete historical BotLens replay for every completed run;
- that a completed lifecycle alone implies report, replay, or golden readiness.

## Follow-up Required Before Full Completed Replay

1. Fix divergent event material sharing one BotLens event identity.
2. Prove replay equality after the collision fix.
3. Bound and measure terminal `ensure_run_snapshot` reconstruction latency.
4. Reconcile full persisted decisions/trades against reconstructed BotLens
   counts, timestamps, markers, and overlays.
5. Expose authoritative report readiness independently from stale
   `report_materialization` state.
6. Add a paged cold decision ledger to BotLens or explicitly route completed
   forensic review to the persisted report datasets.

Until those pass, the UI labels terminal evidence **Rebuildable**, enforces a
30-second bootstrap timeout, and surfaces the failure instead of claiming a
usable replay.
