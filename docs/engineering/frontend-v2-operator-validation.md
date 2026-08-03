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

- 1,742 `botlens_ingest_persist_failed` events, all caused by one
  `SERIES_METADATA_REPORTED` event identity being reused with divergent timing
  material; canonical decisions and trades remained durable, but exact
  live-to-ledger reconciliation was not certifiable;
- 23 slow database-write diagnostics;
- a pessimistic intrabar fallback because the frozen dataset did not contain
  one-minute candles;
- transport retry/loss diagnostics.

Although the run inventory later marked historical BotLens evidence
rebuildable, a cold BotLens bootstrap did not produce a chart within a bounded
70-second browser observation. This means the current completed-run experience
does not yet satisfy a promise of full decision/overlay replay.

## Post-remediation Validation (2026-08-03)

The campaign kept the original run as a hard historical fixture and repaired
the read boundaries rather than hiding its earlier diagnostics. Current local
validation found:

| Check | Result |
| --- | --- |
| Terminal inventory eligibility | Durable BotLens-ledger evidence is reported without reconstruction |
| Exact terminal bootstrap | 5.565 seconds cold; 0.098 seconds warm; bootstrap_ready |
| Initial completed chart | 240 frozen hourly bars in 0.979 seconds |
| Chart provenance | Frozen dataset ID, hash, series ID, and max commit sequence returned |
| Decision replay | Two consecutive 200-event pages in 1.67 seconds |
| Replay cursor | Stable ascending after_seq/after_row_id continuation |
| Event coverage | Signals, decisions, entry/exit fills, and trade lifecycle facts |

The SERIES_METADATA_REPORTED identity now includes observation known-at time, so
retries of one observation remain idempotent while later observations cannot
reuse the same identity with different material. The historical fixture still
contains its already-persisted collision diagnostics; this change does not
rewrite old evidence or retroactively make that run a golden candidate.

The completed chart now reads only the run's frozen dataset commit boundary.
The decision surface merges bounded snapshot rows with cursor-paged durable
domain truth and owns its own loading/error/end-of-stream state. Full historical
overlay equivalence is still not claimed.

## Honest Product Boundary

The frontend can promise:

- near-real-time updates for one eligible active BotLens projection, with
  sequence and resynchronization contracts;
- explicit on-schedule collector delivery evidence;
- API connectivity and definition-stream connectivity as separate facts;
- completed persisted report and dataset evidence when readiness says so;
- frozen-dataset chart paging and typed decision/trade replay when a terminal
  run reports durable BotLens evidence.

The frontend cannot promise:

- near-real-time status for every container;
- collector process liveness;
- complete historical overlay replay or usable replay for every completed run;
- that a completed lifecycle alone implies report, replay, or golden readiness.

## Remaining Proof And Observation Gates

Completed in this campaign:

1. Distinct series-metadata observations no longer share event identity.
2. Terminal reconstruction is measured and bounded by the 30-second UI timeout.
3. Durable decision/trade evidence is available through the typed cursor path.
4. Dataset-bound chart reads are frozen and provenance-labeled.
5. Replay eligibility comes from durable evidence without inventory-time replay.

Still required before a golden/full-replay claim:

1. Run a fresh post-fix backtest and prove duplicate-delivery, replay, and
   persisted report agreement without historical collision contamination.
2. Reconcile complete decision/trade counts, timestamps, markers, and overlays
   for that fresh run.
3. Prove full historical overlay equivalence or keep overlays explicitly bounded.
4. Complete the deferred 24-hour observational gate after Phase 4. The campaign
   retains the one-hour soak as its implementation-time stability gate.

The UI labels only runs with hot or durable BotLens evidence as selectable,
enforces a 30-second bootstrap timeout, and surfaces component-local failure
instead of claiming universal replay.
