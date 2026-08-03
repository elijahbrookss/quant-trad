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
  - portal/backend/service/bots/botlens_transport.py
  - portal/backend/service/bots/botlens_domain_events.py
  - portal/backend/controller/market_data.py
  - portal/backend/workers/market_data_collector.py
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
| Terminal run/catalog bootstrap | 2.487 seconds cold over HTTP; 8,727 bytes; `bootstrap_ready` |
| Selected-symbol snapshot | 4.972 seconds cold in parallel; 319,742 bytes |
| Snapshot evidence tails | 32 signals, 32 decisions, 64 trade states, one of 32 logs; truncation explicit |
| Initial completed chart | 240 frozen hourly bars; 2.157 seconds while cold symbol replay ran |
| Chart provenance | Frozen dataset ID, hash, series ID, and max commit sequence returned |
| Decision replay | First 200-event page in 2.171 seconds while cold symbol replay ran; stable second-page proof retained |
| Replay cursor | Stable ascending after_seq/after_row_id continuation |
| Event coverage | Signals, decisions, entry/exit fills, and trade lifecycle facts |

The SERIES_METADATA_REPORTED identity now includes observation known-at time, so
retries of one observation remain idempotent while later observations cannot
reuse the same identity with different material. The historical fixture still
contains its already-persisted collision diagnostics; this change does not
rewrite old evidence or retroactively make that run a golden candidate.

The completed chart now reads only the run's frozen dataset commit boundary.
Terminal run bootstrap returns run/catalog scope without embedding selected-symbol
history. Frozen chart and durable forensic reads begin from that scope in parallel
with the cold symbol projector. Selected-symbol transport reports explicit
latest-tail included/available counts; the decision surface merges those rows
with cursor-paged durable domain truth and owns its own loading, error, and
end-of-stream state. At that validation point, full historical overlay
equivalence was not yet claimed.

## Final Exact Acceptance And Replay Contract (2026-08-03)

A third exact provider-free year run exercised the performance and transport
repairs without changing the frozen dataset or strategy semantics.

| Check | Result |
| --- | --- |
| Run | `40551139-b904-408b-ae0b-772b999dd4da` |
| Wall/runtime time | 478.403 / 444.652 seconds |
| Performance | 19.8% faster than the 596.541-second post-identity baseline |
| Semantic result | 599 decisions; 508 accepted; 91 rejected; 508 trades; net P&L -2,200 |
| Semantic fingerprint | `9aac4007e701a2c36a6e010b1c30805a5c15347e2998171c876d4d7ace64583d` |
| Transport | Zero connection-loss or send-failure events; 23 client pongs for 23 server pings |
| Observability | Complete 42-of-42 event coverage; not truncated |
| Trade marker reconciliation | 508 of 508 entry/exit coordinates matched the durable ledger across six chart pages |
| Report materialization | 837.8 ms from the warmed immutable dataset; subsequent ready read 0.15 seconds |

The normalization hash warning was also traced to six unreferenced stale pytest
rows with obsolete 40-character fixture identities. Executable-catalog reads
now quarantine only that narrow legacy residue. Referenced rows, current
identity formats, and every real hash disagreement still fail loud. The live
operator snapshot reports nine executable specifications and no normalization
component error; no persisted row was rewritten or deleted.

Historical indicator geometry now has a deterministic bounded replay contract
for runs created after overlay retention is deployed. The runtime emits scoped
overlay clocks and a forced terminal checkpoint. The backend retains the compact
timeline, causally replays only events before a chart page end, rejects clock
gaps, and suppresses completeness for cadence/window gaps, missing terminal
evidence, or payload truncation. Terminal timelines are queried once through an
eight-entry LRU and causally sliced per page. The frontend keeps at most 64
loaded pages and distinguishes **ledger verified**, **bounded replay**, and
**not retained**.

The broad storage-shape gate rejected an attempted 640-point overlay payload at
56,374 bytes. The implementation therefore keeps the established 160-point
durable evidence budget and reports truncation explicitly; it does not purchase
visual completeness by allowing unbounded runtime events.

This is page-by-page data-geometry equivalence, not a claim that UI pixels are
research truth. Old runs without retained deltas remain unavailable. A fresh
post-deployment run must still prove transport-to-ledger overlay persistence in
the live stack, and browser screenshot/pixel reconciliation remains a visual QA
gate. Autonomous research must consume typed outputs, durable facts, frozen
datasets, and materialized reports—not chart pixels.

## Fresh Post-fix Run Proof (2026-08-03)

A second provider-free run used the same frozen one-year dataset after the
series-metadata identity repair. It did not rewrite or reuse the historical run.

| Check | Result |
| --- | --- |
| Run | `adb5eb06-bcb1-42f2-b061-6a249ec58dfd` |
| Wall time | 596.5 seconds; completed |
| Outcome | 508 trades; net P&L -2,200, matching the earlier fixture |
| Durable ledger | 8,388 rows with contiguous `run_seq` 1 through 8,388 |
| Decision evidence | 599 signals and 599 decisions |
| Trade evidence | 508 entries, exits, opens, and closes |
| Series observations | 5,135 distinct `SERIES_METADATA_REPORTED` events |
| Prior collision signature | Zero `botlens_ingest_persist_failed` diagnostics |
| Active exact bootstrap | 0.952 seconds initially and 0.206 seconds later; selected tails remained bounded |
| Terminal run/catalog bootstrap | 0.078 seconds; 73,321 bytes including the bounded 120-warning health summary |
| Terminal selected-symbol snapshot | 0.033 seconds; explicit 32 signal, 32 decision, 64 trade, and 32 log tails |
| Frozen chart | 0.140 seconds; 240 bars from the recorded dataset commit |
| First forensic page | 0.393 seconds; 200 documents and a stable continuation cursor |
| Report readiness | Dataset and results ready; safe to compare with caveats; clean data quality; degraded execution/operational evidence |

The report retains 310 slow database-write diagnostics, 14 telemetry transport
reconnect/failure diagnostics, the 120-bar pessimistic intrabar fallback
summary, and truncated operational observability. Those caveats do not alter the
reconciled canonical decision/trade ledger, but they correctly degrade
operational and execution confidence and block golden promotion.

The active proof also exposed a separate readiness contradiction: canonical
`phase=live` and `status=running` evidence was being projected as
`run_live=false` because the domain-event builder treated an omitted redundant
boolean as false. The builder now uses the existing canonical lifecycle
normalizer, and the lifecycle/bootstrap contract suite covers the repair.
Persisted evidence from this already-completed run was not mutated.

## One-hour Implementation Gate

The implementation-time soak ran continuously for one hour while the collector,
operator projections, a frontend rolling restart, and the fresh one-year
backtest were active.

| Check | Result |
| --- | --- |
| Duration | 3,600 seconds |
| Probe cycles | 79 |
| Failures | Zero |
| Collector success-version advances | 79 |
| Maximum API health latency | 0.062 seconds |
| Maximum frontend-proxy health latency | 0.089 seconds |
| Maximum operator bootstrap latency | 3.132 seconds |
| Maximum chart latency | 0.636 seconds |
| Maximum collector projection latency | 0.162 seconds |
| Maximum forensic-page latency | 0.240 seconds |
| Maximum market projection latency | 0.081 seconds |
| Maximum run-inventory latency | 0.097 seconds |

Each cycle checked direct and proxied API health, collector heartbeat liveness
and successful progress, the market snapshot, run inventory, and a bounded
historical bootstrap. Frozen chart reads ran each cycle; a forensic page ran
every fifth cycle. The only market component failure was the already-known
normalization-spec hash mismatch, which remained isolated as typed component
evidence rather than poisoning valid definitions. Per the campaign decision,
the 24-hour observational gate remains deferred until after Phase 4.

## Deployed Live-readiness Proof

After deploying the lifecycle-normalization repair, dedicated run
`d99c599c-52b0-4d74-82de-dcffb4ce4713` returned `bootstrap_ready` with
`run_live=true`, `phase=live`, `status=running`, and live transport eligibility
while active. The exact bootstrap completed in 0.147 seconds and returned
207,696 bytes. The run was then explicitly stopped and is now canceled. Its
immutable event ledger contains `RUN_READY` with `live=true` at sequence 22 and
`RUN_CANCELLED` with `live=false` at sequence 1,091. The terminal bootstrap
returned HTTP 200 in 0.045 seconds, reported `run_live=false`, and disabled live
transport eligibility. This proves the deployed projection preserves canonical
live readiness and clears it on terminal evidence without leaving an active bot.

## Honest Product Boundary

The frontend can promise:

- near-real-time updates for one eligible active BotLens projection, with
  sequence and resynchronization contracts;
- explicit scheduled-collector heartbeat liveness and on-schedule delivery evidence;
- API connectivity and definition-stream connectivity as separate facts;
- completed persisted report and dataset evidence when readiness says so;
- frozen-dataset chart paging and typed decision/trade replay when a terminal
  run reports durable BotLens evidence;
- conditionally complete bounded overlay pages for post-retention runs only when
  the returned overlay evidence is ledger verified.

The frontend cannot promise:

- near-real-time status for every container;
- liveness for every platform container or any process that does not publish a typed heartbeat;
- complete historical overlay replay or usable replay for every completed run;
- pixel-level equivalence between browser output and canonical research truth;
- that a completed lifecycle alone implies report, replay, or golden readiness.

## Remaining Proof And Observation Gates

Completed in this campaign:

1. Distinct series-metadata observations no longer share event identity.
2. Terminal reconstruction is measured and bounded by the 30-second UI timeout.
3. Durable decision/trade evidence is available through the typed cursor path.
4. Dataset-bound chart reads are frozen and provenance-labeled.
5. Replay eligibility comes from durable evidence without inventory-time replay.
6. A fresh post-fix one-year run produced contiguous durable event order, exact
   decision/trade counts, and zero prior collision diagnostics.
7. Canonical live lifecycle evidence now drives `run_live` readiness.
8. The fresh persisted report is dataset- and results-ready, safe to compare with
   caveats, and reports clean data quality. It remains correctly blocked from
   golden status by unavailable market state and truncated observability.
9. Trade entry/exit marker coordinates reconcile 508 of 508 across the full
   loaded year, and bounded overlay replay fails closed on gaps or truncation.
10. Legacy normalization residue is narrowly quarantined while real or
    referenced integrity failures remain blocking.

Still required before a golden/full-replay claim:

1. Run a fresh post-deployment overlay-producing fixture and prove persisted
   page coverage through the live transport/DB path.
2. Complete browser-level visual QA if a pixel-equivalence claim is desired;
   pixels remain non-authoritative for autonomous research.
3. Complete the deferred 24-hour observational gate after Phase 4. The campaign
   retains the one-hour soak as its implementation-time stability gate.

Browser-driven visual QA could not run in this environment because the desktop
browser bridge rejected the WSL workspace URI (`sandboxCwd is not a local file
URI`). Automated contract tests, production compilation, HTTP checks, and live
API measurements passed; this record does not claim a completed pixel-level or
interactive browser review.

The UI labels only runs with hot or durable BotLens evidence as selectable,
enforces a 30-second bootstrap timeout, and surfaces component-local failure
instead of claiming universal replay.
