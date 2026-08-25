---
component: plan-backtest-dataset-boundary
subsystem: execution-runtime
layer: plan
doc_type: plan
status: historical
tags:
  - backtest
  - dataset
  - determinism
  - performance
code_paths:
  - cli/main.py
  - portal/backend/service/bots
  - portal/backend/service/market
  - portal/backend/service/reports
  - portal/backend/service/storage
  - src/engines/bot_runtime
  - src/market_data
---
# Dataset-Bound Backtesting And Performance Campaign

> Historical campaign record. Branch, PR, production-path, and validation
> statements below describe the recorded July 2026 campaign state and do not
> assert current integration or deployment status.

## Baseline

- Campaign branch: `feat/backtest-dataset-boundary`
- Starting commit: `3fc84b1aaa3121fc8559cfe1db30463b38f0a012`
- Starting source branch: `feat/platform-baseline-cleanup`
- Upstream: `origin/feat/backtest-dataset-boundary`
- PR #186 state at campaign start: open, green, not merged; merged into
  `develop` on 2026-07-26 as `9dd4858f54ab56fa3bb711f2b4a997d2b818128a`.
- Branch policy: retain the reviewed cleanup head as ancestry; after PR #186
  merges, merge updated `origin/develop` into this branch without rebasing before
  preparing the final PR.
- Measured execution head: `cf94f840b090b47b7f222ca5700d30eb3602cbfa`,
  which contains reviewed `develop` baseline
  `9dd4858f54ab56fa3bb711f2b4a997d2b818128a` without rewriting campaign
  history.
- Validated implementation head: `88fb256f3eb0f685774c640bb23e73be8d15b377`.
- Integration PR: [#187](https://github.com/elijahbrookss/quant-trad/pull/187),
  `feat/backtest-dataset-boundary` into `develop`; open, ready for review,
  mergeable, and green at acceptance.
- Unrelated worktree state: none; worktree was clean.

## Mission

Require every canonical backtest to execute exclusively against one immutable,
validated dataset identity; prove one-year deterministic and reconciled
execution; add low-overhead phase evidence; and improve the dominant measured
Python-owned bottleneck without changing semantic results.

## Architectural Decisions

| Decision | Status | Evidence |
| --- | --- | --- |
| Reuse canonical market dataset, provenance, continuity, quality, and runtime snapshot contracts | accepted | ADRs 0044, 0046, 0050 |
| Dataset preparation and backtest execution are separate operator phases | accepted | campaign mission |
| Backtest execution may not call a provider or read mutable latest candle state | accepted | campaign invariant |
| Paper/live behavior and generic collection remain out of scope | accepted | campaign exclusions |
| Market dataset identity contains source facts, provenance, ranges, and quality only; request and actor metadata remain operational | accepted | `market_dataset.v1`, repository identity tests |
| One separate `backtest_dataset_binding.v1` admits exact dataset, strategy, indicator, execution-policy, instrument, and run-effective config identity | accepted | `src/market_data/backtest.py` |
| Exact execution instrument snapshots are frozen and mutable instrument-table reads are forbidden for bound backtests | accepted | runtime dependency binding and substitution tests |
| Warmup derives from the transitive indicator graph plus runtime ATR requirements; no historical 100-bar floor remains | accepted | plan and series-builder regression tests |
| All dataset and replay windows are half-open | accepted | plan, repository, and series-builder tests |
| Historical acquisition is explicit and defaults to disabled in experiment orchestration | accepted | CLI and experiment tests |
| Content reuse is reported from the actual insert conflict outcome rather than inferred from commit-watermark movement | accepted | PostgreSQL identity test |

## Workstreams

| Workstream | Status | Dependencies | Evidence |
| --- | --- | --- | --- |
| Mandatory dataset contract | implemented; pre-commit validation green | canonical dataset/store contracts | focused contract/runtime/database tests |
| Preparation and execution CLI separation | implemented; pre-commit validation green | mandatory contract | CLI, experiment, API, and MCP tests |
| Provider/latest-state isolation proof | implemented; pre-commit validation green | execution binding | bound-read, range-expansion, substitution, and post-freeze correction tests |
| One-year public dataset | complete and admitted | preparation workflow, public provider | frozen dataset identity and independent integrity audit recorded |
| Three-run deterministic baseline | complete | accepted dataset | three one-year runs share semantic fingerprint `864f268c...`; canonical comparisons report semantic match with operational drift only |
| Accounting/lifecycle reconciliation | complete for accepted baseline | accepted runs | gapless event ledger, 508 closed trades, wallet replay, P&L/equity and report totals reconcile within the recorded representation tolerance |
| Phase-level observability | implemented; validation green | run/report contracts | preparation timings, persisted runtime step rollups, report materialization duration, corrected weighted averages and explicit histogram method |
| Opt-in profiling | complete | accepted baseline | one-year run `5af4731c-f8b8-4e8a-98a5-2f6eb5fdba3a`; bounded cProfile/pstats/tracemalloc artifact exposed in canonical report |
| Evidence-backed optimization | complete; target exceeded with semantic parity | three baseline and three post-change samples plus completed profile | `93a6701` selects raw events by marker before serializing only new decision facts; `step_push_update` median fell 81.980% and runtime-loop median fell 43.324% |
| Complete validation and PR | complete | all workstreams | complete non-DB and PostgreSQL profiles, clean and repeated bootstrap, documentation contracts, source-isolation audits, synchronized branch, and green PR #187 |

## Dataset Contract

The canonical source dataset remains `market_dataset.v1`. Its semantic identity
contains exact series/range material hashes, provenance hashes, quality hashes,
source summaries, quality summaries, row counts, and per-series frozen commit
watermarks. Dataset name, purpose, actor, request IDs, creation time, and other
operational metadata do not participate in semantic identity.

`backtest_dataset_binding.v1` is a run-admission contract rather than a second
source-dataset identity. It binds the source dataset to:

- exact compiled strategy, effective strategy configuration, transitive indicator
  graph, ATM/risk execution policy, instrument snapshots, and run-effective config;
- requested evaluation, required warmup, complete materialization, actual loaded,
  and decision-producing ranges;
- exact candle fact contract, timeframe, material/provenance/quality hashes,
  row counts, disclosed gaps, and frozen commit watermarks;
- provider-free validation status and admitted quality status.

Execution rejects missing/unknown IDs, unsupported contracts, instrument or
strategy substitution, range expansion, insufficient warmup, malformed or
unordered candles, undisclosed gaps, hash/provenance/quality disagreement, and
post-freeze revisions. Warmup initializes state only; the decision range is the
half-open evaluation range.

## One-Year Acceptance Contract

Recorded before public provider acquisition on revision `da8a44f`:

- Bot: `e487b31e-750b-4dbc-af5e-310188d271bb`
  (`baseline-atr-expansion-btcusd`).
- Strategy: `a6e95615-9a5b-42eb-ad5e-310188d271bb`
  (`Baseline ATR Expansion BTC/USD`), variant
  `ab70d825-7751-4126-9a1f-6af6a2ee5480` (`default`).
- Compiled strategy hash:
  `f9d2ebaed75dc76ece4be14c19ee6e865b6ca408a74c317e8911816dbfafc827`.
- Effective strategy configuration hash:
  `7618d7c35d4e9309a56339a4b3abf4293f828e0edc799b2c3d36b05835716345`.
- Execution-policy hash:
  `7bfb83ed0549847f1bc02fed967279730ad45c93be7ae15ec3ec335aca880eb4`.
- Run-effective execution configuration hash:
  `d7fb05b46cd99f9444fa7fea00754a213933d0447c601a6171cc3f865e88bd6c`.
- Instrument: `f2eea7b3-a5b1-43f2-927a-eb8723efc21a`, `BTC/USD`,
  `CCXT` / `COINBASE`, spot; immutable execution snapshot hash
  `90f315ae3a2b3532addafaeee3972a954812b877ddff0da07132dfafe4a1f9e3`.
- Required fact: `candle.ohlcv` / `candle.ohlcv.v1`, one-hour
  (`3600` seconds).
- Evaluation and decision range:
  `[2024-01-01T00:00:00Z, 2025-01-01T00:00:00Z)`.
- Warmup: 20 one-hour bars, derived from the attached
  `candle_stats` indicator; range
  `[2023-12-31T04:00:00Z, 2024-01-01T00:00:00Z)`. Runtime ATR needs
  14 bars and is covered by the larger declared requirement.
- Complete materialization range:
  `[2023-12-31T04:00:00Z, 2025-01-01T00:00:00Z)`.
- Execution: deterministic backtest simulator, full execution mode, spot
  semantics, initial wallet `USD 100000`.
- Fees: no configured fee model and no instrument maker/taker rates; expected
  zero-fee simulation must be reported as a realism caveat.
- Slippage: no configured model; expected zero-slippage simulation must be
  reported as a realism caveat.
- Exit policy: explicit ATR(14) 1x initial stop and one full-size 1R target;
  breakeven, trailing, fixed-horizon, and stop-adjustment rules are disabled.
- Known limitations: public provider candle availability and continuity remain
  to be proven; no interpolation or forward-fill is permitted; this acceptance
  run proves deterministic platform semantics, not live execution quality or
  strategy profitability.

## Performance Evidence

The three-sample pre-optimization baseline is established below. No optimization
claim exists yet. Measurements from the previous cleanup campaign remain separate
and are not used as this backtest execution baseline.

Dataset preparation emits low-overhead wall/CPU timings for requirement
resolution, coverage inspection, provider acquisition, ingestion validation,
dataset hashing/freezing, and dataset admission. The accepted execution baseline
below uses persisted runtime step rollups and separately timed report builds.

The accepted dataset is
`mds_3e5c6926722d852bd43a3fc79a859c40`, with semantic hash
`3e5c6926722d852bd43a3fc79a859c403e7e1206513127c1ef10b3227567d1d9`
and frozen commit watermark `10155`. It contains 8,804 ordered one-hour candles:
20 warmup bars and exactly 8,784 decision-window bars. Its material,
provenance, and quality hashes are respectively
`b7dd56c3628413cef2b85d027b06c2a81f72c1dfcc620789f9449e28196f22b8`,
`4a1b108b8ce825cf6d636c39b451f952dff7b546b6ee542110f2841952adcb00`,
and
`6927b8cacc119a3e3b312f51839c37e300882d5c622cc39c0c5cd0fcafe5f0bb`.
Independent SQL validation found zero duplicate opens, non-hourly transitions,
pre-close known-at values, malformed OHLC rows, negative volumes, or
post-initial revisions. Repeated preparation reused the same semantic identity,
performed no provider call, and completed in 6.04 seconds wall time.

Three accepted pre-optimization runs executed revision `93f41b1` against that
exact dataset and configuration:

| Run | Runtime loop | Total runtime summary | Report build | Result |
| --- | ---: | ---: | ---: | --- |
| `8d6faa48-19c1-4a6a-86a0-c2783bd68a11` | 841.525s | 842.761s | 38.139s | accepted |
| `d1fd2f75-5098-4c96-b985-5a69e6ab9616` | 850.158s | 851.431s | 39.385s | accepted |
| `9042e4c7-a2ee-47e8-89d6-b4e3e5fa3892` | 832.090s | 833.168s | 39.685s | accepted |
| **Median** | **841.525s** | **842.761s** | **39.385s** | baseline |

Every run produced 599 signals and decisions, 508 accepted entries, 91 explicit
decision rejections, 508 exits, 508 closed trades, gross/net P&L `-2200`, fees
`0`, ending report equity `97800`, maximum drawdown `2500`, and exact semantic
fingerprint
`864f268ccc0d364718ee73dc965e94e03338a71ac4667809729f8d5eac16eb44`.
The canonical report comparator found no first semantic divergence and classified
each pair as `semantic_match_operational_drift`; data snapshot, strategy,
configuration, decisions, trades, P&L, drawdown, and wallet projection all match.

The median nested runtime timing evidence is: series state 842.071s, finalize
332.547s, push update 289.124s, decision flow 224.787s, settlement 160.645s,
execution prime 37.627s, trade-event processing 37.198s, signal evaluation
33.322s, and state update 20.430s. These timings overlap by design and must not
be added together.

The opt-in full-year profile ran revision `0caf796` as
`5af4731c-f8b8-4e8a-98a5-2f6eb5fdba3a` against the exact accepted dataset and
configuration. It completed in 4,620.413s wall and 4,439.989s CPU, reported
268,506,113 bytes peak traced memory, and processed 8,804 materialized rows.
Profiling overhead is intentionally excluded from the unprofiled timing baseline.
The container sampler observed a separate transient peak near 840 MiB while
pstats finalized the 2.198-billion-call profile; this post-execution peak is a
profiler cost, not an unprofiled runtime claim.

The function evidence identifies repeated historical decision serialization as
the dominant controllable Python path:

- `RuntimePushStreamMixin._decision_facts` ran once per bar and accumulated
  1,561.761 profiled seconds.
- `RuntimeProjectionMixin.decision_events` accumulated 1,133.820 seconds.
- `EventEnvelope.serialize` was called 1,719,442 times and accumulated
  2,294.743 seconds.
- recursive `serialize_value` executed 99,556,074 calls and consumed 873.493
  seconds of self time.

These cumulative values overlap and are distorted upward by cProfile plus
tracemalloc, so they are ranking evidence rather than additive wall time. The
root behavior is direct: every bar serializes the bounded historical decision
window before the existing event marker discards already-emitted entries.

**Optimization target recorded before code change:** make decision-fact emission
select immutable raw events by `event_id` under the existing lock, preserve the
current 200-entry visibility window and configured batch truncation semantics,
and serialize only the selected new events. Public decision snapshots, canonical
fact shapes, event ordering, marker fallback, truncation warnings, persistence,
and report behavior must remain unchanged. The measured target phase is
`step_push_update`, whose pre-optimization median is 289.124s; success requires
at least a 20% median reduction in that phase across three comparable unprofiled
post-change runs, or a precise constraint-backed explanation.

The profiled run produced the same 3,253 contiguous canonical events and the
canonical comparator reported `semantic_match_operational_drift` against
baseline run `9042e4c7-a2ee-47e8-89d6-b4e3e5fa3892`. Dataset snapshot,
strategy/config hashes, 508 trades, `-2200` net P&L, zero fees, 2,500 maximum
drawdown, semantic fingerprint, and final wallet value `97800.00006863201` all
match exactly.

Revision `93a6701` implements the recorded target without changing the public
`decision_events()` snapshot. The production push path copies at most the same
200 immutable raw events under the existing runtime lock, applies the existing
event-ID marker and batch limit, and serializes only the selected new suffix.
Missing-marker fallback, newest-entry truncation, warnings, ordering, and fact
payload shape remain unchanged. Focused tests count serialization calls and
prove that repeated pushes serialize nothing, a newly appended event is
serialized once, and an evicted marker retains the prior bounded fallback.

Three comparable post-optimization runs executed measured head `cf94f84`
against the exact same dataset, strategy, configuration, execution mode, fee
assumptions, and slippage assumptions:

| Sample | Run | Persisted wall | Runtime loop | `step_push_update` | Report build | Result |
| ---: | --- | ---: | ---: | ---: | ---: | --- |
| 1 | `72d65acf-2b00-4b22-b210-40dc5b282bad` | 498.997s | 475.473s | 52.100s | 28.034s | accepted |
| 2 | `68c8432b-1c30-4358-bc98-1f317bb0af2e` | 505.037s | 477.477s | 51.969s | 28.937s | accepted |
| 3 | `abb5d2f3-81bb-4fed-a6e2-5dc2c83a5f11` | 505.354s | 476.940s | 52.209s | 29.254s | accepted |
| **Median** |  | **505.037s** | **476.940s** | **52.100s** | **28.937s** | accepted |

The targeted phase median fell from 289.124s to 52.100s, an **81.980%**
reduction. Median runtime loop fell from 841.525s to 476.940s, a **43.324%**
reduction. The required work was not removed: each run still processed 8,804
loaded bars and 8,784 decision bars, persisted 3,253 canonical events, produced
599 decisions, 508 accepted entries, 91 explicit rejections, 508 exits, and 508
closed trades, and materialized the canonical report. Persisted wall time
includes startup and finalization; runtime-loop timing is the comparable
execution-only measure.

Every post-change report comparison against accepted pre-change run
`9042e4c7-a2ee-47e8-89d6-b4e3e5fa3892` returned
`semantic_match_operational_drift` with no first divergence. Dataset, candle
snapshot, strategy, and material configuration identities match; all behavior,
trade, fee, P&L, drawdown, and report deltas are zero; the semantic fingerprint
remains
`864f268ccc0d364718ee73dc965e94e03338a71ac4667809729f8d5eac16eb44`;
and wallet replay remains exactly `97800.00006863201` with complete trace and no
open position. Only run IDs, request IDs, wall timestamps, operational event
identity, durations, and other explicitly operational evidence differ.

After optimization, the largest separately useful nested phases are decision
flow (174.136s median) and settlement/accounting application (121.667s median).
Finalize-bar is 83.828s and push update is 52.100s. These phases overlap and are
not additive. Report materialization remains a separate 28.937s median. Further
optimization is intentionally deferred until a new profile establishes the
next dominant controllable path; this campaign does not infer a target from
nested timing alone.

Each accepted run has 3,253 canonical events with contiguous, unique run sequence
`1..3253`, runtime-assigned ordering, no missing known-at value, and no event with
`known_at < bar_time`. Baseline 3 independently persisted 508 closed trades with
gross/net P&L `-2200` and fees `0`. Wallet replay ends at
`97800.00006863201`, while rounded trade/report accounting ends at `97800`; the
`0.00006863201` difference is accepted only as floating quantity/representation
tolerance and is identical across all three runs. No open position remains.

## Validation Ledger

| Date | Revision | Command or evidence | Result | Duration | Notes |
| --- | --- | --- | --- | --- | --- |
| 2026-07-26 | `3fc84b1` | PR #186 metadata and checks | open; all checks green; not merged | n/a | reviewed cleanup head is campaign base |
| 2026-07-26 | `3fc84b1` | branch/worktree inspection | clean; upstream configured | n/a | no unrelated changes |
| 2026-07-26 | worktree | isolated Timescale/PostgreSQL clean and repeated bootstrap | passed twice | recorded during campaign | isolated port 15433; existing user containers untouched |
| 2026-07-26 | worktree | affected contract/runtime/CLI/startup regression set (13 files) | 159 passed; 3 deprecation warnings | 13.98s | no failures; rerun before commit |
| 2026-07-26 | worktree | complete affected experiment/API/MCP suites | 44 passed | 6.92s | preparation and start remain separate |
| 2026-07-26 | worktree | PostgreSQL content-identity and post-freeze correction tests | 2 passed; 4 pre-existing warnings | 2.12s | isolated Timescale database; rerun before commit |
| 2026-07-26 | worktree | changed production module compile audit | passed | <1s | Python 3.12 bytecode compilation |
| 2026-07-26 | worktree | architecture index regeneration and documentation contract | 2 passed | 0.02s tests | generated index unchanged |
| 2026-07-26 | worktree | production provider-reference audit under backtest execution paths | no acquisition call path found | n/a | provider use remains in explicit preparation and paper intake |
| 2026-07-26 | worktree | `git diff --check` | passed | <1s | no whitespace errors |
| 2026-07-26 | worktree | CCXT pagination and historical-ingestion regressions | 8 passed | 2.33s | real segmented acquisition exposed an inclusive provider end; adapter now returns canonical half-open windows |
| 2026-07-26 | `b3ecadf` | public Coinbase BTC/USD 1h acquisition, dataset admission, and independent SQL integrity audit | 8,804 rows accepted; 8,784 evaluation rows; no gaps or malformed material | provider acquisition and freeze completed before client timeout | backend completion was verified before retry; retry reused identity without acquisition |
| 2026-07-26 | `b3ecadf` | first one-year execution attempt, run `f933be2c-18d4-479b-a6d6-47526c52fcde` | rejected as `degraded_terminal` | stopped after first trading event | producer canonical-fact persistence incorrectly required a transport bridge session; incomplete result excluded from baseline |
| 2026-07-26 | worktree | canonical-fact, domain-event, artifact, appender, runtime-push, and container-transport regressions | 171 passed; 14 pre-existing deprecation warnings in first group | 4.67s combined | producer persistence no longer fabricates transport identity; bridge ingress still rejects a missing session |
| 2026-07-26 | worktree | changed runtime modules compile audit and `git diff --check` | passed | <1s | narrow correction ready for commit |
| 2026-07-26 | worktree | drawdown, artifact-binding, report-identity, trust, and research-dataset regressions | 84 passed | 6.29s | first completed year run disagreement converted into protected canonical ownership rules |
| 2026-07-27 | `93f41b1` | three one-year accepted backtests and materialized report comparisons | all completed; semantic fingerprints identical | runtime loops 841.525s, 850.158s, 832.090s | operational fingerprints differ as expected; golden remains blocked only by out-of-scope market state |
| 2026-07-27 | `93f41b1` | baseline 3 canonical SQL reconciliation | 3,253 gapless events; 508 closed trades; no known-at ordering defects; P&L/fees/report/wallet reconciled | n/a | no external orders; zero fee and absent slippage remain realism caveats |
| 2026-07-27 | worktree | profiler, CLI, experiment, dataset, report, container-transport, runtime-control, startup, and projection regressions | 184 passed across focused groups | 15.60s combined | includes fixture-only mandatory-dataset correction, semantic-hash exclusion for profiling, and profiler-failure isolation |
| 2026-07-27 | worktree | changed production module compile audit and `git diff --check` | passed | <1s | no syntax or whitespace defects |
| 2026-07-27 | worktree | architecture index regeneration and documentation contract | 2 passed | 0.03s | default pytest capture hit the known local temporary-file defect; `-s` validation passed |
| 2026-07-27 | `0caf796` | opt-in one-year cProfile/pstats/tracemalloc run `5af4731c-f8b8-4e8a-98a5-2f6eb5fdba3a` | completed; 3,253 events; profile artifact ready | 4,620.413s profiled wall; report 28.650s | 8,804 rows; 4,439.989s CPU; 268,506,113-byte traced peak; no external orders |
| 2026-07-27 | `0caf796` | current-builder report rebuild and canonical comparison against baseline 3 | `semantic_match_operational_drift`; no first divergence | 28.202s baseline rebuild; comparison <1s | semantic fingerprint, snapshot/config hashes, trades, P&L, fees, drawdown, and wallet all match |
| 2026-07-27 | `3124c15` | persisted causal prefix test reproduced in isolated pre-optimization worktree | failed with excluded minute-3 boundary bar | 12.02s | proves disagreement predates optimization; fixture encoded four half-open bars with `end=start+3m` |
| 2026-07-27 | `28878ce` | persisted runtime repeatability, prefix invariance, and adapter parity | 1 passed; 14 dependency deprecation warnings | 11.85s | fixture now expresses N one-minute bars as `[start, start+N minutes)`; production semantics unchanged |
| 2026-07-27 | `93a6701` | affected runtime, persistence, projection, transport, control, and canonical-event regression set | 237 passed; 14 dependency deprecation warnings | 13.83s | includes marker-before-serialization call-count proof and persisted known-at prefix invariance |
| 2026-07-27 | `93a6701` | changed production module compile audit and `git diff --check` | passed | <1s | no syntax or whitespace defects |
| 2026-07-27 | `cf94f84` | three post-optimization one-year backtests | all completed with 508 trades, `-2200` net P&L, zero fees, and 2,500 maximum drawdown | runtime loops 475.473s, 477.477s, 476.940s | exact accepted dataset and configuration; no external orders |
| 2026-07-27 | `cf94f84` | three canonical report builds and comparisons against accepted pre-change baseline 3 | all `semantic_match_operational_drift`; no first divergence | report builds 28.034s, 28.937s, 29.254s | semantic fingerprint and complete wallet projection match; all material deltas zero |
| 2026-07-27 | `cf94f84` | persisted pre/post runtime rollup median comparison | target exceeded: push update `289.124s -> 52.100s` (`81.980%`); loop `841.525s -> 476.940s` (`43.324%`) | six comparable unprofiled runs | medians computed from three samples per cohort in isolated acceptance database |
| 2026-07-27 | `9891b9e` + bootstrap worktree | complete non-PostgreSQL pytest profile with capture disabled | 1,420 passed; 49 warnings | 35.47s | warnings are dependency deprecations plus the known stale local-credential import warning; no failures |
| 2026-07-27 | `9891b9e` + bootstrap worktree | complete PostgreSQL-marked pytest profile | 14 passed; 1,420 deselected; 19 warnings | 9.72s | isolated validation database; acceptance dataset and existing user containers untouched |
| 2026-07-27 | bootstrap worktree | fresh Timescale/PostgreSQL extension and schema bootstrap | passed; canonical market and observability schemas created | 15.49s | optional `system_stats` is unavailable and remains an explicit non-fatal warning |
| 2026-07-27 | bootstrap worktree | repeated bootstrap against the same clean database | passed without schema mutation | 0.85s | proves idempotent startup |
| 2026-07-27 | bootstrap worktree | optional-extension savepoint regression against real PostgreSQL | passed | <1s | an intentionally missing extension no longer poisons readiness queries in the surrounding startup transaction |
| 2026-07-27 | `cf94f84` | post-optimization golden repeatability check, samples 1 and 3 | no decision, trade, lifecycle, order, wallet, ordering, or summary divergence | n/a | golden promotion remains correctly blocked by unavailable market-state evidence and truncated observability-event retention |
| 2026-07-27 | `cf94f84` | independent post-optimization lifecycle and accounting reconciliation | exact reconciliation | n/a | 508 accepted decisions, entries, opens, exits, and closes; 91 reasoned rejections; ending equity `97800`; equation delta zero |
| 2026-07-27 | `9891b9e` + bootstrap worktree | Python compile, architecture-index regeneration, documentation contracts, and source-isolation audits | passed; generated architecture index unchanged | <1s compile plus focused checks | no backtest execution provider caller, mutable-latest fallback, or `AGENTS.md` modification found |
| 2026-07-27 | `88fb256` | PR #187 checks and mergeability | mergeable; PR suite, changelog, and security checks passed | PR suite 1m26s | open, ready-for-review PR from campaign branch into `develop`; not merged |

## Discovered Defects And Disagreements

| Finding | Resolution or status |
| --- | --- |
| A frozen dataset previously left execution instrument metadata mutable | fixed by exact detached instrument snapshots and aggregate hash |
| ATM/risk policy could drift between dataset admission and worker execution | fixed by execution-policy and run-effective execution-config hashes |
| Experiment and MCP starts could bypass dataset preparation/admission | fixed; backtest starts require and propagate `dataset_id` |
| Content reuse was inferred from global commit movement | fixed; repository reports the actual dataset insert-conflict outcome |
| Runtime used an implicit 100-bar warmup floor and inclusive replay end | fixed; declared warmup plus ATR requirement and half-open end are canonical |
| Segmented CCXT acquisition returned the shared boundary candle from both adjacent requests | fixed at the provider adapter: canonical fetch results now enforce `start <= timestamp < end`; duplicate source rows within a segment still fail loudly |
| Producer-owned canonical-fact persistence reused the transport projection constructor and therefore demanded a nonexistent bridge session | fixed by separating producer persistence batch construction from transport ingress construction; transport still enforces bridge identity and the failed acceptance run remains preserved as disagreement evidence |
| Runtime and canonical report calculated 2,500 maximum drawdown, but artifact finalization replaced the run summary with a 2,400 daily-close drawdown | fixed; maximum drawdown now preserves every ordered closed-trade equity transition, while daily aggregation remains limited to daily analytics |
| Artifact finalization dropped the admitted dataset binding and report instrument identity mixed a strategy-link row ID with the canonical instrument ID | fixed; artifact snapshots retain the binding, reports expose dataset ID/hash, semantic fingerprints include them, and instrument identity uses the canonical ID |
| Fee and slippage limitations were present in detailed sections but absent from top-level trust caveats | fixed; missing fee role/rate and slippage evidence now degrades the relevant report sections and remains visible in readiness/trust caveats |
| Trades explicitly used the unconfigured `default_zero` fee source, but readiness did not name that assumption | fixed; `unconfigured_zero_fee_model` is now a top-level trust caveat |
| Terminal run projections could display stale in-memory `starting` status after persisted completion | fixed; persisted terminal truth now wins over stale telemetry snapshots |
| Runtime step `avg_ms` averaged per-bucket p95 values instead of dividing total duration by sample count | fixed; averages are weighted and exact, merged histogram p95 values state their upper-bound method, and run-level rollup aggregation is caveated |
| Runtime telemetry WebSocket transport repeatedly disconnected during all accepted runs | deferred operational durability defect; canonical producer persistence remained gapless and semantic results were observer-invariant, but terminal auto-materialization required an explicit build request |
| Push-stream decision fact emission serialized the same bounded historical decision window on every bar before applying its marker | fixed in `93a6701`; raw marker selection now precedes serialization and protected payload/order/fallback semantics are unchanged; three-run proof measured an 81.980% targeted-phase median reduction with exact semantic parity |
| Persisted prefix-invariance fixture described N one-minute bars with `end=start+(N-1)m` after half-open ranges became canonical | fixed in fixture-only `28878ce`; the isolated committed pre-optimization revision reproduced the premature terminal close, and the corrected test now includes the intended final decision bar |
| Data-boundary documentation still said backtests did not automatically create reusable manifests | fixed; data and reporting boundaries plus ADR 0051 now describe the required preparation/admission flow |
| An unavailable optional PostgreSQL extension aborted the surrounding startup transaction, causing later readiness queries to fail with `InFailedSqlTransaction` | fixed with one savepoint per optional extension creation; a real PostgreSQL regression proves that a failed optional create is isolated and subsequent readiness SQL succeeds |
| Repository default local database credentials are stale for an existing user container | isolated campaign Timescale database used; user volume and secrets untouched |

## Deferred Limitations

- Frontend work.
- Generic market-data collection plane.
- Open interest, basis, funding, L2, order flow, options, and market-state
  expansion.
- Golden promotion remains blocked by unavailable market-state evidence and the
  deliberately bounded observability-event projection; neither gate was
  weakened to make this campaign appear complete.
- Fee and slippage models are unconfigured for the acceptance strategy, so the
  deterministic result proves platform semantics rather than venue execution
  realism.
- Live trading and distributed execution.

## Final Acceptance

Status: **accepted** on 2026-07-27. The campaign completed mandatory frozen
dataset admission, provider-free execution, one-year repeated semantic proof,
lifecycle and accounting reconciliation, before/after performance evidence,
complete local and PostgreSQL validation, clean and repeated bootstrap,
synchronized branch publication, and one green unmerged PR into `develop`.

Acceptance preserves the deferred limitations above. In particular, it does
not promote the run as golden while market-state and complete observability
evidence are unavailable, and it does not claim fee, slippage, collector,
non-candle fact, frontend, or live-trading capability.
