---
component: plan-backtest-dataset-boundary
subsystem: execution-runtime
layer: plan
doc_type: plan
status: active
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

## Baseline

- Campaign branch: `feat/backtest-dataset-boundary`
- Starting commit: `3fc84b1aaa3121fc8559cfe1db30463b38f0a012`
- Starting source branch: `feat/platform-baseline-cleanup`
- Upstream: `origin/feat/backtest-dataset-boundary`
- PR #186 state at campaign start: open, green, not merged
- Branch policy: retain the reviewed cleanup head as ancestry; after PR #186
  merges, merge updated `origin/develop` into this branch without rebasing before
  preparing the final PR.
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
| Three-run deterministic baseline | in progress; first attempt exposed a fail-loudly runtime persistence defect | accepted dataset | failed run preserved; narrow ownership correction validated |
| Accounting/lifecycle reconciliation | in progress; first completed run exposed a report-summary ownership disagreement | accepted runs | trade-path drawdown ownership corrected; rerun pending |
| Phase-level observability | preparation phases implemented; execution/report phases pending | run/report contracts | preparation payload timings |
| Opt-in profiling | pending | accepted baseline | pending |
| Evidence-backed optimization | pending | three baseline profiles | pending |
| Complete validation and PR | pending | all workstreams | pending |

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

No campaign performance claim exists yet. The previous cleanup campaign measured
canonical candle storage independently; those measurements are not a backtest
execution baseline.

Dataset preparation now emits low-overhead wall/CPU timings for requirement
resolution, coverage inspection, provider acquisition, ingestion validation,
dataset hashing/freezing, and dataset admission. Execution baselines remain pending.

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
| Data-boundary documentation still said backtests did not automatically create reusable manifests | fixed; data and reporting boundaries plus ADR 0051 now describe the required preparation/admission flow |
| Repository default local database credentials are stale for an existing user container | isolated campaign Timescale database used; user volume and secrets untouched |

## Deferred Limitations

- Frontend work.
- Generic market-data collection plane.
- Open interest, basis, funding, L2, order flow, options, and market-state
  expansion.
- Live trading and distributed execution.

## Final Acceptance

Status: **not accepted**. Completion requires every item in the campaign
objective, including one-year repeated semantic proof, reconciliation,
before/after performance evidence, complete validation, synchronized branch,
and one unmerged PR into `develop`.
