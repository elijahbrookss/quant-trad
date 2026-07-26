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
| One-year public dataset | pending | preparation workflow, public provider | pending |
| Three-run deterministic baseline | pending | accepted dataset | pending |
| Accounting/lifecycle reconciliation | pending | accepted runs | pending |
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

## Performance Evidence

No campaign performance claim exists yet. The previous cleanup campaign measured
canonical candle storage independently; those measurements are not a backtest
execution baseline.

Dataset preparation now emits low-overhead wall/CPU timings for requirement
resolution, coverage inspection, provider acquisition, ingestion validation,
dataset hashing/freezing, and dataset admission. Execution baselines remain pending.

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

## Discovered Defects And Disagreements

| Finding | Resolution or status |
| --- | --- |
| A frozen dataset previously left execution instrument metadata mutable | fixed by exact detached instrument snapshots and aggregate hash |
| ATM/risk policy could drift between dataset admission and worker execution | fixed by execution-policy and run-effective execution-config hashes |
| Experiment and MCP starts could bypass dataset preparation/admission | fixed; backtest starts require and propagate `dataset_id` |
| Content reuse was inferred from global commit movement | fixed; repository reports the actual dataset insert-conflict outcome |
| Runtime used an implicit 100-bar warmup floor and inclusive replay end | fixed; declared warmup plus ATR requirement and half-open end are canonical |
| Data-boundary documentation still says backtests do not automatically create reusable manifests | pending documentation correction before final campaign validation |
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
