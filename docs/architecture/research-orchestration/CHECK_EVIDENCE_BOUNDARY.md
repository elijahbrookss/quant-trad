---
component: check-evidence-boundary
subsystem: research-orchestration
layer: boundary
doc_type: architecture
status: active
tags:
  - research
  - checks
  - evidence
  - datasets
  - replay
  - observations
  - known-at
  - cli
  - mcp
code_paths:
  - src/research_science/check.py
  - src/market_data/frozen.py
  - portal/backend/service/research/registry.py
  - portal/backend/service/research/planning.py
  - portal/backend/service/research/execution.py
  - portal/backend/service/research/event_fact_evaluator.py
  - portal/backend/service/research/service.py
  - portal/backend/service/research/result_reference.py
  - portal/backend/service/market/frozen_dataset_service.py
  - portal/backend/service/indicators/indicator_service/runtime_validation.py
  - portal/backend/controller/research.py
  - cli/research_operations.py
  - cli/main.py
  - cli/mcp_server.py
---
# Check Evidence Boundary

## Purpose

A Check is QT's bounded analytical operation. It may calculate analytical
occurrences, align typed facts, resolve outcomes, calculate statistics, and
evaluate optional assertions. It does not acquire data, emit an
Indicator-defined event, make a trading decision, or grant promotion or
execution authority.

The canonical flow is:

```text
objective
  -> versioned Check definition and normalized request
  -> direct and transitive requirement plan
  -> optional explicit data preparation
  -> mutable preview OR immutable Dataset freeze
  -> provider-free Indicator graph and Check evaluator
  -> durable result and hashes
  -> optional evidence-backed Observation
  -> report/trail projection
```

Orchestration chooses and sequences these operations. The Check, Indicator,
data, Dataset, outcome, Strategy, and Backtest owners define their semantics.
There is no Campaign resource or second research engine.

## Assurance Levels

| Level | Input | Persistence | Guarantee |
| --- | --- | --- | --- |
| Check preview | Current store pinned to a commit/watermark | Ephemeral only | Causal exploratory feedback with explicit mutable-store provenance. It cannot support an evidence-bearing Observation. |
| Frozen Check evidence | `FrozenMarketDataReadBinding` plus exact Check definition | Durable Check result | Provider-free, definition-pinned, input-pinned, quality-pinned, hash-verifiable, exactly replayable at the producing source revision. |
| Strategy Backtest | Frozen binding plus exact Strategy snapshot and execution configuration | Durable run/report evidence | Preserves the existing strict Strategy-bound execution guarantees. |
| Scientific Protocol | Canonical Check or Backtest reference plus protocol authority | Attempt/certification evidence | Adds search budgets, multiplicity, holdout custody, validation, and certification. A Check verdict alone has no such authority. |

## FrozenMarketDataReadBinding

The reusable frozen read binding is independent of Strategy. It binds:

- Dataset ID, Dataset hash, and commit watermark;
- canonical subject snapshots and hashes;
- resolved series IDs and exact source identity keys;
- half-open time ranges, row counts, and maximum revisions;
- material, provenance, quality, gap, subject, and composite binding hashes;
- recorded immutable gaps and exact quality evidence;
- causal `known_at` reads; and
- `provider_access=disabled`.

The binding reader rejects range expansion, series substitution, source
substitution, subject substitution, hash disagreement, and provider transport.
A structured Fact history used for causal Check sampling is frozen as every
canonical revision, including later corrections and invalidations below the
watermark. Its row count, material hash, provenance hash, source summary, and
raw-archive lineage therefore bind the same revision set that replay consumes.
Older structured Datasets without the `all_canonical_revisions.v1` selection
marker are not silently reinterpreted; causal history asks the operator to
re-freeze them.
A Check adds a definition and optional Indicator graph. A Backtest adds an
exact Strategy snapshot and execution configuration. A Check never needs a
fake Strategy.

## Definitions And Requirements

Every durable Check has a versioned, hashed definition. Material rules include
the evaluator version, input aliases, typed fact requirements, Indicator graph
and parameters, alignment/staleness, gap policy, sample eligibility, outcomes,
horizons, statistics, and assertions. Any material change changes the
definition or plan hash.

Requirement planning runs before execution and exposes:

- explicit typed fact requirements;
- transitive Indicator facts and graph configuration;
- warmup, evaluation, materialization, and outcome-tail ranges;
- alignment and staleness constraints;
- exact or deterministic allowlist source constraints;
- missing coverage; and
- current quality/gap evidence.

Planning never calls a provider. Preparation may call an existing acquisition
service only when the operator explicitly authorizes network access and
budgets. Check execution never acquires.

## Fact Meaning, Source Binding, And Provenance

These identities remain separate:

1. Semantic meaning, such as `market.reference_price.v1`.
2. Canonical subject, such as the ETH-USD instrument ID.
3. Source binding, such as one exact public-contract source identity.
4. Observation provenance, such as contract, round, block, timestamps,
   adapter version, and raw payload hash.

Provider names never appear in semantic fact types. A Check input declares an
alias, fact type/version, subject, alignment, staleness, and source policy.
`exact` is the durable-evidence default. `allowlist` resolves a controlled set
deterministically. Unconstrained provider selection is rejected for evidence.
The same semantic fact may be bound under multiple aliases to compare exact
sources without identity collision.

## Generic Event-And-Fact Checks

`event_fact_analysis` is a registered generic evaluator. Configuration selects
typed fact aliases, registered Indicator signal outputs, causal alignment,
bounded feature operators, outcomes, folds, statistics, and optional scalar
assertions. It contains no provider-specific or Chainlink-specific evaluator.

Definition v4 also supports a Check-owned `fact_snapshot` occurrence for Level
2 research. The initial admitted structured schemas are frozen
`market.bbo.v1` and `market.depth_band.v1`; raw `market.l2_book.v1` is
operational reconstruction evidence and remains Dataset-ineligible. A snapshot
is sampled once per primary-bar close, is neutral rather than long/short, and
uses only schema-declared numeric query fields. Depth-band selection is an
explicit normalized predicate such as `payload.band_bps=5`; an under-specified
same-time selection fails loud instead of choosing by ingestion order.

Bucketed L2 inputs retain `exact_interval` alignment. Market sampling uses the
primary candle close while causal visibility uses the candle's `known_at`.
The matching BBO/depth `bucket_end` must equal that market boundary and its
greatest visible revision must be active. An older one-second bucket is never
carried forward merely because it is within a staleness threshold. Missing,
late, stale, ambiguous, invalidated, or gap-covered frames are counted and
excluded. Outcomes are unsigned forward returns because a Check-owned L2
sample has no trading direction. Selection is keyed by both `known_at` and
`bucket_end`, because several historical candles may legitimately share one
batched availability timestamp without sharing a market interval.

Only an Indicator signal output can supply an Indicator-defined event. The
evaluator verifies output ownership and direction rather than relabeling a
metric row as a signal. A raw Check may define an analytical occurrence, but it
must label it as Check-owned and cannot create a trading action.

Facts are selected only at the event decision `known_at`. Delayed entry does
not admit facts learned after the decision. Events outside the evaluation
range, or not known by its end, are excluded. Forward outcomes preserve, for
every horizon, resolved/unresolved counts, horizon kind, and reason.

## Gap Ownership

Dataset freeze records known reality, including gaps. It does not certify that
every consumer can use the Dataset.

- Data owns facts, source lineage, acquisition coverage, and immutable gaps.
- Dataset owns the pinned snapshot, revisions, ranges, quality, gaps, and
  watermark.
- Indicator owns state reset, re-warm, degraded continuation, readiness, and
  whether an Indicator event exists.
- Check owns analytical sample eligibility, unresolved outcomes, statistics,
  assertions, and verdict.
- Strategy/Backtest owns execution continuity and trading decisions.
- Orchestration reports those decisions without reimplementing them.

New durable evidence must declare `reject`, `reset_rewarm`, or
`continue_degraded`. Historical undeclared behavior remains readable but is not
silently upgraded.

For a Check-owned `fact_snapshot`, `reset_rewarm` is invalid because no
Indicator state exists to reset. `reject` blocks before samples are emitted;
`continue_degraded` retains the frozen gap evidence and excludes affected
samples under a Check action. High-rate fact requirements end at the final
decision boundary, not at the candle-only forward-outcome tail.

## Result, Verdict, And Replay

Evidence persistence pins the normalized request, definition version/hash,
resolved plan, frozen binding, Indicator graph/configuration and outputs, code
revision, gaps, quality, input hashes, semantic result hash, and composite
evidence hash.

Checks may return metrics, distributions, eligibility/sample counts,
resolved/unresolved outcomes, statistical results, assertions, and a verdict:

- no assertions: `verdict=null`;
- all assertions resolved and true: `passed`;
- a resolved assertion false: `failed`;
- any required assertion unresolved: `indeterminate`.

Performance timing is excluded from the semantic result hash. Replay executes
the same canonical path, requires the exact clean producing source revision,
performs no provider call, and compares plan, evidence, and result hashes.

Historical v1 mutable-store Checks remain readable as `legacy_unpinned`.
Older incomplete frozen contracts remain `legacy_frozen_unverifiable` or the
applicable replay-only/diagnostic classification. Original payloads, hashes,
and revisions are never rewritten, and legacy records cannot be represented as
new replayable evidence.

Registered `event_fact_analysis` definition v3 remains bound byte-for-byte to
evaluator v2 and result payload v2 for replay. New requests materialize from
definition v4/evaluator v3; adding L2 sampling does not route old evidence
through the new semantics.

## Shared Operator Surface

The API is the application boundary; CLI and MCP use
`cli.research_operations.ResearchOperations` rather than reconstructing
payload semantics.

| Goal | CLI | API | MCP |
| --- | --- | --- | --- |
| Requirements | `qt research check requirements --request-json ...` | `POST /api/research/checks/requirements` | `get_research_check_requirements` |
| Preview | `qt research check preview --request-json ...` | `POST /api/research/checks/evaluate` | `preview_research_check` |
| Prepare/freeze | `qt research check prepare --request-json ... [--freeze]` | `POST /api/research/checks/prepare` | `prepare_research_check_evidence` |
| Evidence | `qt research check run --request-json ... --dataset-id ...` | `POST /api/research/checks/run` | `run_research_check_evidence` |
| Async evidence | add `--dispatch`; inspect with `qt research jobs status/result` | `POST /api/research/jobs/checks/run`; job reads | dispatch/status/result tools |
| Replay | `qt research check replay <check_id>` | `POST /api/research/checks/{id}/replay` | `replay_research_check` |
| Observation | `qt research observe-from-check <check_id> ...` | `POST /api/research/checks/{id}/observations` | `create_observation_from_check` |
| Trail | `qt research trail <item_id>` | `GET /api/research/items/{id}/trail` | resource/tool trail read |

Low-level acquisition and Dataset freezing remain expert primitives. The
deprecated `qt experiments run-bot` wrapper validates before mutation and emits
a structured replacement message; new workflows should use the normal Bot or
experiment-plan operations.

## Invariants

- Orchestration and adapters contain no research-domain calculations.
- Provider acquisition never occurs inside Check, Indicator, Strategy, or
  Backtest execution.
- Only Indicator decides whether an Indicator event exists.
- Only Check owns Check-specific statistics and assertions.
- Only Strategy owns trading decisions; Backtest requires a Strategy.
- A Check verdict grants no promotion, certification, paper, or live authority.
- Durable Observations reference verified durable evidence.
- Reports and dossiers project canonical records; they are not primary
  calculated evidence.
- Every durable conclusion traces to code revision, operation definition,
  exact inputs/source bindings, gaps/quality, and output/evidence hashes.
