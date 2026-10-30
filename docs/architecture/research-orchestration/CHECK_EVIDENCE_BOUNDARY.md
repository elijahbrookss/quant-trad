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
  - src/research_science/recognition.py
  - src/research_science/reversal_recognition.py
  - src/research_science/reversal_outcomes.py
  - src/research_science/event_barriers.py
  - portal/backend/service/research/event_barrier_check.py
  - src/research_science/reversal_matched_progress.py
  - portal/backend/service/research/saved_selection_binding.py
  - portal/backend/service/research/saved_selection_measurements_evaluator.py
  - src/research_science/reversal_matched_outcomes.py
  - src/research_science/completed_event_context.py
  - src/research_science/reversal_completed_recovery.py
  - portal/backend/service/research/reversal_completed_recovery_evaluator.py
  - src/research_science/reversal_population.py
  - src/research_science/price_paths.py
  - src/research_science/weighted_summaries.py
  - portal/backend/service/research/reversal_population_evaluator.py
  - src/research_science/reversal_selection.py
  - src/research_science/reversal_selection_timing.py
  - scripts/reporting/reversal_recognition_preview.py
  - src/market_data/frozen.py
  - portal/backend/service/research/registry.py
  - portal/backend/service/research/catalog.py
  - portal/backend/service/research/planning.py
  - portal/backend/service/research/execution.py
  - portal/backend/service/research/candle_distribution_evaluator.py
  - portal/backend/service/research/volume_cohort_evaluator.py
  - portal/backend/service/research/liquidity_episode_evaluator.py
  - portal/backend/service/research/raw_candle_evaluator.py
  - portal/backend/service/research/controlled_reversal_evaluator.py
  - portal/backend/service/research/event_fact_evaluator.py
  - portal/backend/service/research/event_features.py
  - portal/backend/service/research/event_configuration.py
  - portal/backend/service/research/event_time.py
  - portal/backend/service/research/retrospective_numeric.py
  - portal/backend/service/research/event_facts.py
  - portal/backend/service/research/event_preparation.py
  - portal/backend/service/research/event_observations.py
  - portal/backend/service/research/event_outcomes.py
  - portal/backend/service/research/event_statistics.py
  - portal/backend/service/research/directional_model.py
  - portal/backend/service/research/candle_state_comparison_evaluator.py
  - portal/backend/service/research/candle_state_comparison_v2_evaluator.py
  - portal/backend/service/research/candle_state_comparison_v3_evaluator.py
  - portal/backend/service/research/matched_origin_evaluator.py
  - portal/backend/service/research/shared_landmark_evaluator.py
  - portal/backend/service/research/first_return_evaluator.py
  - portal/backend/service/research/forward_risk_evaluator.py
  - portal/backend/service/research/crossing_state_comparison.py
  - portal/backend/service/research/service.py
  - portal/backend/service/research/preparation_acquisition.py
  - portal/backend/service/market/feed_service.py
  - portal/backend/service/research/result_reference.py
  - portal/backend/service/market/frozen_dataset_service.py
  - portal/backend/service/indicators/indicator_service/runtime_validation.py
  - portal/backend/controller/research.py
  - cli/research_operations.py
  - cli/main.py
  - cli/mcp_server.py
---
# Check Evidence Boundary

## Current execution and historical custody

[ADR 0082](../decisions/0082-separate-historical-custody-from-active-research.md)
defines the current Indicator/Check/Study boundary and current-only execution
catalog. Earlier version-specific descriptions below are historical method
meaning, not promises that retired factories remain executable. Original
records remain integrity-checked and readable through data-only custody.


## Bounded input delivery

Indicator Checks consume candle windows through the existing candle service,
with at most 2,048 nominal bars per window. Sparse ranges stay sparse. A single
canonical engine spans all pages; rolling features are not independently computed
per page. Raw-frame evaluators retain their existing materializing contract.
Canonical SQL hydration uses a 512-row server cursor; causal revision choice
still happens before invalidation filtering and may retain revisions within the
requested candle window. Supported hot/cold and retained-history readers remain
the only storage boundary.

Semantic hashing emits the same canonical JSON incrementally instead of copying
the complete normalized evidence and encoded history. These changes reduce
avoidable copies; evaluator arrays and retained outputs still require explicit
total limits. They do not introduce a computation cache or claim a year-scale
speedup without a representative measurement.

Research compatibility during storage migration is scoped in
[ADR 0078](../decisions/0078-evolve-research-within-existing-data-boundaries.md)
and [Storage Management](../persistence/STORAGE_MANAGEMENT.md#storage-capacity-and-workload-admission).
They add no new Check capability or scientific authority. Only actual schema,
read or resource dependencies should couple research to a storage transition;
passing storage gates does not resume paused research.

## Discovering implemented methods

The code-owned registry exposes its registered CheckDefinitions through read-only
`GET /api/research/checks/definitions` and
`GET /api/research/checks/definitions/{definition_id}/{definition_version}`.
`qt research check definitions` and `qt research check definition ID --version V`
use the same ResearchOperations adapter. No latest version is selected implicitly.

Catalogue metadata describes purpose, eligibility, inputs, fixed/configurable
settings, result shape and binding templates. It is separate from the scientific
material rules and leaves definition hashes unchanged. Examples are checked by
the existing request normalizer and requirement declaration; actual input
resolution and freezing remain in requirements/prepare. Discovery performs no
acquisition, evidence execution, publication or permission change. Historical
base version 3 remains visible for replay but has no currently selectable request
example. Preview-only families remain preview-only.

The generic boundary is CheckDefinition/CheckRequest/ResolvedCheckPlan and
CheckEvaluator. An evaluator is an ordinary Python implementation, not another
managed entity. Definition 11/evaluator 10 remains a fixed volatility-specific
method. Shared functions are appropriate when calculations actually recur;
discovery adds no DSL, schema, new engine or configurable scientific policy.

## Shared calculations and procedure ownership

The event-and-fact evaluator orchestrates named preparation, causal Fact,
observation/entry, outcome and statistical functions. Settings normalization lives
in `event_configuration.py`; the registry keeps literal historical version
precedence in `_resolve_definition_version`. Feature calculation and paired model
fitting/scoring use `event_features.py` and `directional_model.py`.

Matched-origin, shared-landmark and first-return keep their distinct orchestration.
A call-scoped `EventCandlePreparation` shares candle conversion/indexes and ATR on
the same unchanged input object. Readonly row/index views prevent consumer writes;
they are shallow, and the owned frozen inputs must remain unchanged during the
call. Causal Fact/event selection and outcomes are recalculated for each detector
or shifted endpoint. No preparation survives a public evaluation or is shared
with another job. Forward-risk retains its separate calculation.

This extraction changes no accepted settings, scientific choices, definition IDs,
result schemas or evaluator versions. Producing source identity changes with code;
old source-bound evidence retains its exact-source custody rather than being
relabeled. Verification compares scientific material on identical input fixtures;
wall-clock performance metadata and producing-source identity are separate from
that comparison. Synthetic coverage does not establish saved real-Check parity.

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

## Explicit preparation acquisition

`POST /api/research/checks/prepare`, `qt research check prepare`, and MCP
`prepare_research_check_evidence` share one preparation operation. With no write
flags it only inspects source-bound requirements and coverage. `freeze=true`
freezes available canonical facts and recorded gaps. Explicit
`acquire_missing=true` first acquires unrecorded native candle ranges using
`HistoricalCandleIngestor`, re-plans against a fresh watermark, and optionally
freezes when `freeze=true`. Acquisition without freeze returns inspection
results for a later freeze. No variant executes a Check or creates a finding.

The native preparation route admits only the instrument's declared PUBLIC
historical-candle feature and a provider-owned native interval declaration;
Coinbase currently supplies that declaration through its existing granularity
map. Unsupported providers/timeframes, credential/external integrations,
non-candle Facts, source/series mismatches and ambiguous requirements are
blockers, not permission to derive or substitute inputs. Existing numeric
acquisition contracts, useful lower-level data operations and Strategy/Bot
preparation retain their independent authorities and callers. No fake Bot or
Strategy is constructed for research preparation.

All missing aliases are checked before the first ingestion. Native interval
declarations are read from provider classes before constructing any transport
adapter; unsupported providers cannot issue constructor-time metadata calls. Compatible
alias ranges are merged only over their requested union. Existing shared
research admission, deadline and cumulative input-row budgets apply before
writes; acquisition charges nominal native slots as well as subsequent actual
coverage reads. Checkpoints run between ingestor segments and Coinbase
transport pages and before publication. A synchronous provider request itself
is not forcibly interrupted, and the API supplies no latency guarantee.
Unclosed or off-grid acquisition ranges refuse; no range is silently extended.

The v3 preparation receipt retains initial missing coverage, source-bound
requirements, individual successful ingestion receipts, current plan and any
blocker. A provider failure stops the sequence without retry or freeze.
Successful prior facts and gap evidence remain durable. If the failing action's
provider/storage effects cannot be established, `provider_call_performed` and
`acquisition_performed` are null (or true when prior successful receipts prove
an effect), with `partial_effects_possible=true`; false must not imply a rollback.
An ingestion receipt never proves coverage: only the fresh plan authorizes the
next freeze step. Recorded provider gaps stay evidence, and the consumer's
existing readiness and gap policy decide whether a frozen input is usable.
A frozen Dataset is not proof that the Check or Indicator is ready.

CLI uses explicit `--acquire-missing` and `--freeze` flags. MCP requires
`confirm=true` for either write flag. Both adapters override corresponding
embedded request JSON fields with the explicit operation flags, preventing
embedded JSON from bypassing inspect-only defaults or confirmation. HTTP flags
must be literal booleans. The returned `next_request` is input to the separately
reviewed requirements/admission/Check execution step, never an automatic run.

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

For periodic inputs, missing-interval checks follow the canonical source's
observed cadence, not the request start. A five-minute entry delay can put a
thirty-minute dependency range off-grid; that does not create candles at new
minute offsets. Planning extrapolates the source grid backward to detect leading
holes and checks only timestamps within the half-open range. Actual missing
intervals still require source-bound evidence. No provider or UTC-session anchor
is invented by coverage. Dataset preparation expands off-grid candle snapshot
edges outward to whole source candles, records those snapshot bounds, and keeps
the consumer's exact requirement range. Strict candle duration, known-at,
continuity and frozen-range checks remain in force. Extra boundary candles do
not extend the Check's decision window or admit future information.

Candle planning and pre-freeze source resolution use the existing repository's
[header selection projection](../data/DATA_BOUNDARY.md#candle-selection-for-research-planning).
They avoid repeated payload hydration for coverage and source discovery. This
projection retains source/revision and gap selection semantics but does not
certify payload custody: freeze and frozen binding validation still read and
validate the full material. Existing Check definitions, result hashes, scientific
budgets, and the canonical state-engine timeline are unchanged.

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

## Canonical raw-candle evidence

`raw_forward_outcome` definition/evaluator v3 is explicitly selected by
`outcomes.schema_version: raw_candle_forward_outcome.v1`. Without that selector,
v2 remains the default preview method. Its definition, arithmetic and historical
reconstruction are unchanged. Eligibility is definition/evaluator-version aware
in normalization, preparation, persistence, saved-evidence projection and method
discovery; enabling a whole legacy family cannot qualify v2.

V3 uses the existing canonical candle reader, frozen binding, source ownership,
generic raw `all`/`any`/`not` predicates, shared outcome calculation, summaries and
Check persistence/replay. It adds no Indicator, provider adapter, strategy,
statistical procedure or table. Required volume is declared only when a predicate
uses volume. Immediately previous fields require the actual prior interval, not
the previous retained row. Missing, nonfinite or negative predicate volume is an
explicit candidate exclusion; a real source-established zero remains zero.
Canonical source/clock fields must exist, identities must be ordered and unique,
and malformed/nonpositive OHLC, inconsistent observed cadence, ambiguous sources
or clocks fail the input
contract rather than being repaired or dropped. Source admission still owns
whether retained volume and availability provenance suit a scientific question.

The analytical price proxy is the configured exact contiguous entry candle's
**close**. Every required predicate candle must be known by that close, and the
entry price must itself be available by its close. Lag paths cannot bridge gaps.
Forward outcomes use exact contiguous subsequent candles. Entry and target close
must be strictly before the evaluation end; planning adds no future tail. Missing
paths, late entry inputs and boundary censoring retain separate reason counts.
Retrospective outcome known-at is the maximum entry/path availability, distinct
from decision information. Known-at method labels are retained: inferred interval
close is not measured provider publication latency.

The result reports matched, complete, predicate-eligible and excluded populations,
per-horizon resolution, bounded examples and descriptive summaries. Retained
`*_pct` summary names contain **fractions**, as existing raw summaries do; multiply
by 100 only for percentage display. An all-bar baseline edge is not an opposing
cohort contrast. Overlap, descriptive support thresholds and a screening
recommendation confer no independence, significance, causality, profitability,
scientific allocation or trading authority.

The evaluator accepts at most 100,000 candles, 20 declared horizons, a total
lag+horizon of 10,000 bars and 1,000 examples, within the existing execution byte,
ownership and cancellation limits. Requirement declaration rejects oversized
nominal materialization ranges before source inspection or hydration; existing
read-byte/row limits still bound actual canonical reads. The initial policy is explicit
`continue_degraded` with local complete-path exclusions. Existing history cache,
source-binding, frozen Dataset and exact producing-source replay guards remain
unchanged. Deployment exposes this capability; scientific dispatch requires its
own admitted fixed protocol, source and budget.

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

## Availability-Triggered Fact Samples

Definition v5 (`event_fact_analysis`, evaluator v4) adds
`detector.evaluation_trigger: required_facts_available` to Check-owned
`fact_snapshot` analysis. The existing `primary_bar_close` sampling rule still
identifies the market interval being studied. The trigger is a separate rule:
evaluate at the first time the primary candle, required baseline candle history,
and every declared fact input are simultaneously usable. Predicates on the
detector and enriched features remain part of input selection. Exact interval
matching, active revisions, source selection, gaps, and explicit staleness
limits still apply. Arrival transitions are provider-neutral `known_at` values;
no grace duration or source timestamp rewrite is involved.

The evaluator sweeps causal revisions for bounded candidate availability times.
It emits one observation per primary sample, so later updates do not retrigger
or rewrite an already emitted decision. The evaluation end is exclusive for the
new trigger. If readiness is never reached, the sample has
`required_facts_unavailable`, no decision time, and no entry price. Requests
exceeding 100,000 candidate times fail with an instruction to narrow the range.

Outcomes use the first supported candle close at or after the decision and the
configured entry lag. A close whose own availability is later than that close
is not a contemporaneous price proxy. `entry_sample_time` records the selected
close; `entry_price_rule` identifies the rule. Subsequent returns and excursions
start after that price sample. This is an analytical candle-price proxy, not a
fill or second-resolution execution claim. Planning includes one extra candle
in the outcome tail. Missing price/path evidence remains unresolved.

For example, a sample at 12:01 whose book input becomes available at 12:01:07
has decision time 12:01:07. With one-minute candles, its outcome price starts at
12:02, never at the earlier 12:01 close. The actual source known-at remains
unchanged, including any collection batching delay.

Definitions v3/v4 and their evaluator identities remain registered unchanged.
Requests with typed fact inputs and without the new trigger retain v4 behavior. The new trigger selects
v5 and is rejected under older definitions. Frozen evidence, replay and
Check-to-Observation admission use the same existing workflow.

## Candle-Only Indicator Evidence

Definition v6 (evaluator v5) admits `indicator_event` Checks with no additional
fact aliases. The Indicator's candle requirements, transitive graph, warmup and
outcome tail still resolve and freeze through the existing evidence boundary.
No dummy reference feed or Strategy is required. Fact-snapshot sampling and
unbound enriched features remain invalid without their typed inputs.

New candle-only requests without matched-origin attribution select v6; definitions v3/v4/v5 and their evaluator
payloads remain unchanged. V6 adds a descriptive per-horizon outcome summary
from eligible events before the feature complete-case filter: resolved and
unresolved counts, positive counts, mean/median direction-signed returns and
mean favorable/adverse excursions. Returns are fractions, not percentages.
Required unresolved outcomes and incomplete baseline features exclude events
from this population. Optional unresolved horizons are counted separately.
Empty populations report null averages. Eligibility gates remain authoritative;
a descriptive summary does not grant significance, independent samples,
execution realism, profitability, or permission to create a validated claim.

## Matched-Origin Descriptive Attribution

Definition v7 (evaluator v6, result `event_fact_analysis_result.v6`) adds an
explicit `outcomes.matched_origin` configuration to candle-only Indicator Checks.
It uses the same admitted Indicator graph and public snapshot evidence for both
origin and follow-up signals. It does not rerun the engine, ingest saved Check
payloads as inputs, or create inferred price Facts. Existing definitions and their
hashes/results remain unchanged.

The configuration declares a distinct follow-up `detector` plus
`origin_time_path`, `origin_event_key_path`, and `reference_path` metadata paths.
For Market Profile confirmation these are `metadata.breakout_time`,
`metadata.breakout_event_key`, and `metadata.reference`. Only bar horizons and
descriptive sample/day eligibility are admitted initially; additional features,
models, inferential tests, and fold/class thresholds are rejected rather than
silently reinterpreted.

Matching is exact within the request's single instrument/binding: Indicator ID,
direction, origin event key/time, and the complete immutable reference must agree.
Duplicate origins/follow-ups and missing identity fail loud. Changed references
and absent origins are retained with reasons. Both arms use the original
half-open evaluation window and unchanged outcome tail. An unmatched origin is
not proof that it never confirms: follow-ups outside that window are not admitted.
This is retrospective outcome classification, never a feature available at the
origin decision.

For each horizon the evaluator reports A (all eligible raw returns), B (raw
returns on exact pairs), C (confirmed sample prices to the original raw endpoint),
and D (confirmed prices to their own endpoints). B/C/D use the identical resolved
pair set. C uses the already bound target candle close, not reconstruction from
rounded summaries. The reported selection B-A, repricing C-B and extension D-C
are descriptive accounting, not causal effects. A separate all-follow-up mean
and its residual against D reconcile unmatched/ineligible confirmations; full
population reconciliation is explicit. Empty pair means/differences are null.

Per-pair output preserves event identities, open labels, entry close/known-at
clocks, both endpoint closes, and inside/outside/at-boundary entry state. Positive
finite prices and contemporaneously available post-decision samples are required
for every contributing arm row, including unmatched rows. A follow-up sample
after the common endpoint is unresolved. Existing outcome gap/eligibility rules
remain in force; shortened-window excursions are not inferred from old extrema.

Eligibility is checked for both arms and the common pairs at each required
horizon, including distinct origin UTC days. A sufficient raw population cannot
qualify an insufficient follow-up or paired population. Descriptive values remain
visible with `insufficient_evidence`; neither replay nor sample thresholds grant
independence, significance, out-of-sample generality or trading authority.
The nested follow-up result and attribution are covered by the canonical result
hash and existing frozen persistence/replay/Observation contract.

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
| Async evidence | add `--dispatch` (optional `--single-attempt`); inspect with `qt research jobs status/result` | `POST /api/research/jobs/checks/run`; job reads | dispatch/status/result tools |
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


### Availability timing inspection

The `required_facts_available` result includes a structured `timing` object per
sample (`research.check_sample_timing.v1`). It records `sample_end`,
`primary_known_at`, `checked_at`, `required_inputs`, `decision_time`,
`outcome_price_time`, `evaluation_end_exclusive`, and the sample's existing
`exclusion_reasons`. These fields project the actual evaluation, not a narrative
or an inferred explanation of provider gaps.

`required_inputs` describes the last attempted evaluation (the successful one
when ready). Each selector retains its alias, filter, selected `known_at`, status,
and existing failure reason. Evaluation stops at its first failed requirement;
subsequent selectors explicitly remain `not_evaluated`. A null input timestamp
means no fact was selected at that attempt, not that the provider never produced
one. A sample with no permitted attempt has null `checked_at` and an empty input
list. Unresolved samples retain null decision and outcome-price timestamps.

The disposable DB regression in
`tests/test_market_data/test_check_availability_workflow_db.py` exercises native
preparation, archived book lineage, freezing, Check execution, and replay, including a mutable revision
after freezing. The test image verifies its source tree using the same provenance
attestation as the runtime image; the Check's provenance checks remain active.

## Producer Range Evidence At Admission

Source-bound producer evidence can explain quiet intervals without relaxing
exact-interval sample selection. Generic planning validates its normalized range
status, not provider-specific heartbeat or sequence rules. Complete quiet evidence
is included in pinned quality but excluded from the consumer's recorded gaps.
Interrupted ranges remain recorded gaps, and unexplained intervals remain admission
failures. A missing sampled bucket still yields an unavailable sample; quiet
coverage does not manufacture a value. Frozen replay uses its pinned classification
and witnesses, preserving older Dataset and Check behavior.

## Shared-Landmark Groups and Dependence

Definition v8 (evaluator v7, result `event_fact_analysis_result.v7`) additionally
requires `outcomes.shared_landmark`. Its fixed classification and later sample
lags preserve each raw origin's original endpoint. The existing registered owner
reuses the same canonical public Indicator outputs and candles; no engine rerun
or saved-result analytical input is introduced. Earlier definitions are unchanged.

The initial readiness contract is `market_profile.value_location.v1`: every
classification bar must have available public context identifying the same opaque
original profile. A confirmation must match the original reference and be known
by the fixed cutoff. Complete observation with no such confirmation is a separate
comparison group, not a claim of failure or no eventual confirmation. Missing or
late evidence and right-edge censoring remain incomplete; profile/reference
identity changes remain unresolved. Subsequent events cannot rewrite cutoff labels.
Only reject-gap candle evidence is admitted. The later close sample must precede
all original endpoints; known-at clocks and shortened holding durations are explicit.

Outputs separate outside, inside, and boundary-equal samples relative to the
original directional boundary, not full value-area membership. Each horizon
includes group distributions, explicit group sample/day eligibility, profile/day
contributions to means, and positive-duration outcome-window overlaps. Insufficient
groups retain descriptive results and cannot inherit a sufficient raw-arm status.
The declared `leave_one_original_profile_out.v1` sensitivity deletes a contributing
original profile simultaneously from both groups, including its cross-day events.
It reports undefined empty-group deletions; this is influence analysis, not a
confidence interval or independence correction. Deterministic chronological,
median-nearest, adverse, and largest-absolute episodes remain inspectable. The full
analysis is included in the canonical result hash and provider-free replay.


## Declared-window candle risk comparison

Event-fact definition 10 / evaluator 9 is an explicit candle-only descriptive
owner. `outcomes.forward_risk` selects `candle_risk_comparison.v1`; older
versions reject that field. This version pins Candle Stats v1 defaults, 1m
source candles, a 200-bar finite pre-period initialization and `reset_rewarm`.
It supports one UTC calendar year or a bounded subwindow for qualification.
It does not reconstruct an uninterrupted pre-seed EMA history.

Calendar labels use the same UTC pandas formatting in batches of at most 2,048
timestamps, with execution checkpoints between batches. This limits native
formatting work that can delay ownership and cancellation helpers; it does not
change calendar meaning, result hashes, scientific versions, or the materialized
input contract. It is not a universal stop-latency guarantee.

The collector optionally captures each public output's readiness from the
canonical engine snapshot. Contiguous identical readiness and gap-segment
states compress into half-open candle-open intervals. Missing candles are not
invented. The optional material participates in the input hash only for Checks
that request it; existing Check hash inputs remain unchanged.

Every observable decision candle is assigned to emitted ATR shock crossing,
ordinary current z-score at most 2, or persistent high without a crossing.
Public metric and signal readiness are required; absence of a signal alone is
not an ordinary observation. Output identity, pinned configuration, current
source availability and the segment's accumulated known-at watermark are
validated. Undetectable intervals, overlapping dependency flags, and mutually
exclusive first-blocking reasons retain unknown event counts as unknown.

The baseline uses 120 squared successive log-close increments from 121 exact
closes strictly preceding the trigger candle, all known by decision. The first
available close at or after decision is the price sample. Subsequent 30, 120
and 360 elapsed minutes independently resolve mean squared log returns and
high-low path range divided by sample price. The trigger/sample high and low
are excluded. Raw risk requires valid closes; range additionally requires
valid highs/lows. Complete retrospective paths retain late reports with explicit
outcome known-at (maximum sample/path source availability), late-after-target
counts and delay distributions by cohort and month. This label availability is
never a decision feature. Missing/invalid paths remain unresolved independently
of other horizons. Zero or
unavailable prior risk censors only the ratio, preserving valid raw outcomes.

The requirement declaration uses `outcome_boundary=evaluation_end_exclusive`:
planning adds no future tail. Entry and target close must be strictly before
the evaluation end; later candidates retain administrative censoring. This
prevents a declared discovery window from reading a reserved next year.

Source coverage uses candle-open time; cohort calendar strata and day/week
contributions use decision known-at. Preceding observable context accompanies
excluded intervals without imputing missing periods or assuming random missingness.
Annual and monthly distributions and fixed month/six-hour/prior-RMS strata
report shock-count-weighted contrasts, matched and unmatched coverage, whole
day/week deletion sensitivity, actual interval overlaps and connected episodes,
reset segments, concentration, and first-per-nonoverlapping-six-hour sensitivity.
These are descriptive associations, with no IID, causal, effective-sample-size,
trading or promotion claim. The owner persists an ordered all-clock-row digest
and bounded chronological examples, not a second year-sized observation copy;
exact frozen inputs, source version and result hashes own replay.

See [ADR 0076](../decisions/0076-declare-candle-risk-observability-and-calendar-boundaries.md).

## Current-state matched candle risk

The opt-in definition 11 / evaluator 10 compares public ATR crossings with
persistent-high observations using a fixed outcome-blind pair map. The existing
Candle Stats engine owns both states and the existing Check owns prior/future
risk. Original immutable Datasets can be reused when the same exact requirements
fit their binding; a new analytical definition does not authorize range expansion
or re-freezing. Matching, common-support rejection, whole-pair missingness and
cross-arm dependence rules are specified in
[ADR 0076](../decisions/0076-declare-candle-risk-observability-and-calendar-boundaries.md#fixed-crossing-versus-persistent-high-comparison).
Full pair provenance and diagnostics are canonical Check output, subject to the
normal evidence/result budgets. Observations and Studies interpret that output;
they do not recalculate it.


### Fixed ATR-ratio pair filter

Definition 12 / evaluator 11 (`event_fact_analysis_result.v11`) is an explicit
extension of the same comparison owner. Select
`outcomes.forward_risk.schema_version: candle_risk_matched_state.v2`, retain
`matching_contract: crossing_state_matched_pairs.v1`, and add
`filter_contract: crossing_state_atr_ratio_filter.v1`. All other fixed settings
remain unchanged. Definition 11 and its results/hashes retain their meaning.

The unchanged original matcher freezes its pair map first. Before future
outcomes, the extension retains whole pairs only when both public ATR ratios
are finite and positive and their binary64 quotient `max(a,b)/min(a,b) <= 1.05`.
The boundary is inclusive, without epsilon, rounding, rematching, or a tunable
threshold. A missing, invalid-type, nonfinite or nonpositive arm excludes the
whole pair. Those categories, in that precedence order, followed by
`above_1_05`, give disjoint primary exclusion counts; per-arm flags preserve
all invalid causes. Overflow of an otherwise positive quotient is above-boundary.

The Check retains the original map digest and matching accounting, filtered
map digest, both public arm ratios, all original-pair filter decisions, and
12 monthly retention rows. Excluded-pair filter evidence carries no outcomes.
The comparison's primary, balance, missingness, day/week deletions and joint
nonoverlap operate on filtered pairs using the existing outcome rules. Accepted
pair outcome records also carry both ATR ratios. No tables, persisted entities,
additional input data or second state-engine path are introduced.

Support has separate denominators: original matching must cover at least 70%
of original eligible crossings overall and 50% in each month; ATR filtering
must retain at least 70% of original matched pairs overall and 50% each month.
At least 95% of filtered pairs must have both outcomes, with at least 200
complete pairs and 10 in every month. Complete-pair absolute SMD must be at most
0.10 for current z, log prior RMS and raw ATR ratio. Equal zero-SD means give
zero; unequal/undefined balance fails. Empty denominators fail. The existing
nonoverlap sensitivity requires 50 pairs in six months. Failure remains
`unsupported`; descriptive outputs do not authorize relaxation or promotion.

Existing frozen Datasets, evidence budgets, cancellation, correction isolation,
Observation admission and provider-free replay own this extension. It requires
no physical storage cutover. Deployment exposes the method; scientific dispatch
still requires a prospective protocol and its separately allocated budget.


## Comparing engulfing with comparable bullish reversals

`raw_forward_outcome` version 4 selects the explicit
`controlled_reversal_comparison.v1` outcome schema and
`controlled_bullish_reversal` detector. It owns one joint comparison; running
separate treatment and control Checks cannot substitute for its common-support
matching. Versions 2 and 3 retain their prior behavior and identities.

The method uses one-minute canonical candles and declares 21 prior candles,
without an Indicator, extra Fact aliases or volume. A candidate has a bullish
current candle, bearish immediately preceding candle, and negative close return
from t-6 to t-1. The treatment inclusively contains the previous candle's body;
controls are the complementary candidates. Features are log of positive current
log-body size, log of RMS of the 20 simple returns ending at t-1, and the prior
five-interval decline divided by sqrt(5) times that RMS. Missing history and
features known after nominal entry close are explicit exclusions.

The Check matches within the UTC date and hour of current candle close. Treated
candidates are processed chronologically. Controls are never reused. Absolute
feature differences must be at most log(1.25), log(1.25), and 0.5 respectively;
the minimum sum of squared caliper-normalized differences wins. Ties prefer the
smaller absolute event-time distance, then the earlier control. Comparisons use
binary64 inclusive boundaries without epsilon or rounding. Entry is the next
candle close and the endpoint is five further bars later, strictly before the
evaluation end. Closed entry-to-target intervals within each pair must be
strictly disjoint, so touching endpoints count as overlap.

Features, matched identities and the joint nonoverlap subset are frozen before
entry availability or future paths are resolved. The subset processes pairs in
treated chronological order and retains a pair only when neither interval
intersects an already retained interval. Missing either outcome removes the
whole pair, with both reasons retained and no replacement or rematching. The
existing canonical candle validator and forward-outcome resolver own input and
path semantics; the existing population-SMD helper owns balance arithmetic.

The primary statistic is mean paired return difference, engulfing minus control,
with both arm means, medians and positive fractions. Support requires 100
complete pairs on 10 UTC days, 50% of eligible treated candidates matched, 95%
complete matched pairs, and absolute population SMD at most 0.10 for every
feature. Undefined SMD fails support; equal constant arms have SMD zero.
Leave-one-UTC-day-out comparisons never rematch. The fixed nonoverlap subset
requires 30 complete pairs on six days. A positive descriptive lead requires all
primary support gates, positive primary contrast, every defined day-deletion
contrast positive, and a supported positive nonoverlap contrast. Other outcomes
are `unsupported`, `nonpositive`, or `mixed`; none establishes significance,
causality, live fill quality or profitability.

Requests cannot tune this procedure. The range must have whole-minute boundaries
and span at most 32 days, with exactly 21 warmup bars when supplied and no tail.
The existing 100,000-candle, execution, cancellation, evidence and result budgets
remain enforced, plus a three-million candidate-comparison guard. Results retain
the feature-map hash, every matched pair and unmatched treated identity, clocks,
subset membership, outcome missingness, balance and dependence outputs. An
unsupported result is still a completed Check, not permission to search new
thresholds or dates. Inferred interval-close labels remain explicit and do not
certify measured publication latency.


## Configurable controlled reversal and retrospective numeric comparison

`raw_forward_outcome@5` extends the existing controlled-reversal procedure with
explicit `controlled_reversal_comparison.v2` choices. V4 retains its immutable
1m/21-prior/5-return/5-outcome/date-hour rules and result hashes. V5 accepts
1m, 15m, 30m or 1h sampling without another evaluator per interval. The scope
timeframe must equal `interval`; UTC boundaries align to that interval and the
evaluation lasts at most 32 days. Trend and volatility lookbacks, entry lag and
outcome horizon are integer interval counts. Decline must fit the prior RMS
window; warmup equals volatility returns plus one prior candle. Every expected
timestamp must exist: gaps never compress physical time.

The January 2022 prospective example is 15m, 12 prior decline returns, 96 prior
RMS returns, next-candle-close entry and 12 forward intervals. That means a
three-hour prior decline, a 24-hour prior RMS window excluding the pattern,
entry 15 minutes after pattern close, and three hours from entry to target.
RMS is a fractional-return summary, not annualized volatility. Features must be
known by entry close; missing entry/outcome paths remove a whole frozen pair
without replacement. Exact-source coarsening remains an explicit preparation
operation outside the evaluator.

Matching keeps existing calipers, ranking, no control reuse and closed-interval
overlap rules. `same_utc_close_slot` admits dates within an explicit 1..31-day
radius; the example fixes seven days. Controls may be later observations, so
this is a retrospective matched association. `same_date_hour` needs radius zero
and enough within-hour separation for two disjoint closed outcome paths. Support gates retain treated-close day
counts; both-arm coverage is separately reported. Day deletion omits a whole
pair when either arm's candle-close date is omitted, over the union of dates,
without rematching. The feature-frozen joint nonoverlap subset remains shared.
Neither diagnostic establishes independence, significance or causality.

`event_fact_analysis@13` requires an explicit
`outcomes.retrospective.schema_version: retrospective_numeric_comparison.v1`,
real `analysis_as_of`, and `bucket_time_interpretation: bucket_start_unverified`.
It compares one next-hour raw-return-direction outcome in an aligned hourly
sample of at most 60 days. The fixed baseline is one-bar lagged return and
ATR fraction period 14; one or two exact numeric `latest_value` features use
exact completed-hour alignment (`dimensions.bucket_seconds=3600`) and explicit
3600-second staleness. Exactly one chronological train/validation logistic
comparison reuses existing training-only standardization, L2 fitting, paired
complete-case scoring with a one-bar purge and zero embargo. No additional bins/tests/bootstrap
are admitted. Those chronological windows are analytical partitions, not a
point-in-time forecast or a fresh holdout claim.

The existing source-constrained frozen-history reader runs at the declared
analysis cutoff for this version only. Latest active revisions are selected
per observation at that real cutoff, then matched to the exact completed
bucket; at most 10,000 frozen revisions per alias are admitted. Missing observations never become zero. Real `known_at`, publication,
receipt, acceptance, revision, source identity and provider provenance remain
in output references. Coinalyze's current adapter interprets provider `t` as
bucket start; this remains explicitly unverified. Its long and short BTC
volumes are separate exact numeric series for Binance BTCUSDT_PERP.A; Coinbase
BTC-USD price outcomes are a different venue and spot product.

V13 results identify `retrospective_conditional_association`, replace historical
decision labels with sample time, and explicitly set point-in-time predictive,
backtest-certified and scientific-certification eligibility to false. Durable
frozen/replayable evidence means repeatable arithmetic under those assumptions,
not certification. V3/V4/V5 causal Fact selection is unchanged. Existing exact-bucket gap semantics
remain: missing buckets exclude samples (with gap/missing reasons), while real
present observations may be used under `continue_degraded` with recorded gaps
and completeness uncertainty retained; `reject` blocks recorded gaps. No zero
is invented and no provider-completeness claim follows; no timestamp
is backdated. Existing Fact payload registry, providers, collection, persistence,
scientific budgets and concurrency are unchanged by these method extensions.
Catalogue examples are templates; prepare/freeze and scientific dispatch remain
separate authorized operations.


## Retrospective complete paired inputs

The hourly v13 numeric comparison optionally declares
`outcomes.retrospective.input_coverage_policy: complete_paired_rows`. This is an
explicit input-admission branch, pinned in the request, configured definition,
plan and frozen binding. Omitting it preserves the historical planning rules.
It changes no feature, sample-selection calculation, logistic fit, chronological
fold, purge or score rule.

This branch requires only completed numeric buckets inside the evaluation
range, retaining the existing price lookback. The primary materialization ends
at the fixed exclusive boundary rather than acquiring an outcome tail. The
existing evaluator counts missing numeric values and unavailable final outcomes
as exclusions and fits baseline/enriched models on identical complete rows.

The planner and frozen binding accept source-bound immutable `partial` range
custody in addition to `complete` custody only under this explicit policy.
`failed`, unknown and unacquired intervals still fail admission. Partial does
not become complete: coverage status, gaps, source identity, revision clocks and
provider-completeness uncertainty remain pinned. Acquisition caches and ordinary
causal consumers still require complete coverage. Check execution remains
provider-free. The real analysis cutoff and `bucket_start_unverified` limitation
remain mandatory; this is conditional retrospective association, with no
historical-availability, predictive, causal or certification claim.


## Complete distributions and bounded liquidity episodes

### Indicator-owned barrier profiles

`event_fact_analysis` v15 selects `indicator_event_barriers.v1`. It consumes
public long signal events from one existing typed Indicator instance; it does
not recognize patterns or branch on a Reversal event type. The request projects
an event-close or public numeric reference, a whole-window or public numeric
failure level, a risk multiple or public numeric target, and one to 32 public
numeric or reference-window-duration features. A non-Reversal breakout fixture
exercises the same registered evaluator and proves this ownership boundary.

The method requires evidence mode, one complete exact native historical 15m
frozen series, an explicit computation origin, reject-on-gap behavior and at
most 19,104 input bars, 10,000 events, 32 features and 12 outcome bars. It
retains all event IDs and simultaneous completion clocks. Root weights are
computed once across every completed event clock and split across events at the
same clock. Every continuation, failure, neither and unresolved event retains
its original mass; missing feature mass remains visible. Conditional means and
first-CDF-crossing quantiles normalize only by observed feature mass.

Inclusive high/low first touches preserve the original four-label outcome.
When target and failure first touch in the same bar, ordering is unresolved.
The same Check also records whether an original failure later reaches its
original target on a strictly later bar before the unchanged original deadline.
That observation never changes the failure label, extends the horizon, or
creates a second attempt. Complete negatives, censored unknowns and the
last-bar structural no-opportunity case remain distinct.

The pre-label population hash projects an explicit field list plus direct
support/common-origin dependence. Results include descriptive target-minus-
failure and target-minus-neither feature profiles only. They provide no
independent-sample, predictive, causal, trading or deployment authority.

`raw_forward_outcome` v6 explicitly selects `raw_candle_distribution.v1`.
It reuses v3 preparation/predicate/close-outcome arithmetic through an internal
complete-event collector; historical v3 output and registrations are unchanged.
Complete volume-cohort events, fixed tail/quantile/day diagnostics and overlap
identities belong to the Check, not shell/report calculations. A complete-table
resource limit fails rather than truncating scientific support.

`event_fact_analysis` v14 explicitly selects `liquidity_episode.v1` with a real
analysis cutoff. It uses canonical typed frozen Fact histories and the existing
revision selector at that cutoff, then admits complete short book/flow/trade
paths. It retains actual information clocks and prohibits retrospective sequence
inspection from claiming historical decisions. It is a distinct descriptive
procedure, not a reinterpretation of causal v5 or hourly retrospective v13.

The fixed exposed-data pilot rules, threshold rationale, missingness, reuse,
allocation and stopping decisions are recorded in
[the bounded C/A protocol](../../research-campaigns/episode-distribution-pilots-20261014.md).
Method registration does not authorize deployment, acquisition, extra attempts,
sealed-data access or trading. Historical definitions/results remain replayable.


### Configured volume-cohort matching

`raw_forward_outcome` v7 explicitly selects `volume_cohort_matched.v1`.
The request supplies the raw event predicate, source/Dataset, instrument, range,
timeframe, direction, entry lag and one outcome horizon. Its `comparison` selects
the supported volume cohort split and matching policy, prior volatility candle
count and each feature ratio caliper. There are no study-specific candle IDs,
dates, venues, assets, outcome values or expected membership counts in the method.

Raw predicate preparation is shared with v3/v6; prior context is shared with v6.
The procedure freezes its outcome-free candidate ledger, pairs and feature hash
before calling the existing distribution/outcome evaluator. Missing context and
unmatched cases stay in the original event ledger. One-bar source binding is
retained; requesting a longer prior feature window does not acquire extra data.
The full-population v6 context remains explicitly a 20-prior-candle diagnostic;
`events[].matching_context` uses the configured window for the v7 comparison.

V7 configures the existing chronological nearest-caliper matching and paired
support/deletion calculations with explicit feature names and arm labels. V4/v5
callers retain their old defaults. Full feature-through-target support is an
explicit new caller choice for the joint nonoverlap sensitivity; within-pair
matching still checks nominal outcome intervals only. Selected and complete pair
balance, original/eligible matching denominators, calendar-day missingness, both
arm overlap components, and all original and complete-pair tails are emitted.
No imputation, trimming, control reuse, future-valid replacement or automatic
rematching is provided. The support gates are descriptive requirements, not a
power calculation. Exact source qualification and separate attempt allocation
remain prerequisites for scientific execution.


### Native episode admission

Requirement planning and frozen-input preparation retain the canonical Fact
view for native trades and trade-flow buckets. The legacy typed read API is
unchanged; its projection is not used to reconstruct source identities during
admission. Canonical source, revision, watermark and known-at predicates remain
owned by the existing repository. Native effective timestamps use the shared
canonical record accessor when no legacy common clock is present.

Liquidity episode v14 declares its Fact histories bounded by the evaluation
range. Explicit external lookback requests fail rather than being clamped.
Generic staleness settings do not create a predecessor outside its fixed
window; all pre/post support remains inside that window. Other procedures retain
their existing lookback policy. HTTP requirements and preparation dry-run tests
cover the complete five-alias path with disposable canonical records; the native
PostgreSQL fixture additionally checks real source selection, explicit sparse
coverage, preparation resolution and cutoff/watermark-bound causal history.
These tests do not execute a scientific Check or authorize acquisition/freezing.


### Nearby-date volume comparison

`raw_forward_outcome` v8 selects `volume_cohort_matched.v2`. It reuses v7 raw
selection, preparation, feature calculation, outcome resolution, balance,
support and dependence calculations. The caller explicitly supplies
`comparison_window_days` (integer 1..31) and `matching_slot_seconds` (60 or 3600)
with `matching_policy=same_utc_close_slot`. The existing controlled-reversal
`prepare_pairs` performs the inclusive elapsed-radius test unchanged. V5's
minute slot and v7's same-date/hour semantics are unchanged.

A 3600-second slot preserves UTC close hour while allowing nearby dates; a
60-second slot preserves the exact UTC close minute. Neither implies permission
to search these settings. Candidate records must still come from the declared
evaluation range; a wider match radius never expands Dataset reads. Pair
identities are frozen before outcomes. Both arm dates and signed control-time
distances are retained, with whole-pair deletion on either arm date. These are
influence/dependence diagnostics, not independent-day or causal assurance.

V8 has its own immutable registration, rules and result schema; v7 remains
replayable with unchanged default output and hashes. No new persistence,
framework, provider or deployment capability is introduced.


## Composable recognition candidates before outcome science

`raw_forward_outcome@9` explicitly selects `reversal_recognition.v1`. It calls
the domain `ReversalRecognizer` through typed candle/context/event contracts;
the offline preview calls the same `initialize -> apply_bar -> snapshot` state.
Origin and validated settings enter material identity. The adapter uses the
existing canonical candle preparation and pinned Check evidence without a new
registry, persistence model or workflow language. Historical methods keep their
versions and semantics.

The first consumer records provisional decline/rebound/lower-low/recovery/reclaim
chains. Numeric reference availability and reference-role availability are
separate clocks. Source-event links preserve the exact confirmed reference,
definition version and consumption clock. Competing chains remain visible;
neither implicit deduplication nor a minimum-size filter settles Study ownership.
Candidate counts are not an independent event population: sample count and
admitted Study events are zero, and no outcomes or statistics are calculated.

The method requires fixed-origin contiguous close-available candles, rejects gaps
and delayed-close reinterpretation, and fails explicit resource bounds rather
than returning a partial population. It has no outcome tail. Candidate completion
membership is `[evaluation_start,evaluation_end)`; contextual references and
pending chains are labeled with the full input snapshot clock at evaluation end.
The [definition guide](../../guides/research-recognition-definitions.md) describes
settings, ordinary-function composition, immutable outputs, shared visual use
and the reviewed separation from Indicator signals and execution consumers.
Registration and synthetic qualification do not authorize a real-data dispatch.


## Completed-formation first-passage description

`raw_forward_outcome@10` explicitly selects `reversal_first_passage.v1`. It pins
the user's retreat-inclusive reference choice and reuses v9's canonical input
preparation and sequential recognizer without changing historical v9 outputs.
The domain outcome procedure anchors at the observational completion close E,
locks the entire formation low L, verifies E>L, and tracks inclusive first touches
of L or E+(E-L) over the next twelve 15-minute candles. Ambiguous same-bar order,
invalid risk and unavailable horizons remain explicit unresolved cases; a full
untouched horizon is neither. The evaluation boundary prevents a future tail.

Before reading outcomes, closed formation-start through nominal-horizon
intervals form transitive overlap components. Every chain is retained. Outputs
show all four classes, raw chain shares, equal-component within-component class
shares, component sizes, UTC-day counts and descriptive concentration. Neither
component counts nor inverse HHI claim independent observations. Existing
Check/version/input binding, job admission, evidence and Research Memory paths
own persistence and execution; there is no new scientific runtime or matching
campaign. See the [fixed January protocol](../../research-campaigns/reversal-first-passage-20261016.md)
for exposure, availability, censoring, resource and single-attempt stopping rules.


## Completed reversal selection without outcomes

`raw_forward_outcome@11` is the fixed smaller-design selection-only method.
It consumes the same canonical sequential recognizer as v9/v10; historical
definitions and evidence remain unchanged. It declares no outcome horizons or
tail reads and imports no first-passage procedure. New stage-availability views
distinguish candle clocks from role/parent recognition without rewriting v1.

Selection matches only prior volatility, fractional boundary distance and
strictly pre-decision movement. Body/range and derived normalized distance are
diagnostics. Every chain remains explicit, while reporting gives completion
moments equal weight and exposes partially matched moments. Within-pair direct
histories must be disjoint; cross-pair and shared recognition ancestry remain
reported dependence, not independent observations. There are no numeric
coverage or power pass gates and no rescue tuning.

See [the fixed procedure](../../guides/reversal-selection-only.md) and its
retained selected protocol JSON. A qualified method, frozen data and normal
admission remain prerequisites. Method availability and a selection report do
not authorize an outcome run or a capacity-limit bypass in data preparation.


## Provider native candle source policy

New candle evidence uses the provider's requested timeframe and is frozen
directly. Internal coarsening, higher-bar splitting/upsampling and synthetic gap
fills are not new research preparation routes. Transport pagination does not
change the candle timeframe. Historical derived evidence and replay definitions
remain immutable and accurately labeled; this policy adds no subsystem or
automatic source substitution. Record any source-policy amendment before
selection, without changing a historical result.


## Fixed six-block reversal selection extension

`raw_forward_outcome@12` / `reversal_selection_only.v2` extends the decision
range to February1-July19,2025, retaining January1 recognition origin and six
28-day blocks. The shared selection kernel keeps v1 output unchanged. A separate
B2 identity-namespace ordering key preserves same-clock matching order while
retaining actual expanded event IDs. Stage view v3 removes only duplicated
availability fields and decodes losslessly to v2. Cases/pairs appear once;
old/new/combined summaries use separately normalized completion-moment weights
and report their own direct and ancestry-aware dependence. No outcomes or
automatic follow-up are supplied. The fixed19,104bar ceiling is explicit;
existing comparison/time/input/result bounds fail rather than truncate or expand.
See the [six-block procedure](../../guides/reversal-six-block-selection.md) and
its [fixed protocol](../../guides/reversal-six-block-selection-v1.json).


## Saved reversal matched outcomes

`raw_forward_outcome@13` consumes the pinned BTC B3 selection Check on its
original native frozen input. It validates existing typed Check result/evidence
hashes without replay, restores only the historical v12 integer histogram key
types for its internal manifest hash, validates saved barriers against frozen
candles and reuses `first_passage_path`. It never fabricates recognized controls
or reruns recognition/matching. Exact selection reference hashes are method
configuration and copied into the result; missing/changed prior evidence fails.

All saved pairs and completion-moment weights remain. Old/new/combined and
block/day views normalize only within their reporting population; every
continuation/failure/neither/unresolved class remains in the denominator.
The original frozen end remains the outcome boundary, so unresolved censoring
is retained. Saved selection coverage and support/ancestry dependence remain
visible; there is no coverage gate or independent-trial inference. Historical
first-passage wrappers, definitions and results are unchanged. See the
[fixed consumer procedure](../../guides/reversal-matched-outcomes.md).


## Explicit clean-prefix reversal selection

`raw_forward_outcome@14` fixes January1recognition and the first two original
28daydecision blocks, February 1–March 29,2025exclusive. Exact8,352bar native
input ends before the retained ETH April25gap. It reuses the existing selector
and recognizer with unchanged formation,matches,calipers,strata,support,reuse,
weights and19,104bar resource-derived recognition namespace. The changed scope
and block1/block2/combined summaries belong to this explicit version; historical
versions, the incomplete full-window Dataset and BTC B3 evidence stay intact.
No post-gap reset, synthetic candle, outcome or coverage pass gate is added.
See [two-block procedure](../../guides/reversal-two-block-selection.md).

## Saved BTC target progress

`raw_forward_outcome@15` consumes the exact v12 selection and v13 first-passage
Checks on their identical original frozen input. It retains original classes,
pairs, weights and barriers, verifies the saved classes through the shared
first-passage kernel, then reads all twelve bars for uncapped target progress.
Full-window progress may occur after failure. Pre-failure OHLC ordering is
reported as conservative bounds. Partial horizons retain observed lower bounds,
counts and original mass outside complete-window distributions. Fixed quarter
bins and weighted quantiles are post-outcome descriptions, not optimized targets.
No schema, provider, acquisition or UI changes are introduced. Historical methods
and evidence stay unchanged. See the [fixed procedure](../../guides/reversal-matched-progress.md).


## Validated saved-population composition

`raw_forward_outcome@16` accepts the existing typed v12/v14 selection contracts
through a shared saved-selection/frozen-context binding adapter. Declaration
uses bounded caller metadata for planning only; execution validates source
Check/definition/request/plan/result/evidence/manifest linkage and derives the
actual context before calculations. Historical v12 integer-key manifest
compatibility is explicit; v14 and the new output use JSON-stable projections.
The ordinary frozen execution path passes its existing resolved read binding
into evaluator inputs. It remains existing evidence material, not a new
historical execution-input hash, dataset resolver or admission bypass.

The adapter compares that actual binding with the saved selection's complete
native frozen binding, checks rows/clocks/availability, preserves pairs, weights,
nominal support and ancestry, and calls the existing first-passage and progress
functions. In-memory composition has explicit provenance in a new output schema;
it creates no fake persisted outcome reference. Historical v13/v15 adapters and
all calculation kernels remain unchanged. No recognition/rematching, tail read,
new persistence, generic composition framework, or inferential claim is added.
See [the procedure](../../guides/reversal-saved-measurements.md) for context
fields, fixed twelve-bar semantics and limits. Source qualification and a separate
activation decision precede any normal single-attempt scientific dispatch.


## Configured populations and reusable measurements

For new supported reversal populations, `raw_forward_outcome@17` configures the
existing selector with explicit origin, dates, instrument and reporting views.
One to six whole 28-day decision blocks must fit the existing 19,104-bar native
15-minute input bound. At most two named block views accompany the combined
view. Views affect summaries only: matching, original root weights, nominal
support and event identities remain governed by the same fixed scientific policy.
The selector declares no outcome horizons and calls no measurement procedure.
Recognition/selection adapters share the same outcome-blind requirement builder
and canonical preparation. Fixed v11/v12/v14 entry points retain their exact
request/result behavior and replay identity.

`raw_forward_outcome@18` explicitly admits saved v12/v14/v17 selections through
the shared binding and measurement adapter. It validates the configured v17
population recipe against its pinned request and JSON-stable manifest. Method16
continues to admit only its original v12/v14 contracts. Both profiles reuse the
same first-passage/progress functions and retain the 999-pair measurement ceiling;
a larger selection is not silently truncated. No population is rematched to fit
a downstream measurement request.

`price_paths.py` owns reusable fractional endpoint/excursion arithmetic. The
legacy difference-over-entry and event/fact ratio-minus-one policies remain
separate named functions because floating rounding, signed zero and zero-entry
behavior can differ. Callers still own native clock/known-at validation, entry,
complete versus unresolved horizons and output projection. Barrier first passage
and full-window target progress retain their explicit distinct kernels. This is
ordinary function composition, not a universal evaluator or new timeline.

`weighted_summaries.py` shares first-CDF-crossing projection while callers retain
quantiles, units, bins, filtering and original available/missing mass. Historical
selection uses sequential prefixes; progress uses accurate `math.fsum` prefixes.
The exact-boundary difference is regression-tested. Unweighted summaries with
different means, finite filtering or interpolation are not silently standardized.

The catalog marks fixed historical profiles and recommends the reusable routes
for supported new contexts. These are discovery annotations, not admission.
API, CLI and MCP read the same catalog through existing operations; explicit
historical requests remain supported. See the existing selection and measurement
guides for settings, constraints and stopping behavior.


## Controlled-reversal saved path observations

`raw_forward_outcome@19` / `controlled_reversal_comparison.v3` adds saved
per-arm observations to the existing configurable controlled-reversal recipe.
Historical v4/v5 definitions, outputs and replay stay unchanged. All v5 interval,
lookback, lag, horizon, matching, caliper, boundary and support choices remain;
there is no new matching policy, summary estimand or resource allowance.

Every matched pair retains treated and control records, including unresolved
arms. Resolved records retain the already calculated MFE/MAE in fractional units
relative to the long entry-candle close: maximum future high / entry minus one,
and minimum future low / entry minus one. Neither is clamped to zero, so MFE may
be negative and MAE positive. Endpoint return and the existing descriptive
analysis are unchanged. Unresolved records keep their original reason and null
excursion/timing fields rather than a partial-path estimate or replacement pair.

Timing uses that same admitted future path, offsets 1 through the declared
horizon; the entry candle is excluded. Ties select the first chronological
candle attaining the extreme. Save one-based bar offset and that candle's open,
close and known-at. These are candle/availability clocks, not an intrabar high/low
order, tick time or executable fill. The extreme's own known-at can differ from
its close; whole-outcome known-at still covers the complete admitted path.
There is no new tail, provider call or historical evidence rewrite. Results save
the complete pair observations, not only truncated display examples.


## All-completed reversal description

`raw_forward_outcome@20` explicitly selects `reversal_completed_recovery.v1`.

### Ordinary-close candle-state comparison

`raw_forward_outcome@21` explicitly selects
`candle_state_paired_comparison.v1`. It samples every ordinary native candle
close, binds causal public Indicator metrics through the registered runtime,
and compares baseline and enriched logistic forecasts on identical complete
rows. Exact elapsed endpoints, gap resets, train-only standardization,
configured fold buffers, fixed UTC-calendar cluster intervals, opportunity
counts, exclusions, and dependence limitations are canonical Check material.

This method does not reinterpret Indicator events as ordinary closes, fabricate
Facts, bridge missing candles, or grant trading/scientific authority. See
[Candle-State Paired Comparison](../../guides/candle-state-paired-comparison.md)
for the request and result contract.

`raw_forward_outcome@22` explicitly selects
`candle_state_paired_comparison.v2`. It preserves version 21 and its evidence,
while permitting the existing typed causal candle features on either comparison
side. The enriched side may be Indicator-free or may retain the versioned public
Indicator binding. Its additional `candle_state_selection_audit.v1` evidence is
outcome-blind: every nominal close slot retains only causal source/known-at,
episode, candle, feature, missingness, and exclusion context. Future endpoints,
labels, returns, and paths remain outside selection admission. Feature bindings,
selection rows, paired population, predictions, definition, request, plan, and
result stay hashed under the canonical Check envelope.

`raw_forward_outcome@23` explicitly selects
`candle_state_paired_comparison.v3`. It preserves versions 21 and 22 and adds
the existing identity and `times_return_sign` transforms to typed candle
features on either side. Historical sign uses only exact contiguous native
candles known by the decision close; every intermediate candle through the
configured sign lookback is required, and an exact flat change maps to zero.
Transform parameters remain in the configured definition, feature-binding hash,
selection audit, and canonical evidence lineage. No provider, Fact, persistence,
model, target, fold, buffer, or execution contract changes.
It composes the existing sequential retreat-inclusive recognizer, the shared
call-scoped `CompletedEventContext`, reversal-owned inclusive recovery duration,
and unchanged `first_passage_path`. The matched selector also uses this context
constructor, retaining historical arithmetic, records, matching and hashes.
The native population input validator is shared with version17. No new reader,
provider, storage table, registry framework or scientific workflow is introduced.

Dates, instrument and up to two reporting block views are configuration. The
input is one exact frozen native15m historical series, at most19104bars, with
one through six whole28day decision blocks, unchanged19104/10000 recognition
identity/candidate bounds and no extra outcome tail. Provider transport, gaps,
source substitution, watermark/range/cardinality disagreement and extra
scientific knobs refuse admission through the existing boundaries.

Every completed chain is retained, including simultaneous completions and
missing context. `recovery_candles` is one plus the number of15m intervals from
the final `lowest_push.observed_at` close to completion close. Equal lows retain
the recognizer's first attaining candle; only a strict lower low replaces it.
This is duration, not intrabar speed. Context fields remain completion-observable;
sigma96 and z_pre12 exclude the completion return. Missing context is explicit
and never filters an outcome or duration summary.

Each reporting view computes equal-completion-clock weights over ALL its chains,
splitting a clock's mass across simultaneous chains. Four separate label summaries
retain original label mass, observed and missing N mass/count, weighted mean and
first weighted-CDF-crossing quantiles0/.25/.5/.75/.9/.95/1. The conditional N mean
and CDF divide by observed N mass within the label; the weights are never rebuilt
from a label subset. Completion E, whole-formation L, E+(E-L), inclusive first
touches, twelve subsequent bars, same-bar ambiguity and boundary censoring retain
the existing path semantics. Late completions receive no additional tail.

Direct supports include the earlier of formation start and prior97 context
through nominal completion+12bars; conservative common-origin ancestry remains
separate. There are no new pairs, calipers, strata, adjusted contrasts, SMD tests,
day-deletion recipe, classifier or independence/inference/trading claim. The
completion-population hash uses JSON-canonical cases with only `outcome` removed,
dependence and population_recipe; the outer normal Check result/evidence hashes
retain complete output and frozen-source custody. Historical definitions and
results are preserved; actual replay requires a currently supported executor.
Retired versions use the verified custody boundary described above.


## Shared execution observations

Input binding, input preparation and evaluator execution use the existing
execution-control meter; persistence and I/O retain their existing stages.
Common sizes/status and timing belong here, not in each evaluator. The meter
observes known container counts and existing budget byte counters without
consuming generators or serializing payloads solely for observability.
Procedure-specific diagnostics remain in their declared result/report schema.
Operational metrics stay outside Check definition/result/evidence hash material.
See [ADR0082](../decisions/0082-separate-historical-custody-from-active-research.md#shared-execution-observability)
for inclusive wall time, nested stages, worker-owned reporting and resource limits.
