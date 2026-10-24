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
  - known-at
code_paths:
  - src/research_science/check.py
  - src/market_data/frozen.py
  - portal/backend/service/research/registry.py
  - portal/backend/service/research/catalog.py
  - portal/backend/service/research/planning.py
  - portal/backend/service/research/execution.py
  - portal/backend/service/research/preparation_acquisition.py
  - portal/backend/service/research/result_reference.py
  - portal/backend/controller/research.py
  - cli/research_operations.py
---
# Check Evidence Boundary

A Check owns a bounded analytical procedure over declared evidence. This page
explains the shared execution and custody contract. The code-owned catalog owns
supported procedures, settings, result schemas and limits; a prose description
of an old method does not make it executable.

Actual requests, selections, frozen bindings, Check outputs, findings and Study
interpretations are private research records. Keep them in QT's private record
stores under the [public infrastructure/private research contract](../../contracts/platform/00_system_contract.md#public-infrastructure-and-private-research).
Public examples must use synthetic material or placeholders. Repository cleanup
is not a migration or verification of stored QT records.


## From a question to frozen evidence

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
| Frozen Check evidence | `FrozenMarketDataReadBinding` plus exact Check definition | Durable Check result | Provider-free, definition-pinned, input-pinned, quality-pinned and hash-verifiable. Execution replay additionally requires a supported executor and its exact producing source. |
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

Current registry admission determines which requests may execute and undergo
fresh evidence validation. Retired records remain inspectable as
`legacy_frozen_unverifiable`, with verification, execution, replay, new
evidence-based admission and saved-selection reuse unavailable. Reading does
not re-admit the stored result; no record-specific runtime allowlist grants an
exception. Original payloads and existing Observation/Study relationships remain
intact. Existing Study interpretation history and saved image bindings remain
readable; fresh publication references, image attachments and verified exports
require supported Check validation. See
[ADR 0082](../decisions/0082-separate-historical-custody-from-active-research.md#decision).
This documentation does not attest that particular private records were retained,
migrated or replayed.

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
- Reports and interpretation views project canonical records; they are not primary
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

The disposable DB regression defined in
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

## Discover supported procedures

Use `qt research check definitions` and
`qt research check definition <definition_id> --version <version>`, or the
matching definition GET routes and MCP discovery tools. Detail exposes purpose,
input requirements, fixed/configurable settings, output fields and examples.
Replace example bindings with authorized resources. Discovery performs no
acquisition, execution, publication or budget allocation.

Normalizers and the registry decide supported versions. Do not infer the latest
version, replay eligibility or a retirement exception from this manual. A new
instrument or supported window is configuration; changed scientific meaning
requires a reviewed version or a distinct procedure. Preserve prior records and
never relabel historical outputs with new semantics.

Public reusable families include raw-candle analysis, typed event-and-fact
analysis, configured population selection and saved-population measurements.
Their detailed arithmetic, allowed settings and resource ceilings belong to the
registered definition, evaluator and catalog rather than a second inventory of
study recipes in this page. Saved-population consumers validate exact existing
references and frozen context; they do not rematch a population to fit a later
measurement request. See [the research workflow](../../guides/research-workflow.md#reusing-check-recipes).

## Supported reusable method shapes

The catalog distinguishes a reusable procedure from a particular private study.
Keep method settings and limits explicit before dispatch; a method's registration
never selects dates, instruments or evidence on the researcher's behalf.

| Procedure | Public analytical meaning | Limits and ownership |
| --- | --- | --- |
| Raw candle predicates (`raw_forward_outcome@3`) | Evaluate declared raw predicates and contiguous forward candle paths from a causal close-price proxy. | Up to 100,000 input candles, 20 horizons, 10,000 combined lag/horizon bars and 1,000 examples. Required history and paths must actually exist; recorded gaps and exclusions remain visible. |
| Event-and-fact analysis (`event_fact_analysis`) | Select public Indicator events or explicitly Check-owned samples, align typed Facts and calculate the registered outcomes/statistics. | The exact version owns trigger, alignment, staleness, gap, eligibility and result rules. Fact sampling does not manufacture a directional Indicator signal. |
| Indicator barrier profiles (`event_fact_analysis@15`) | Measure existing public long signal events against locked references, target/failure barriers and completion-known features. | One complete native 15m frozen series, explicit computation origin, reject-on-gap, at most 19,104 input bars, 10,000 events, 32 features and 12 outcome bars. Same-bar first touches remain unresolved; later target observation never relabels an earlier failure. |
| Configured population selection (`raw_forward_outcome@17`) | Configure an existing outcome-blind recognizer/selector with explicit scope and reporting views. | One to six whole 28-day decision blocks inside the 19,104-bar native 15m input bound; at most two named block views plus combined. Views alter summaries, not matching, original root weights, support or event identity. No outcome horizons or measurement procedure are called. |
| Saved-population measurements (`raw_forward_outcome@18`) | Measure a validated supported saved selection using shared first-passage and full-window progress functions. | Complete source linkage and original frozen context must agree. At most 999 pairs; a larger selection rejects as a whole, without truncation or rematching. The original input boundary still owns censoring. |
| Controlled path observations (`raw_forward_outcome@19`) | Retain per-arm endpoint returns, fractional excursions and first-attaining extreme candle clocks from the admitted matched paths. | Entry candle excluded; unresolved arms retain reasons and null path fields. Candle offsets do not reveal intrabar ordering or executable fills. |
| All-completed description (`raw_forward_outcome@20`) | Retain every completed recognized chain and describe recovery duration and original first-passage labels. | Same native population bounds as the configured selector; no extra outcome tail. Each reporting view weights all completion clocks before label summaries. Missing context stays explicit and does not filter the population. |

A public Indicator owns reusable causal recognition and its typed signals,
references, stages, duration and context. A Check consumes those outputs to own
selection, analytical labels and summaries. A Study holds the private question
and interpretation; it is not an executable definition registry. Where a Check
uses a shared sequential kernel directly, its adapter still cannot publish a
runtime trading signal or bypass the canonical Indicator execution contract.

Generic price paths, weights and dependence diagnostics are reusable engineering
meaning. A selected instrument/window, concrete pair ledger, fixed study request,
source hash or outcome record is private research content and does not belong
in this table or the public repository.

The Check18 definition's versioned input-rule metadata remains immutable to
preserve existing payload identities. Current runtime admission accepts supported
Check17 selections only; retained metadata mentioning older input contracts does
not make those retired selections reusable.

## Causal sampling and analytical outcomes

Only a public Indicator signal output supplies an Indicator-defined event. A
Check may instead own an explicitly labeled analytical occurrence; that does
not create a trading signal or execution action. Typed Fact selection preserves
exact source, active revision, known-at, interval and staleness rules. A missing
bucket is unavailable evidence, not zero or permission to carry another bucket.

Market sampling and evaluation triggers are separate. The registered
`required_facts_available` trigger evaluates a sampled interval at the first
permitted time all required inputs are usable. Later revisions cannot retrigger
an already emitted decision. For example, if a sample closes at 12:01 and its
required Fact becomes usable at 12:01:07, its decision is at 12:01:07. A candle
price proxy must then use a permitted close at or after that decision, never the
earlier 12:01 close. The source clock is not rewritten.

Outcome prices and horizons are analytical measurements, not fills. Preserve
resolved/unresolved counts, entry/endpoint clocks, censoring, gap reasons, units
and dependence. A retrospective procedure must disclose its actual analysis
cutoff and later-received revisions; repeatable arithmetic does not establish
historical availability, prediction, causality or scientific certification.

## Bounded delivery and shared calculations

Indicator Checks use candle windows of at most 2,048 nominal bars with one
canonical engine spanning pages. Sparse ranges stay sparse. Raw-frame evaluators
retain their materializing contract. SQL hydration uses a 512-row server cursor;
causal revision choice precedes invalidation filtering. Incremental hashing
avoids redundant complete JSON copies, but evaluator arrays and output records
still need explicit total bounds. Paging alone does not promise constant memory
or a measured speedup.

Preparation and reusable calculations may be shared within one owned evaluation
on unchanged inputs. Public evaluations start fresh; causal selection and shifted
outcomes remain procedure-owned. Price-path and weighted-summary helpers retain
named arithmetic policies where rounding, signed zero, accumulation or filtering
differ. Reuse does not authorize changing a fixed scientific choice.

Common input-binding, preparation and evaluator timing use the existing execution
meter. Operational metrics stay outside definition/result/evidence hash material.
The meter observes existing counters without consuming generators or serializing
scientific payloads solely for logging. Cooperative checkpoints, cancellation,
input/result limits and ownership remain governed by the
[async job and execution boundary](RESEARCH_ASYNC_JOB_BOUNDARY.md).
