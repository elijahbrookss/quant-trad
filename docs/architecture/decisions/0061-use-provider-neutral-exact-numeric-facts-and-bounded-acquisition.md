---
component: adr-provider-neutral-exact-numeric-facts
subsystem: data
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - market-data
  - exact-numeric
  - provider-neutral
  - chainlink
  - on-demand-acquisition
  - known-at
  - provenance
  - datasets
  - explicit-migration
code_paths:
  - src/market_data/contracts.py
  - src/market_data/fact_registry.py
  - src/data_providers/numeric_facts.py
  - src/data_providers/providers/chainlink.py
  - portal/backend/db/market_data_models.py
  - portal/backend/db/session.py
  - portal/backend/service/market/numeric_fact_acquisition.py
  - portal/backend/service/market/backtest_dataset_service.py
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/controller/market_data.py
  - cli/main.py
  - scripts/db/manual_migration_numeric_fact_store_v1.sql
  - config/market-data/numeric-facts
  - tests/test_market_data/test_numeric_fact_contracts.py
  - tests/test_market_data/test_numeric_fact_acquisition.py
  - tests/test_data_providers/test_chainlink_provider.py
---
# ADR 0061: Use Provider-Neutral Exact Numeric Facts And Bounded Acquisition

## Status

Accepted on 2026-08-07.

## Context

Externally published numeric observations do not all fit candle, open-interest,
or funding-rate storage. Reference prices, reserve balances, and future numeric
facts can have values larger or more precise than binary floating point can
preserve. Their meaning depends on contract-owned units and dimensions, while
their causal history depends on distinct effective, publication, confirmation,
receipt, acceptance, and known-at clocks.

Historical acquisition also cannot be a read fallback. A provider may charge
for requests, rate-limit log scans, omit old state, change a proxy's underlying
implementation, or return a range that cannot be proven complete. Backtests
must continue to execute from frozen local evidence without network access.

Chainlink AggregatorV3 is the first implementation pressure. It exposes public,
read-only EVM data through a proxy whose phase-specific aggregators emit the
historical events. Correct history therefore requires phase-aware log scans,
round reconciliation, block provenance, and a declared confirmation boundary;
calling only `latestRoundData` is not historical acquisition.

## Decision

Separate three concerns:

1. The fact registry owns provider-neutral meaning: fact type, contract version,
   exact numeric type, valid unit, contract-enumerated dimensions, value domain,
   record time, and dataset eligibility.
2. A provider adapter owns source translation and capability proof. It returns
   exact values, raw source values, source-event identity, causal timestamps,
   provenance, explicit gaps, and measured budget use. It never creates schema
   or becomes a consumer read path.
3. The market-data repository owns one provider-neutral append-only exact
   numeric storage shape, revisions, causal reads, acquisition coverage, and
   frozen-dataset material.

`market.reference_price.v1` and `market.reserve_balance.v1` are the first exact
numeric contracts. Values enter as `Decimal`, integer, or decimal string;
binary floats are rejected. The canonical row retains both normalized unbounded
numeric material and the raw provider value. Contract dimensions are validated
before series registration and participate in series identity, so a dimension
that changes meaning cannot be changed within one logical series.

One logical source event has a stable event key plus optional group/component
keys. A changed value, causal field, state, or source-event material hash
appends a correction revision under the shared market fact commit clock.
Disappeared events are retained as explicit invalidation revisions only after a
complete repair scan. No accepted fact or acquisition record is updated or
deleted.

Acquisition is an explicit, bounded operation. Network authorization defaults
to denied and requires an actor and reason. Every call has positive request,
log, and block budgets plus bounded retries. Historical requests consult durable
coverage first; a matching complete interval, including a proven zero-event
interval, is reused without constructing a provider. Partial or failed scans
retain gap and coverage evidence but never claim complete cache coverage.

Coverage identity includes the canonical series and source, binding ID,
manifest hash, the binding source's `adapter_version` as interface identity,
and confirmation depth. A material
binding or finality change therefore does not silently reuse older coverage.
Dataset preparation may invoke acquisition only when the operator supplies both
`acquire_missing` and an explicit numeric acquisition context. Dataset freeze
then pins the exact revisions, material, provenance, quality, and commit scope.
Backtest admission, execution, replay, strategy evaluation, and reporting remain
provider-free.

The first adapter is `chainlink_aggregator_v3.v1`. It is public and read-only:
no wallet, signer, transaction, websocket subscription, or LINK balance is
required. It validates chain, proxy metadata, decimals, description, optional
version, declared history lower bound, archive block access, confirmation head,
phase aggregators, bounded `AnswerUpdated` log pages, `getRoundData`,
latest-round reconciliation, and a gap-based current-staleness policy. EVM block
time is source publication time; AggregatorV3 `updatedAt` is fact effective time;
the configured confirmation block determines known-at. Block, transaction, log,
proxy-round, phase, and confirmation hashes remain provenance.

The reference Chainlink manifests are data, not executable registration code.
They declare instrument role, update/deviation schedule, maximum staleness,
official catalog/risk/deprecation review, and adapter configuration, and are
disabled at both manifest and binding levels. Enabling a reviewed copy and
supplying the endpoint environment reference are explicit operator acts.
Multi-variable response feeds, sequencer-uptime feeds, continuous collection,
and transaction-signing workflows are outside this decision.

The new schema objects are intentionally migration-owned. Operators apply
`scripts/db/manual_migration_numeric_fact_store_v1.sql` while database writers
are stopped. Startup excludes `market.numeric_fact_versions` and
`market.fact_acquisition_coverage` from generic create-table/index DDL. It
validates the tables and dimensions column; the numeric-fact primary key,
unbounded numeric type, required checks/indexes; coverage indexes; and immutable
triggers. The migration owns all definitions, including dimensions and coverage
checks and commit defaults that startup does not independently re-derive. This
exception is scoped to these new numeric objects; legacy schema bootstrap
behavior is otherwise unchanged.

## Open-Interest And Funding Consolidation Gate

The gate result is **`NUMERIC_FACT_CONSOLIDATION_DEFERRED`**. Existing
`derivatives.open_interest.v1` and `derivatives.funding_rate.v1` remain on their
specialized storage and read paths. Routing them through exact numeric storage
in place would destroy or misstate existing evidence:

- `src/data_providers/providers/coinbase.py` converts provider decimal text to
  binary `float` before constructing both observations, and the v1 contract and
  `open_interest_versions` / `funding_rate_versions` columns retain floats.
  The original decimal string cannot be reconstructed exactly afterward.
- Funding interval is row-scoped `funding_interval_seconds`, while the current
  v1 logical series identity does not include it. Moving a meaning-changing
  interval into the new dimensional identity would create a different series,
  not a storage-only rewrite.
- Frozen datasets reference immutable series, commit watermarks, ingestion
  provenance, revisions, v1 row hashes, and v1 material hashes. They do not
  embed a replacement copy that can be silently rerouted. Reusing the old
  identity with new rows or hashes would make prior dataset evidence unstable.
- Runtime, API, collector, and normalization consumers still depend on the
  specialized v1 record fields and tables. A partial consolidation would create
  two truths for the same fact.

The convergence mapping was evaluated across every ownership boundary:

| V1 invariant or surface | Existing OI/funding representation | Exact-numeric result |
| --- | --- | --- |
| canonical series identity | instrument, fact type, contract version, and nullable timeframe; no v1 semantic dimensions | funding interval cannot be added without a new series identity |
| numeric value and raw evidence | `Float` value/rate after Coinbase converts provider decimal text to `float`; raw text is not retained | not lossless; exact decimal text cannot be reconstructed |
| unit and quantity meaning | OI is nonnegative `contracts`; funding is a `fraction`; both have contract-specific validation | representable only through new contracts that retain these stricter rules |
| interval and effective-time meaning | OI/funding `sample_time` is collector schedule; funding also retains `funding_time` and row-scoped `funding_interval_seconds` | clocks are representable, but interval must become a v2 series dimension rather than be silently moved |
| causal clocks | optional publication/receipt plus acceptance, `known_at`, and both method fields | structurally representable and must be copied exactly in any v2 intake |
| revision and source identity | `(series_id, sample_time, revision)`, ingestion run, canonical source, provenance, row hash, shared commit sequence | structurally representable, but no lossless source-event-key migration exists for historical rows |
| current, paper, and runtime reads | collector, market-state, requirement delivery, and specialized record APIs consume v1 records | all direct consumers require an explicit v2 cutover; parity is not proven |
| historical and dataset reads | specialized repository dispatch, material hashes, commit watermarks, and frozen series references | rerouting changes material/identity evidence and would invalidate compatibility |
| API, report, and operator projections | market-data endpoints and dataset/report evidence expose or depend on the specialized fields and frozen identities | projection parity is not proven; partial routing would expose two truths |
| retention and query performance | dedicated time/revision, commit, known-at, and funding-time indexes | no v2 retention/query benchmark or index-parity proof exists |
| migration and rollback | no exact raw-value backfill exists; v1 tables and frozen artifacts are immutable evidence | in-place migration is rejected; rollback requires v1 ownership to remain intact |

The bounded follow-up is a v2 contract migration, not a v1 backfill. A future
slice may introduce `derivatives.open_interest.v2` and
`derivatives.funding_rate.v2` only after adapters preserve raw decimal text and
exact `Decimal` values at acquisition, funding interval is fixed in series
identity before registration, consumers explicitly support the v2 records, and
tests prove all existing v1 dataset IDs/material hashes and provider-free reads
remain unchanged. Source rows lacking retained raw decimal evidence cannot be
promoted as exact v2 facts. The v1 tables remain immutable legacy evidence even
after consumers adopt v2 datasets.

## Invariants

- Fact meaning is contract-owned; providers do not define canonical schema.
- Exact numeric acquisition never accepts binary floating point.
- Unit and dimensions are validated before series identity is registered.
- A consumer read, dataset replay, or backtest execution never contacts a
  provider.
- Network use is explicit, actor-attributed, reason-attributed, and bounded.
- Empty complete coverage is evidence; partial or failed coverage is not a
  cache hit.
- Corrections and invalidations append; accepted evidence is immutable.
- Effective, publication, acceptance, confirmation, and known-at times are not
  collapsed into one timestamp.
- New numeric schema objects are applied by the explicit migration and only
  validated at startup.
- Existing OI/funding v1 rows and frozen datasets are not rewritten or rerouted.

## Consequences

New exact numeric facts can share one storage and dataset path without creating
a provider-specific table or widening candle/OI/funding rows. Providers remain
replaceable adapters, while source-specific provenance stays inspectable.
Operators must choose and fund bounded acquisition explicitly, and incomplete
historical capability remains visible instead of being presented as data
absence.

The scoped migration exception makes a clean database require one explicit
numeric migration before this application revision starts. It also prevents
startup from disguising migration ordering or creating only part of the new
contract. Existing bootstrap behavior remains in force for all other model-owned
objects.

## Rejected Alternatives

- Store every numeric fact as binary float plus provider JSON.
- Create a Chainlink-owned table or encode proxy addresses in application
  branches.
- Treat `latestRoundData` polling as historical acquisition.
- Call the provider automatically on a repository or runtime read miss.
- Cache partial scans or infer that no logs means complete coverage without a
  successful bounded scan.
- Overwrite events after reorgs or delete disappeared events.
- Auto-enable checked-in reference manifests.
- Rewrite OI/funding v1 rows and frozen manifests into the new storage shape.
- Let generic startup DDL create the migration-owned numeric tables.

## Enforcing Tests Or Evidence

- `tests/test_market_data/test_numeric_fact_contracts.py` covers exact large
  values, raw values, float rejection, units, dimensions, value domains, and
  stable exact material hashes.
- `tests/test_market_data/test_numeric_fact_acquisition.py` covers default-deny
  authorization, complete zero-event caching, partial gaps, corrections,
  disappearance invalidation, and provenance-only reorg revisions.
- `tests/test_data_providers/test_chainlink_manifest.py` covers disabled
  reference manifests, data-driven feed deployments, and strict manifest
  validation.
- `tests/test_data_providers/test_chainlink_provider.py` covers phase-aware
  bounded history, exact reserve values, distinct publication/confirmation
  clocks, current-round reconciliation, metadata quarantine, retries, and
  explicit partial capability gaps.
- `tests/test_portal/test_database_bootstrap.py` proves fresh startup requires
  the explicit numeric migration, validates migrated objects, and continues to
  create unrelated model-owned objects.

## References

- [ADR 0044: Enforce Known-At Prefix Invariance](0044-enforce-known-at-prefix-invariance.md)
- [ADR 0050: Use One Canonical Append-Only Market-Data Store](0050-use-one-canonical-append-only-market-data-store.md)
- [ADR 0051: Require Frozen Datasets For Canonical Backtests](0051-require-frozen-datasets-for-canonical-backtests.md)
- [ADR 0052: Use Typed Fact Collectors And Explicit Instrument Roles](0052-use-typed-fact-collectors-and-explicit-instrument-roles.md)
- [Numeric Facts And On-Demand Acquisition](../data/NUMERIC_FACTS_AND_ON_DEMAND_ACQUISITION.md)
- [Chainlink Numeric Facts Operator Guide](../../guides/chainlink-numeric-facts.md)
