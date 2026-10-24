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
# ADR 0061: Use Provider-Neutral Exact Numeric Facts and Bounded Acquisition

Accepted August 7, 2026 for exact scalar acquisition. The scalar-only storage
and clean-install migration exception were superseded by the accepted
[canonical Fact cutover](0063-use-schema-registered-canonical-facts.md).
Current clean bootstrap uses code-owned canonical schema definitions.
The original migration SQL is lineage for pre-cutover deployments, not a
current installation prerequisite.

## Preserve numeric and causal meaning at intake

External numeric observations can exceed binary floating-point precision.
Their meaning depends on declared units and dimensions, while effective,
publication, receipt, acceptance, confirmation and known-at clocks answer
different questions.

The fact registry owns provider-neutral meaning and typed payload contracts.
Provider adapters own source translation and capability evidence. The canonical
repository owns append-only revisions, causal reads, acquisition coverage and
frozen material. Providers do not become consumer read fallbacks.

Exact contracts accept Decimal, integer or canonical decimal text and reject
binary floats. Retain raw provider material and validate semantic dimensions
before registering series identity. Changed events append revisions; disappearance
becomes an invalidation only after a complete repair scan. Partial history is
not evidence of absence.

## Acquisition is explicit and bounded

Network authority defaults to denied and requires an actor and reason.
Calls consume explicit request, log, block, retry and pacing budgets.
Reuse matching complete durable coverage, including proven empty intervals,
without creating another provider request. Binding, adapter-interface or finality
changes cannot silently reuse incompatible coverage.

Dataset preparation may acquire missing data only through explicit authority;
freeze then pins exact material, provenance, quality and revisions. Runtime,
replay and reporting remain provider-free.

The Chainlink adapter illustrates why this boundary matters: a latest-round
request is not a historical scan. Phase-aware event acquisition must reconcile
rounds, chain and confirmation provenance and explicitly expose unavailable
archive access or incomplete history. Source effective time is not automatically
publication or known-at time.

Reference manifests are disabled configuration examples, not execution authority.
Enabling a reviewed binding and supplying its endpoint reference are operator
actions. No signer, transaction or trading authority follows from read access.

## Historical precision cannot be recovered by relabeling

The initial consolidation analysis rejected converting existing float-based
OI/funding evidence into alleged exact provider decimals. Original raw decimal
text had not been retained; funding-interval identity and frozen hashes also
needed explicit preservation.

The later canonical Fact cutover resolved storage convergence through registered
historical v1 codecs and exact-decimal v2 intake. It preserved the old evidence's
numeric meaning and hashes instead of inventing lost precision. Historical
schemas remain interpretation contracts inside one canonical store, not separate
active family tables.

This costs typed schema/codec and migration work, but keeps providers replaceable,
frozen evidence stable and unavailable precision honest.

References: [canonical Facts](0063-use-schema-registered-canonical-facts.md),
[numeric acquisition](../data/NUMERIC_FACTS_AND_ON_DEMAND_ACQUISITION.md),
[Chainlink operator guide](../../guides/chainlink-numeric-facts.md).
