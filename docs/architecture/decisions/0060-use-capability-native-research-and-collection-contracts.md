---
component: adr-capability-native-research-collection-contracts
subsystem: system
layer: decision
doc_type: adr
status: accepted
tags:
  - research
  - market-data
  - nomenclature
  - safety
  - autonomy
code_paths:
  - src/research_science/study.py
  - src/research_science/temporal.py
  - src/market_data/stream_enrollment.py
  - portal/backend/service/market/collector_safety.py
  - portal/backend/service/market/collector_supervisor.py
  - portal/backend/service/storage/repos/market_structure.py
  - config/market_data/coinbase_perpetual_trade_fleet.v1.json
  - tests/contract/test_capability_nomenclature.py
---
# ADR 0060: Use Capability-Native Research and Collection Contracts

## Status

Accepted on 2026-08-06.

## Context

Roadmap words and agent instructions had leaked into executable schemas. A
single research operation had become a generic runtime noun, cross-fact joins
were encoded as though all studies required the same OI/funding/trade
composition, and collector enablement depended on manually supplied storage
budgets. Product enrollment also required editing Python dictionaries.

These shapes confused an operator's conversational request with stable domain
authority. They made new studies and products look like exceptions to one past
operation instead of ordinary compositions of reusable capabilities.

## Decision

Executable research uses `ResearchBrief`, `StudyDefinition`,
`ScientificProtocol`, `ResearchRun`, `ResearchFamily`, `Trial`, `Candidate`, and
`Certificate`. Facts, replay clocks, and joins are declared through
`FactRequirement`, `AvailabilityTransform`, and `TemporalJoinSpec`. A study
selects exact-version feature, search-space, evaluator, and availability
bundles. The generic research boundary cannot fetch providers, choose private
datasets, trade, or promote.

Collection uses `ProductContract`, `StreamEnrollment`, `CollectorFleet`,
`CollectorSafetyPolicy`, `QualificationEvidence`, and `SafetyHalt`.
Product/stream enrollment is a validated immutable manifest consumed through
canonical source, series, product, and stream registries. Provider facts remain
inside adapters and registered contracts, never a generic supervisor switch.

Continuous collection is admitted by current system-derived qualification,
not an operator-authored capacity estimate. Warning thresholds emit immutable
evidence and Grafana alerts without stopping collection. Critical thresholds
gracefully drain and persist a global, fleet, or stream latch. Restarts cannot
clear a latch; a separate acknowledgement event is required. The local spool
limit remains fail-closed.

Words used to organize human/agent work are not executable domain identities.
Historical documents may retain their original vocabulary, but active schemas,
configuration, APIs, and capability code may not introduce numbered roadmap
stage names or the retired research-operation facade. A contract test enforces
that boundary.

## Consequences

- A different fact composition can use the same research authority without a
  new orchestrator.
- Adding a compatible product is a manifest/registry change, not a Python
  branch.
- `economic_claim_intent` stays immutable because it belongs to the brief and
  protocol hashes, so exploration cannot be relabeled after a run.
- BIP, ETP, and SLP perpetual trade streams share one fleet and policy while
  retaining exact product-contract identities.
- No change opens L2 continuous collection, provider expansion, external
  trading, promotion, or capital authority.

## Removed alternative

The prior research-operation schema, runner facade, active JSON definitions,
manual collector capacity approval, and Python product dictionaries are
deleted. They have no compatibility parser or translation path. Immutable
generic research-authority rows remain authoritative; the terminal V3 dossier
is retained only as a historical postmortem.
