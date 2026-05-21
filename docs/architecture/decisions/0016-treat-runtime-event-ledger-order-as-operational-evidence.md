---
component: adr-runtime-event-ledger-order-operational-evidence
subsystem: persistence
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - persistence
  - replay
  - run-seq
  - runtime-events
code_paths:
  - portal/backend/service/storage/repos/runtime_events.py
  - portal/backend/service/bots/botlens_projection_batches.py
  - portal/backend/service/reports/run_research_dataset.py
  - scripts/reporting/golden_repeatability.py
  - docs/architecture/persistence/PERSISTENCE_BOUNDARY.md
---
# ADR 0016: Treat Runtime Event Ledger Order As Operational Evidence

## Status

Accepted on 2026-05-13.

## Context

`run_seq` is the durable runtime event-ledger spine. It proves that retained
runtime-event rows for one run are complete, gapless, cursorable, and replayable
as an event ledger.

That does not make `run_seq` a semantic trading clock. Semantically equivalent
runs can assign different `run_seq` values to the same trading decision because
of generated event IDs, async worker publishing, source batch composition,
telemetry batching, or same-source-batch tie-breaking. A single source batch
can also contain multiple retained facts, so source `seq` and durable `run_seq`
answer different operational questions.

## Decision

`run_seq` remains operational evidence for durable ledger completeness and
projection/reporting cursors. It must not also be used as the semantic trading
order for golden certification.

Trading determinism is certified through scoped semantic identifiers and
clocks: market time, symbol/timeframe/bar key, decision ID, wallet commit
sequence and wallet facts where applicable, position/trade lifecycle, wallet
and position projections, and the semantic fingerprint.

Runtime event ordering can block certification when the ledger is missing,
gapped, duplicated, mixed, or not runtime-assigned. It does not by itself prove
trading divergence when scoped semantic evidence matches.

## Consequences

- Reporting and BotLens can continue using `run_seq` as a durable replay cursor.
- Golden comparison does not fail solely because semantically equivalent runs
  interleaved generated event rows differently.
- Wallet and trade replay must prefer wallet-scoped and position-scoped clocks
  over runtime publication order when those scoped clocks are present.
- Operational ordering drift belongs in diagnostics and operational
  fingerprints, not in semantic trading-behavior certification.
- The system avoids overloading one sequence with both event-ledger and trading
  semantics.

## References

- [Persistence boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [Reporting boundary](../reporting/REPORTING_BOUNDARY.md)
- [BotLens projection boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
- [ADR 0007: Use scoped causal clocks for runtime replay](0007-use-scoped-causal-clocks-for-runtime-replay.md)
- [ADR 0015: Split semantic and operational golden fingerprints](0015-split-semantic-and-operational-golden-fingerprints.md)
