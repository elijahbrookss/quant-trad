---
component: adr-semantic-operational-golden-fingerprints
subsystem: reporting
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - comparison
  - golden-repeatability
  - reporting
  - research-dataset
code_paths:
  - portal/backend/service/reports/run_research_dataset.py
  - portal/backend/service/reports/schemas.py
  - scripts/reporting/golden_repeatability.py
  - docs/architecture/reporting/REPORTING_BOUNDARY.md
  - docs/architecture/reporting/REPORTING_CONTRACT_REDESIGN.md
---
# ADR 0015: Split Semantic And Operational Golden Fingerprints

## Status

Accepted on 2026-05-13.

## Context

Golden repeatability should certify trading behavior. The previous material
fingerprint mixed trading behavior with run-instance and runtime-observability
artifacts. Two runs could have identical decisions, verdicts, trade lifecycle,
wallet projection, runtime ledger density, and summary metrics, while still
failing because generated or operational identifiers differed.

Examples of non-semantic artifacts include `run_seq`, source batch `seq`,
generated event IDs, generated `signal_id`, generated `trade_id`, wall-clock
diagnostic timestamps, and operational counters.

## Decision

Golden certification gates on a semantic trading-behavior fingerprint.
Operational/runtime evidence is fingerprinted separately.

The semantic fingerprint covers stable trading behavior and material identity:
strategy/config/data identity, decisions and signals by logical market
identity, trade lifecycle, summary metrics, and compact decision-boundary
indicator/market-state context.

The operational fingerprint covers diagnostics, section availability, candle
continuity evidence, generated identifiers, runtime/logging evidence, and other
operational traces.

`material_fingerprint` remains a compatibility alias for the semantic
fingerprint.

## Consequences

- Golden PASS can still report operational fingerprint drift when semantic
  trading behavior matches.
- Generated IDs and runtime-event publication artifacts no longer create false
  material repeatability failures.
- Operational drift remains inspectable instead of being discarded.
- Consumers should use `semantic_fingerprint` for trading-behavior
  certification and `operational_fingerprint` for runtime audit.
- Existing consumers that read `material_fingerprint` continue to receive the
  semantic compatibility value.

## References

- [Reporting boundary](../reporting/REPORTING_BOUNDARY.md)
- [Reporting contract redesign](../reporting/REPORTING_CONTRACT_REDESIGN.md)
- [ADR 0010: Use RunResearchDataset v1 as the reporting contract](0010-use-run-research-dataset-as-reporting-contract.md)
- [ADR 0016: Treat runtime event ledger order as operational evidence](0016-treat-runtime-event-ledger-order-as-operational-evidence.md)
