---
component: adr-indicator-truth-projections
subsystem: indicator-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - indicators
  - typed-outputs
  - overlays
  - known-at
code_paths:
  - src/engines/indicator_engine/contracts.py
  - src/engines/indicator_engine/runtime_engine.py
  - src/indicators
  - tests/test_indicator_engine_overlays.py
  - docs/architecture/indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md
---
# ADR 0004: Separate Indicator Truth From Projections

## Status

Accepted, backfilled on 2026-05-13.

## Context

Indicators need to serve different consumers. Strategy logic needs stable,
typed, known-at values. Charting needs visual overlays. Debugging sometimes
needs larger details. Mixing those surfaces lets strategy code depend on chart
or debug state and invites full-history overlay reconstruction during live
runtime.

## Decision

Indicator runtime exposes three surfaces:

- `snapshot()` for canonical strategy-visible typed outputs,
- `overlay_snapshot()` for chart and preview projections,
- `detail_snapshot()` for debug/operator inspection.

The indicator execution engine owns dependency order, output validation, and
the indicator commit sequence. Strategy and runtime consumers use typed outputs
only.

## Consequences

- Overlays and details can evolve for inspection without changing strategy
  semantics.
- Indicators must return declared outputs every bar, with `ready=false` meaning
  unusable now.
- Consumers that need new decision inputs add typed outputs instead of reading
  overlays, details, or private indicator state.
- Overlay transport can use its own projection clocks while retaining source
  indicator commit provenance.

## References

- [Indicator runtime boundary](../indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md)
- [Runtime contract](../../contracts/platform/01_runtime_contract.md)
- [Decision layer boundary](../decision-layer/DECISION_LAYER_BOUNDARY.md)

