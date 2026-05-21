---
component: adr-data-boundary-source-facts
subsystem: data
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - data
  - providers
  - candles
  - gaps
code_paths:
  - src/data_providers
  - src/core/candle_continuity.py
  - portal/backend/service/providers
  - portal/backend/service/market
  - docs/architecture/data/DATA_BOUNDARY.md
---
# ADR 0003: Preserve Data Boundary Source Facts

## Status

Accepted, backfilled on 2026-05-13.

## Context

Provider responses can be sparse, failed, out of order, duplicated, or shaped by
venue/session behavior that Quant-Trad cannot always prove. Treating gaps as
normal candles would make downstream decisions and reports look more certain
than the source data supports.

Provider quirks also differ enough that they need an anti-corruption boundary
before candle, instrument, and continuity facts reach runtime code.

## Decision

The data boundary owns provider access, candle normalization, instrument
metadata, persistence/cache access, and continuity diagnostics. It preserves
source evidence and gap classifications. It does not synthesize missing OHLCV
rows, make strategy decisions, or hide provider defects.

Unknown gaps remain unknown unless closure/session or provider evidence proves
a more specific classification.

## Consequences

- Downstream runtime can reject, degrade, or use contract-defined fallbacks, but
  the source-data incompleteness remains visible.
- Provider-specific behavior stops at adapters and provider services.
- BotLens and reporting can explain candle gaps without inventing data.
- Golden-run readiness can distinguish provider-backed sparse truth from
  pipeline loss or unknown gaps.

## References

- [Data boundary](../data/DATA_BOUNDARY.md)
- [Runtime contract: BotLens candle continuity audit surface](../../contracts/platform/01_runtime_contract.md)
- [Reporting boundary](../reporting/REPORTING_BOUNDARY.md)

