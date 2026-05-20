---
component: data-boundary
subsystem: data
layer: boundary
doc_type: architecture
status: active
tags:
  - data
  - providers
  - candles
  - instruments
  - gaps
  - cache
code_paths:
  - src/data_providers
  - src/core/candle_continuity.py
  - portal/backend/service/providers
  - portal/backend/service/market
  - docs/architecture/data/diagrams/data-boundary-flow.mmd
  - docs/architecture/data/diagrams/candle-continuity-flow.mmd
---
# Data Boundary

## Purpose

The data boundary turns external market data into source facts Quant-Trad can evaluate deterministically. It owns provider selection, adapter construction, credential access, candle normalization, instrument metadata, cache/persistence, and continuity diagnostics.

Related diagrams:

- [data-boundary-flow.mmd](diagrams/data-boundary-flow.mmd)
- [candle-continuity-flow.mmd](diagrams/candle-continuity-flow.mmd)

## Boundary Contract

The data boundary provides evidence. It does not make trading decisions, execute orders, fill missing candles with synthetic OHLCV rows, or hide provider defects.

| Owns | Does Not Own |
| --- | --- |
| provider registry and factory | indicator state |
| provider/venue capability checks | strategy rules |
| adapter anti-corruption logic | execution semantics |
| instrument metadata | wallet or margin effects |
| candle fetch/cache/persistence | BotLens projection state |
| sparse candle and gap classification | report readiness decisions |

## Diagram Walkthrough: Data Flow

[data-boundary-flow.mmd](diagrams/data-boundary-flow.mmd) shows the normal path:

1. Operator/runtime config selects provider, venue, symbol, timeframe, and window.
2. Provider registry and factory select an adapter.
3. Required/optional credential keys come from registry metadata; secret values are resolved through credential refs, not runtime config.
4. The adapter isolates external API details, symbol formats, credentials, pagination, and provider metadata.
5. Provider-backed rows are persisted or read through cache paths.
6. Continuity checks classify sparse data and gaps.
7. Market services pass source facts to the indicator runtime and execution runtime.

Provider adapters are anti-corruption boundaries. External provider quirks should not leak into strategy, execution, BotLens, or reporting.

## Diagram Walkthrough: Candle Continuity

[candle-continuity-flow.mmd](diagrams/candle-continuity-flow.mmd) shows how gaps remain explicit:

1. The requested window and interval define expected timestamps.
2. Provider/cache rows are normalized and ordered.
3. Missing, duplicate, malformed, or out-of-order rows are detected.
4. Known closures may explain expected gaps.
5. Remaining gaps are classified as provider, ingestion, runtime, projection, or unknown gaps.
6. Missing-range evidence is attached without leaking provider-specific contracts: reason code, evidence source, provider response metadata when available, and exception type/message/stack trace for failed calls.
7. Summaries flow to runtime diagnostics, BotLens, and RunResearchDataset.

Unknown gaps are safer than false certainty. If the system cannot prove a market closure or provider explanation, the gap remains unknown.

## Inputs

- Provider, venue, exchange, symbol, timeframe, start/end.
- Provider credential references and runtime settings.
- Provider registry metadata.
- Cached candle rows and closure/session rows when available.
- Instrument metadata requests.

## Outputs

- Normalized candle rows.
- Instrument metadata and validation results.
- Provider/cache provenance.
- Gap and continuity summaries.
- Missing candle evidence for sparse/failed provider fetch ranges.
- Source warnings that downstream layers can surface.

## State And Truth

Provider-backed candles are source facts. Cache rows are persisted source facts with provenance. Continuity summaries are diagnostics over those facts.

The data boundary should not manufacture alternate execution truth. If source data is incomplete, downstream runtime can reject, degrade, or fall back according to its own contract, but the data layer should keep the incompleteness visible.

## Failure And Recovery

- Missing credentials fail before runtime starts.
- Provider API keys must not be read from centralized settings/env bindings.
- Unsupported provider/venue/symbol combinations fail with provider context.
- Provider fetch defects become explicit warnings or errors.
- Provider sparse responses and fetch exceptions attach provider-agnostic missing-range evidence to continuity classifications. Empty or out-of-window successful responses may be closure-backed; failed calls remain ingestion/fetch defects and should not be treated as known market closures.
- Missing required instrument metadata fails before execution uses the instrument.
- Unknown candle gaps remain unknown and are surfaced to BotLens/reports.

## Invariants

- No synthetic candles unless an explicit modeled source says they are synthetic.
- Provider-specific behavior stops at the adapter boundary.
- Candle continuity is diagnostic truth, not a strategy decision.
- Instrument metadata must be validated before execution depends on tick size, contract size, fees, shorting, or margin.
- Provider credentials flow through credential refs; bot config and runtime config must not transport provider API keys.

## Related Docs

- [System model](../system/SYSTEM_MODEL.md)
- [Engine state model](../engine/ENGINE_STATE_MODEL.md)
- [Execution runtime boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [Reporting boundary](../reporting/REPORTING_BOUNDARY.md)
- [Security layer](../security/SECURITY_LAYER.md)

## Known Gaps

- Session/calendar evidence is not complete enough to classify every closure.
- Provider lifecycle docs intentionally stop at the boundary model. Full provider tutorials belong in guides, not architecture.
