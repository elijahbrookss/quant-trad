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
  - src/core/candle_snapshot.py
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

## Source Facts Handed Downstream

The data layer starts from an explicit request: provider or venue, exchange,
symbol, timeframe, start/end, credential refs, registry metadata, cache rows,
session evidence, and instrument metadata. It returns normalized candle rows,
instrument validation, provider/cache provenance, continuity summaries,
missing-range evidence, and source warnings.

Provider-backed candles are source facts. Cache rows are the same source facts
with persistence provenance. Continuity summaries explain those facts; they do
not turn missing data into usable market data.

Series construction also records the exact normalized candle values consumed by
runtime as `candle_series_snapshot.v1`. The snapshot is ordered by timestamp and
uses a canonical exact numeric representation for OHLC, ATR, and volume.
Terminal producer-owned continuity facts propagate this identity downstream.
Continuity, closure, provider, warmup, confidence, and caveat evidence remains
separate: it describes trust in the source rows but does not replace their
material value identity.

When source data is incomplete, the data layer keeps the incompleteness visible.
Runtime can reject, degrade, or fall back according to its own contract, but the
data layer should not manufacture alternate execution truth.

## Failure And Recovery

- Missing credentials fail before runtime starts.
- Provider API keys must not be read from centralized settings/env bindings.
- Unsupported provider/venue/symbol combinations fail with provider context.
- Provider fetch defects become explicit warnings or errors.
- Provider sparse responses and fetch exceptions attach provider-agnostic missing-range evidence to continuity classifications. Empty or out-of-window successful responses may be closure-backed; failed calls remain ingestion/fetch defects and should not be treated as known market closures.
- Provider adapters may retry transient transport failures such as exchange rate
  limits, but unresolved failed calls must remain `ingestion_failure` evidence.
  A downstream replay consumer that requires complete candles must fail rather
  than treating the partial frame as a valid market window.
- Missing required instrument metadata fails before execution uses the instrument.
- Unknown candle gaps remain unknown and are surfaced to BotLens/reports.

## Invariants

- No synthetic candles unless an explicit modeled source says they are synthetic.
- Provider-specific behavior stops at the adapter boundary.
- Candle continuity is diagnostic truth, not a strategy decision.
- Instrument metadata must be validated before execution depends on tick size, contract size, fees, shorting, or margin.
- Instrument `instrument_type` is source metadata owned by the instrument record.
  Runtime execution semantics such as `proxy_derivative` are run bindings and
  must not mutate the canonical instrument type.
- A spot instrument may carry a `proxy_derivative_reference`,
  `proxy_derivative_margin_rates`, and `proxy_derivative_instrument_fields`
  derived from a validated derivative sibling. Those fields are execution
  evidence for a research binding; they do not change the source instrument
  type or candle provider identity.
- Strategy instrument links preserve canonical instrument identity. If linked
  instruments have different providers/venues, candle fetches must use the
  linked instrument record rather than assuming the strategy-level provider.
- CCXT-backed instruments must persist provider identity as `datasource=CCXT`
  and venue identity in `exchange`, even when the venue is Coinbase. Do not
  collapse that into Coinbase Direct provider identity.
- Provider credentials flow through credential refs; bot config and runtime config must not transport provider API keys.
- Exact candle identity and candle quality are separate contracts. Changing a
  consumed value changes identity; changing diagnostic gap metadata does not.
- Missing or malformed runtime candle snapshots cannot be represented as valid
  empty evidence.

## Related Docs

- [System model](../system/SYSTEM_MODEL.md)
- [Engine state model](../engine/ENGINE_STATE_MODEL.md)
- [Execution runtime boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [Reporting boundary](../reporting/REPORTING_BOUNDARY.md)
- [Security layer](../security/SECURITY_LAYER.md)
- [ADR 0046: Exact candle inputs and separate quality](../decisions/0046-fingerprint-exact-candle-inputs-and-keep-quality-separate.md)

## Known Gaps

- Session/calendar evidence is not complete enough to classify every closure.
- Provider lifecycle docs intentionally stop at the boundary model. Full provider tutorials belong in guides, not architecture.
