# Data Layer

The data layer connects Quant-Trad to market data providers while preserving source-data truth.

## What It Is

Provider adapters normalize external provider behavior into the candle and metadata shapes used by services, runtime, indicators, BotLens, and reports.

Current provider code lives under:

- `src/data_providers/`
- `portal/backend/service/providers/`
- `portal/backend/service/market/`

## Provider Adapters

Adapters should hide provider-specific client details without hiding data quality problems. Provider/venue validation, credentials, and factory routing belong at provider boundaries, not in strategy or runtime logic.

Provider API keys flow through credential refs. Do not add provider-specific API
key fields to settings, bot env, or run config.

## Candle Cache

The candle cache exists to avoid repeated provider fetches while preserving runtime semantics. Cache keys must include semantic inputs such as provider, venue/exchange, symbol, timeframe, and requested window.

Cached candles must match provider-backed truth for the same request.

## Sparse Candles

Sparse source data remains sparse truth. A missing candle should not be silently replaced with synthetic OHLCV unless a future feature explicitly models synthetic candles as their own artifact type.

## Gap Classification

Candle gaps should be classified conservatively:

- expected session gaps,
- provider missing data,
- ingestion failures,
- runtime missing data,
- projection missing data,
- unknown gaps.

If session/calendar proof is unavailable, prefer `unknown_gap` over assuming the gap is expected.

## How It Fits

Provider data feeds indicators and runtime. Runtime and BotLens can surface gap diagnostics, but they should not mutate source series to make charts look complete.

## Next

- Minimal extension guide: [adding a provider](../guides/adding-a-provider.md).
- Archive-backed research data setup: [Binance futures public data](../guides/binance-futures-public-data.md).
- Live-stream/paper setup: [Coinbase derivatives paper setup](../guides/coinbase-derivatives-paper-setup.md).
- Provider and candle architecture: [data boundary](../architecture/data/DATA_BOUNDARY.md).
- Credential and trust-boundary reference: [security layer](../architecture/security/SECURITY_LAYER.md).
- Runtime contract: [runtime contract](../contracts/platform/01_runtime_contract.md).
- Runtime diagnostics: [execution runtime boundary](../architecture/execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md).
