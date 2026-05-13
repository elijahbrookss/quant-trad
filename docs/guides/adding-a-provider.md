# Adding A Provider

This is a minimal provider extension checklist.

## Where Providers Live

Provider code lives under:

- [provider adapters](../../src/data_providers/providers/)
- [provider registry](../../src/data_providers/registry.py)
- [provider factory](../../src/data_providers/providers/factory.py)
- [backend provider services](../../portal/backend/service/providers/)

## Adapter Concept

A provider adapter should isolate external API/client behavior and return normalized data to Quant-Trad services. It should not leak provider-specific credentials, pagination, symbol formatting, or error quirks into strategy or runtime code.

## Candle Requirements

Candles must preserve source-data truth:

- include stable timestamps,
- preserve OHLCV semantics,
- be ordered deterministically,
- avoid duplicate bars,
- expose missing data instead of silently filling it,
- carry enough provider/venue/symbol/timeframe context for diagnostics.

## Caching

Provider caching should preserve runtime semantics. Cache keys must include semantic request inputs, and cached results must be equivalent to a fresh provider fetch for that request.

## Gap Handling

Sparse candles are valid source facts. Do not synthesize missing OHLCV rows unless a future feature explicitly models synthetic candles as separate artifacts.

Classify gaps conservatively and keep diagnostics visible to runtime, BotLens, and reports.

## Testing Expectations

Add focused tests for:

- provider registry routing and aliases,
- provider factory behavior,
- candle normalization,
- error handling and credential failures,
- gap/sparse-data behavior,
- cache semantics if caching is changed.

Useful examples:

- [provider contract tests](../../tests/contract/providers/)
- [data provider tests](../../tests/test_data_providers/)
- [Coinbase runtime session tests](../../tests/integration/runtime/test_coinbase_runtime_session.py)

## Next

- Data overview: [data layer](../engineering/data-layer.md).
- Provider boundary: [data boundary](../architecture/data/DATA_BOUNDARY.md).
- Runtime contract: [runtime contract](../contracts/platform/01_runtime_contract.md).
