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

## Credential Contract

Provider credentials must use the provider credential-reference boundary:

- declare required and optional secret keys in `src/data_providers/registry.py`,
- resolve secrets through `src/data_providers/services/credential_store.py`,
- return only safe credential metadata from backend/API/UI/CLI reads,
- never add provider API keys to `src/core/settings.py`, `config/defaults.yaml`, bot env, or run config.

CLI setup should work without exposing secrets in logs:

```bash
python -m cli.main providers credentials schema --provider COINBASE --venue COINBASE_DIRECT
python -m cli.main providers credentials add --provider COINBASE --venue COINBASE_DIRECT
```

For agent/automation workflows, prefer stdin or env-var mapping:

```bash
python -m cli.main providers credentials add \
  --provider COINBASE \
  --venue COINBASE_DIRECT \
  --secrets-json - \
  --no-input
```

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
- credential schema/metadata behavior,
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
- Archive-backed example: [Binance futures public data setup](binance-futures-public-data.md).
- Live-stream/paper example: [Coinbase derivatives paper setup](coinbase-derivatives-paper-setup.md).
- Runtime contract: [runtime contract](../contracts/platform/01_runtime_contract.md).
