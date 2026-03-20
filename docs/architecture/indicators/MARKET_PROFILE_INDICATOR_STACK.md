---
component: market-profile-indicator-stack
subsystem: indicators
layer: contract
doc_type: architecture
status: active
tags:
  - indicators
  - market-profile
  - runtime
  - overlays
  - contract
code_paths:
  - src/indicators/market_profile/compute
  - src/indicators/market_profile/runtime
  - src/indicators/market_profile/overlays
  - src/signals/engine/market_profile
---
# Market Profile Indicator Stack

Market Profile uses an explicit layered stack. The runtime indicator is a composite indicator that owns both typed outputs and optional overlays.

## Canonical contract

- Compute owns the canonical domain objects: `Profile` and `ValueArea`.
- Runtime owns state progression and typed output emission.
- The indicator also owns canonical overlay emission for chart rendering.
- Tests should target these seams directly instead of relying on legacy class-private helpers.

## Layer boundaries

### Compute

Code path:
- `src/indicators/market_profile/compute`

Responsibilities:
- build session profiles from OHLCV,
- merge profiles using value-area overlap rules,
- expose typed domain outputs.

Rules:
- `MarketProfileIndicator` is the compute boundary,
- internal helpers may support compute, but downstream consumers should rely on published typed outputs,
- compute does not leak hidden mutable state to runtime consumers.

### Runtime

Code path:
- `src/indicators/market_profile/runtime`

Responsibilities:
- maintain sequential `apply_bar -> snapshot -> overlay_snapshot` semantics,
- consume immutable Market Profile source facts prepared before replay,
- publish manifest-declared typed outputs:
  - `value_area_metrics`
  - `value_location`
  - `balance_state`
  - `balance_breakout`
- publish manifest-declared overlays:
  - `value_area`
  - `breakout_markers`

Rules:
- runtime outputs derive from canonical profile state,
- runtime construction may prepare profile source facts before replay, but it must not prebuild final chart-history overlays,
- `apply_bar()` resolves which profiles are known on the current bar and updates current indicator state only,
- `overlay_snapshot()` materializes the current value-area overlay from that current state when a consumer asks for it,
- strategies consume typed outputs only,
- overlay consumers consume canonical overlay payloads only,
- runtime consumers do not reconstruct value areas from hidden engine state.

### Overlay

Code path:
- `src/indicators/market_profile/runtime`

Responsibilities:
- emit canonical market-profile overlay payloads directly from indicator-owned state,
- keep value-area boxes and breakout markers aligned with the same bar timeline as typed outputs.

Rules:
- overlays are optional and chart-only,
- overlays are not strategy inputs,
- runtime/BotLens may filter, trim, and stream overlays, but they do not reinterpret overlay meaning.

## Time normalization

The Market Profile stack normalizes time at the boundary before profile computation.

- request window inputs are converted to UTC-aware timestamps before provider fetches,
- provider outputs are compared only after UTC normalization,
- profile start/end times remain on a single UTC timeline through compute, runtime, and overlay stages.

This prevents tz-naive/tz-aware drift and keeps merge/visibility decisions deterministic.

## Source facts vs replay

Market Profile is allowed one explicit pre-replay seam because it requires alternate source data (`30m`) and profile-domain preprocessing.

Allowed before replay:
- fetch `30m` source candles,
- compute source session profiles,
- apply merge policy if configured,
- project profile boundaries to the strategy timeframe,
- assign `formed_at` and strategy-timeframe `known_at`.

Not allowed before replay:
- prebuilding final rendered box histories,
- bypassing the runtime timeline with a chart-only overlay payload,
- returning a parallel overlay artifact that QuantLab/BotLens use instead of runtime state.
