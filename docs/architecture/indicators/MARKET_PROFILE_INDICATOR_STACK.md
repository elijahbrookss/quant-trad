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

Market Profile uses an explicit layered stack. Each layer owns one artifact form and one timeline responsibility.

## Canonical contract

- Compute owns the canonical domain objects: `Profile` and `ValueArea`.
- Runtime owns snapshot/state progression and runtime payload emission.
- Overlay owns projection of canonical payloads into chart boxes and markers.
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
- internal helpers may support compute, but downstream consumers should rely on typed `Profile` outputs,
- compute does not own signal bubble or chart overlay construction.

### Runtime

Code path:
- `src/indicators/market_profile/runtime`
- `src/signals/engine/market_profile`

Responsibilities:
- maintain sequential `initialize -> apply_bar -> snapshot` semantics,
- emit payloads that preserve known-at timing and source-session identity,
- pass merge configuration through payloads explicitly.

Rules:
- runtime payloads derive from canonical profiles,
- runtime consumers do not reconstruct value areas from hidden engine state.

### Overlay

Code path:
- `src/indicators/market_profile/overlays`

Responsibilities:
- project runtime payloads into display boxes/markers,
- clamp projected artifacts to the visible chart window,
- preserve runtime timing semantics when rendering merged or unmerged profiles.

Rules:
- overlay adapters consume payloads only,
- chart-specific behavior stays outside compute.

## Time normalization

The Market Profile stack normalizes time at the boundary before profile computation.

- request window inputs are converted to UTC-aware timestamps before provider fetches,
- provider outputs are compared only after UTC normalization,
- profile start/end times remain on a single UTC timeline through compute, runtime, and overlay stages.

This prevents tz-naive/tz-aware drift and keeps merge/visibility decisions deterministic.
