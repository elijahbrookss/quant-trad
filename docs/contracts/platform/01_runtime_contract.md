# Runtime Contract

## Canonical Runtime Flow

All derived outputs should follow one timeline:

1. initialize runtime components
2. process one bar in dependency order
3. mutate indicator-owned state via `apply_bar(bar, inputs)`
4. publish typed outputs via `snapshot()`
5. publish optional canonical overlays via `overlay_snapshot()`
6. publish optional runtime details via `detail_snapshot()`
7. evaluate strategies from published typed outputs only
8. publish canonical decision artifacts from the same bar result
9. build downstream read models and downstream rejection artifacts from the same bar result

## Artifact Contract

Indicators are computation units with internal state.

Public runtime surfaces are:
- typed outputs for strategy/runtime truth,
- optional canonical overlays for chart rendering,
- optional runtime details for operator/debug inspection.

Rules:
- outputs are the only strategy-visible indicator interface,
- overlays are not strategy inputs,
- runtime details are not strategy inputs,
- decision artifacts must derive from the published typed outputs for the same bar,
- indicator overlays represent the full current visual state for the bar,
- indicator details are non-core inspection artifacts and must stay separate from render overlays,
- indicators may prepare immutable source facts before walk-forward execution when those facts are true source inputs rather than reconstructed chart history,
- indicators must not prebuild full chart-history overlays before walk-forward execution starts,
- `apply_bar()` advances indicator-owned state only; it must not rebuild full-history overlay payloads on every bar,
- `overlay_snapshot()` is a read of current indicator state and may be requested selectively by consumers,
- `detail_snapshot()` is a read of current indicator state and may be transported independently from overlays,
- chart readouts that depend on the same live timeline should prefer canonical overlay payloads over a parallel detail refetch path,
- runtime transport may diff those full overlay snapshots and stream only deltas downstream,
- every declared output must be returned every bar,
- every declared overlay must be returned every bar,
- every declared detail must be returned every bar,
- `ready=False` means unusable now, not pending,
- runtime never waits, retries, or substitutes missing values,
- runtime and preview consumers must not fetch overlays through a parallel overlay service path.
- runtime and preview consumers must not reconstruct decision artifacts through a parallel rule-evaluation path.

## Cache Contract

Caching is valid only when it preserves runtime semantics:
- key includes semantic inputs
- outputs match non-cached walk-forward execution
- output readiness and overlay visibility semantics are unchanged

## Single-Path Rule

Do not add alternate reconstruction paths for the same artifact class.

Rules:
- strategies must not inspect indicator internals,
- downstream overlay consumers must not reinterpret indicator-local overlay blobs,
- if a surface needs overlay history, it must assemble that history from the runtime timeline instead of asking indicators to rebuild it inside `apply_bar()`,
- if required data is missing from the public runtime surface, extend the contract instead of reading hidden state.
