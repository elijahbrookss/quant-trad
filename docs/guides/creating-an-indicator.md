# Creating An Indicator

This is a minimal authoring checklist, not a full tutorial.

## Where Indicators Live

Indicator code lives under `src/indicators/`. Existing examples:

- [candle stats runtime](../../src/indicators/candle_stats/runtime.py)
- [regime runtime](../../src/indicators/regime/runtime.py)
- [market profile typed runtime](../../src/indicators/market_profile/runtime/typed_indicator.py)
- [indicator manifests](../../src/indicators/manifest.py)

## Expected Shape

An indicator should define a manifest and a runtime implementation that follows the engine contract:

- declare typed outputs,
- initialize internal state,
- advance with `apply_bar(bar, inputs)`,
- publish every declared output through `snapshot()`,
- optionally publish overlays through `overlay_snapshot()`,
- optionally publish debug details through `detail_snapshot()`.

Typed output categories include `context`, `metric`, and `signal`.

## Authoring Rules

- Strategies consume typed outputs only.
- Overlays are for charts and should represent current visual state.
- Details are for operator/debug inspection.
- Do not prebuild future chart history.
- Return every declared output every bar.
- Use `ready=False` when an output is not usable yet.
- Fail loudly when required state or dependencies are invalid.

## Testing Expectations

Add focused tests for:

- manifest validation,
- declared output coverage,
- ready/not-ready behavior,
- dependency behavior,
- overlay/detail separation when relevant,
- walk-forward timing.

Useful examples:

- [indicator engine overlay tests](../../tests/test_indicator_engine_overlays.py)
- [indicator tests](../../tests/test_indicators/)

## Next

- Source of truth: [runtime contract](../contracts/platform/01_runtime_contract.md).
- Architecture boundary: [indicator runtime boundary](../architecture/indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md).
- Engine model: [engine state model](../architecture/engine/ENGINE_STATE_MODEL.md).
