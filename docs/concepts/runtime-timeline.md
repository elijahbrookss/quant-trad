# Runtime Timeline

The runtime timeline is the idea Quant-Trad keeps coming back to when the
system gets complicated. Everything derived during a run should be explainable
from one walk-forward sequence:

```text
initialize -> apply_bar -> snapshot
```

`initialize` prepares the state. `apply_bar` advances that state with one known
market event. `snapshot` exposes what is now known. Indicators, strategies, bot
runtime, BotLens, and reports all need to respect that order even though they
serve different users.

Derived artifacts have a known-at time. A strategy or playback view can use an
artifact only when it would have existed by the current evaluation time. That is
why an overlay, signal, rejected decision, trade marker, or report metric should
never appear early just because another layer could reconstruct it later.

This means:

- Indicators must not prebuild future overlay history.
- Strategies consume typed outputs, not indicator internals.
- Playback shows what runtime knew and did.
- Reports summarize runtime facts after the run.

Given fixed inputs, parameters, versions, and provider data, walk-forward
execution should produce stable outputs. The runtime does not wait for missing
values, retry silently, or substitute data that did not exist. If required state
is invalid, the honest behavior is to fail with context instead of producing a
plausible result from hidden assumptions.

This is what keeps QuantLab, strategy preview, bot execution, BotLens, and
reports from drifting apart. If a consumer needs data that is missing from
`snapshot.payload`, the fix is to extend the runtime contract. Reading mutable
engine internals or rebuilding a parallel path makes bugs look like user error,
which is exactly what this model is supposed to prevent.

## Next

- Source of truth: [runtime contract](../contracts/platform/01_runtime_contract.md).
- Execution behavior: [execution model](execution-model.md).
- Strategy flow: [strategies and signals](strategies-and-signals.md).
- Engineering summary: [runtime engine](../engineering/runtime-engine.md).
