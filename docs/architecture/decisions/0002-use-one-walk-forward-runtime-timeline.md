---
component: adr-walk-forward-runtime-timeline
subsystem: engine
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - runtime
  - engine
  - known-at
  - deterministic
code_paths:
  - src/engines/indicator_engine
  - src/engines/bot_runtime/runtime
  - src/strategies/evaluator.py
  - docs/contracts/platform/00_system_contract.md
  - docs/contracts/platform/01_runtime_contract.md
  - docs/architecture/engine/ENGINE_STATE_MODEL.md
---
# ADR 0002: Use One Walk-Forward Runtime Timeline

## Status

Accepted, backfilled on 2026-05-13.

## Context

Quant-Trad models market knowledge as incrementally discovered. Indicators,
strategy decisions, execution outcomes, BotLens views, and reports are only
trustworthy when they can be explained from what the system knew at the time.

Parallel reconstruction paths are tempting for previews, charts, reports, and
debuggers, but they create semantic drift. The platform contracts already state
the common timeline as `initialize -> apply_bar -> snapshot`.

## Decision

All derived runtime artifacts use one walk-forward timeline:

```text
initialize -> apply_bar -> snapshot
```

Indicators advance state from known source facts. Strategies consume the
published typed outputs for that bar. Execution resolves decisions from those
same known-at facts. BotLens and reports project from runtime and durable truth
instead of rerunning hidden alternate logic.

## Consequences

- If a consumer needs data missing from a snapshot contract, the contract is
  extended rather than reading mutable internals.
- Strategy previews, bot runtime, playback, and reports must stay aligned with
  runtime semantics.
- Missing or malformed state fails loud with context instead of being silently
  substituted.
- Performance optimizations are valid only when they preserve the same
  walk-forward outputs.

## References

- [System contract](../../contracts/platform/00_system_contract.md)
- [Runtime contract](../../contracts/platform/01_runtime_contract.md)
- [Engine state model](../engine/ENGINE_STATE_MODEL.md)
- [Runtime timeline concept](../../concepts/runtime-timeline.md)

