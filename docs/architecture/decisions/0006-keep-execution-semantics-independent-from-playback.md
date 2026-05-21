---
component: adr-execution-semantics-independent-from-playback
subsystem: execution-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - execution
  - playback
  - deterministic
  - runtime
code_paths:
  - src/engines/bot_runtime/runtime/components/runtime_policy.py
  - src/engines/bot_runtime/runtime/components/intrabar.py
  - src/engines/bot_runtime/runtime/mixins/setup_prepare.py
  - portal/backend/service/bots/config_service.py
  - portal/frontend/src/features/bots/executionMode.js
  - docs/contracts/platform/02_execution_playback_contract.md
---
# ADR 0006: Keep Execution Semantics Independent From Playback

## Status

Accepted, backfilled on 2026-05-13.

## Context

Playback speed and UI animation are inspection concerns. Execution mode is a
runtime semantics choice that affects stop/target ordering and therefore run
results. Conflating the two would make the same bot produce different trades
because a user changed visualization pacing.

## Decision

Execution mode is explicit runtime configuration:

- `FAST` resolves exits from strategy-timeframe OHLC with pessimistic same-bar
  stop-first behavior.
- `FULL` uses ordered 1-minute intrabar candles when available.

If FULL cannot prove intrabar order, runtime falls back to the FAST pessimistic
policy and emits `execution_intrabar_fallback_pessimistic` with reason and
series context. Playback controls pacing/debug visualization only.

## Consequences

- Bot config, run metadata, BotLens, reports, and diagnostics must expose the
  selected execution mode.
- UI playback changes cannot alter fills, wallet effects, or metrics.
- Intrabar data gaps are visible diagnostics, not silent optimistic fills.
- Comparisons must account for execution mode before interpreting result
  differences.

## References

- [Execution and playback contract](../../contracts/platform/02_execution_playback_contract.md)
- [Execution runtime boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [Execution model concept](../../concepts/execution-model.md)

